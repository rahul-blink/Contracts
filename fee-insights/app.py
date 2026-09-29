"""
Satellite / Defects / KAM / Contracts fee insights -- API server.

Serves static/index.html plus JSON endpoints over the rollup tables in
fees.duckdb (built by build_db.py). Nothing here reads Parquet; every response
is an aggregation over a pre-built table, so the whole API stays interactive on
a dataset whose raw form is 1.1 GB.

Fee definitions live in build_db.py's module docstring. In one line each:
  satellite fee = sat_fee_applicable = per-unit rate * net qty sold
  defect fee    = Rs 50 * bad_returns_qty
  KAM fee       = kam_charges accrued once per calendar month since effective_date
  recall fee    = recall_fee as reported on the BCPL RTV claim (never derived),
                  with dump_qty as the returned quantity on the same line

Scope, everywhere on the satellite endpoints: `scope=sat` (default) restricts to
cities on the satellite master list; `scope=all` includes the off-list cities the
extract also contains. That toggle is the requirement-7 validation -- see
/api/cities/validation.

Run:  python app.py            (or: uvicorn app:app --host 127.0.0.1 --port 8000)
Then open http://127.0.0.1:8000
"""
import os
import io
import csv
import json
import re
import time
import threading
import duckdb
from fastapi import FastAPI, Query, HTTPException, Body
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.environ.get("DB_PATH", os.path.join(HERE, "fees.duckdb"))
STATIC = os.path.join(HERE, "static")

# One lock serialising DB access. Reads are short; DuckDB's read-only connection
# is not guaranteed thread-safe across concurrent execute() calls, and uvicorn
# runs handlers in a threadpool.
_lock = threading.Lock()
con = None


def _open():
    global con
    con = duckdb.connect(DB, read_only=True) if os.path.exists(DB) else None


_open()
app = FastAPI(title="Satellite, Defects, KAM & Contracts Insights")


def q(sql, params=None):
    """Run a query, return list-of-dicts. 503 when there is no DB yet."""
    with _lock:
        if con is None:
            raise HTTPException(
                503, "fees.duckdb not found -- run `python build_db.py` first.")
        cur = con.execute(sql, params or [])
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def one(sql, params=None):
    rows = q(sql, params)
    return rows[0] if rows else {}


def has(table):
    """Whether a rollup table exists -- lets a tab degrade instead of 500."""
    try:
        return bool(q("SELECT 1 FROM duckdb_tables() WHERE table_name = ?", [table]))
    except HTTPException:
        return False


# --------------------------------------------------------------------------
# shared filter plumbing
# --------------------------------------------------------------------------

def scope_clause(scope, col="is_satellite"):
    """SQL fragment for the satellite-city scope toggle.

    'sat'  -> only cities on the master list (the validated number, default)
    'off'  -> only cities NOT on the list (the leakage)
    'all'  -> everything the extract contains
    """
    if scope == "all":
        return "TRUE"
    if scope == "off":
        return f"NOT {col}"
    return col


def like(col, term):
    """Case-insensitive contains, as (fragment, params). ('TRUE', []) when blank.

    Parameterised -- the search box never reaches the SQL text.
    """
    term = (term or "").strip()
    if not term:
        return "TRUE", []
    return f"lower({col}) LIKE ?", ["%" + term.lower() + "%"]


def and_eq(col, val):
    """Equality fragment, or ('TRUE', []) when the filter is unset."""
    if val is None or val == "" or val == "all":
        return "TRUE", []
    return f"{col} = ?", [val]


# --------------------------------------------------------------------------
# satellite fee exclusion list
#
# Items (variant_id) the user has taken out of the satellite fee calculation.
# fees.duckdb is opened read-only and is never touched: the list lives in a
# small JSON file (SAT_EXCLUSIONS_PATH, on its own volume in the deployment)
# and is applied as a WHERE term at query time, so removing an item from the
# list restores it instantly and exactly.
#
# Only sat_cube carries variant_id, so only queries over sat_cube -- or ones
# that can subtract a sat_cube slice (bucket / product type / contract state
# splits) -- honour the list. The daily trend, facility ranking, rate
# histogram and the Satellite cities tab have no item grain; the UI says so
# rather than showing them as if they were adjusted.
#
# Manufacturer opt-outs (SAT_OPTOUT_PATH, beside the item list) work the same
# way on mfr_id. The satellite fee clause is signed, but each manufacturer
# decides in the opt-in window; every manufacturer counts as opted in until
# marked otherwise, so an empty file is exactly the original numbers. mfr_id
# is also in sat_day_city_mfr's grain, so the daily trend honours opt-outs
# (but not item exclusions).
# --------------------------------------------------------------------------

EXCL_PATH = os.environ.get("SAT_EXCLUSIONS_PATH",
                           os.path.join(HERE, "state", "sat_exclusions.json"))
OPTOUT_PATH = os.environ.get("SAT_OPTOUT_PATH",
                             os.path.join(os.path.dirname(os.path.abspath(EXCL_PATH)),
                                          "sat_optouts.json"))
_excl_lock = threading.Lock()


def _load(path, key):
    try:
        with open(path) as f:
            items = json.load(f).get("items", [])
        return [i for i in items if i.get(key)]
    except FileNotFoundError:
        return []


def _save(path, items):
    """Atomic write: a crash mid-save never leaves a half-written list."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"items": items}, f, indent=1)
    os.replace(tmp, path)


def _excl_load():
    return _load(EXCL_PATH, "variant_id")


def _excl_save(items):
    _save(EXCL_PATH, items)


def _optout_load():
    """Stored opt-in entries. Only non-default states are stored.

    Three states: 'pending' (Opt-in confirmation pending -- the default for every
    manufacturer whose contract has the satellite clause, never stored),
    'confirmed' (manually validated) and 'not_opted' (manually marked as opted
    out). Entries written before the three-state model carry no status: they were
    opt-outs, so they read as 'not_opted'.
    """
    items = _load(OPTOUT_PATH, "mfr_id")
    for i in items:
        i["status"] = i.get("status") or "not_opted"
    return items


OPTIN_STATES = ("pending", "confirmed", "not_opted")
OPTIN_LABELS = {"pending": "Confirmation pending", "confirmed": "Confirmed",
                "not_opted": "Not_opted"}


def parse_status(v):
    """Accept the stored key, the UI label, or common spellings of either."""
    k = re.sub(r"[^a-z]+", "_", str(v or "").strip().lower()).strip("_")
    return {"pending": "pending", "confirmation_pending": "pending",
            "opt_in_confirmation_pending": "pending", "default": "pending",
            "confirmed": "confirmed", "validated": "confirmed",
            "opt_in_confirmed": "confirmed",
            "not_opted": "not_opted", "opted_out": "not_opted",
            "not_opted_in": "not_opted", "opt_out": "not_opted"}.get(k)


def optin_status_map():
    with _excl_lock:
        return {str(i["mfr_id"]): i for i in _optout_load()}


def _ids_with(status):
    with _excl_lock:
        return [str(i["mfr_id"]) for i in _optout_load() if i["status"] == status]


def excl_ids():
    with _excl_lock:
        return [str(i["variant_id"]) for i in _excl_load()]


def optout_ids():
    """Manufacturers removed from the figures: only 'not_opted'."""
    return _ids_with("not_opted")


def confirmed_ids():
    return _ids_with("confirmed")


def _removed_terms(item_col, mfr_col):
    """SQL terms (and params) matching rows removed by either list."""
    terms, params = [], []
    ids = excl_ids() if item_col else []
    if ids:
        terms.append(f"list_contains(?::VARCHAR[], CAST({item_col} AS VARCHAR))")
        params.append(ids)
    mids = optout_ids() if mfr_col else []
    if mids:
        terms.append(f"list_contains(?::VARCHAR[], CAST({mfr_col} AS VARCHAR))")
        params.append(mids)
    return terms, params


def excl_clause(excl="on", col="variant_id", mfr_col="mfr_id"):
    """(fragment, params) dropping excluded items and opted-out manufacturers.

    excl='off' ignores both lists. Pass col=None on a table with no item grain
    to apply only the manufacturer opt-outs.
    """
    if excl == "off":
        return "TRUE", []
    terms, params = _removed_terms(col, mfr_col)
    if not terms:
        return "TRUE", []
    return "NOT (" + " OR ".join(terms) + ")", params


def only_excl_clause(col="variant_id", mfr_col="mfr_id"):
    """(fragment, params) keeping ONLY removed rows -- to size what was removed."""
    terms, params = _removed_terms(col, mfr_col)
    if not terms:
        return "FALSE", []
    return "(" + " OR ".join(terms) + ")", params


def removal_active(excl):
    return excl != "off" and bool(excl_ids() or optout_ids())


# --------------------------------------------------------------------------
# global period filter
#
# The period is a MONTH range that the UI applies to every tab except Contracts
# (a contract book is a current-state snapshot, not a period fact) and Data.
#
# It is a WHERE clause, not a second set of tables: `month` is part of the grain
# of sat_cube / sat_split / sat_rate / sat_facility / city_month / kam_month /
# rtv_month, so the same query body serves both the filtered and unfiltered case
# and an empty period is byte-identical to the behaviour before this existed.
#
# Month rather than day is deliberate, and it is what makes the feature cheap: at
# day grain the satellite cube would be 15.2M rows (it was OOM-killed while being
# built); at month grain it is 2.1M -- the same size it already was -- so the full
# city -> manufacturer -> item drill keeps working inside a period.
#
# Two limitations, surfaced in the UI rather than buried here:
#   * The defect keyword cloud is tokenised at build time with no date attached,
#     so it always covers the full window and says so.
#   * A month is the smallest slice. Nothing here can answer "the first week of
#     June", and the UI does not pretend otherwise.
# --------------------------------------------------------------------------

def month_range(frm, to, col="month_date"):
    """(fragment, params) for an inclusive month range over a month-start DATE.

    Takes 'YYYY-MM' (a full date also works -- it is truncated) and compares
    against month starts, which is what every period-filtered table stores:
    sat_cube, sat_split, sat_rate, sat_facility and city_month carry month_date,
    and kam_month / rtv_month already did. Blank ends are open.
    """
    where, params = [], []
    if frm:
        where.append(f"{col} >= CAST(? AS DATE)")
        params.append(frm[:7] + "-01")
    if to:
        where.append(f"{col} <= CAST(? AS DATE)")
        params.append(to[:7] + "-01")
    return (" AND ".join(where) or "TRUE"), params


def day_month_range(frm, to, col="day"):
    """The same month range against a DAY column (daily trends, def_line)."""
    return month_range(frm, to, f"date_trunc('month', {col})")


def active(frm, to):
    return bool(frm or to)


def city_stats_src(frm, to):
    """city_stats, or the same master-list join rebuilt over city_month.

    The master list is a master list -- it is not period-scoped, so a satellite
    city that billed nothing in the window still appears with has_fees false,
    exactly as city_stats represents a city that billed nothing all window.
    """
    if not active(frm, to):
        return "city_stats", []
    w, p = month_range(frm, to)
    return (f"""(
        WITH f AS (
            SELECT city_key, any_value(city) AS city,
                   round(sum(fee), 2) AS fee, sum(net_qty) AS net_qty,
                   sum(qty_sold) AS qty_sold, sum(qty_returned) AS qty_returned,
                   sum(n_rows) AS n_rows, max(mfrs) AS mfrs, max(items) AS items,
                   max(outlets) AS outlets, max(facilities) AS facilities,
                   sum(days) AS days, min(first_day) AS first_day,
                   max(last_day) AS last_day
            FROM city_month WHERE {w} GROUP BY city_key)
        SELECT COALESCE(m.city, f.city) AS city,
               COALESCE(m.city_key, f.city_key) AS city_key,
               (m.city_key IS NOT NULL) AS is_satellite, m.distance_km,
               COALESCE(m.band, 'Not in master list') AS band,
               COALESCE(m.band_ord, 9) AS band_ord,
               (f.city_key IS NOT NULL) AS has_fees,
               COALESCE(f.fee, 0) AS fee, COALESCE(f.net_qty, 0) AS net_qty,
               COALESCE(f.qty_sold, 0) AS qty_sold,
               COALESCE(f.qty_returned, 0) AS qty_returned,
               COALESCE(f.n_rows, 0) AS n_rows, COALESCE(f.mfrs, 0) AS mfrs,
               COALESCE(f.items, 0) AS items, COALESCE(f.outlets, 0) AS outlets,
               COALESCE(f.facilities, 0) AS facilities,
               COALESCE(f.days, 0) AS days, f.first_day, f.last_day
        FROM city_master m FULL OUTER JOIN f ON f.city_key = m.city_key)""", p)


def def_src(frm, to):
    """Defect complaint lines, month-filtered.

    Reads `def_line`, the materialised copy, NOT the `draw` view: `draw` reads the
    Parquet in ../data, which is absent from the deployed image. One row per
    complaint is the grain every defect rollup is built from, so the period path
    re-aggregates the same expressions build_defects uses.
    """
    w, p = day_month_range(frm, to)
    return f"(SELECT * FROM def_line WHERE {w})", p


@app.get("/api/period")
def period_coverage():
    """Which months each tab actually has rows for, so the picker cannot lie.

    Returns the explicit month LIST per source, not just first/last. The defect
    extract covers Apr-early May *and* a week of late June with nothing in between,
    so a first-to-last band would claim two months of coverage that do not exist.
    The UI tests a selected period against this list, and a period that lands in a
    gap is reported as empty rather than as "the fee fell to zero".

    The sources do not share a window at all -- satellite is two months, KAM is
    thirteen -- which is why the bar draws a band per source.

    `partial` carries the months a source has rows for but not a *whole* month of
    them (see sat_coverage in build_db). Having the month in the list is not the
    same as the month being comparable, and the satellite tab now spans more than
    one month, so the difference has to reach the UI rather than sit in the total.
    """
    def months(sql):
        return [r["m"] for r in q(sql) if r.get("m")]

    out = {}
    if has("sat_cube"):
        out["satellite"] = months("SELECT DISTINCT strftime(month_date, '%Y-%m') AS m "
                                  "FROM sat_cube ORDER BY 1")
    if has("def_line"):
        out["defects"] = months("SELECT DISTINCT strftime(day, '%Y-%m') AS m "
                                "FROM def_line ORDER BY 1")
    elif has("def_daily"):
        out["defects"] = months("SELECT DISTINCT strftime(day, '%Y-%m') AS m "
                                "FROM def_daily ORDER BY 1")
    if has("kam_month"):
        out["kam"] = months("SELECT DISTINCT month AS m FROM kam_month ORDER BY 1")
    if has("rtv_month"):
        out["recall"] = months("SELECT DISTINCT month AS m FROM rtv_month ORDER BY 1")

    partial = {}
    if has("sat_coverage"):
        partial["satellite"] = q(
            "SELECT month, days, cal_days, CAST(last_day AS VARCHAR) AS last_day, "
            "       last_fee, med_fee, last_day_frac "
            "FROM sat_coverage WHERE NOT complete ORDER BY month")

    tabs = {k: {"frm": v[0], "to_": v[-1], "months": v,
                # a source is gappy when it has fewer months than its own span
                "gaps": _span(v[0], v[-1]) != len(v)}
            for k, v in out.items() if v}
    every = sorted({m for v in out.values() for m in v})
    return {"tabs": tabs,
            "partial": partial,
            "min": every[0] if every else None,
            "max": every[-1] if every else None,
            "not_filtered": ["defect keyword cloud", "KAM contract-state mix"]}


def _span(frm, to):
    """Inclusive count of calendar months between two 'YYYY-MM' strings."""
    fy, fm = int(frm[:4]), int(frm[5:7])
    ty, tm = int(to[:4]), int(to[5:7])
    return (ty - fy) * 12 + (tm - fm) + 1


def csv_response(rows, name):
    """Stream list-of-dicts as a CSV download."""
    if not rows:
        rows = [{"note": "no rows"}]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(rows[0].keys()), extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]), media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{name}.csv"'})


# --------------------------------------------------------------------------
# meta
# --------------------------------------------------------------------------

@app.get("/api/meta")
def meta():
    """Build provenance, headline KPIs and every build-time caveat.

    The UI reads this once on load: it drives the KPI row, the data-health
    banner, and which tabs are enabled.
    """
    kpis = one("SELECT * FROM kpis") if has("kpis") else {}
    extra = {r["key"]: r["value"] for r in q("SELECT * FROM kpis_extra")} \
        if has("kpis_extra") else {}
    return {
        "meta": {r["key"]: r["value"] for r in q("SELECT * FROM build_meta")}
                if has("build_meta") else {},
        "kpis": kpis,
        "extra": extra,
        "notes": q("SELECT level, label, note FROM build_notes "
                   "ORDER BY CASE level WHEN 'error' THEN 0 WHEN 'warn' THEN 1 "
                   "ELSE 2 END, label") if has("build_notes") else [],
        "files": q("SELECT label, file, size_mb FROM build_files "
                   "ORDER BY label, file") if has("build_files") else [],
        "tabs": {
            "cities":    has("city_master"),
            "satellite": has("sat_cube"),
            "defects":   has("def_item"),
            "kam":       has("kam_month"),
            "recall":    has("rtv_month"),
            "contracts": has("contract_list"),
        },
        "db_mb": round(os.path.getsize(DB) / 1e6, 1) if os.path.exists(DB) else 0,
    }


# --------------------------------------------------------------------------
# 1. satellite cities -- the searchable master list
# --------------------------------------------------------------------------

CITY_SORTS = {
    "city": "city ASC",
    "distance_desc": "distance_km DESC NULLS LAST",
    "distance_asc": "distance_km ASC NULLS LAST",
    "fee_desc": "fee DESC",
    "fee_asc": "fee ASC",
    "qty_desc": "net_qty DESC",
    "items_desc": "items DESC",
    "mfrs_desc": "mfrs DESC",
}


@app.get("/api/cities")
def cities(q_: str = Query("", alias="q"),
           scope: str = "sat",
           band: str = "all",
           billing: str = "all",
           sort: str = "distance_desc",
           limit: int = Query(500, le=5000),
           offset: int = 0,
           frm: str = "", to: str = "",
           fmt: str = "json"):
    """The satellite city list, searchable by name.

    scope=sat (default) is the master list; scope=off is the cities that appear
    in the fee extract but are NOT on the master list; scope=all is both.
    billing=yes/no filters to cities that did / did not record fee this period.
    """
    src, sp = city_stats_src(frm, to)
    where = [scope_clause(scope)]
    params = []
    f, p = like("city", q_)
    where.append(f)
    params += p
    f, p = and_eq("band", band)
    where.append(f)
    params += p
    if billing == "yes":
        where.append("has_fees")
    elif billing == "no":
        where.append("NOT has_fees")

    w = " AND ".join(where)
    order = CITY_SORTS.get(sort, CITY_SORTS["distance_desc"])
    total = one(f"SELECT count(*) AS n, round(sum(fee),2) AS fee, "
                f"sum(net_qty) AS net_qty FROM {src} WHERE {w}", sp + params)
    rows = q(f"""
        SELECT city, distance_km, band, band_ord, is_satellite, has_fees,
               fee, net_qty, qty_sold, qty_returned, mfrs, items, outlets,
               facilities, days, CAST(first_day AS VARCHAR) AS first_day,
               CAST(last_day AS VARCHAR)  AS last_day
        FROM {src} WHERE {w}
        ORDER BY {order}, city ASC LIMIT ? OFFSET ?
    """, sp + params + [limit, offset])
    if fmt == "csv":
        return csv_response(rows, "satellite_cities")
    return {"total": total, "rows": rows, "sort": sort, "scope": scope}


@app.get("/api/cities/map")
def city_map(frm: str = "", to: str = "", fmt: str = "json"):
    """Every master satellite city with its period fee, for the map.

    Master list only, and deliberately not scope-aware: coordinates are keyed off
    `city_master`, so the 45 off-list cities in the fee extract have none and would
    silently vanish from a scope=all map. A map that quietly plots 217 of 262
    cities is worse than one that plots 227 and says what it is. The tab states the
    exclusion rather than offering a scope toggle that cannot be honoured.

    `city_key` is the join key to static/city_coords.json; the browser holds the
    coordinates because they are map geometry, like the outline, not a fee fact.
    """
    src, sp = city_stats_src(frm, to)
    rows = q(f"""
        SELECT city_key, city, distance_km, band, band_ord, has_fees,
               fee, net_qty, mfrs, items, outlets, facilities
        FROM {src} WHERE is_satellite ORDER BY fee DESC, city ASC
    """, sp)
    if fmt == "csv":
        return csv_response(rows, "satellite_city_map")
    return {"rows": rows}


@app.get("/api/cities/bands")
def city_bands(scope: str = "sat", frm: str = "", to: str = ""):
    """Distance-band rollup -- the ordinal breakdown behind the city list."""
    src, sp = city_stats_src(frm, to)
    return q(f"""
        SELECT band, min(band_ord) AS band_ord, count(*) AS cities,
               count(*) FILTER (WHERE has_fees) AS billing_cities,
               round(sum(fee), 2) AS fee, sum(net_qty) AS net_qty,
               round(avg(distance_km), 1) AS avg_distance
        FROM {src} WHERE {scope_clause(scope)}
        GROUP BY band ORDER BY band_ord
    """, sp)


@app.get("/api/cities/validation")
def city_validation(limit: int = Query(100, le=1000),
                    frm: str = "", to: str = ""):
    """Requirement 7: is the satellite fee actually being charged on satellite cities?

    Returns the two-sided reconciliation:
      off_list  -- cities carrying fee that are NOT on the master list. The fee
                   extract is not pre-filtered, and these include metros with
                   their own mother warehouses, which cannot be satellite.
      no_fees   -- master satellite cities that recorded no fee at all.
    """
    src, sp = city_stats_src(frm, to)
    tot = one(f"""
        SELECT
          count(*) FILTER (WHERE is_satellite)                  AS master_cities,
          count(*) FILTER (WHERE is_satellite AND has_fees)     AS master_billing,
          count(*) FILTER (WHERE is_satellite AND NOT has_fees) AS master_idle,
          count(*) FILTER (WHERE NOT is_satellite)              AS offlist_cities,
          round(COALESCE(sum(fee) FILTER (WHERE is_satellite), 0), 2)     AS fee_on_list,
          round(COALESCE(sum(fee) FILTER (WHERE NOT is_satellite), 0), 2) AS fee_off_list,
          round(COALESCE(sum(fee), 0), 2)                                  AS fee_total,
          COALESCE(sum(net_qty) FILTER (WHERE is_satellite), 0)     AS qty_on_list,
          COALESCE(sum(net_qty) FILTER (WHERE NOT is_satellite), 0) AS qty_off_list
        FROM {src}
    """, sp)
    t = tot.get("fee_total") or 0
    tot["pct_off_list"] = round((tot.get("fee_off_list") or 0) / t * 100, 1) if t else 0
    return {
        "totals": tot,
        "off_list": q(f"""
            SELECT city, fee, net_qty, mfrs, items, outlets, n_rows
            FROM {src} WHERE NOT is_satellite
            ORDER BY fee DESC LIMIT ?""", sp + [limit]),
        "no_fees": q(f"""
            SELECT city, distance_km, band
            FROM {src} WHERE is_satellite AND NOT has_fees
            ORDER BY distance_km DESC LIMIT ?""", sp + [limit]),
    }


# --------------------------------------------------------------------------
# 2. satellite fees -- drilldown by city / manufacturer / date / item
# --------------------------------------------------------------------------

# Each drill dimension maps to the cube columns that identify and label it.
SAT_DIMS = {
    "city":     ("city_key", "any_value(city)", "city"),
    "mfr":      ("mfr_id", "any_value(mfr)", "manufacturer"),
    "item":     ("variant_id", "any_value(item_name)", "item"),
    "bucket":   ("bucket", "any_value(bucket)", "MRP bucket"),
    "type":     ("product_type", "any_value(product_type)", "product type"),
    "state":    ("contract_state", "any_value(contract_state)", "contract state"),
}


def sat_filters(scope, city, mfr, item, bucket, ptype, state, search, dim,
                frm="", to="", excl="on", optin="in"):
    """Build the WHERE for a sat_cube query from the shared filter row.

    `search` applies to whichever dimension is being listed, so the same search
    box filters cities on the city view and items on the item view. The global
    period is just another term here: `month` is in sat_cube's grain.

    `optin` picks the manufacturer set when the lists are applied: 'in'
    (default) = confirmed + confirmation pending (Not_opted dropped), 'all' =
    every manufacturer with the satellite clause, 'out' = Not_opted only,
    'confirmed' / 'pending' = that state only. Item exclusions apply in every
    mode.
    """
    where = [scope_clause(scope)]
    params = []
    f, p = month_range(frm, to)
    where.append(f)
    params += p
    if optin == "in" or excl == "off":
        f, p = excl_clause(excl)
    else:
        f, p = excl_clause(excl, mfr_col=None)
        if optin == "out":
            fo, po = only_excl_clause(None, "mfr_id")
            f, p = f"{f} AND {fo}", p + po
        elif optin in ("confirmed", "pending"):
            conf, outs = confirmed_ids(), optout_ids()
            if optin == "confirmed":
                f, p = (f"{f} AND list_contains(?::VARCHAR[], CAST(mfr_id AS VARCHAR))",
                        p + [conf])
            else:
                f, p = (f"{f} AND NOT list_contains(?::VARCHAR[], CAST(mfr_id AS VARCHAR))"
                        f" AND NOT list_contains(?::VARCHAR[], CAST(mfr_id AS VARCHAR))",
                        p + [conf, outs])
    where.append(f)
    params += p
    for col, val in (("city_key", city), ("mfr_id", mfr), ("variant_id", item),
                     ("bucket", bucket), ("product_type", ptype),
                     ("contract_state", state)):
        f, p = and_eq(col, val)
        where.append(f)
        params += p
    if search:
        label_col = {"city": "city", "mfr": "mfr", "item": "item_name"}.get(dim)
        if label_col:
            f, p = like(label_col, search)
            where.append(f)
            params += p
    return " AND ".join(where), params


@app.get("/api/sat/kpis")
def sat_kpis(scope: str = "sat", city: str = "", mfr: str = "", item: str = "",
             bucket: str = "", ptype: str = "", state: str = "",
             frm: str = "", to: str = "", excl: str = "on"):
    """Totals for the current slice, plus the same totals unscoped for contrast.

    `excluded_*` sizes what the exclusion list removed from this same slice.
    """
    w, p = sat_filters(scope, city, mfr, item, bucket, ptype, state, "", "", frm, to,
                       excl)
    cur = one(f"""
        SELECT round(sum(fee), 2) AS fee, sum(net_qty) AS net_qty,
               sum(qty_sold) AS qty_sold, sum(qty_returned) AS qty_returned,
               count(DISTINCT city_key) AS cities,
               count(DISTINCT mfr_id)   AS mfrs,
               count(DISTINCT variant_id) AS items,
               sum(n_rows) AS n_rows,
               round(sum(fee) / nullif(sum(net_qty), 0), 3) AS rate_per_unit
        FROM sat_cube WHERE {w}
    """, p)
    # Same filters, both scopes -- so the UI can always show what the scope
    # toggle is excluding without a second round trip.
    w_all, p_all = sat_filters("all", city, mfr, item, bucket, ptype, state, "", "",
                               frm, to, excl)
    split = one(f"""
        SELECT round(sum(fee) FILTER (WHERE is_satellite), 2)     AS fee_on_list,
               round(sum(fee) FILTER (WHERE NOT is_satellite), 2) AS fee_off_list
        FROM sat_cube WHERE {w_all}
    """, p_all)
    cur.update(split)
    w_x, p_x = sat_filters(scope, city, mfr, item, bucket, ptype, state, "", "",
                           frm, to, "off")
    fx, px = only_excl_clause()
    cur.update(one(f"""
        SELECT round(COALESCE(sum(fee), 0), 2) AS excluded_fee,
               COALESCE(sum(net_qty), 0) AS excluded_net_qty,
               count(DISTINCT variant_id) AS excluded_items
        FROM sat_cube WHERE {w_x} AND {fx}
    """, p_x + px))
    # The two lists sized separately: opt-outs first, then item exclusions
    # among the manufacturers still opted in (so nothing is counted twice).
    fo, po = only_excl_clause(None, "mfr_id")
    fi, pi = only_excl_clause("variant_id", None)
    cur.update(one(f"""
        SELECT round(COALESCE(sum(fee) FILTER (WHERE {fo}), 0), 2) AS optout_fee,
               count(DISTINCT mfr_id) FILTER (WHERE {fo})           AS optout_mfrs,
               round(COALESCE(sum(fee) FILTER (WHERE {fi} AND NOT {fo}), 0), 2)
                                                                    AS item_excl_fee,
               count(DISTINCT variant_id) FILTER (WHERE {fi} AND NOT {fo})
                                                                    AS item_excl_items
        FROM sat_cube WHERE {w_x}
    """, po + po + pi + po + pi + po + p_x))
    cur["exclusions_applied"] = removal_active(excl)
    return cur


@app.get("/api/sat/trend")
def sat_trend(scope: str = "sat", city: str = "", mfr: str = "",
              frm: str = "", to: str = "", excl: str = "on"):
    """Daily fee for the current slice.

    Reads sat_day_city_mfr, whose grain is exactly (day, city, manufacturer) --
    so unfiltered, city-filtered, manufacturer-filtered and both-filtered
    trends all come off the same small table.
    """
    where = [scope_clause(scope)]
    params = []
    f, p = and_eq("city_key", city)
    where.append(f)
    params += p
    f, p = and_eq("mfr_id", mfr)
    where.append(f)
    params += p
    # sat_day_city_mfr carries `day`, so the month period is a predicate on it.
    f, p = day_month_range(frm, to)
    where.append(f)
    params += p
    # mfr_id is in this table's grain, so manufacturer opt-outs apply exactly;
    # item exclusions cannot (no variant_id here). The opt-outs are a FILTER
    # rather than a WHERE so the same pass also returns fee_potential: the
    # day's fee if every manufacturer in scope opted in.
    f, p = excl_clause(excl, col=None)
    return q(f"""
        SELECT CAST(day AS VARCHAR) AS day,
               round(COALESCE(sum(fee) FILTER (WHERE {f}), 0), 2) AS fee,
               COALESCE(sum(net_qty) FILTER (WHERE {f}), 0) AS net_qty,
               round(sum(fee) FILTER (WHERE is_satellite AND {f}), 2)     AS fee_on_list,
               round(sum(fee) FILTER (WHERE NOT is_satellite AND {f}), 2) AS fee_off_list,
               round(sum(fee), 2) AS fee_potential
        FROM sat_day_city_mfr WHERE {" AND ".join(where)}
        GROUP BY day ORDER BY day
    """, p * 4 + params)


@app.get("/api/sat/by")
def sat_by(dim: str = "city", scope: str = "sat",
           city: str = "", mfr: str = "", item: str = "",
           bucket: str = "", ptype: str = "", state: str = "",
           search: str = "", sort: str = "fee",
           limit: int = Query(100, le=5000), offset: int = 0,
           frm: str = "", to: str = "",
           fmt: str = "json", excl: str = "on", optin: str = "in"):
    """Rank one dimension within the current slice -- the drilldown workhorse.

    dim=city with no filters is the city ranking; dim=mfr with city=X is
    "manufacturers in that city"; dim=item with city=X&mfr=Y is the item-level
    leaf. Same endpoint, so the UI's drill stack is just a growing filter set.
    """
    if dim not in SAT_DIMS:
        raise HTTPException(400, f"dim must be one of {sorted(SAT_DIMS)}")
    key, label, _human = SAT_DIMS[dim]
    w, p = sat_filters(scope, city, mfr, item, bucket, ptype, state, search, dim,
                       frm, to, excl, optin)
    order = {"fee": "fee DESC", "fee_asc": "fee ASC", "qty": "net_qty DESC",
             "name": "label ASC", "rate": "rate_per_unit DESC",
             "items": "items DESC", "mfrs": "mfrs DESC",
             "cities": "cities DESC"}.get(sort, "fee DESC")

    # Distance only means anything on the city dimension; MRP only on the item
    # dimension. Both are max() over the group, which is a no-op at that grain.
    extra = ""
    if dim == "city":
        extra = ", max(distance_km) AS distance_km, max(is_satellite) AS is_satellite"
    elif dim == "item":
        extra = (", max(variant_mrp) AS variant_mrp, max(mrp_threshold) AS mrp_threshold,"
                 " any_value(bucket) AS bucket, any_value(product_type) AS product_type,"
                 " any_value(mfr) AS mfr, any_value(mfr_id) AS mfr_id")
    elif dim == "mfr":
        extra = ", any_value(contract_state) AS contract_state"

    rows = q(f"""
        SELECT {key} AS id, {label} AS label,
               round(sum(fee), 2) AS fee, sum(net_qty) AS net_qty,
               sum(qty_sold) AS qty_sold, sum(qty_returned) AS qty_returned,
               count(DISTINCT city_key)   AS cities,
               count(DISTINCT mfr_id)     AS mfrs,
               count(DISTINCT variant_id) AS items,
               sum(n_rows) AS n_rows,
               round(sum(fee) / nullif(sum(net_qty), 0), 3) AS rate_per_unit
               {extra}
        FROM sat_cube WHERE {w}
        GROUP BY {key} ORDER BY {order} LIMIT ? OFFSET ?
    """, p + [limit, offset])
    if dim == "mfr":
        sm = optin_status_map()
        for r in rows:
            st = (sm.get(str(r["id"])) or {}).get("status", "pending")
            r["optin_status"] = st
            r["opted_in"] = st != "not_opted"
    if fmt == "csv":
        return csv_response(rows, f"satellite_fees_by_{dim}")
    tot = one(f"""
        SELECT count(DISTINCT {key}) AS groups, round(sum(fee), 2) AS fee,
               sum(net_qty) AS net_qty
        FROM sat_cube WHERE {w}
    """, p)
    return {"dim": dim, "total": tot, "rows": rows}


@app.get("/api/sat/splits")
def sat_splits(scope: str = "sat", frm: str = "", to: str = "", excl: str = "on"):
    """The categorical splits: MRP bucket, product type, contract state, rate."""
    s = scope_clause(scope)
    # month is in the grain of sat_split / sat_rate, so the period is a predicate.
    mf, mp = month_range(frm, to)
    s = f"{s} AND {mf}"
    cs, cp = city_stats_src(frm, to)
    apply_excl = removal_active(excl)

    def split(col):
        rows = q(f"""SELECT {col} AS label, round(sum(fee),2) AS fee,
                     sum(net_qty) AS net_qty, sum(n_rows) AS n_rows
                     FROM sat_split WHERE {s} GROUP BY 1 ORDER BY fee DESC""", mp)
        if not apply_excl:
            return rows
        # sat_split has no item grain, so subtract the excluded items' slice of
        # sat_cube, which carries the same label columns. Totals reconcile
        # exactly; per-label, sat_cube holds one bucket / state per item and
        # manufacturer where sat_split is per row, so a label can shift by the
        # rare item that changed bucket or contract state inside the window.
        fx, px = only_excl_clause()
        gone = {r["label"]: r for r in q(f"""
            SELECT {col} AS label, sum(fee) AS fee, sum(net_qty) AS net_qty,
                   sum(n_rows) AS n_rows
            FROM sat_cube WHERE {s} AND {fx} GROUP BY 1""", mp + px)}
        out = []
        for r in rows:
            g = gone.get(r["label"])
            if g:
                r = dict(r, fee=round((r["fee"] or 0) - (g["fee"] or 0), 2),
                         net_qty=(r["net_qty"] or 0) - (g["net_qty"] or 0),
                         n_rows=(r["n_rows"] or 0) - (g["n_rows"] or 0))
            if r["n_rows"]:
                out.append(r)
        return sorted(out, key=lambda r: -(r["fee"] or 0))

    return {
        "bucket": split("bucket"),
        "product_type": split("product_type"),
        "contract_state": split("contract_state"),
        # Top per-unit rates by fee contribution. fee_amt is a RATE, so this is
        # a histogram over rate values, never a sum of them.
        "rate": q(f"""SELECT CAST(rate AS VARCHAR) AS label, rate,
                        round(sum(fee),2) AS fee, sum(net_qty) AS net_qty,
                        sum(n_rows) AS n_rows
                        FROM sat_rate WHERE {s} GROUP BY 1,2
                        ORDER BY fee DESC LIMIT 15""", mp),
        "band": q(f"""SELECT band AS label, min(band_ord) AS band_ord,
                        round(sum(fee),2) AS fee, sum(net_qty) AS net_qty,
                        count(*) AS cities
                        FROM {cs} WHERE {scope_clause(scope)} AND has_fees
                        GROUP BY 1 ORDER BY band_ord""", cp),
    }


@app.get("/api/sat/facilities")
def sat_facilities(scope: str = "sat", city: str = "", search: str = "",
                   limit: int = Query(50, le=2000),
                   frm: str = "", to: str = "", fmt: str = "json"):
    """Serving-facility ranking -- which mother site the satellite fee sits behind."""
    where = [scope_clause(scope)]
    params = []
    f, p = and_eq("city_key", city)
    where.append(f)
    params += p
    f, p = like("facility_name", search)
    where.append(f)
    params += p
    f, p = month_range(frm, to)
    where.append(f)
    params += p
    rows = q(f"""
        SELECT facility_id, any_value(facility_name) AS facility_name,
               count(DISTINCT city_key) AS cities,
               round(sum(fee), 2) AS fee, sum(net_qty) AS net_qty,
               sum(outlets) AS outlets, max(mfrs) AS mfrs
        FROM sat_facility WHERE {" AND ".join(where)}
        GROUP BY facility_id ORDER BY fee DESC LIMIT ?
    """, params + [limit])
    if fmt == "csv":
        return csv_response(rows, "satellite_facilities")
    return rows


# --- exclusion list CRUD. Search reads sat_cube over the full window and all
# cities, so any item that ever carried a satellite fee can be found.

@app.get("/api/sat/exclusions")
def sat_exclusions():
    """The exclusion list, each item with the fee it carries (full window)."""
    with _excl_lock:
        items = _excl_load()
    ids = [str(i["variant_id"]) for i in items]
    fees = {}
    if ids:
        fees = {r["id"]: r for r in q("""
            SELECT CAST(variant_id AS VARCHAR) AS id, round(sum(fee), 2) AS fee,
                   sum(net_qty) AS net_qty,
                   round(sum(fee) FILTER (WHERE is_satellite), 2) AS fee_on_list
            FROM sat_cube WHERE list_contains(?::VARCHAR[], CAST(variant_id AS VARCHAR))
            GROUP BY 1""", [ids])}
    for i in items:
        f = fees.get(str(i["variant_id"]), {})
        i.update(fee=f.get("fee") or 0, net_qty=f.get("net_qty") or 0,
                 fee_on_list=f.get("fee_on_list") or 0)
    return {"items": items,
            "total_fee": round(sum(i["fee"] for i in items), 2),
            "total_fee_on_list": round(sum(i["fee_on_list"] for i in items), 2)}


@app.get("/api/sat/exclusions/search")
def sat_exclusion_search(q_: str = Query("", alias="q"),
                         limit: int = Query(25, le=200)):
    """Items matching a name, manufacturer or exact variant id."""
    term = q_.strip()
    if len(term) < 2:
        return []
    ids, mids = excl_ids(), optout_ids()
    rows = q("""
        SELECT CAST(variant_id AS VARCHAR) AS variant_id,
               any_value(item_name) AS item_name, any_value(mfr) AS mfr,
               CAST(any_value(mfr_id) AS VARCHAR) AS mfr_id,
               max(variant_mrp) AS variant_mrp,
               round(sum(fee), 2) AS fee, sum(net_qty) AS net_qty,
               count(DISTINCT city_key) AS cities
        FROM sat_cube
        WHERE lower(item_name) LIKE ? OR lower(mfr) LIKE ?
           OR CAST(variant_id AS VARCHAR) = ?
        GROUP BY 1 ORDER BY fee DESC LIMIT ?
    """, ["%" + term.lower() + "%", "%" + term.lower() + "%", term, limit])
    for r in rows:
        r["excluded"] = r["variant_id"] in ids
        r["mfr_opted_out"] = r["mfr_id"] in mids   # i.e. Not_opted
    return rows


@app.post("/api/sat/exclusions")
def sat_exclusion_add(body: dict = Body(...)):
    """Add an item. Name and manufacturer come from the DB, not the client."""
    vid = str(body.get("variant_id") or "").strip()
    reason = str(body.get("reason") or "").strip()[:300]
    if not vid:
        raise HTTPException(400, "variant_id is required")
    hit = one("""SELECT any_value(item_name) AS item_name, any_value(mfr) AS mfr,
                        any_value(mfr_id) AS mfr_id
                 FROM sat_cube WHERE CAST(variant_id AS VARCHAR) = ?""", [vid])
    if not hit.get("item_name"):
        raise HTTPException(404, f"variant_id {vid} not found in the satellite fee data")
    with _excl_lock:
        items = _excl_load()
        if not any(str(i["variant_id"]) == vid for i in items):
            items.append({"variant_id": vid, "item_name": hit["item_name"],
                          "mfr": hit["mfr"], "mfr_id": hit["mfr_id"],
                          "reason": reason,
                          "added_at": time.strftime("%Y-%m-%d %H:%M:%S")})
            _excl_save(items)
    return sat_exclusions()


@app.delete("/api/sat/exclusions/{variant_id}")
def sat_exclusion_remove(variant_id: str):
    """Put an item back into the satellite fee calculation."""
    with _excl_lock:
        items = _excl_load()
        keep = [i for i in items if str(i["variant_id"]) != variant_id]
        if len(keep) != len(items):
            _excl_save(keep)
    return sat_exclusions()


# --- manufacturer opt-in. Same manufacturer list as the rest of the tab
# (sat_cube, full window, all cities). Only non-default states are stored.

def _optin_rows(search="", status="all"):
    sm = optin_status_map()
    f, p = like("mfr", search)
    rows = q(f"""
        SELECT CAST(mfr_id AS VARCHAR) AS id, any_value(mfr) AS mfr,
               any_value(contract_state) AS contract_state,
               round(sum(fee), 2) AS fee,
               round(COALESCE(sum(fee) FILTER (WHERE is_satellite), 0), 2) AS fee_on_list,
               sum(net_qty) AS net_qty, count(DISTINCT variant_id) AS items
        FROM sat_cube WHERE {f} GROUP BY 1 ORDER BY fee DESC NULLS LAST
    """, p)
    for r in rows:
        o = sm.get(r["id"]) or {}
        r["status"] = o.get("status", "pending")
        r["status_label"] = OPTIN_LABELS[r["status"]]
        r["opted_in"] = r["status"] != "not_opted"
        r["note"] = o.get("note", "")
        r["changed_at"] = o.get("changed_at", "")
    if status in OPTIN_STATES:
        rows = [r for r in rows if r["status"] == status]
    return rows


@app.get("/api/sat/optin")
def sat_optin(search: str = "", status: str = "all",
              limit: int = Query(100, le=2000), offset: int = 0):
    """Manufacturers with their fee and current opt-in status, plus totals per state."""
    everyone = _optin_rows()
    tot = {"mfrs": len(everyone)}
    for st in OPTIN_STATES:
        sel = [r for r in everyone if r["status"] == st]
        tot[st] = len(sel)
        tot[st + "_fee"] = round(sum(r["fee"] or 0 for r in sel), 2)
        tot[st + "_fee_on_list"] = round(sum(r["fee_on_list"] or 0 for r in sel), 2)
    rows = _optin_rows(search, status)
    return {"rows": rows[offset:offset + limit], "matches": len(rows), "total": tot}


def _set_status(items, mid, mfr, status, note):
    items = [i for i in items if str(i["mfr_id"]) != mid]
    if status != "pending":
        items.append({"mfr_id": mid, "mfr": mfr, "status": status, "note": note,
                      "changed_at": time.strftime("%Y-%m-%d %H:%M:%S")})
    return items


@app.post("/api/sat/optin")
def sat_optin_set(body: dict = Body(...)):
    """Set one manufacturer's status: {"mfr_id", "status", "note"}.

    status is 'pending' | 'confirmed' | 'not_opted' (labels accepted too). The
    older {"opted_in": bool} form still works: false -> not_opted, true -> pending.
    """
    mid = str(body.get("mfr_id") or "").strip()
    if not mid:
        raise HTTPException(400, "mfr_id is required")
    if "status" in body:
        status = parse_status(body.get("status"))
        if not status:
            raise HTTPException(400, "status must be Confirmation pending, Confirmed or Not_opted")
    else:
        status = "pending" if body.get("opted_in") else "not_opted"
    note = str(body.get("note") or "").strip()[:300]
    hit = one("SELECT any_value(mfr) AS mfr FROM sat_cube "
              "WHERE CAST(mfr_id AS VARCHAR) = ?", [mid])
    if not hit.get("mfr"):
        raise HTTPException(404, f"mfr_id {mid} not found in the satellite fee data")
    with _excl_lock:
        _save(OPTOUT_PATH, _set_status(_optout_load(), mid, hit["mfr"], status, note))
    return {"mfr_id": mid, "status": status}


OPTIN_CSV_COLS = ["mfr_id", "manufacturer", "workdesk_cl_approval", "optin_status",
                  "note", "fee_all_cities", "fee_satellite_cities", "items",
                  "changed_at"]


@app.get("/api/sat/optin/export")
def sat_optin_export():
    """Every manufacturer with its current opt-in status, as an editable CSV.

    Edit `optin_status` (Confirmation pending / Confirmed / Not_opted) and `note`,
    then re-upload. The other columns are for reference and ignored on upload.
    """
    rows = [{"mfr_id": r["id"], "manufacturer": r["mfr"],
             "workdesk_cl_approval": r["contract_state"],
             "optin_status": r["status_label"], "note": r["note"],
             "fee_all_cities": r["fee"], "fee_satellite_cities": r["fee_on_list"],
             "items": r["items"], "changed_at": r["changed_at"]}
            for r in _optin_rows()]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=OPTIN_CSV_COLS)
    w.writeheader()
    w.writerows(rows)
    return StreamingResponse(
        iter([buf.getvalue()]), media_type="text/csv",
        headers={"Content-Disposition":
                 f'attachment; filename="satellite_optin_status_{time.strftime("%Y%m%d")}.csv"'})


@app.post("/api/sat/optin/import")
def sat_optin_import(body: dict = Body(...), dry_run: int = 0):
    """Apply an edited export: {"csv": "<file text>"}. All-or-nothing.

    Only manufacturers present in the file change; rows whose status (and note)
    already match are left alone. Any bad row rejects the whole file, so a
    half-applied upload cannot happen. dry_run=1 validates and returns the
    changes without saving -- the UI shows that preview before applying.
    """
    text = str(body.get("csv") or "")
    if text.startswith(chr(0xFEFF)):          # UTF-8 BOM from Excel
        text = text[1:]
    reader = csv.DictReader(io.StringIO(text))
    cols = {c.strip().lower(): c for c in (reader.fieldnames or [])}
    if "mfr_id" not in cols or "optin_status" not in cols:
        raise HTTPException(400, "CSV needs mfr_id and optin_status columns "
                                 "(download the current status to get the template)")
    known = {r["id"]: r for r in _optin_rows()}
    changes, errors, seen, unchanged = [], [], set(), 0
    for n, row in enumerate(reader, start=2):
        mid = str(row.get(cols["mfr_id"]) or "").strip()
        if not mid:
            continue
        if mid.endswith(".0") and mid[:-2].isdigit():
            mid = mid[:-2]               # Excel turns 117 into 117.0
        st = parse_status(row.get(cols["optin_status"]))
        note = str(row.get(cols["note"]) or "").strip()[:300] if "note" in cols else None
        if mid not in known:
            errors.append({"line": n, "mfr_id": mid, "error": "unknown mfr_id"})
            continue
        if not st:
            errors.append({"line": n, "mfr_id": mid,
                           "error": f"bad optin_status '{row.get(cols['optin_status'])}'"})
            continue
        if mid in seen:
            errors.append({"line": n, "mfr_id": mid, "error": "duplicate mfr_id"})
            continue
        seen.add(mid)
        cur = known[mid]
        new_note = cur["note"] if note is None else note
        if st == cur["status"] and (st == "pending" or new_note == cur["note"]):
            unchanged += 1
            continue
        changes.append({"mfr_id": mid, "mfr": cur["mfr"], "from": cur["status"],
                        "to": st, "note": new_note, "fee": cur["fee"]})
    out = {"rows": len(seen) + len(errors), "changes": changes,
           "unchanged": unchanged, "errors": errors[:50], "n_errors": len(errors),
           "applied": False}
    if errors or dry_run:
        return out
    with _excl_lock:
        items = _optout_load()
        for c in changes:
            items = _set_status(items, c["mfr_id"], c["mfr"], c["to"], c["note"] or "")
        _save(OPTOUT_PATH, items)
    out["applied"] = True
    return out


@app.get("/api/sat/options")
def sat_options(kind: str = "mfr", search: str = "", scope: str = "sat",
                limit: int = Query(200, le=2000),
                frm: str = "", to: str = "", excl: str = "on"):
    """Typeahead options for the city / manufacturer filter pickers."""
    if kind == "city":
        key, label = "city_key", "city"
    elif kind == "mfr":
        key, label = "mfr_id", "mfr"
    else:
        raise HTTPException(400, "kind must be city or mfr")
    f, p = like(label, search)
    mf, mp = month_range(frm, to)
    xf, xp = excl_clause(excl)
    return q(f"""
        SELECT {key} AS id, any_value({label}) AS label, round(sum(fee),2) AS fee
        FROM sat_cube WHERE {scope_clause(scope)} AND {f} AND {mf} AND {xf}
        GROUP BY {key} ORDER BY fee DESC LIMIT ?
    """, p + mp + xp + [limit])


# --------------------------------------------------------------------------
# 3. defective products
# --------------------------------------------------------------------------

def defect_rate():
    """The Rs/unit knob, as the build recorded it."""
    return float(one("SELECT value FROM kpis_extra WHERE key='def_rate'")
                 .get("value", 50) or 50)


def defect_where(ctype, mfr):
    where, params = [], []
    for col, val in (("complaint_type", ctype), ("mfr_id", mfr)):
        f, p = and_eq(col, val)
        where.append(f)
        params += p
    return " AND ".join(where), params


@app.get("/api/defects/kpis")
def defect_kpis(ctype: str = "", mfr: str = "",
                frm: str = "", to: str = ""):
    """Totals at Rs 50 per defective unit returned, for the current slice."""
    # In-period: one query over the complaint lines, which is the grain every
    # rollup below is built from -- so no cross-table stitching is needed.
    if active(frm, to):
        src, sp = def_src(frm, to)
        w, p = defect_where(ctype, mfr)
        row = one(f"""
            SELECT sum(qty) AS qty, round(sum(fee),2) AS fee,
                   count(*) AS complaints,
                   count(DISTINCT item_id) AS items,
                   count(DISTINCT mfr_id) AS mfrs,
                   count(DISTINCT complaint_type) AS types
            FROM {src} WHERE {w}
        """, sp + p)
        row["rate"] = defect_rate()
        return row
    if ctype or mfr:
        where, params = [], []
        f, p = and_eq("complaint_type", ctype)
        where.append(f)
        params += p
        f, p = and_eq("mfr_id", mfr)
        where.append(f)
        params += p
        base = one(f"""
            SELECT sum(qty) AS qty, round(sum(fee),2) AS fee,
                   sum(complaints) AS complaints,
                   count(DISTINCT mfr_id) AS mfrs
            FROM def_mfr_type WHERE {" AND ".join(where)}
        """, params)
        # def_mfr_type has no item grain, so item count comes from the item side
        # with the same filters applied there.
        iw, ip = [], []
        f, p = and_eq("complaint_type", ctype)
        iw.append(f)
        ip += p
        items = one(f"""
            SELECT count(DISTINCT t.item_id) AS items
            FROM def_item_type t JOIN def_item i USING (item_id)
            WHERE {" AND ".join(iw)} AND {and_eq('i.mfr_id', mfr)[0]}
        """, ip + and_eq("i.mfr_id", mfr)[1])
        base.update(items)
        base["rate"] = 50.0
        return base
    row = one("""
        SELECT sum(qty) AS qty, round(sum(fee),2) AS fee,
               sum(complaints) AS complaints, count(*) AS items,
               count(DISTINCT mfr_id) AS mfrs
        FROM def_item
    """)
    row["types"] = one("SELECT count(*) AS n FROM def_type").get("n")
    row["rate"] = defect_rate()
    return row


@app.get("/api/defects/by")
def defects_by(dim: str = "item", ctype: str = "", mfr: str = "",
               search: str = "", sort: str = "qty",
               limit: int = Query(50, le=5000), offset: int = 0,
               frm: str = "", to: str = "",
               fmt: str = "json"):
    """Rank items / manufacturers / complaint types within the current slice."""
    order = {"qty": "qty DESC", "fee": "fee DESC", "complaints": "complaints DESC",
             "name": "label ASC", "qty_asc": "qty ASC"}.get(sort, "qty DESC")

    # In-period every dimension is one GROUP BY over the complaint lines, which
    # mirrors exactly how build_defects derives def_item / def_mfr / def_type
    # (same mode()/sum()/count(DISTINCT) expressions) -- so a period-filtered
    # ranking is the same number the rollup would hold for that window.
    if active(frm, to) and dim in ("item", "mfr", "type"):
        src, sp = def_src(frm, to)
        w, p = defect_where(ctype, mfr)
        # (group key, label expression, dim-specific columns, column searched)
        key, label, extra, search_col = {
            "item": ("item_id", "any_value(item_name)",
                     ", any_value(mfr) AS mfr, any_value(mfr_id) AS mfr_id,"
                     " round(sum(gmv),2) AS gmv, mode(complaint_type) AS top_type,"
                     " count(DISTINCT complaint_type) AS types,"
                     " count(DISTINCT day) AS days",
                     "item_name"),
            "mfr":  ("mfr_id", "any_value(mfr)",
                     ", count(DISTINCT item_id) AS items,"
                     " count(DISTINCT complaint_type) AS types,"
                     " round(sum(gmv),2) AS gmv, mode(complaint_type) AS top_type",
                     "mfr"),
            "type": ("complaint_type", "any_value(complaint_type)",
                     ", count(DISTINCT item_id) AS items,"
                     " count(DISTINCT mfr_id) AS mfrs",
                     "complaint_type"),
        }[dim]
        sf, sp2 = like(search_col, search)
        rows = q(f"""
            SELECT {key} AS id, {label} AS label,
                   sum(qty) AS qty, round(sum(fee),2) AS fee,
                   count(*) AS complaints{extra}
            FROM {src} WHERE {w} AND {sf}
            GROUP BY {key} ORDER BY {order} LIMIT ? OFFSET ?
        """, sp + p + sp2 + [limit, offset])
        if fmt == "csv":
            return csv_response(rows, f"defects_by_{dim}")
        return {"dim": dim, "rows": rows}

    if dim == "type":
        f, p = and_eq("mfr_id", mfr)
        rows = q(f"""
            SELECT complaint_type AS id, complaint_type AS label,
                   sum(qty) AS qty, round(sum(fee),2) AS fee,
                   sum(complaints) AS complaints
            FROM def_mfr_type WHERE {f}
            GROUP BY 1 ORDER BY {order} LIMIT ? OFFSET ?
        """, p + [limit, offset])

    elif dim == "mfr":
        # With a complaint-type filter the manufacturer totals must come from
        # the type-grained table, otherwise they would silently include every
        # other complaint type.
        if ctype:
            f, p = and_eq("t.complaint_type", ctype)
            sf, sp = like("m.mfr", search)
            rows = q(f"""
                SELECT t.mfr_id AS id, any_value(m.mfr) AS label,
                       sum(t.qty) AS qty, round(sum(t.fee),2) AS fee,
                       sum(t.complaints) AS complaints,
                       1 AS types
                FROM def_mfr_type t JOIN def_mfr m USING (mfr_id)
                WHERE {f} AND {sf}
                GROUP BY t.mfr_id ORDER BY {order} LIMIT ? OFFSET ?
            """, p + sp + [limit, offset])
        else:
            sf, sp = like("mfr", search)
            rows = q(f"""
                SELECT mfr_id AS id, mfr AS label, qty, fee, complaints,
                       items, types, top_type
                FROM def_mfr WHERE {sf} ORDER BY {order} LIMIT ? OFFSET ?
            """, sp + [limit, offset])

    elif dim == "item":
        sf, sp = like("i.item_name", search)
        mf, mp = and_eq("i.mfr_id", mfr)
        if ctype:
            tf, tp = and_eq("t.complaint_type", ctype)
            rows = q(f"""
                SELECT t.item_id AS id, any_value(i.item_name) AS label,
                       any_value(i.mfr) AS mfr, any_value(i.mfr_id) AS mfr_id,
                       sum(t.qty) AS qty, round(sum(t.fee),2) AS fee,
                       sum(t.complaints) AS complaints,
                       any_value(i.gmv) AS gmv, ? AS top_type
                FROM def_item_type t JOIN def_item i USING (item_id)
                WHERE {tf} AND {sf} AND {mf}
                GROUP BY t.item_id ORDER BY {order} LIMIT ? OFFSET ?
            """, [ctype] + tp + sp + mp + [limit, offset])
        else:
            rows = q(f"""
                SELECT i.item_id AS id, i.item_name AS label, i.mfr, i.mfr_id,
                       i.qty, i.fee, i.complaints, i.types, i.gmv, i.top_type, i.days
                FROM def_item i WHERE {sf} AND {mf}
                ORDER BY {order} LIMIT ? OFFSET ?
            """, sp + mp + [limit, offset])
    else:
        raise HTTPException(400, "dim must be item, mfr or type")

    if fmt == "csv":
        return csv_response(rows, f"defects_by_{dim}")
    return {"dim": dim, "rows": rows}


@app.get("/api/defects/terms")
def defect_terms(ctype: str = "", ngram: int = 1,
                 limit: int = Query(80, le=500), fmt: str = "json"):
    """The defect keyword cloud: terms customers actually used.

    Mined from the free-text remarks with the returns-UI boilerplate and the
    item name stripped first, counted once per complaint. ngram=1 for single
    words, 2 for phrases ("not working", "leakage issue").
    """
    if ctype:
        rows = q("""
            SELECT term, ngram, mentions, qty,
                   round(qty * 50.0, 2) AS fee, ? AS top_type
            FROM def_type_terms WHERE complaint_type = ? AND ngram = ?
            ORDER BY mentions DESC LIMIT ?
        """, [ctype, ctype, ngram, limit])
    else:
        rows = q("""
            SELECT term, ngram, mentions, qty, fee, items, top_type
            FROM def_terms WHERE ngram = ?
            ORDER BY mentions DESC LIMIT ?
        """, [ngram, limit])
    if fmt == "csv":
        return csv_response(rows, "defect_terms")
    return rows


# Failure mode -> the raw complaint types that belong to it.
#
# This grouping is EDITORIAL, and the UI says so: the extract carries the 14 raw
# `complaint_type` values only, and which team owns a complaint is a judgement, not
# a column. It is here rather than in SQL string-building so the mapping is
# reviewable in one place, and the raw per-type table on the same tab remains the
# source of truth for anyone who disagrees with the buckets.
DEFECT_BUCKETS = [
    ("handling", "Damaged in handling or packaging",
     ["COMPLAINT_DAMAGED_ITEM", "COMPLAINT_PACKAGING_ISSUE"],
     "Arrived broken, crushed, leaking or already open. Owned by packaging spec, "
     "warehouse handling and last-mile, not by the vendor's product quality."),
    ("quality", "Product does not work or is off",
     ["COMPLAINT_FAULTY_ITEM", "COMPLAINT_QUALITY_ISSUE",
      "COMPLAINT_SMELL_OR_TASTE_ISSUE", "COMPLAINT_AUDIO_ISSUE",
      "COMPLAINT_BLUETOOTH_ISSUE"],
     "Dead on arrival, poor quality, or spoiled. A vendor-quality and "
     "shelf-life/QC problem — the one bucket where the defective-product penalty "
     "clause is the right lever."),
    ("fulfilment", "Something missing from the pack",
     ["COMPLAINT_MISSING_PART", "COMPLAINT_PARTIAL_ITEM_MISSING",
      "COMPLAINT_FREEBIE_MISSING"],
     "Incomplete kit, missing accessory or missing freebie. A picking and "
     "catalogue-composition problem."),
    ("listing", "Not what the listing implied",
     ["COMPLAINT_EXPECTATION_MISMATCH", "COMPLAINT_SIZE_ISSUE",
      "COMPLAINT_PRICE_MISMATCH"],
     "The item is fine but not what the customer expected from the page. Fixed by "
     "catalogue copy, images and size/variant data, not by the supply chain."),
    ("authenticity", "Suspected fake or duplicate",
     ["COMPLAINT_FAKE_OR_DUPLICATE_ITEM"],
     "Low volume but the highest-severity category: a sourcing and "
     "brand-protection escalation regardless of quantity."),
]


@app.get("/api/defects/summary")
def defects_summary(frm: str = "", to: str = ""):
    """Executive summary inputs for the Defective products tab.

    Everything here is measured, not asserted: bucket shares, how concentrated the
    volume is across manufacturers and items, how many days the worst items recur
    on, and the words customers actually used. The tab turns these into "what to
    fix" wording; the numbers behind each statement are rendered next to it so the
    reasoning can be checked rather than taken on trust.

    The keyword counts are the one input the period cannot reach -- terms are
    tokenised at build time with no day attached -- so they are returned with an
    explicit `terms_period_filtered: false` rather than looking filtered.
    """
    src, p = def_src(frm, to)

    tot = one(f"""SELECT sum(qty) AS qty, round(sum(fee),2) AS fee,
                         count(*) AS complaints,
                         count(DISTINCT item_id) AS items,
                         count(DISTINCT mfr_id) AS mfrs,
                         count(DISTINCT complaint_type) AS types,
                         count(DISTINCT day) AS days,
                         CAST(min(day) AS VARCHAR) AS first_day,
                         CAST(max(day) AS VARCHAR) AS last_day
                  FROM {src}""", p)

    # Buckets. CASE is built from the whitelist above, and every type name is
    # bound as a parameter -- none of it is interpolated.
    when, bp = [], []
    for key, _lbl, types, _why in DEFECT_BUCKETS:
        ph = ", ".join("?" for _ in types)
        when.append(f"WHEN complaint_type IN ({ph}) THEN ?")
        bp += types + [key]
    case = "CASE " + " ".join(when) + " ELSE 'other' END"
    buckets = q(f"""
        SELECT {case} AS bucket, sum(qty) AS qty, round(sum(fee),2) AS fee,
               count(*) AS complaints, count(DISTINCT item_id) AS items,
               count(DISTINCT mfr_id) AS mfrs
        FROM {src} GROUP BY 1 ORDER BY qty DESC
    """, p + bp)

    types = q(f"""SELECT complaint_type, sum(qty) AS qty, count(*) AS complaints,
                         count(DISTINCT item_id) AS items
                  FROM {src} GROUP BY 1 ORDER BY qty DESC""", p)

    # How concentrated is the volume? This is what decides whether the answer is
    # "fix these vendors" or "fix the process".
    conc = one(f"""
        WITH m AS (SELECT mfr_id, sum(qty) AS qty FROM {src} GROUP BY 1),
             i AS (SELECT item_id, sum(qty) AS qty FROM {src} GROUP BY 1)
        SELECT (SELECT sum(qty) FROM (SELECT qty FROM m ORDER BY qty DESC LIMIT 10)) AS top10_mfr_qty,
               (SELECT sum(qty) FROM (SELECT qty FROM m ORDER BY qty DESC LIMIT 50)) AS top50_mfr_qty,
               (SELECT sum(qty) FROM (SELECT qty FROM i ORDER BY qty DESC LIMIT 50)) AS top50_item_qty,
               (SELECT sum(qty) FROM (SELECT qty FROM i ORDER BY qty DESC LIMIT 500)) AS top500_item_qty,
               (SELECT count(*) FROM i WHERE qty = 1) AS single_unit_items
    """, p + p + p + p + p)

    # Chronic vs incidental: an item returning on nearly every day in the window is
    # a standing defect, not a bad batch. That distinction changes the fix.
    chronic = q(f"""
        SELECT item_name, mfr, sum(qty) AS qty, count(DISTINCT day) AS days,
               count(DISTINCT complaint_type) AS types,
               mode(complaint_type) AS top_type
        FROM {src} GROUP BY item_name, mfr
        HAVING count(DISTINCT day) >= 0.8 * (SELECT count(DISTINCT day) FROM {src})
        ORDER BY qty DESC LIMIT 10
    """, p + p)

    top_mfr = q(f"""SELECT mfr, sum(qty) AS qty, count(DISTINCT item_id) AS items,
                           mode(complaint_type) AS top_type
                    FROM {src} GROUP BY mfr ORDER BY qty DESC LIMIT 8""", p)

    # Customer language, per bucket, so "what are they complaining about" is
    # answered in their words and not only in taxonomy codes.
    words = {}
    for key, _lbl, tps, _why in DEFECT_BUCKETS:
        ph = ", ".join("?" for _ in tps)
        words[key] = q(f"""
            SELECT term, sum(mentions) AS mentions
            FROM def_type_terms
            WHERE ngram = 2 AND complaint_type IN ({ph})
            GROUP BY term ORDER BY mentions DESC LIMIT 6
        """, tps)

    return {
        "totals": tot,
        "buckets": [dict(b, label=lbl, why=why, types=tps)
                    for key, lbl, tps, why in DEFECT_BUCKETS
                    for b in buckets if b["bucket"] == key],
        "other": [b for b in buckets if b["bucket"] == "other"],
        "types": types,
        "concentration": conc,
        "chronic": chronic,
        "top_mfr": top_mfr,
        "words": words,
        "terms_period_filtered": False,
        "rate": defect_rate(),
    }


@app.get("/api/defects/trend")
def defects_trend(ctype: str = "", frm: str = "", to: str = ""):
    """Daily defective quantity and fee. def_daily already has `day`."""
    f, p = and_eq("complaint_type", ctype)
    df, dp = day_month_range(frm, to)
    return q(f"""
        SELECT CAST(day AS VARCHAR) AS day, sum(qty) AS qty,
               round(sum(fee),2) AS fee, sum(complaints) AS complaints
        FROM def_daily WHERE {f} AND {df} GROUP BY day ORDER BY day
    """, p + dp)


@app.get("/api/defects/item")
def defect_item(item_id: str, frm: str = "", to: str = ""):
    """One item's complaint-type breakdown -- the item drilldown leaf."""
    if active(frm, to):
        src, sp = def_src(frm, to)
        return {
            "item": one(f"""
                SELECT item_id, any_value(item_name) AS item_name,
                       any_value(mfr) AS mfr, any_value(mfr_id) AS mfr_id,
                       sum(qty) AS qty, round(sum(fee),2) AS fee,
                       round(sum(gmv),2) AS gmv, count(*) AS complaints,
                       count(DISTINCT complaint_type) AS types,
                       count(DISTINCT day) AS days,
                       mode(complaint_type) AS top_type
                FROM {src} WHERE item_id = ? GROUP BY item_id""", sp + [item_id]),
            "types": q(f"""
                SELECT complaint_type, sum(qty) AS qty, round(sum(fee),2) AS fee,
                       count(*) AS complaints
                FROM {src} WHERE item_id = ?
                GROUP BY complaint_type ORDER BY qty DESC""", sp + [item_id]),
        }
    return {
        "item": one("SELECT * FROM def_item WHERE item_id = ?", [item_id]),
        "types": q("""SELECT complaint_type, qty, fee, complaints
                      FROM def_item_type WHERE item_id = ?
                      ORDER BY qty DESC""", [item_id]),
    }


# --------------------------------------------------------------------------
# 4. KAM fees
#
# Billable = APPROVED + PENDING APPROVAL (a pending contract is signed and
# charged); DRAFT / EXPIRED are contracted but not billable.
#
# KAM addendum dates (KAM_ADDENDA_PATH, on the state volume beside the
# satellite lists): when a contract has one, its KAM charge accrues from the
# addendum's month instead of the contract's effective month. kam_month in
# fees.duckdb is never modified; with addenda present the same month spine is
# regenerated at query time from kam_contracts (see kam_src), and with none the
# pre-built table is read as-is, so numbers are unchanged until one is added.
# --------------------------------------------------------------------------

KAM_BILLABLE = ("APPROVED", "PENDING APPROVAL")
ADDENDA_PATH = os.environ.get("KAM_ADDENDA_PATH",
                              os.path.join(os.path.dirname(os.path.abspath(EXCL_PATH)),
                                           "kam_addenda.json"))
DATE_ONLY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _addenda_load():
    return _load(ADDENDA_PATH, "contract_id")


def kam_end_month():
    """Last accrual month, as a month-start DATE string (build_meta, else kam_month)."""
    end = (one("SELECT value FROM build_meta WHERE key = 'kam_accrual_end'")
           if has("build_meta") else {}).get("value")
    if not end:
        end = one("SELECT strftime(max(month_date), '%Y-%m') AS v FROM kam_month").get("v")
    return (end or "1970-01")[:7] + "-01"


def kam_src():
    """(from-clause, params) with kam_month's columns, addenda applied."""
    with _excl_lock:
        ad = [(str(a["contract_id"]), a["date"]) for a in _addenda_load()
              if DATE_ONLY.match(str(a.get("date", "")))]
    if not ad:
        return "kam_month", []
    values = ", ".join(["(?, CAST(? AS DATE))"] * len(ad))
    end = kam_end_month()
    params = [v for pair in ad for v in pair] + [end, end]
    return (f"""(
        WITH ad(contract_id, start_date) AS (VALUES {values}),
        c AS (
            SELECT k.*, date_trunc('month', COALESCE(ad.start_date, k.effective_date)) AS m0
            FROM kam_contracts k LEFT JOIN ad ON ad.contract_id = CAST(k.contract_id AS VARCHAR)),
        mm AS (
            SELECT c.*, UNNEST(generate_series(c.m0, CAST(? AS DATE), INTERVAL '1' MONTH)) AS m
            FROM c WHERE c.m0 <= CAST(? AS DATE))
        SELECT strftime(m, '%Y-%m') AS month, CAST(m AS DATE) AS month_date,
               mfr_id, mfr, contract_id, contract_state,
               COALESCE(monthly_fee, 0) AS fee, effective_date,
               (date_diff('month', m0, m) + 1) AS month_idx
        FROM mm)""", params)


def kam_state_clause(state):
    if state == "billable":
        return "contract_state IN ('APPROVED', 'PENDING APPROVAL')", []
    return and_eq("contract_state", state)


@app.get("/api/kam/kpis")
def kam_kpis_live(frm: str = "", to: str = ""):
    """Headline KAM numbers computed live, so addenda move them immediately."""
    src, sp = kam_src()
    mf, mp = month_range(frm, to)
    k = one(f"""
        SELECT round(COALESCE(sum(fee), 0), 2) AS fee_to_date,
               round(COALESCE(sum(fee) FILTER (WHERE contract_state IN ('APPROVED','PENDING APPROVAL')), 0), 2)
                   AS billable_to_date,
               round(COALESCE(sum(fee) FILTER (WHERE contract_state = 'APPROVED'), 0), 2)
                   AS approved_to_date,
               count(DISTINCT month) AS months,
               min(month) AS first_month, max(month) AS last_month
        FROM {src} AS km WHERE {mf}""", sp + mp)
    c = one("""
        SELECT count(*) AS contracts,
               count(*) FILTER (WHERE contract_state IN ('APPROVED','PENDING APPROVAL')) AS billable,
               count(*) FILTER (WHERE contract_state = 'APPROVED') AS approved
        FROM kam_contracts""")
    k.update(c)
    base = one(f"SELECT round(COALESCE(sum(fee), 0), 2) AS v FROM kam_month WHERE {mf}", mp)
    k["fee_without_addenda"] = base.get("v") or 0
    with _excl_lock:
        k["addenda"] = len(_addenda_load())
    k["accrual_end"] = kam_end_month()[:7]
    return k


@app.get("/api/kam/addenda")
def kam_addenda():
    """Addendum dates, each with the contract's effective date and the fee it moves."""
    with _excl_lock:
        items = _addenda_load()
    if not items:
        return {"items": [], "fee_delta": 0}
    kc = {str(r["contract_id"]): r for r in q("""
        SELECT CAST(contract_id AS VARCHAR) AS contract_id, mfr, contract_state,
               CAST(effective_date AS VARCHAR) AS effective_date, monthly_fee
        FROM kam_contracts""")}
    src, sp = kam_src()
    now = {r["contract_id"]: r for r in q(f"""
        SELECT CAST(contract_id AS VARCHAR) AS contract_id, round(sum(fee), 2) AS fee,
               count(*) AS months, min(month) AS first_month
        FROM {src} AS km GROUP BY 1""", sp)}
    was = {r["contract_id"]: r for r in q("""
        SELECT CAST(contract_id AS VARCHAR) AS contract_id, round(sum(fee), 2) AS fee,
               count(*) AS months
        FROM kam_month GROUP BY 1""")}
    out = []
    for a in items:
        cid = str(a["contract_id"])
        c, n, w = kc.get(cid, {}), now.get(cid, {}), was.get(cid, {})
        out.append(dict(a, mfr=c.get("mfr") or a.get("mfr"),
                        contract_state=c.get("contract_state"),
                        effective_date=c.get("effective_date"),
                        monthly_fee=c.get("monthly_fee"),
                        first_month=n.get("first_month"), months=n.get("months") or 0,
                        fee=n.get("fee") or 0,
                        fee_delta=round((n.get("fee") or 0) - (w.get("fee") or 0), 2)))
    return {"items": sorted(out, key=lambda r: r.get("mfr") or ""),
            "fee_delta": round(sum(r["fee_delta"] for r in out), 2)}


@app.post("/api/kam/addenda")
def kam_addendum_set(body: dict = Body(...)):
    """{"contract_id", "date": "YYYY-MM-DD", "note"} -- KAM accrues from that month."""
    cid = str(body.get("contract_id") or "").strip()
    d = str(body.get("date") or "").strip()
    note = str(body.get("note") or "").strip()[:300]
    if not cid or not DATE_ONLY.match(d):
        raise HTTPException(400, "contract_id and date (YYYY-MM-DD) are required")
    hit = one("SELECT mfr FROM kam_contracts WHERE CAST(contract_id AS VARCHAR) = ?", [cid])
    if not hit.get("mfr"):
        raise HTTPException(404, f"contract {cid} is not a KAM contract")
    with _excl_lock:
        items = [a for a in _addenda_load() if str(a["contract_id"]) != cid]
        items.append({"contract_id": cid, "mfr": hit["mfr"], "date": d, "note": note,
                      "changed_at": time.strftime("%Y-%m-%d %H:%M:%S")})
        _save(ADDENDA_PATH, items)
    return kam_addenda()


@app.delete("/api/kam/addenda/{contract_id}")
def kam_addendum_remove(contract_id: str):
    with _excl_lock:
        items = _addenda_load()
        keep = [a for a in items if str(a["contract_id"]) != contract_id]
        if len(keep) != len(items):
            _save(ADDENDA_PATH, keep)
    return kam_addenda()


@app.get("/api/kam/contracts")
def kam_contract_options(search: str = "", limit: int = Query(30, le=500)):
    """Typeahead for the addendum form: KAM contracts matching a name or id."""
    term = search.strip().lower()
    return q("""
        SELECT CAST(contract_id AS VARCHAR) AS contract_id, mfr, contract_state,
               CAST(effective_date AS VARCHAR) AS effective_date, monthly_fee
        FROM kam_contracts
        WHERE ? = '' OR lower(mfr) LIKE ? OR CAST(contract_id AS VARCHAR) = ?
        ORDER BY mfr LIMIT ?""", [term, f"%{term}%", term, limit])

@app.get("/api/kam/monthly")
def kam_monthly(state: str = "all", frm: str = "", to: str = ""):
    """Total KAM fee per month, plus contracts live that month.

    Also returns the same months split by contract state, because the
    accrued-vs-billable gap is the whole story on this tab: 149 of 169 KAM
    contracts are not APPROVED, so a single monthly total silently reports a
    contracted ceiling as if it were revenue.
    """
    f, p = kam_state_clause(state)
    # KAM accrues per calendar month, so the period selects whole months.
    mf, mp = month_range(frm, to)
    src, sp = kam_src()
    total = q(f"""
        SELECT month, count(*) AS contracts,
               round(sum(fee), 2) AS fee,
               count(*) FILTER (WHERE month_idx = 1) AS new_contracts,
               count(DISTINCT mfr_id) AS mfrs
        FROM {src} AS km WHERE {f} AND {mf}
        GROUP BY month ORDER BY month
    """, sp + p + mp)
    by_state = q(f"""
        SELECT month, contract_state, round(sum(fee), 2) AS fee,
               count(*) AS contracts
        FROM {src} AS km WHERE {f} AND {mf}
        GROUP BY month, contract_state ORDER BY month
    """, sp + p + mp)
    return {"total": total, "by_state": by_state}


@app.get("/api/kam/by_mfr")
def kam_by_mfr(month: str = "", state: str = "all", search: str = "",
               sort: str = "fee", limit: int = Query(200, le=5000),
               frm: str = "", to: str = "",
               fmt: str = "json"):
    """KAM fee per manufacturer -- for one month, or accrued across all months."""
    where, params = [], []
    f, p = and_eq("month", month)
    where.append(f)
    params += p
    f, p = month_range(frm, to)
    where.append(f)
    params += p
    f, p = kam_state_clause(state)
    where.append(f)
    params += p
    f, p = like("mfr", search)
    where.append(f)
    params += p
    order = {"fee": "fee DESC", "name": "mfr ASC", "months": "months DESC",
             "start": "first_month ASC"}.get(sort, "fee DESC")
    src, sp = kam_src()
    rows = q(f"""
        SELECT mfr_id, any_value(mfr) AS mfr, any_value(contract_id) AS contract_id,
               any_value(contract_state) AS contract_state,
               round(sum(fee), 2) AS fee,
               count(*) AS months,
               max(fee) AS monthly_fee,
               min(month) AS first_month, max(month) AS last_month,
               CAST(min(effective_date) AS VARCHAR) AS effective_date
        FROM {src} AS km WHERE {" AND ".join(where)}
        GROUP BY mfr_id ORDER BY {order} LIMIT ?
    """, sp + params + [limit])
    with _excl_lock:
        ad = {str(a["contract_id"]): a["date"] for a in _addenda_load()}
    for r in rows:
        r["addendum_date"] = ad.get(str(r["contract_id"]), "")
    if fmt == "csv":
        return csv_response(rows, "kam_fees_by_manufacturer")
    tot = one(f"""
        SELECT count(DISTINCT mfr_id) AS mfrs, round(sum(fee),2) AS fee
        FROM {src} AS km WHERE {" AND ".join(where)}
    """, sp + params)
    return {"total": tot, "rows": rows}


@app.get("/api/kam/top_mfr")
def kam_top_mfr(state: str = "all", month: str = "", top: int = Query(12, le=60),
                frm: str = "", to: str = ""):
    """Top manufacturers by KAM fee, for one month or accrued across all.

    Deliberately NOT a stacked-by-manufacturer time series. 164 of 169 KAM
    contracts charge the identical Rs 50,000/month, so every manufacturer is
    ~0.6% of the monthly total: a stack would be one undifferentiated block with
    an "Other" segment swamping the named series. A ranked bar answers "by
    manufacturer" legibly, and the month filter moves it through time.
    """
    where, params = [], []
    f, p = kam_state_clause(state)
    where.append(f)
    params += p
    f, p = and_eq("month", month)
    where.append(f)
    params += p
    f, p = month_range(frm, to)
    where.append(f)
    params += p
    src, sp = kam_src()
    return q(f"""
        SELECT mfr_id, any_value(mfr) AS mfr,
               any_value(contract_state) AS contract_state,
               round(sum(fee), 2) AS fee,
               count(*) AS months, max(fee) AS monthly_fee
        FROM {src} AS km WHERE {" AND ".join(where)}
        GROUP BY mfr_id ORDER BY fee DESC LIMIT ?
    """, sp + params + [top])


@app.get("/api/kam/states")
def kam_states():
    """Contract-state mix for the KAM cohort, plus the wider opt-in picture.

    88% of KAM contracts are PENDING APPROVAL, so the state split is not a
    footnote -- it is the difference between accrued and billable.
    """
    out = {
        "states": q("""
            SELECT contract_state, count(*) AS contracts,
                   round(sum(COALESCE(monthly_fee,0)), 2) AS monthly_fee
            FROM kam_contracts GROUP BY 1 ORDER BY contracts DESC"""),
        "charges": q("""
            SELECT COALESCE(monthly_fee, 0) AS monthly_fee, count(*) AS contracts
            FROM kam_contracts GROUP BY 1 ORDER BY 1"""),
    }
    if has("kam_coverage"):
        out["coverage"] = q("""
            SELECT kam_support, sum(contracts) AS contracts
            FROM kam_coverage GROUP BY 1 ORDER BY contracts DESC""")
    return out


# --------------------------------------------------------------------------
# 5. recall assistance -- PRN recall actuals
#
# Every figure here is read straight off the corrected recall extract
# (rtv_month): recall_assistance_fee is the money, prn_qty is what came back and
# chargeable_prn_qty is the slice with a contract rate behind it. Nothing is
# derived. The one thing these endpoints must not do is let quantity and fee be
# read as the same story -- about half the lines have no recall-assistance
# contract at all, so `sort=qty` and `sort=fee` return genuinely different
# rankings and both are exposed.
# --------------------------------------------------------------------------

# dim -> (group-by key column, display column). Whitelisted: `dim` reaches SQL.
RECALL_DIMS = {
    "mfr":  ("mfr_id", "mfr"),
    "item": ("item_id", "item"),
}
RECALL_SORTS = {
    "fee": "fee DESC",
    "qty": "qty DESC",
    "lines": "lines DESC",
    "rate": "rate DESC NULLS LAST",
    "name": "label ASC",
}


@app.get("/api/recall/monthly")
def recall_monthly(frm: str = "", to: str = ""):
    """Total recall-assistance fee and returned quantity per month.

    `qty_charged` is carried alongside `qty` so the UI can show what share of the
    returned units actually had a contract rate behind them -- without it,
    dividing fee by qty understates the per-unit rate by roughly three times.
    `qty_unrated` is the complement and is the honest label for it: those units
    have no knowable fee rather than a fee of zero.
    """
    # Recall lines carry a month, not a day, so the period selects whole months.
    mf, mp = month_range(frm, to)
    return {
        "total": q(f"""
            SELECT month, CAST(month_date AS VARCHAR) AS month_date,
                   fee, qty, qty_charged, qty_unrated, mfrs, items,
                   mfrs_charged, items_charged, lines, lines_charged,
                   contracts, fee_ineligible, rate
            FROM rtv_month_total WHERE {mf} ORDER BY month""", mp),
        "months": [r["month"] for r in q(
            f"SELECT month FROM rtv_month_total WHERE {mf} ORDER BY month", mp)],
    }


@app.get("/api/recall/by")
def recall_by(dim: str = "item", month: str = "", search: str = "",
              sort: str = "fee", charged: str = "all",
              limit: int = Query(50, le=5000),
              frm: str = "", to: str = "", fmt: str = "json"):
    """Recall fee / returned quantity per manufacturer or per item.

    charged=yes restricts to lines with a contract rate applied to units. That
    filter matters: ranked by raw quantity the top rows are Blinkit's own
    warehouse and placeholder manufacturers (Grofers Warehouse, Blinkit
    Non - trade, Dummy Manufacturer 1), which return in volume and are not
    chargeable at all -- true of the data, but not an answer to "who is being
    charged". The switch is on chargeable quantity rather than fee > 0 so a
    genuine Rs 0 contract rate still counts as charged.
    """
    if dim not in RECALL_DIMS:
        raise HTTPException(400, f"unknown dim: {dim}")
    keycol, labelcol = RECALL_DIMS[dim]

    where, params = [], []
    f, p = and_eq("month", month)
    where.append(f)
    params += p
    f, p = month_range(frm, to)
    where.append(f)
    params += p
    # Item searches should also match on the manufacturer, the way the defect
    # tab behaves -- one box, both fields.
    term = (search or "").strip()
    if term:
        where.append("(lower(item) LIKE ? OR lower(mfr) LIKE ?)")
        params += ["%" + term.lower() + "%"] * 2
    if charged == "yes":
        where.append("qty_charged > 0")
    elif charged == "no":
        where.append("qty_charged = 0")
    w = " AND ".join(where)
    order = RECALL_SORTS.get(sort, RECALL_SORTS["fee"])

    rows = q(f"""
        SELECT {keycol}                    AS key,
               any_value({labelcol})       AS label,
               any_value(mfr)              AS mfr,
               round(sum(fee), 2)          AS fee,
               sum(qty)                    AS qty,
               sum(qty_charged)            AS qty_charged,
               sum(qty_unrated)            AS qty_unrated,
               sum(lines)                  AS lines,
               sum(lines_charged)          AS lines_charged,
               count(DISTINCT month)       AS months,
               count(DISTINCT contract_id) AS contracts,
               count(DISTINCT {'item_id' if dim == 'mfr' else 'mfr_id'})
                                           AS peers,
               -- reported contractual rate where the group is on a single rate;
               -- the fee-weighted effective rate where it spans several. Never a
               -- sum: recall_charge_rs is a per-unit rate.
               CASE WHEN count(DISTINCT rate_contract) = 1 THEN max(rate_contract)
                    ELSE round(sum(fee) / NULLIF(sum(qty_charged), 0), 2) END AS rate
        FROM rtv_month WHERE {w}
        GROUP BY {keycol} ORDER BY {order}, label ASC LIMIT ?
    """, params + [limit])
    if fmt == "csv":
        return csv_response(rows, f"recall_assistance_by_{dim}"
                                  + (f"_{month}" if month else ""))
    total = one(f"""
        SELECT round(sum(fee), 2)            AS fee,
               sum(qty)                      AS qty,
               sum(qty_charged)              AS qty_charged,
               sum(qty_unrated)              AS qty_unrated,
               sum(lines)                    AS lines,
               count(DISTINCT {keycol})      AS keys,
               count(DISTINCT {keycol}) FILTER (WHERE qty_charged > 0) AS keys_charged
        FROM rtv_month WHERE {w}
    """, params)
    return {"total": total, "rows": rows, "dim": dim, "sort": sort}


@app.get("/api/recall/rates")
def recall_rates(frm: str = "", to: str = "", fmt: str = "json"):
    """Contractual Rs/unit the fee was charged on -- the audit of the money.

    Reported, not derived: the extract carries recall_charge_rs per line, and the
    fee reconciles to rate x chargeable quantity exactly. The audit is that the
    money concentrates on the contractual Rs 5 / Rs 2.50 -- they carry ~99.9% of
    it -- rather than scattering across ad-hoc rates.
    """
    if active(frm, to):
        # rtv_rate has no month, so an in-period histogram is recomputed from the
        # claim lines -- same expressions build_recall uses for the rollup.
        mf, mp = month_range(frm, to, "date_trunc('month', claim_date)")
        rows = q(f"""
            SELECT rate_contract AS rate, count(*) AS lines,
                   sum(qty_charged) AS qty, round(sum(fee), 2) AS fee,
                   count(DISTINCT contract_id) AS contracts
            FROM rtv_claim WHERE rate_contract IS NOT NULL AND {mf}
            GROUP BY 1 ORDER BY fee DESC""", mp)
    else:
        rows = q("SELECT rate, lines, qty, fee, contracts "
                 "FROM rtv_rate ORDER BY fee DESC")
    if fmt == "csv":
        return csv_response(rows, "recall_assistance_rates")
    return {"rows": rows}


# --------------------------------------------------------------------------
# 6. contracts -- insights + details
# --------------------------------------------------------------------------

@app.get("/api/contracts/insights")
def contract_insights():
    """Clause adoption, dimension mixes, negotiated-term distributions, timeline."""
    return {
        "clauses": q("""
            SELECT clause, label, clause_group, unit, enabled, declined, not_set,
                   contracts, pct_enabled, enabled_approved, modal_value
            FROM contract_clauses ORDER BY enabled DESC"""),
        "dims": q("""
            SELECT dim, label, value, contracts, mfrs, approved
            FROM contract_dims ORDER BY dim, contracts DESC"""),
        "terms": q("""
            SELECT term, label, unit, value, contracts, approved
            FROM contract_terms ORDER BY term, value"""),
        "timeline": q("""
            SELECT month, contract_state, contracts
            FROM contract_timeline ORDER BY month"""),
    }


CONTRACT_SORTS = {
    "updated": "updated_at DESC NULLS LAST",
    "executed": "execution_date DESC NULLS LAST",
    "executed_asc": "execution_date ASC NULLS LAST",
    "mfr": "mfr ASC",
    "clauses": "clauses_enabled DESC",
    "clauses_asc": "clauses_enabled ASC",
    "kam": "kam_charges DESC NULLS LAST",
}


@app.get("/api/contracts/list")
def contracts_list(q_: str = Query("", alias="q"), state: str = "all",
                   purchase: str = "all", clause: str = "",
                   sort: str = "updated", limit: int = Query(100, le=5000),
                   offset: int = 0, fmt: str = "json"):
    """Searchable contract list. `clause` filters to contracts with that clause on."""
    where, params = ["TRUE"], []
    f, p = like("mfr", q_)
    where.append(f)
    params += p
    f, p = and_eq("contract_state", state)
    where.append(f)
    params += p
    f, p = and_eq("type_of_purchase", purchase)
    where.append(f)
    params += p
    # Only the boolean columns contract_list actually materialises are
    # filterable, so an unknown clause name can never reach the SQL text.
    CLAUSE_COLS = {"kam": "has_kam", "defect": "has_defect_penalty",
                   "fill_rate": "has_fill_rate_penalty", "recall": "has_recall",
                   "sp": "has_sp_benchmarking", "ads": "has_min_ads"}
    if clause:
        col = CLAUSE_COLS.get(clause)
        if not col:
            raise HTTPException(400, f"clause must be one of {sorted(CLAUSE_COLS)}")
        where.append(col)

    w = " AND ".join(where)
    order = CONTRACT_SORTS.get(sort, CONTRACT_SORTS["updated"])
    rows = q(f"""
        SELECT contract_id, mfr_id, mfr, buyer, contract_state,
               CAST(execution_date AS VARCHAR) AS execution_date,
               CAST(effective_date AS VARCHAR) AS effective_date,
               CAST(updated_at AS VARCHAR)     AS updated_at,
               type_of_purchase, valid_until_cancelled, payout_frequency,
               purchase_margin_computation, rtv_days, credit_days,
               has_kam, kam_charges, has_defect_penalty, has_fill_rate_penalty,
               has_recall, has_sp_benchmarking, has_min_ads, clauses_enabled
        FROM contract_list WHERE {w}
        ORDER BY {order}, contract_id LIMIT ? OFFSET ?
    """, params + [limit, offset])
    if fmt == "csv":
        return csv_response(rows, "contracts")
    tot = one(f"SELECT count(*) AS n, count(DISTINCT mfr_id) AS mfrs "
              f"FROM contract_list WHERE {w}", params)
    return {"total": tot, "rows": rows}


@app.get("/api/contracts/detail")
def contract_detail(contract_id: str):
    """Every term on one contract, grouped for the detail drawer.

    Returns the raw row as an ordered list of (field, value) pairs plus the
    clause catalogue, so the drawer shows a clause's switch and its negotiated
    value side by side.
    """
    row = one("SELECT * FROM contracts WHERE contract_id = ?", [contract_id])
    if not row:
        raise HTTPException(404, f"contract {contract_id} not found")
    row.pop("mfr_key", None)
    clauses = q("SELECT clause, label, clause_group, enable_col, value_col, unit "
                "FROM contract_clauses ORDER BY clause_group, label")
    for c in clauses:
        c["enabled_value"] = row.get(c["enable_col"])
        c["term_value"] = row.get(c["value_col"]) if c["value_col"] else None
    # Everything a clause already covers is dropped from `fields`, so the raw
    # dump below the clause grid is only what the grid does not show.
    covered = {c["enable_col"] for c in clauses} | {
        c["value_col"] for c in clauses if c["value_col"]}
    fields = [{"field": k, "value": (str(v) if v is not None else None)}
              for k, v in row.items() if k not in covered]
    return {"contract_id": contract_id, "row": {
        k: (str(v) if v is not None else None) for k, v in row.items()},
        "clauses": clauses, "fields": fields}


@app.get("/api/contracts/options")
def contract_options():
    """Filter options for the contract list."""
    return {
        "states": q("SELECT contract_state AS value, count(*) AS n "
                    "FROM contract_list GROUP BY 1 ORDER BY n DESC"),
        "purchase": q("SELECT COALESCE(type_of_purchase,'Not set') AS value, "
                      "count(*) AS n FROM contract_list GROUP BY 1 ORDER BY n DESC"),
    }


# --------------------------------------------------------------------------
# static
# --------------------------------------------------------------------------

@app.get("/")
def index():
    p = os.path.join(STATIC, "index.html")
    if not os.path.exists(p):
        return JSONResponse({"error": "static/index.html missing"}, 500)
    return FileResponse(p)


@app.get("/healthz")
def healthz():
    return {"ok": con is not None, "db": DB,
            "exists": os.path.exists(DB), "ts": time.time()}


if os.path.isdir(STATIC):
    app.mount("/static", StaticFiles(directory=STATIC), name="static")


if __name__ == "__main__":
    import uvicorn
    # Bound to localhost: this dashboard renders internal contract and fee data.
    uvicorn.run(app, host=os.environ.get("HOST", "127.0.0.1"),
                port=int(os.environ.get("PORT", "8000")))
