"""
Contracts & Fees Insights — API server.

Serves the dashboard (static/index.html) and JSON endpoints backed by the
pre-aggregated rollup tables in insights.duckdb (built by build_db.py).

Satellite "fee" everywhere = sat_fee_applicable (net_qty_sold * per-unit rate,
where the item's MRP meets the bucket threshold) = the actual satellite fee.
See build_db.py for the full definition.

Run:  python app.py          (or: uvicorn app:app --host 127.0.0.1 --port 8000)
Then open http://127.0.0.1:8000
"""
import os
import io
import re
import csv
import glob
import time
import shutil
import zipfile
import fnmatch
import threading
import traceback
import duckdb
from typing import List
from fastapi import FastAPI, Query, UploadFile, File, HTTPException
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

import build_db

HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.environ.get("DB_PATH", os.path.join(HERE, "insights.duckdb"))
# Where uploaded parquet files are staged before a rebuild. Configurable so the
# deployment can point it at a mounted volume; defaults to ./uploads locally.
UPLOAD_DIR = os.environ.get("UPLOAD_DIR", os.path.join(HERE, "uploads"))
# When DB lives on a persistent volume (empty on first boot), seed it once from
# the DB baked into the image so the dashboard ships with data. No-op locally.
SEED_DB = os.environ.get("SEED_DB")
STATIC = os.path.join(HERE, "static")

if SEED_DB and SEED_DB != DB and os.path.exists(SEED_DB) and not os.path.exists(DB):
    os.makedirs(os.path.dirname(os.path.abspath(DB)) or ".", exist_ok=True)
    shutil.copy(SEED_DB, DB)

# Single lock serialising DB access. Reads hold it briefly; the rebuild swap
# holds it only while closing the old connection and opening the new one (the
# heavy aggregation runs against a temp DB file *outside* the lock, so the
# dashboard keeps serving the previous data throughout the build).
_lock = threading.Lock()
con = None


def _open_db():
    """(Re)open the read-only connection if the DB file exists."""
    global con
    con = duckdb.connect(DB, read_only=True) if os.path.exists(DB) else None


_open_db()

app = FastAPI(title="Contracts & Fees Insights")


def q(sql, params=None):
    with _lock:
        if con is None:
            raise HTTPException(status_code=503,
                                detail="No data loaded yet — upload parquet files on the Data tab.")
        cur = con.execute(sql, params or [])
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def one(sql, params=None):
    rows = q(sql, params)
    return rows[0] if rows else {}


# ----------------------------------------------------------------------
@app.get("/api/overview")
def overview():
    k = one("SELECT * FROM kpis")
    for key in ("sat_start", "sat_end"):
        if k.get(key) is not None:
            k[key] = str(k[key])[:10]
    fee_trend = q("SELECT day::VARCHAR AS day, fee, qty FROM sat_daily ORDER BY day")
    pen_trend = q("SELECT day::VARCHAR AS day, penalty, complaints FROM bcpl_daily ORDER BY day")
    return {"kpis": k, "fee_trend": fee_trend, "penalty_trend": pen_trend}


# ----------------------------------------------------------------------
@app.get("/api/satellite")
def satellite(city: str = Query("all"), manufacturer: str = Query("all"), top: int = 15):
    city_f = None if city == "all" else city
    mfr_f = None if manufacturer == "all" else manufacturer

    if city_f:
        daily = q("SELECT day::VARCHAR AS day, fee, qty FROM sat_city_daily WHERE city=? ORDER BY day", [city_f])
    elif mfr_f:
        daily = q("SELECT day::VARCHAR AS day, fee, qty FROM sat_mfr_daily WHERE manufacturer=? ORDER BY day", [mfr_f])
    else:
        daily = q("SELECT day::VARCHAR AS day, fee, qty FROM sat_daily ORDER BY day")

    if mfr_f:
        cities = q(
            "SELECT city, SUM(fee) AS fee, SUM(qty) AS qty FROM sat_city_mfr WHERE manufacturer=? "
            "GROUP BY city ORDER BY fee DESC LIMIT ?", [mfr_f, top])
    else:
        cities = q("SELECT city, fee, qty, net_qty, distance_km, fee_per_unit FROM sat_city ORDER BY fee DESC LIMIT ?", [top])

    if city_f:
        mfrs = q(
            "SELECT manufacturer, SUM(fee) AS fee, SUM(qty) AS qty FROM sat_city_mfr WHERE city=? "
            "GROUP BY manufacturer ORDER BY fee DESC LIMIT ?", [city_f, top])
    else:
        mfrs = q("SELECT manufacturer, fee, qty, fee_per_unit FROM sat_mfr ORDER BY fee DESC LIMIT ?", [top])

    facilities = q("SELECT facility, fee, qty FROM sat_facility ORDER BY fee DESC LIMIT ?", [top])
    split = q("SELECT chain, mrp_tier, fee, qty, rows FROM sat_split ORDER BY fee DESC")
    dist = q("""SELECT band, fee, qty, rows, avg_rate FROM sat_dist
                ORDER BY CASE band
                  WHEN 'Local / non-satellite' THEN 0
                  WHEN '100-150 km' THEN 1 WHEN '150-200 km' THEN 2 WHEN '200-250 km' THEN 3
                  WHEN '250-350 km' THEN 4 WHEN '350-500 km' THEN 5 ELSE 6 END""")
    all_cities = q("SELECT city, distance_km, fee, qty, net_qty, rows FROM sat_cities_list ORDER BY distance_km DESC")

    return {"daily": daily, "cities": cities, "manufacturers": mfrs,
            "facilities": facilities, "split": split, "distance": dist,
            "all_cities": all_cities}


@app.get("/api/satellite/filters")
def satellite_filters():
    cities = [r["city"] for r in q("SELECT city FROM sat_city ORDER BY fee DESC")]
    mfrs = [r["manufacturer"] for r in q("SELECT manufacturer FROM sat_mfr ORDER BY fee DESC LIMIT 200")]
    return {"cities": cities, "manufacturers": mfrs}


# ----------------------------------------------------------------------
@app.get("/api/returns")
def returns(complaint_type: str = Query("all"), top: int = 15):
    ct = None if complaint_type == "all" else complaint_type

    types = q("SELECT complaint_type, penalty, returns_qty, gmv, complaints FROM bcpl_type ORDER BY penalty DESC")
    daily = q("SELECT day::VARCHAR AS day, penalty, returns_qty, complaints FROM bcpl_daily ORDER BY day")

    if ct:
        mfrs = q(
            "SELECT manufacturer, SUM(penalty) AS penalty, SUM(returns_qty) AS returns_qty, SUM(complaints) AS complaints "
            "FROM bcpl_mfr_type WHERE complaint_type=? GROUP BY manufacturer ORDER BY penalty DESC LIMIT ?",
            [ct, top])
        items = q(
            "SELECT item_name, manufacturer, penalty, returns_qty, complaints FROM bcpl_item "
            "ORDER BY penalty DESC LIMIT ?", [top])
    else:
        mfrs = q("SELECT manufacturer, penalty, returns_qty, complaints FROM bcpl_mfr ORDER BY penalty DESC LIMIT ?", [top])
        items = q("SELECT item_name, manufacturer, penalty, returns_qty, complaints FROM bcpl_item ORDER BY penalty DESC LIMIT ?", [top])

    keywords = q("SELECT word, n FROM bcpl_defect_keywords ORDER BY n DESC LIMIT 45")

    # defect summary: top complaint categories + most-cited defect words
    total_cmp = sum(t["complaints"] or 0 for t in types) or 1
    total_qty = sum(t["returns_qty"] or 0 for t in types) or 1
    top_types = [
        {"label": t["complaint_type"].replace("COMPLAINT_", "").replace("_", " ").title(),
         "pct_complaints": round(100.0 * (t["complaints"] or 0) / total_cmp, 1),
         "pct_units": round(100.0 * (t["returns_qty"] or 0) / total_qty, 1)}
        for t in types[:3]
    ]
    summary = {"top_types": top_types, "top_defects": [k["word"] for k in keywords[:8]]}
    return {"types": types, "daily": daily, "manufacturers": mfrs, "items": items,
            "keywords": keywords, "summary": summary}


@app.get("/api/returns/filters")
def returns_filters():
    return {"complaint_types": [r["complaint_type"] for r in q("SELECT complaint_type FROM bcpl_type ORDER BY penalty DESC")]}


# ----------------------------------------------------------------------
@app.get("/api/purchase-returns")
def purchase_returns():
    by_mfr = q("SELECT manufacturer, return_qty, fee, notices, items FROM recall_by_mfr ORDER BY return_qty DESC")
    rows = q("""SELECT strftime(duration,'%Y-%m-%d') AS date, manufacturer, item_name,
                       entity, prn_qty, recall_charges
                FROM recall_assistance ORDER BY duration""")
    tot = one("""SELECT SUM(prn_qty) AS qty, SUM(COALESCE(recall_charges,0)) AS fee,
                        COUNT(*) AS notices, COUNT(DISTINCT manufacturer) AS mfrs,
                        COUNT(DISTINCT item_id) AS items
                 FROM recall_assistance""")
    return {"by_manufacturer": by_mfr, "rows": rows, "totals": tot}


# ----------------------------------------------------------------------
@app.get("/api/ads")
def ads():
    total = one("SELECT COUNT(*) AS n FROM ads_spend")["n"]
    aligned = one("SELECT COUNT(*) AS n FROM ads_spend WHERE lower(trim(min_ads_spend_enabled))='yes'")["n"]
    n_mfr = one("SELECT COUNT(DISTINCT manufacturer) AS n FROM ads_spend WHERE lower(trim(min_ads_spend_enabled))='yes'")["n"]
    # manufacturers ranked by committed minimum ads-spend % (the value aligned)
    by_mfr = q("""
        SELECT manufacturer,
               AVG(TRY_CAST(min_ads_spend_value_percent AS DOUBLE)) AS avg_pct,
               COUNT(*) AS contracts
        FROM ads_spend
        WHERE lower(trim(min_ads_spend_enabled))='yes'
          AND TRY_CAST(min_ads_spend_value_percent AS DOUBLE) IS NOT NULL
        GROUP BY 1 ORDER BY avg_pct DESC, contracts DESC LIMIT 20""")
    basis = q("""
        SELECT COALESCE(NULLIF(trim(min_ads_basis),''),'(unspecified)') AS label, COUNT(*) AS value
        FROM ads_spend WHERE lower(trim(min_ads_spend_enabled))='yes'
        GROUP BY 1 ORDER BY value DESC""")
    # alignment status — everything that isn't an explicit "Yes" is "Not aligned"
    enabled = q("""
        SELECT CASE WHEN lower(trim(min_ads_spend_enabled))='yes' THEN 'Aligned'
                    ELSE 'Not aligned' END AS label, COUNT(*) AS value
        FROM ads_spend GROUP BY 1 ORDER BY value DESC""")
    return {"total": total, "aligned": aligned, "n_mfr": n_mfr,
            "by_manufacturer": by_mfr, "basis": basis, "enabled": enabled}


# ----------------------------------------------------------------------
# Contract term feature-adoption columns (Yes/No style)
FEATURES = [
    ("right_to_set_off", "Right to set-off"),
    ("pod_aligned", "POD aligned"),
    ("damages_lost_provision", "Damages/lost provision"),
    ("recall_assistance_fees", "Recall assistance fees"),
    ("fill_rate_penalty", "Fill-rate penalty"),
    ("po_clubbing", "PO clubbing"),
    ("sp_benchmarking", "SP benchmarking"),
    ("kam_support", "KAM support"),
    ("min_ads_spend_enabled", "Min ads spend"),
    ("advance_payment", "Advance payment"),
    ("early_pay_enable", "Early pay"),
    ("purchase_margin_off_invoice_enable", "Margin off-invoice"),
    ("quality_check_penalty_festive_enable", "QC penalty (festive)"),
    ("complaints_penalty_enable", "Complaints penalty"),
    ("ullage_enable", "Ullage"),
    ("target_based_incentives", "Target incentives"),
    ("defective_product_penalty", "Defective penalty"),
]


@app.get("/api/contracts")
def contracts():
    states = q("SELECT contract_state AS label, COUNT(*) AS value FROM contracts GROUP BY 1 ORDER BY value DESC")
    purchase = q(
        "SELECT COALESCE(type_of_purchase,'(unspecified)') AS label, COUNT(*) AS value "
        "FROM contracts GROUP BY 1 ORDER BY value DESC")

    # Payout frequency — only where a turnover-discount alignment exists.
    payout = q("""
        SELECT COALESCE(NULLIF(trim(payout_frequency),''),'(unspecified)') AS label, COUNT(*) AS value
        FROM turnover_discount
        WHERE lower(trim(target_based_incentives))='yes'
        GROUP BY 1 ORDER BY value DESC""")
    payout_mfrs = one("""SELECT COUNT(DISTINCT manufacturer) AS n FROM turnover_discount
                         WHERE lower(trim(target_based_incentives))='yes'""")["n"]

    total = one("SELECT COUNT(*) AS n FROM contracts")["n"]
    adoption = []
    for col, label in FEATURES:
        n = one(f"SELECT COUNT(*) AS n FROM contracts WHERE lower(trim({col}))='yes'")["n"]
        adoption.append({"feature": label, "count": n, "pct": round(100.0 * n / total, 1) if total else 0})
    adoption.sort(key=lambda x: x["count"], reverse=True)

    timeline = q("""
        SELECT strftime(execution_date, '%Y-%m') AS month, COUNT(*) AS value
        FROM contracts WHERE execution_date IS NOT NULL
        GROUP BY 1 ORDER BY 1""")

    top_mfr = q("""
        SELECT manufacturer, COUNT(*) AS contracts
        FROM contracts GROUP BY 1 ORDER BY contracts DESC LIMIT 15""")

    # SP benchmarking alignment — full list of aligned manufacturers (searchable)
    sp_by_mfr = q("""
        SELECT manufacturer,
               COUNT(*) AS contracts,
               AVG(TRY_CAST(sp_benchmark_aligned_rm_percent AS DOUBLE)) AS avg_pct
        FROM sp_benchmarking
        WHERE lower(trim(sp_benchmarking))='yes'
        GROUP BY 1 ORDER BY contracts DESC, manufacturer""")
    sp_total = one("SELECT COUNT(*) AS n FROM sp_benchmarking")["n"]
    sp_on = one("SELECT COUNT(*) AS n FROM sp_benchmarking WHERE lower(trim(sp_benchmarking))='yes'")["n"]
    sp_mfrs = one("SELECT COUNT(DISTINCT manufacturer) AS n FROM sp_benchmarking WHERE lower(trim(sp_benchmarking))='yes'")["n"]

    # Turnover-discount alignment by manufacturer (with payout cadence)
    turnover_by_mfr = q("""
        SELECT manufacturer,
               COUNT(*) AS contracts,
               MAX(COALESCE(NULLIF(trim(payout_frequency),''),'(unspecified)')) AS payout
        FROM turnover_discount
        WHERE lower(trim(target_based_incentives))='yes'
        GROUP BY 1 ORDER BY contracts DESC, manufacturer""")
    turnover_on = one("SELECT COUNT(*) AS n FROM turnover_discount WHERE lower(trim(target_based_incentives))='yes'")["n"]
    turnover_total = one("SELECT COUNT(*) AS n FROM turnover_discount")["n"]

    ads_on = one("SELECT COUNT(*) AS n FROM ads_spend WHERE lower(trim(min_ads_spend_enabled))='yes'")["n"]
    ads_total = one("SELECT COUNT(*) AS n FROM ads_spend")["n"]

    return {
        "total": total,
        "states": states,
        "purchase": purchase,
        "payout": payout,
        "payout_mfrs": payout_mfrs,
        "adoption": adoption,
        "timeline": timeline,
        "top_manufacturers": top_mfr,
        "sp_by_manufacturer": sp_by_mfr,
        "turnover_by_manufacturer": turnover_by_mfr,
        "terms": {
            "ads": {"on": ads_on, "total": ads_total},
            "turnover": {"on": turnover_on, "total": turnover_total},
            "spbench": {"on": sp_on, "total": sp_total, "mfrs": sp_mfrs},
        },
    }


# ----------------------------------------------------------------------
# Raw-data downloads. Only aggregated rollups ship in the container (the
# 39.6M/763K raw fact rows are not), so "raw data" here is the finest
# date-keyed rollup that backs each chart. Table/date-column names come from
# this fixed registry (never user input); only the date bounds are user-supplied.
# ----------------------------------------------------------------------
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# id -> (label, table, date_col or None, [pages])
EXPORTS = {
    "satellite_fee_by_day":          ("Satellite fee by day",                       "sat_daily",         "day",            ["overview", "satellite"]),
    "satellite_fee_split_by_day":    ("Satellite fee by chain, MRP tier & day",     "sat_split_daily",   "day",            ["overview", "satellite"]),
    "satellite_fee_by_state_day":    ("Satellite fee by contract state & day",      "sat_state_daily",   "day",            ["satellite"]),
    "satellite_fee_by_city":         ("Satellite fee by city (with distance)",      "sat_city",          None,             ["satellite"]),
    "satellite_cities_list":         ("Satellite city list (distance & fee)",       "sat_cities_list",   None,             ["satellite"]),
    "satellite_fee_by_city_day":     ("Satellite fee by city & day",                "sat_city_daily",    "day",            ["satellite"]),
    "satellite_fee_by_mfr":          ("Satellite fee by manufacturer",              "sat_mfr",           None,             ["satellite"]),
    "satellite_fee_by_mfr_day":      ("Satellite fee by manufacturer & day",        "sat_mfr_daily",     "day",            ["satellite"]),
    "satellite_fee_by_facility":     ("Satellite fee by facility",                  "sat_facility",      None,             ["satellite"]),
    "satellite_fee_by_distance_day": ("Satellite fee by distance band & day",       "sat_dist_daily",    "day",            ["satellite"]),
    "returns_penalty_by_day":        ("Defective return penalty by day",            "bcpl_daily",        "day",            ["overview", "returns"]),
    "returns_penalty_by_type_day":   ("Defective return by complaint type & day",   "bcpl_type_daily",   "day",            ["overview", "returns"]),
    "returns_penalty_by_mfr_day":    ("Defective return by manufacturer & day",     "bcpl_mfr_daily",    "day",            ["returns"]),
    "returns_penalty_by_item_day":   ("Defective return by item & day",             "bcpl_item_daily",   "day",            ["returns"]),
    "returns_comment_keywords":      ("Defective comment keyword frequencies",      "bcpl_keywords",     None,             ["returns"]),
    "purchase_returns_recall_log":   ("Recall-assistance PRN log",                  "recall_assistance", "duration",       ["purchase_returns"]),
    "purchase_returns_by_mfr":       ("Recall qty & fee by manufacturer",           "recall_by_mfr",     None,             ["purchase_returns"]),
    "ads_spend_extract":             ("Ads-spend term extract",                     "ads_spend",         None,             ["ads"]),
    "contracts_master":              ("Contracts master",                           "contracts",         "execution_date", ["contracts"]),
    "contracts_turnover_discount":   ("Turnover-discount term extract",             "turnover_discount", None,             ["contracts"]),
    "contracts_sp_benchmarking":     ("SP-benchmarking term extract",               "sp_benchmarking",   None,             ["contracts"]),
}


def _valid_date(s):
    return bool(s) and bool(DATE_RE.match(s))


def _export_cursor(table, date_col, start, end):
    if date_col and _valid_date(start) and _valid_date(end):
        sql = (f"SELECT * FROM {table} "
               f"WHERE {date_col} IS NOT NULL AND CAST({date_col} AS DATE) BETWEEN ? AND ? "
               f"ORDER BY CAST({date_col} AS DATE)")
        return con.execute(sql, [start, end])
    order = f" ORDER BY CAST({date_col} AS DATE)" if date_col else ""
    return con.execute(f"SELECT * FROM {table}{order}")


def _csv_bytes(table, date_col, start, end):
    cur = _export_cursor(table, date_col, start, end)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([c[0] for c in cur.description])
    w.writerows(cur.fetchall())
    return buf.getvalue().encode("utf-8")


def _bounds(table, col):
    r = one(f"SELECT MIN(CAST({col} AS DATE)) AS a, MAX(CAST({col} AS DATE)) AS b "
            f"FROM {table} WHERE {col} IS NOT NULL")
    if r.get("a") is None:
        return {"start": "", "end": ""}
    return {"start": str(r["a"]), "end": str(r["b"])}


@app.get("/api/export/manifest")
def export_manifest():
    datasets = [
        {"id": k, "label": v[0], "pages": v[3], "date_filtered": v[2] is not None}
        for k, v in EXPORTS.items()
    ]
    return {
        "datasets": datasets,
        "pages": {
            "overview": _bounds("sat_daily", "day"),
            "satellite": _bounds("sat_daily", "day"),
            "returns": _bounds("bcpl_daily", "day"),
            "purchase_returns": _bounds("recall_assistance", "duration"),
            "ads": {"start": "", "end": ""},
            "contracts": _bounds("contracts", "execution_date"),
        },
    }


@app.get("/api/export")
def export(dataset: str, start: str = Query(None), end: str = Query(None)):
    ds = EXPORTS.get(dataset)
    if not ds:
        return JSONResponse({"error": "unknown dataset"}, status_code=404)
    if (start or end) and not (_valid_date(start) and _valid_date(end)):
        return JSONResponse({"error": "start and end must be YYYY-MM-DD"}, status_code=400)
    _label, table, date_col, _pages = ds
    body = _csv_bytes(table, date_col, start, end)
    span = f"_{start}_to_{end}" if (date_col and _valid_date(start) and _valid_date(end)) else ""
    fname = f"{dataset}{span}.csv"
    return Response(body, media_type="text/csv",
                    headers={"Content-Disposition": f'attachment; filename="{fname}"'})


@app.get("/api/export/page/{page}")
def export_page(page: str, start: str = Query(None), end: str = Query(None)):
    items = [(k, v) for k, v in EXPORTS.items() if page in v[3]]
    if not items:
        return JSONResponse({"error": "unknown page"}, status_code=404)
    if (start or end) and not (_valid_date(start) and _valid_date(end)):
        return JSONResponse({"error": "start and end must be YYYY-MM-DD"}, status_code=400)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for k, (_label, table, date_col, _pages) in items:
            z.writestr(f"{k}.csv", _csv_bytes(table, date_col, start, end))
    span = f"_{start}_to_{end}" if (_valid_date(start) and _valid_date(end)) else ""
    fname = f"{page}_raw_data{span}.zip"
    return Response(buf.getvalue(), media_type="application/zip",
                    headers={"Content-Disposition": f'attachment; filename="{fname}"'})


# ----------------------------------------------------------------------
# Data management — upload raw parquet & rebuild the analytics DB in-app.
#
# Files are classified by filename against build_db.DATASETS (the same registry
# the builder reads), staged in UPLOAD_DIR, then build_db.build() regenerates
# the rollups. The build writes to a temp file and is hot-swapped in, so the
# dashboard keeps serving the current data until the new DB is ready.
# ----------------------------------------------------------------------
_rebuild = {"state": "idle", "log": [], "error": None, "started": None, "finished": None}


def _classify(filename):
    """Dataset key whose glob matches this filename, or None."""
    base = os.path.basename(filename)
    for key, (_label, pat, _multi) in build_db.DATASETS.items():
        if fnmatch.fnmatch(base, pat):
            return key
    return None


def _inventory():
    """Per-dataset view of what is currently staged in UPLOAD_DIR."""
    out = []
    for key, (label, pat, multi) in build_db.DATASETS.items():
        files = build_db.resolve(UPLOAD_DIR, key) if os.path.isdir(UPLOAD_DIR) else []
        # For single-part datasets resolve() returns at most one; show all
        # matches so a user can see (and reset) accidental extras.
        all_matches = sorted(glob.glob(os.path.join(UPLOAD_DIR, pat))) if os.path.isdir(UPLOAD_DIR) else []
        out.append({
            "key": key, "label": label, "pattern": pat, "multi": multi,
            "files": [{"name": os.path.basename(f), "bytes": os.path.getsize(f)} for f in all_matches],
            "ready": bool(files),
        })
    return out


@app.get("/api/data/status")
def data_status():
    inv = _inventory()
    return {
        "datasets": inv,
        "all_ready": all(d["ready"] for d in inv),
        "db_present": con is not None,
        "rebuild": _rebuild,
        "upload_dir": UPLOAD_DIR,
    }


@app.post("/api/data/upload")
async def data_upload(files: List[UploadFile] = File(...)):
    if _rebuild["state"] == "running":
        raise HTTPException(status_code=409, detail="A rebuild is in progress; wait for it to finish.")
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    results = []
    for up in files:
        name = os.path.basename(up.filename or "")
        if not name.lower().endswith(".parquet"):
            results.append({"file": up.filename, "ok": False, "error": "not a .parquet file"})
            await up.close()
            continue
        key = _classify(name)
        if key is None:
            results.append({"file": name, "ok": False,
                            "error": "filename does not match any known dataset"})
            await up.close()
            continue
        label, pat, multi = build_db.DATASETS[key]
        # Single-part datasets keep exactly one file: clear prior matches first.
        if not multi:
            for old in glob.glob(os.path.join(UPLOAD_DIR, pat)):
                os.remove(old)
        dest = os.path.join(UPLOAD_DIR, name)
        try:
            with open(dest, "wb") as out:
                shutil.copyfileobj(up.file, out, length=1024 * 1024)
        finally:
            await up.close()
        results.append({"file": name, "ok": True, "dataset": key,
                        "label": label, "bytes": os.path.getsize(dest)})
    return {"results": results, "datasets": _inventory()}


@app.post("/api/data/reset")
def data_reset():
    if _rebuild["state"] == "running":
        raise HTTPException(status_code=409, detail="A rebuild is in progress; wait for it to finish.")
    if os.path.isdir(UPLOAD_DIR):
        for f in glob.glob(os.path.join(UPLOAD_DIR, "*.parquet")):
            os.remove(f)
    return {"ok": True, "datasets": _inventory()}


def _run_rebuild():
    global con
    tmp = DB + ".building"
    _rebuild.update(state="running", error=None,
                    started=time.strftime("%Y-%m-%d %H:%M:%S"), finished=None, log=[])

    def log(m):
        _rebuild["log"].append(str(m))

    try:
        build_db.build(UPLOAD_DIR, tmp, log=log)
        # Hot-swap: brief lock only for close → atomic replace → reopen.
        with _lock:
            if con is not None:
                con.close()
                con = None
            os.replace(tmp, DB)
            con = duckdb.connect(DB, read_only=True)
        _rebuild.update(state="done", finished=time.strftime("%Y-%m-%d %H:%M:%S"))
        log("Dashboard now serving the rebuilt data.")
    except Exception as e:
        # Old connection is untouched on failure (swap never happened), so the
        # dashboard keeps serving whatever it had. Surface the error to the UI.
        _rebuild.update(state="error", error=str(e),
                        finished=time.strftime("%Y-%m-%d %H:%M:%S"))
        log("ERROR: " + str(e))
        log(traceback.format_exc())
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
        # Re-open the original DB if we had closed it before a swap failure.
        if con is None and os.path.exists(DB):
            with _lock:
                con = duckdb.connect(DB, read_only=True)


@app.post("/api/data/rebuild")
def data_rebuild():
    if _rebuild["state"] == "running":
        raise HTTPException(status_code=409, detail="A rebuild is already in progress.")
    miss = build_db.missing_datasets(UPLOAD_DIR)
    if miss:
        names = ", ".join(lbl for _k, lbl in miss)
        raise HTTPException(status_code=400, detail=f"Missing required dataset(s): {names}")
    t = threading.Thread(target=_run_rebuild, daemon=True)
    t.start()
    return {"state": "started"}


# ----------------------------------------------------------------------
@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC, "index.html"))


@app.get("/health")
def health():
    return {"status": "ok"}


app.mount("/static", StaticFiles(directory=STATIC), name="static")


if __name__ == "__main__":
    import uvicorn
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host=host, port=port)
