"""
Satellite / Defects / KAM fee insights -- pre-aggregation build.

Reads the raw Parquet extracts from ../data and writes a small set of rollup
tables into fees.duckdb, which app.py then serves read-only. Everything the
dashboard shows comes from these tables; the 1.1 GB of satellite fee Parquet is
read exactly once, here.

Five focus areas, in the order the UI presents them:

  1. Satellite cities   -- the master list (city + break-even min distance) that
                           defines what "satellite" means, searchable.
  2. Satellite fees     -- sat_fee_applicable rolled up so the UI can drill
                           city -> manufacturer -> item, and across dates.
  3. Defective products -- BCPL bad-return complaints, Rs 50 per returned unit,
                           plus a keyword cloud mined from the free-text remarks.
  4. KAM fees           -- contracted KAM support charges accrued per month.
  5. Recall assistance  -- BCPL RTV claim actuals: recall fee and returned
                           (dumped) quantity per month x manufacturer x item.

Fee definitions (do not "simplify" these -- they are the business definitions):

  satellite fee = sat_fee_applicable = fee_amt (per-unit rate) * net_qty_sold.
      Verified row-exact on the extract: every row satisfies
      abs(sat_fee_applicable - fee_amt * net_qty_sold) < 0.01.
      fee_amt alone is a RATE, not money -- never sum it.

  defect fee    = DEFECT_RATE (Rs 50) * bad_returns_qty.
      The extract also carries a `penalty` column; it is Rs 50/unit on every
      row today. We recompute from qty so the rate is explicit and auditable,
      and emit a build note if the two ever diverge.

  KAM fee       = kam_charges accrued once per calendar month from the month of
      effective_date onward, for contracts with kam_support = 'Yes'. Every KAM
      contract in the extract is valid_until_cancelled = 'Yes', so there is no
      end date to stop the accrual -- it runs to ACCRUAL_END.

  recall fee    = recall_assistance_fee, taken AS REPORTED from the corrected
      recall_assistance_fee extract, which supersedes the earlier
      BCPL_RTV_Claim CSV drop. The extract carries both the money and the rate
      behind it, and they reconcile row-exact:
          recall_assistance_fee = chargeable_prn_qty * recall_charge_rs
      (verified: 0 of 9,068 rated rows deviate by >= 0.01). So this fee is
      reported AND checkable, and the build asserts the identity rather than
      recomputing the money from the rate.
      prn_qty is everything that came back; chargeable_prn_qty is the slice with
      a contract rate behind it. A line with no contract_id carries no rate and
      therefore no KNOWABLE fee -- it is not a zero-rupee line, and roughly half
      the lines are in that state (see build notes).
      Why this replaced the CSV: the claim CSV put a recall_fee on returns from
      5,407 manufacturers with no check that a recall-assistance clause existed,
      totalling Rs 5.66M. Joined to contracts, only 260 manufacturers are
      actually chargeable and the fee is Rs 1.21M.

Satellite-city validation (the reason city_stats has an is_satellite flag):
the fee extract is NOT pre-filtered to satellite cities. It contains metros with
their own mother warehouses -- Mumbai, Kolkata, Hyderabad, Pune, Chennai -- which
by definition cannot be satellite. Every rollup therefore carries is_satellite so
the UI can scope to validated satellite cities (its default) and still quantify
what is being charged outside the list.

Run:  python build_db.py [--data ../data] [--out fees.duckdb]
"""
import os
import re
import sys
import glob
import time
import shutil
import argparse
import traceback
import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA = os.environ.get("DATA_DIR", os.path.join(HERE, "..", "data"))
DEFAULT_OUT = os.environ.get("DB_PATH", os.path.join(HERE, "fees.duckdb"))

# Rs per defective unit returned. The single knob for area 3.
DEFECT_RATE = 50.0

# KAM accrual runs to the end of this month (inclusive). Contracts are all
# valid_until_cancelled, so something has to bound the spine; we bound it at the
# latest contract activity in the extract rather than at wall-clock "today", so a
# rebuild months later does not silently invent accrual months with no data
# behind them. Overridable for a what-if.
ACCRUAL_END = os.environ.get("KAM_ACCRUAL_END")  # 'YYYY-MM' or None -> derive

# (label, glob, is_multipart)
#
# `recall` is multipart so the extract can grow a `_partNNN` file at a time
# without a code change. The month a row belongs to is always read from its own
# `duration` column, never from a filename -- the superseded claim CSV was named
# for June and already carried 2026-07 rows, and nothing guarantees a future part
# is bounded by its own name either.
DATASETS = {
    "sat_fees":  ("Satellite fee actuals",        "satellite_fees_actuals_part*.parquet", True),
    "cities":    ("Satellite city master list",   "satellite_cities*.parquet",            False),
    "defects":   ("Defective returns (BCPL)",     "bcpl_defective_crbs*.parquet",         True),
    "kam":       ("KAM support fee extract",      "kam_fees_extract*.parquet",            False),
    "contracts": ("Contracts master",             "contracts_extract*.parquet",           False),
    "recall":    ("Recall assistance (PRN)",      "recall_assistance_fee*.parquet",       True),
}

# Without these two there is no dashboard: the city list defines satellite, and
# the fee actuals are the money. Everything else degrades to an empty tab plus a
# build note.
REQUIRED = ("sat_fees", "cities")

_notes = []


def note(level, label, text):
    """Record a build-time caveat that the UI surfaces on the Data tab."""
    _notes.append((level, label, text))
    print(f"  [{level}] {label}: {text}")


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# --------------------------------------------------------------------------
# file resolution
# --------------------------------------------------------------------------

def duckdb_memory():
    """DuckDB memory_limit for a build: ~45% of AVAILABLE RAM, capped at 3 GB.

    A fixed 3 GB got this build OOM-killed (SIGKILL, no traceback) three times on
    an 8 GB box that was already half used: DuckDB happily spills the 42M-row
    satellite aggregation to disk, but only if its limit is under what the OS can
    actually spare. Lower means slower, not broken. Override with DUCKDB_MEMORY.
    """
    env = os.environ.get("DUCKDB_MEMORY")
    if env:
        return env
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    avail = int(line.split()[1]) // 1024          # MB
                    return f"{max(768, min(3072, int(avail * 0.45)))}MB"
    except Exception:
        pass
    return "3GB"


def resolve(data_dir, key):
    """Concrete parquet paths for a dataset. [] when nothing matches.

    Multi-part datasets return every match; single-part datasets return exactly
    one, so a stray duplicate download can never double-read (which, on the city
    join, would multiply every fee row).

    Matches the data dir AND one level of subdirectory, because monthly drops
    arrive as a folder per period (`data/Sat Fee July 2026/`) whose part files
    restart at `_part001` -- so the same basename legitimately exists once per
    month and deduping on filename would drop a real month. Overlap is caught
    where it is actually detectable: build_sat notes any month whose rows come
    from more than one source folder.
    """
    _label, pat, multi = DATASETS[key]
    hits = sorted(glob.glob(os.path.join(data_dir, pat))
                  + glob.glob(os.path.join(data_dir, "*", pat)))
    if not hits:
        return []
    if multi:
        return hits
    if key == "cities":
        # `satellite_cities_recovered.parquet` is the stand-in rebuilt from a
        # previous build's rollup (city + distance only). It is the base list we
        # were asked to validate against, but if the real upstream extract ever
        # lands beside it, that wins.
        real = [h for h in hits if "recovered" not in os.path.basename(h).lower()]
        return (real or hits)[:1]
    return hits[:1]


def readable(paths):
    """Split paths into (ok, bad) by whether the file actually opens.

    A part still being written, or a truncated download, has no magic bytes at
    the end and would abort the whole read. Reading just the footer is a seek,
    so this is a cheap pre-flight that lets one bad part degrade to a note
    instead of failing the build. CSV inputs are probed through the CSV reader
    (LIMIT 0 still forces the sniffer to parse the header), so a mangled claim
    file is caught here rather than mid-build.
    """
    ok, bad = [], []
    probe = duckdb.connect()
    for p in paths:
        try:
            fn = "read_csv_auto" if p.lower().endswith(".csv") else "read_parquet"
            probe.execute(f"SELECT 1 FROM {fn}(?) LIMIT 0", [p])
            ok.append(p)
        except Exception as e:
            bad.append((os.path.basename(p), str(e).split("\n")[0][:160]))
    probe.close()
    return ok, bad


def sql_list(paths):
    """Render paths as a DuckDB list literal, single-quotes escaped."""
    return "[" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"


def read_csv(paths, **opts):
    """read_csv over an explicit path list, with the source filename kept.

    `filename=true` is what lets the claim dedupe pick a deterministic winner
    when two monthly files overlap on the same month (see build_recall).
    """
    extra = "".join(f", {k}={v}" for k, v in opts.items())
    return f"read_csv({sql_list(paths)}, filename=true, union_by_name=true{extra})"


# --------------------------------------------------------------------------
# shared SQL fragments
# --------------------------------------------------------------------------

# City-name normaliser used on BOTH sides of the master-list join. Cities arrive
# with inconsistent casing ('BIJNOR' vs 'Bijnor') and stray double spaces, and
# some carry a disambiguating suffix that must be preserved ('Aurangabad
# (Bihar)' is a different city from 'Aurangabad (Maharashtra)') -- so we upper
# and collapse whitespace, and deliberately do NOT strip parentheticals.
def norm(col):
    return f"upper(trim(regexp_replace({col}, '\\s+', ' ', 'g')))"


# Distance bands for the city list. Ordered, so the UI renders them on an
# ordinal ramp rather than categorical hues.
BAND_CASE = """CASE
        WHEN distance_km IS NULL      THEN 'Unknown'
        WHEN distance_km <  150       THEN '100-150 km'
        WHEN distance_km <  200       THEN '150-200 km'
        WHEN distance_km <  250       THEN '200-250 km'
        WHEN distance_km <  300       THEN '250-300 km'
        ELSE '300+ km'
    END"""

BAND_ORDER = """CASE
        WHEN distance_km IS NULL THEN 9
        WHEN distance_km <  150  THEN 1
        WHEN distance_km <  200  THEN 2
        WHEN distance_km <  250  THEN 3
        WHEN distance_km <  300  THEN 4
        ELSE 5
    END"""


# --------------------------------------------------------------------------
# 1 + 2. satellite cities and fees
# --------------------------------------------------------------------------

def build_cities(con, city_paths):
    """city_master: the base list of satellite cities + break-even distance."""
    log("building city_master")
    con.execute(f"""
        CREATE OR REPLACE TABLE city_master AS
        SELECT
            trim(City)                             AS city,
            {norm('City')}                         AS city_key,
            CAST("BE Min. Distance (km)" AS DOUBLE) AS distance_km
        FROM read_parquet({sql_list(city_paths)})
        WHERE City IS NOT NULL AND trim(City) <> ''
    """)
    # A duplicate city in the master list would fan out every fee row it joins,
    # so collapse to one row per normalised key (keeping the tightest distance)
    # and say so if it happened.
    dupes = con.execute("""
        SELECT count(*) FROM (
            SELECT city_key FROM city_master GROUP BY 1 HAVING count(*) > 1)
    """).fetchone()[0]
    if dupes:
        note("warn", "Satellite city master list",
             f"{dupes} city name(s) appear more than once after normalising case "
             f"and spacing; collapsed to one row each (keeping the smallest "
             f"break-even distance) so the fee join cannot fan out.")
        con.execute("""
            CREATE OR REPLACE TABLE city_master AS
            SELECT any_value(city) AS city, city_key, min(distance_km) AS distance_km
            FROM city_master GROUP BY city_key
        """)
    con.execute(f"""
        CREATE OR REPLACE TABLE city_master AS
        SELECT city, city_key, distance_km,
               {BAND_CASE} AS band, {BAND_ORDER} AS band_ord
        FROM city_master
    """)
    n = con.execute("SELECT count(*) FROM city_master").fetchone()[0]
    log(f"  city_master: {n} cities")
    return n


def build_sat(con, fee_paths):
    """The satellite fee cube plus every rollup the Fees tab reads.

    Grain of sat_cube is (city, manufacturer, item) -- 2.1M rows on the current
    extract, which is small enough that the UI can aggregate any of
    city / manufacturer / item / city->manufacturer / manufacturer->item /
    city->manufacturer->item off this one table in tens of milliseconds.
    Date is dropped here and carried by the sat_*_daily tables instead: nobody
    needs item-level-by-day, and keeping it would multiply the cube by 30.
    """
    files = sql_list(fee_paths)
    log(f"reading satellite fee actuals ({len(fee_paths)} part(s))")

    # Single normalising pass. Everything below reads `raw`, so the column
    # renames, the city join, and the is_satellite flag are defined once.
    #   manufacturer_id_x / manufacturer_name_x carry a pandas merge suffix
    #   upstream; drop it here so the rest of the build reads cleanly.
    con.execute(f"""
        CREATE OR REPLACE VIEW raw AS
        SELECT
            CAST(f.insert_ds_ist AS DATE)   AS day,
            f.manufacturer_id_x             AS mfr_id,
            f.manufacturer_name_x           AS mfr,
            f.outlet_id, f.outlet_name,
            trim(f.outlet_city)             AS city_raw,
            {norm('f.outlet_city')}         AS city_key,
            f.facility_id, f.facility_name,
            f.variant_id, f.item_name,
            f.variant_mrp, f.mrp_threshold,
            f.contract_state, f.product_type, f.bucket,
            f.fee_amt, f.qty_sold, f.qty_returned, f.net_qty_sold,
            f.sat_fee_applicable            AS fee,
            (m.city_key IS NOT NULL)        AS is_satellite,
            m.city                          AS master_city,
            m.distance_km,
            f.filename                      AS src_file
        FROM read_parquet({files}, filename=true) f
        LEFT JOIN city_master m ON m.city_key = {norm('f.outlet_city')}
    """)

    # Guard the fee definition rather than trusting it: fee must equal
    # rate * net qty. If the extract ever changes shape, the dashboard says so
    # instead of quietly reporting a wrong total.
    log("  verifying fee = fee_amt * net_qty_sold")
    bad, total = con.execute("""
        SELECT count(*) FILTER (
                 WHERE abs(COALESCE(fee,0) - COALESCE(fee_amt,0)*COALESCE(net_qty_sold,0)) > 0.01),
               count(*)
        FROM raw
    """).fetchone()
    if bad:
        note("error", "Satellite fee actuals",
             f"{bad:,} of {total:,} rows have sat_fee_applicable != fee_amt * "
             f"net_qty_sold. The dashboard reports sat_fee_applicable as-is; the "
             f"per-unit rate shown for those rows will not reconcile.")
    log(f"  {total:,} fee rows, definition holds on {total - bad:,}")

    # sat_cube, in two cheap passes instead of one that cannot fit in memory.
    #
    # The single-statement version (2.1M groups, each carrying any_value() of three
    # string columns) was OOM-killed by the OS at 3 GB and 2.4 GB, and raised
    # DuckDB's own OutOfMemory at 1.2 GB even single-threaded with insertion order
    # off. The reason is the string aggregate states: DuckDB spills numeric hash
    # aggregates to disk happily, but not those.
    #
    # So: aggregate the money with NO strings in it, and pick up every label from a
    # small dimension table afterwards. Each label is functionally dependent on the
    # key it hangs off, which is also why this is more correct than what it replaces
    # -- the old cube took max(contract_state) per (city, manufacturer, item), where
    # contract state is a property of the manufacturer's contract, full stop.
    log("  sat_cube (month x city x manufacturer x item)")
    months = [r[0] for r in con.execute(
        "SELECT DISTINCT date_trunc('month', day) FROM raw ORDER BY 1").fetchall()]
    log(f"    {len(months)} month(s) in the extract: "
        + ", ".join(str(m)[:7] for m in months))

    # Monthly drops live one folder per period, and each folder restarts its part
    # numbering -- so the one failure mode the filename cannot rule out is the same
    # month being present in two folders (a re-pull dropped beside the original).
    # A month is expected to come from exactly one source folder; if it does not,
    # every figure for that month is doubled, and that has to be said out loud
    # rather than inferred from a suspiciously large total.
    log("    checking each month comes from one source folder")
    con.execute("""
        CREATE OR REPLACE TABLE sat_sources AS
        SELECT strftime(date_trunc('month', day), '%Y-%m')            AS month,
               regexp_extract(src_file, '([^/]*)/[^/]*$', 1)          AS src_dir,
               count(*)                                              AS n_rows,
               round(sum(fee), 2)                                     AS fee
        FROM raw
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)
    dupes = con.execute("""
        SELECT month, count(*) AS dirs, string_agg(src_dir, ', ' ORDER BY src_dir)
        FROM sat_sources GROUP BY month HAVING count(*) > 1 ORDER BY month
    """).fetchall()
    for month, ndirs, dirs in dupes:
        note("error", "Satellite fee actuals",
             f"{month} appears in {ndirs} source folders ({dirs}). Rows from all "
             f"of them are counted, so every {month} figure on this dashboard is "
             f"inflated. Keep one folder per month and rebuild.")
    for m, d, n, f in con.execute("SELECT * FROM sat_sources").fetchall():
        log(f"      {m}  {d or '.'}  {n:,} rows  Rs {f:,.0f}")

    # --- dimensions. Small cardinality (257 cities, 687 manufacturers, 33k items),
    # so one string aggregate each is nothing.
    log("    dimensions (city / manufacturer / item)")
    con.execute("""
        CREATE OR REPLACE TABLE _dim_city AS
        SELECT city_key, any_value(city_raw) AS city,
               max(is_satellite) AS is_satellite, max(distance_km) AS distance_km
        FROM raw GROUP BY city_key
    """)
    con.execute("""
        CREATE OR REPLACE TABLE _dim_mfr AS
        SELECT mfr_id, any_value(mfr) AS mfr,
               max(contract_state) AS contract_state
        FROM raw GROUP BY mfr_id
    """)
    con.execute("""
        CREATE OR REPLACE TABLE _dim_item AS
        SELECT variant_id, any_value(item_name) AS item_name,
               max(variant_mrp) AS variant_mrp, max(mrp_threshold) AS mrp_threshold,
               max(bucket) AS bucket, max(product_type) AS product_type
        FROM raw GROUP BY variant_id
    """)

    # --- the money, keyed on ids only: purely numeric aggregates, which spill.
    # month_date is a constant inside each chunk rather than a fourth group key, so
    # the GROUP BY is the three keys this cube always used.
    NUM_SELECT = """
        SELECT CAST(? AS DATE)    AS month_date,
               city_key, mfr_id, variant_id,
               round(sum(fee), 2) AS fee,
               sum(net_qty_sold)  AS net_qty,
               sum(qty_sold)      AS qty_sold,
               sum(qty_returned)  AS qty_returned,
               count(*)           AS n_rows
        FROM raw
        WHERE date_trunc('month', day) = ?
        GROUP BY city_key, mfr_id, variant_id
    """
    con.execute("SET preserve_insertion_order = false")
    try:
        for i, m in enumerate(months):
            t0 = time.time()
            stmt = ("CREATE OR REPLACE TABLE _sat_num AS " if i == 0
                    else "INSERT INTO _sat_num ")
            con.execute(stmt + NUM_SELECT, [m, m])
            log(f"    {str(m)[:7]}: "
                f"{con.execute('SELECT count(*) FROM _sat_num').fetchone()[0]:,} rows "
                f"so far ({time.time() - t0:.0f}s)")

        # --- glue. 2.1M rows joined to three tiny tables; column order and names
        # are exactly what every consumer already selects.
        con.execute("""
            CREATE OR REPLACE TABLE sat_cube AS
            SELECT n.month_date, n.city_key, c.city,
                   c.is_satellite, c.distance_km,
                   n.mfr_id, m.mfr,
                   n.variant_id, i.item_name, i.variant_mrp, i.mrp_threshold,
                   i.bucket, i.product_type, m.contract_state,
                   n.fee, n.net_qty, n.qty_sold, n.qty_returned, n.n_rows
            FROM _sat_num n
            LEFT JOIN _dim_city c USING (city_key)
            LEFT JOIN _dim_mfr  m USING (mfr_id)
            LEFT JOIN _dim_item i USING (variant_id)
        """)
        con.execute("DROP TABLE IF EXISTS _sat_num")
    finally:
        con.execute("SET preserve_insertion_order = true")

    log("  sat_daily, sat_day_city_mfr")
    con.execute("""
        CREATE OR REPLACE TABLE sat_daily AS
        SELECT day, is_satellite,
               round(sum(fee), 2) AS fee, sum(net_qty_sold) AS net_qty,
               count(*) AS n_rows,
               count(DISTINCT city_key) AS cities,
               count(DISTINCT mfr_id)   AS mfrs
        FROM raw GROUP BY day, is_satellite ORDER BY day
    """)
    con.execute("""
        CREATE OR REPLACE TABLE sat_day_city_mfr AS
        SELECT day, city_key, mfr_id, is_satellite,
               round(sum(fee), 2) AS fee, sum(net_qty_sold) AS net_qty
        FROM raw GROUP BY day, city_key, mfr_id, is_satellite
    """)
    log(f"    sat_day_city_mfr: "
        f"{con.execute('SELECT count(*) FROM sat_day_city_mfr').fetchone()[0]:,} rows")

    # --- per-month completeness. With more than one month loaded, the headline
    # invites a month-on-month read, and that read is wrong if a month is short.
    # Two ways a month comes up short, both worth saying out loud:
    #   missing days  -- the extract stops before the calendar month does;
    #   a partial tail -- the last day is present but only fractionally loaded,
    #                     which draws as a cliff on the daily chart and looks
    #                     like a collapse in demand rather than a truncated pull.
    # Stated as facts and a size, so a reader can decide whether the comparison
    # is safe; nothing is scaled up or extrapolated to hide it.
    log("  checking each month's day coverage")
    con.execute("""
        CREATE OR REPLACE TABLE sat_coverage AS
        WITH d AS (
            SELECT day, sum(fee) AS fee FROM sat_daily GROUP BY day
        ), m AS (
            SELECT strftime(date_trunc('month', day), '%Y-%m')      AS month,
                   count(*)                                        AS days,
                   extract('day' FROM last_day(max(day)))          AS cal_days,
                   max(day)                                        AS last_day,
                   median(fee)                                     AS med_fee
            FROM d GROUP BY date_trunc('month', day)
        )
        SELECT m.month, m.days, CAST(m.cal_days AS INTEGER) AS cal_days,
               m.last_day,
               round(d.fee, 2)                              AS last_fee,
               round(m.med_fee, 2)                          AS med_fee,
               round(d.fee / NULLIF(m.med_fee, 0), 4)       AS last_day_frac,
               (m.days = m.cal_days
                AND d.fee >= 0.8 * m.med_fee)               AS complete
        FROM m JOIN d ON d.day = m.last_day ORDER BY m.month
    """)
    for (month, days, cal_days, last_day, last_fee, med_fee) in con.execute(
            "SELECT month, days, cal_days, last_day, last_fee, med_fee "
            "FROM sat_coverage ORDER BY month").fetchall():
        short = cal_days - days
        frac = (last_fee / med_fee) if med_fee else 1.0
        log(f"    {month}: {days}/{cal_days} days, last day {last_day} at "
            f"{frac * 100:.0f}% of the month's median day")
        if short or frac < 0.8:
            bits = []
            if short:
                bits.append(f"{short} of its {cal_days} calendar day(s) are absent "
                            f"(data stops at {last_day})")
            if frac < 0.8:
                bits.append(f"its last day, {last_day}, carries only "
                            f"Rs {last_fee:,.0f} against a median day of "
                            f"Rs {med_fee:,.0f} ({frac * 100:.0f}%), so that day "
                            f"looks partially loaded")
            note("warn", "Satellite fee actuals",
                 f"{month} is an incomplete month: " + "; and ".join(bits) + ". "
                 f"Its total is therefore understated and must not be compared "
                 f"like-for-like against a full month -- read the daily chart, "
                 f"where the shortfall is visible, rather than the month total.")

    # --- facility / outlet reach. Facility is the serving warehouse, so
    # facility-vs-city is how you see which mother site is paying satellite fees.
    log("  sat_facility")
    con.execute("""
        CREATE OR REPLACE TABLE sat_facility AS
        SELECT date_trunc('month', day) AS month_date,
               facility_id,
               any_value(facility_name)   AS facility_name,
               city_key,
               any_value(city_raw)        AS city,
               max(is_satellite)          AS is_satellite,
               round(sum(fee), 2)         AS fee,
               sum(net_qty_sold)          AS net_qty,
               count(DISTINCT outlet_id)  AS outlets,
               count(DISTINCT mfr_id)     AS mfrs
        FROM raw GROUP BY month_date, facility_id, city_key
    """)

    # --- splits. bucket x product_type x contract_state is tiny and answers
    # "what kind of rows drive the fee".
    log("  sat_split, sat_rate")
    con.execute("""
        CREATE OR REPLACE TABLE sat_split AS
        SELECT date_trunc('month', day) AS month_date,
               bucket, product_type, contract_state, is_satellite,
               round(sum(fee), 2) AS fee, sum(net_qty_sold) AS net_qty,
               count(*) AS n_rows
        FROM raw GROUP BY ALL
    """)
    # Per-unit rate distribution. fee_amt is a rate, so this is a histogram of
    # rates, never a sum of them.
    con.execute("""
        CREATE OR REPLACE TABLE sat_rate AS
        SELECT date_trunc('month', day) AS month_date,
               fee_amt AS rate, is_satellite, product_type, bucket,
               count(*) AS n_rows, sum(net_qty_sold) AS net_qty,
               round(sum(fee), 2) AS fee
        FROM raw GROUP BY ALL
    """)

    # --- city-level stats, then the full city list (master LEFT JOIN stats so
    # master cities with zero fee activity still show up in the searchable list).
    # Per-city stats per MONTH. city_stats itself stays one row per city (it is
    # the searchable master list); this is what the period filter re-joins to
    # city_master when a window is selected.
    log("  city_month")
    con.execute("""
        CREATE OR REPLACE TABLE city_month AS
        SELECT date_trunc('month', day)      AS month_date,
               city_key,
               any_value(city_raw)           AS city,
               round(sum(fee), 2)            AS fee,
               sum(net_qty_sold)             AS net_qty,
               sum(qty_sold)                 AS qty_sold,
               sum(qty_returned)             AS qty_returned,
               count(*)                      AS n_rows,
               count(DISTINCT mfr_id)        AS mfrs,
               count(DISTINCT variant_id)    AS items,
               count(DISTINCT outlet_id)     AS outlets,
               count(DISTINCT facility_id)   AS facilities,
               count(DISTINCT day)           AS days,
               min(day)                      AS first_day,
               max(day)                      AS last_day
        FROM raw GROUP BY month_date, city_key
    """)

    log("  city_stats")
    con.execute("""
        CREATE OR REPLACE TABLE _city_fee AS
        SELECT city_key,
               any_value(city_raw)          AS city,
               round(sum(fee), 2)            AS fee,
               sum(net_qty_sold)             AS net_qty,
               sum(qty_sold)                 AS qty_sold,
               sum(qty_returned)             AS qty_returned,
               count(*)                      AS n_rows,
               count(DISTINCT mfr_id)        AS mfrs,
               count(DISTINCT variant_id)    AS items,
               count(DISTINCT outlet_id)     AS outlets,
               count(DISTINCT facility_id)   AS facilities,
               count(DISTINCT day)           AS days,
               min(day)                      AS first_day,
               max(day)                      AS last_day
        FROM raw GROUP BY city_key
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE city_stats AS
        SELECT
            COALESCE(m.city, f.city)                        AS city,
            COALESCE(m.city_key, f.city_key)                AS city_key,
            (m.city_key IS NOT NULL)                        AS is_satellite,
            m.distance_km,
            COALESCE(m.band, 'Not in master list')          AS band,
            COALESCE(m.band_ord, 9)                         AS band_ord,
            (f.city_key IS NOT NULL)                        AS has_fees,
            COALESCE(f.fee, 0)          AS fee,
            COALESCE(f.net_qty, 0)      AS net_qty,
            COALESCE(f.qty_sold, 0)     AS qty_sold,
            COALESCE(f.qty_returned, 0) AS qty_returned,
            COALESCE(f.n_rows, 0)       AS n_rows,
            COALESCE(f.mfrs, 0)         AS mfrs,
            COALESCE(f.items, 0)        AS items,
            COALESCE(f.outlets, 0)      AS outlets,
            COALESCE(f.facilities, 0)   AS facilities,
            COALESCE(f.days, 0)         AS days,
            f.first_day, f.last_day
        FROM city_master m
        FULL OUTER JOIN _city_fee f ON f.city_key = m.city_key
    """)
    con.execute("DROP TABLE _city_fee")

    # The validation headline: how much fee sits on cities that are not on the
    # satellite master list at all.
    row = con.execute("""
        SELECT
            count(*) FILTER (WHERE is_satellite)                    AS master_cities,
            count(*) FILTER (WHERE is_satellite AND has_fees)       AS master_with_fees,
            count(*) FILTER (WHERE is_satellite AND NOT has_fees)   AS master_no_fees,
            count(*) FILTER (WHERE NOT is_satellite)                AS offlist_cities,
            round(sum(fee) FILTER (WHERE is_satellite), 2)          AS fee_on_list,
            round(sum(fee) FILTER (WHERE NOT is_satellite), 2)      AS fee_off_list
        FROM city_stats
    """).fetchone()
    master_cities, with_fees, no_fees, offlist, fee_on, fee_off = row
    fee_on = fee_on or 0.0
    fee_off = fee_off or 0.0
    total_fee = fee_on + fee_off
    if offlist:
        pct = (fee_off / total_fee * 100) if total_fee else 0
        note("error", "Satellite city validation",
             f"{offlist} city(ies) in the fee extract are NOT on the satellite "
             f"master list, carrying Rs {fee_off:,.0f} of fee ({pct:.1f}% of the "
             f"Rs {total_fee:,.0f} total). They include metros with their own "
             f"mother warehouses, which cannot be satellite by definition. The "
             f"fee extract is not pre-filtered to satellite cities -- scope to "
             f"'Satellite only' for the validated number.")
    if no_fees:
        note("info", "Satellite city validation",
             f"{no_fees} of {master_cities} master satellite cities recorded no "
             f"fee at all in this period. Expected for cities with no satellite "
             f"outlet live yet; worth checking if one recently launched.")
    return {"fee_on_list": fee_on, "fee_off_list": fee_off, "fee_total": total_fee,
            "master_cities": master_cities, "master_with_fees": with_fees,
            "offlist_cities": offlist}


# --------------------------------------------------------------------------
# 3. defective products
# --------------------------------------------------------------------------

# Phrases the returns UI and the support bot inject into every remark. They are
# not what the customer said, so they would otherwise dominate the keyword cloud
# ("promo code", "main menu", "original payment method"). Stripped as PHRASES
# before tokenising, which is what lets genuinely meaningful words that also
# appear in the boilerplate ("damaged", "quality", "leakage") survive when a
# customer actually typed them.
BOILERPLATE = [
    # returns-flow canned selections
    r"i have received \*\*[^*]*\*\* quality item\(s\)",
    r"i have received \*\*[^*]*\*\* item\(s\)",
    r"the item\(s\) are \*\*[^*]*\*\*",
    r"the item\(s\) have a \*\*[^*]*\*\*, \*\*[^*]*\*\* or \*\*[^*]*\*\* issue",
    r"the item\(s\) have a[^,]*issue",
    r"\*\*replace\*\* approved item\(s\)",
    r"\*\*refund\*\* approved item\(s\)",
    r"add comment",
    r"open camera",
    r"please replace this order",
    # refund-method and support-bot navigation. These are the chat widget's own
    # words; without them the cloud fills with "promo code" and "main menu".
    r"original (payment )?(method|source)",
    r"source account",
    r"promo ?code[a-z ]*",
    r"upi[a-z ]*",
    r"back to main menu",
    r"main menu",
    r"talk to (an? )?agent",
    r"chat with (us|agent)",
    r"looking for help[a-z ]*",
    r"is there anything else[a-z ]*",
    r"anything else i can help[a-z ]*",
    r"connect(ing)? you to[a-z ]*",
    r"(choose |upload )?from gallery",
    r"i have received",
]

# Contraction normalisation, applied BEFORE the split on non-letters. Splitting
# first turns "doesn't work" into the tokens "doesn" + "work", which puts the
# meaningless fragment "doesn" high in the cloud and loses the negation. Mapping
# the contraction to "not" first yields the bigram "not work", which is the
# actual complaint. Handles the apostrophe-less spellings too, since customers
# type both.
CONTRACTIONS = [
    (r"n[''’]t", " not "),
    (r"\b(dont|doesnt|didnt|isnt|wasnt|arent|werent|cant|cannot|couldnt"
     r"|wouldnt|shouldnt|wont|havent|hasnt|hadnt|aint)\b", " not "),
]

# A bigram is kept only when it reads as a phrase: two content words, or a
# negation followed by a content word. Without this, every "<stopword> <content>"
# pair ("the box", "issue with", "broken and") outranks the real phrases.
NEGATIONS = ["not", "no", "never", "nor"]

# NOISE: hard-removed from both unigrams and bigrams. Process vocabulary of a
# returns flow, support-chat furniture, and month names bleeding in from the
# bot's menu text -- none of it describes a defect.
NOISE = """
item items itemss product products produt prodct order orders ordered ordering
replace replaced replacement replacing refund refunded refunding return returned
returning cancel cancelled cancelling exchange exchanged pickup pick picked
comment comments camera photo photos image images picture pictures upload uploaded
delivery delivered deliver boy agent executive customer support team representative
blinkit blink app amount money rupees paid payment method source account wallet
promo code coupon voucher cashback upi gpay paytm bank card
chat video call calling menu main back help helping looking assist assistance
gallery upload choose select selected option options click tap press
january february march april june july august september october november december
kindly please pls plz asap urgent urgently soon
one two three four five six seven eight nine ten pcs piece pieces qty quantity
sure okay thanks thank thankyou hello hi dear sir madam yeah yep yup nope
already also still just even much many lot lots really quite very
like take send check know tell give come want need make made doing going
received receive receiving original
""".split()

# STOPWORDS: filtered out of the UNIGRAM cloud but deliberately still allowed
# inside bigrams -- "not working" and "not upto mark" are exactly the phrases
# that describe a defect, and they only survive because "not" is a stopword
# rather than noise. See build_defect_terms for how the two lists are applied.
STOPWORDS = """
a about above after again against all am an and any are aren as at be because
been before being below between both but by came can cant cannot could couldnt did
didnt do does doesnt doing dont down during each ever few for from further get
gets getting give given go goes going got had hadnt has hasnt have havent having he
her here hers herself him himself his how however i if in into is isnt it its itself
may me might mine more most must my myself need needed no
nor not now of off on once only or other ought our ours ourselves out over own per
put said same say says see seen shall she should shouldnt since so
some such than that thats the their theirs them themselves then
there these they this those though through to too under until up upon us use used
using want wanted was wasnt way we well were werent what when where whether
which while who whom why will with within without wont would wouldnt yes yet you
your yours yourself
kk hmm na nil none nothing time times day days today yesterday
xx xxx xxxx xxxxx xxxxxx xxxxxxx xxxxxxxx xxxxxxxxx xxxxxxxxxx
""".split()


def build_defects(con, paths):
    """Defect rollups + the remarks keyword cloud."""
    files = sql_list(paths)
    log(f"reading defective returns ({len(paths)} part(s))")

    con.execute(f"""
        CREATE OR REPLACE VIEW draw AS
        SELECT CAST(duration AS DATE) AS day,
               entity_id, entity_name,
               item_id, item_name,
               manufacturer_id AS mfr_id, manufacturer AS mfr,
               complaint_type,
               remarks,
               COALESCE(bad_returns_qty, 0) AS qty,
               COALESCE(bad_returns_qty, 0) * {DEFECT_RATE} AS fee,
               COALESCE(penalty, 0) AS penalty_extract,
               COALESCE(gmv, 0) AS gmv
        FROM read_parquet({files})
    """)

    # Reconcile our recomputed Rs 50/unit against the penalty the extract
    # already carries. They agree today; if a rate change lands upstream this
    # note is how the dashboard admits its Rs 50 assumption went stale.
    mism, tot, ours, theirs = con.execute("""
        SELECT count(*) FILTER (WHERE abs(fee - penalty_extract) > 0.01),
               count(*), round(sum(fee), 2), round(sum(penalty_extract), 2)
        FROM draw
    """).fetchone()
    if mism:
        note("warn", "Defective returns",
             f"{mism:,} of {tot:,} rows: recomputed fee at Rs {DEFECT_RATE:g}/unit "
             f"(Rs {ours:,.0f} total) does not match the extract's own penalty "
             f"column (Rs {theirs:,.0f}). The dashboard shows the Rs "
             f"{DEFECT_RATE:g}/unit figure; the upstream rate may have changed.")
    else:
        log(f"  Rs {DEFECT_RATE:g}/unit reconciles exactly with the extract's "
            f"penalty column on all {tot:,} rows")

    log("  def_item, def_mfr, def_type, def_daily")
    con.execute("""
        CREATE OR REPLACE TABLE def_item AS
        SELECT item_id,
               any_value(item_name)          AS item_name,
               any_value(mfr_id)             AS mfr_id,
               any_value(mfr)                AS mfr,
               sum(qty)                      AS qty,
               round(sum(fee), 2)            AS fee,
               round(sum(gmv), 2)            AS gmv,
               count(*)                      AS complaints,
               count(DISTINCT complaint_type) AS types,
               count(DISTINCT day)           AS days,
               mode(complaint_type)          AS top_type
        FROM draw GROUP BY item_id
    """)
    con.execute("""
        CREATE OR REPLACE TABLE def_item_type AS
        SELECT item_id, complaint_type,
               sum(qty) AS qty, round(sum(fee), 2) AS fee, count(*) AS complaints
        FROM draw GROUP BY item_id, complaint_type
    """)
    con.execute("""
        CREATE OR REPLACE TABLE def_mfr AS
        SELECT mfr_id,
               any_value(mfr)             AS mfr,
               sum(qty)                   AS qty,
               round(sum(fee), 2)         AS fee,
               round(sum(gmv), 2)         AS gmv,
               count(*)                   AS complaints,
               count(DISTINCT item_id)    AS items,
               count(DISTINCT complaint_type) AS types,
               mode(complaint_type)       AS top_type
        FROM draw GROUP BY mfr_id
    """)
    con.execute("""
        CREATE OR REPLACE TABLE def_mfr_type AS
        SELECT mfr_id, complaint_type,
               sum(qty) AS qty, round(sum(fee), 2) AS fee, count(*) AS complaints
        FROM draw GROUP BY mfr_id, complaint_type
    """)
    con.execute("""
        CREATE OR REPLACE TABLE def_type AS
        SELECT complaint_type,
               sum(qty)                AS qty,
               round(sum(fee), 2)      AS fee,
               count(*)                AS complaints,
               count(DISTINCT item_id) AS items,
               count(DISTINCT mfr_id)  AS mfrs
        FROM draw GROUP BY complaint_type ORDER BY qty DESC
    """)
    con.execute("""
        CREATE OR REPLACE TABLE def_daily AS
        SELECT day, complaint_type,
               sum(qty) AS qty, round(sum(fee), 2) AS fee, count(*) AS complaints
        FROM draw GROUP BY day, complaint_type ORDER BY day
    """)
    con.execute("""
        CREATE OR REPLACE TABLE def_entity AS
        SELECT entity_id, any_value(entity_name) AS entity_name,
               sum(qty) AS qty, round(sum(fee), 2) AS fee, count(*) AS complaints
        FROM draw GROUP BY entity_id
    """)

    # The complaint lines themselves, as a real table rather than the `draw` view.
    # `draw` reads the Parquet in ../data, which does NOT exist in the deployed
    # image (only fees.duckdb is baked in), so anything the app queries at request
    # time has to be materialised here. 283k rows -- a rounding error on disk, and
    # it is what makes the global period filter work on this tab.
    # EXCLUDE remarks: the free text is ~130 MB of the 149 MB this table would
    # otherwise cost, and nothing queries it at request time -- the keyword cloud
    # is tokenised here at build time and is the one figure the period filter
    # explicitly does not reach.
    con.execute("CREATE OR REPLACE TABLE def_line AS "
                "SELECT * EXCLUDE (remarks) FROM draw")
    log(f"    def_line: {con.execute('SELECT count(*) FROM def_line').fetchone()[0]:,} rows")

    build_defect_terms(con)


def build_defect_terms(con):
    """Mine the free-text remarks into a keyword cloud.

    Pipeline, in order -- the order is what makes it work:
      1. lowercase
      2. delete the item name (every remark is prefixed with it, so otherwise
         the cloud is just the catalogue)
      3. delete the returns-UI and support-bot boilerplate PHRASES (before the
         ** markers go, since the patterns match on them)
      4. strip markdown, split on non-letters
      5. drop NOISE and anything under 3 characters  -> the bigram token list
      6. additionally drop STOPWORDS                 -> the unigram token list
      7. count DISTINCT complaints per term, not raw occurrences, so one
         customer typing "damaged damaged damaged" counts once

    Steps 5 and 6 are separate on purpose. Bigrams are built BEFORE stopword
    removal so that "not working", "not upto mark" and "seal broken" survive --
    those phrases are the defect. Unigrams are built after, so "not" and "the"
    never reach the cloud. A bigram whose both halves are stopwords is dropped.
    """
    log("  def_terms (remarks keyword cloud)")
    strip = "lower(COALESCE(remarks, ''))"
    # Remove the item name first. replace() is a plain substring swap, so no
    # regex escaping worry with names full of ( ) [ ] + . characters.
    strip = f"replace({strip}, lower(COALESCE(item_name, '\\x00')), ' ')"
    for pat in BOILERPLATE:
        strip = f"regexp_replace({strip}, '{pat}', ' ', 'g')"
    strip = f"regexp_replace({strip}, '\\*+', ' ', 'g')"
    # Contractions last, so the boilerplate patterns above still match the
    # original text, but before the split on non-letters strips apostrophes.
    for pat, rep in CONTRACTIONS:
        strip = f"regexp_replace({strip}, '{pat}', '{rep}', 'g')"

    lit = lambda ws: "[" + ", ".join("'" + w + "'" for w in sorted(set(ws))) + "]"
    noise, stop, neg = lit(NOISE), lit(STOPWORDS), lit(NEGATIONS)

    # cid gives every source complaint a stable id, so the same word typed three
    # times in one rant collapses to a single mention below.
    #   phrase_toks -> bigram source (noise gone, stopwords kept)
    #   word_toks   -> unigram source (stopwords gone too)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _tok AS
        SELECT cid, complaint_type, item_id, qty, phrase_toks,
               list_filter(phrase_toks, t -> NOT list_contains({stop}, t)) AS word_toks
        FROM (
            SELECT row_number() OVER () AS cid,
                   complaint_type, item_id, qty,
                   list_filter(
                       regexp_split_to_array({strip}, '[^a-z]+'),
                       t -> length(t) >= 3 AND NOT list_contains({noise}, t)
                   ) AS phrase_toks
            FROM draw
            WHERE remarks IS NOT NULL AND length(trim(remarks)) > 0
        )
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _terms AS
        SELECT DISTINCT * FROM (
            SELECT cid, complaint_type, item_id, qty, 1 AS ngram,
                   unnest(word_toks) AS term
            FROM _tok
            UNION ALL
            SELECT cid, complaint_type, item_id, qty, 2 AS ngram, term FROM (
                SELECT cid, complaint_type, item_id, qty,
                       unnest(list_transform(
                           range(0, greatest(len(phrase_toks) - 1, 0)),
                           -- Keep only phrase-shaped pairs: content+content, or
                           -- negation+content. NULL everything else and filter
                           -- it below, so "the box" and "issue with" never
                           -- outrank "not working" and "seal broken".
                           i -> CASE
                                  WHEN list_contains({stop}, phrase_toks[i + 2])
                                  THEN NULL
                                  WHEN list_contains({stop}, phrase_toks[i + 1])
                                   AND NOT list_contains({neg}, phrase_toks[i + 1])
                                  THEN NULL
                                  ELSE phrase_toks[i + 1] || ' ' || phrase_toks[i + 2]
                                END)) AS term
                FROM _tok WHERE len(phrase_toks) >= 2
            ) WHERE term IS NOT NULL
        )
    """)
    # Bigrams carry a higher floor than unigrams: the phrase space is ~6x larger,
    # and a phrase seen 5 times across 280k complaints is noise, not a pattern.
    con.execute(f"""
        CREATE OR REPLACE TABLE def_terms AS
        SELECT term, ngram,
               count(*)                 AS mentions,
               sum(qty)                 AS qty,
               round(sum(qty) * {DEFECT_RATE}, 2) AS fee,
               count(DISTINCT item_id)  AS items,
               mode(complaint_type)     AS top_type
        FROM _terms
        GROUP BY term, ngram
        HAVING count(*) >= CASE WHEN ngram = 1 THEN 5 ELSE 25 END
        ORDER BY mentions DESC
    """)
    con.execute("""
        CREATE OR REPLACE TABLE def_type_terms AS
        SELECT complaint_type, term, ngram,
               count(*) AS mentions, sum(qty) AS qty
        FROM _terms
        GROUP BY complaint_type, term, ngram
        HAVING count(*) >= CASE WHEN ngram = 1 THEN 3 ELSE 10 END
    """)
    n1, n2 = con.execute(
        "SELECT count(*) FILTER (WHERE ngram=1), count(*) FILTER (WHERE ngram=2) "
        "FROM def_terms").fetchone()
    log(f"    {n1:,} unigrams, {n2:,} bigrams above the frequency floor")
    con.execute("DROP TABLE IF EXISTS _terms")
    con.execute("DROP TABLE IF EXISTS _tok")


# --------------------------------------------------------------------------
# 4. KAM fees
# --------------------------------------------------------------------------

def build_kam(con, paths, contract_paths):
    """KAM contracts and the per-month accrual spine.

    kam_charges is the contracted monthly KAM support charge. Contracts are all
    valid_until_cancelled = 'Yes', so a contract that went effective in
    2025-07 has been accruing every month since. kam_month is that accrual
    exploded one row per (month, contract), which is what lets the UI show both
    "per manufacturer per month" and "total per month" off one table.
    """
    files = sql_list(paths)
    log("reading KAM fee extract")
    con.execute(f"""
        CREATE OR REPLACE TABLE kam_contracts AS
        SELECT contract_id,
               manufacturer_id AS mfr_id,
               manufacturer    AS mfr,
               contract_state,
               CAST(execution_date AS DATE) AS execution_date,
               CAST(effective_date AS DATE) AS effective_date,
               valid_until_cancelled,
               kam_support,
               TRY_CAST(kam_charges AS DOUBLE) AS monthly_fee
        FROM read_parquet({files})
        WHERE upper(COALESCE(kam_support, '')) = 'YES'
    """)
    n = con.execute("SELECT count(*) FROM kam_contracts").fetchone()[0]
    if not n:
        note("warn", "KAM support fees",
             "No contracts in the extract have kam_support = 'Yes', so there is "
             "nothing to accrue.")
        for t, cols in (("kam_month", "month VARCHAR, mfr_id VARCHAR, mfr VARCHAR, "
                                      "contract_id VARCHAR, contract_state VARCHAR, "
                                      "fee DOUBLE, month_idx INTEGER"),
                        ("kam_month_total", "month VARCHAR, contracts INTEGER, "
                                            "fee DOUBLE, fee_approved DOUBLE, "
                                            "new_contracts INTEGER")):
            con.execute(f"CREATE OR REPLACE TABLE {t} ({cols})")
        return {}

    nofee = con.execute(
        "SELECT count(*) FROM kam_contracts WHERE monthly_fee IS NULL").fetchone()[0]
    if nofee:
        note("warn", "KAM support fees",
             f"{nofee} contract(s) have kam_support = 'Yes' but a kam_charges "
             f"value that will not parse as a number; they accrue Rs 0.")
    zero = con.execute(
        "SELECT count(*) FROM kam_contracts WHERE COALESCE(monthly_fee,0) = 0").fetchone()[0]
    if zero:
        note("info", "KAM support fees",
             f"{zero} contract(s) have KAM support enabled with a charge of Rs 0. "
             f"They are counted as KAM contracts but contribute no fee.")

    # Bound the spine. Default to the latest contract activity in the extract
    # rather than wall-clock today, so a rebuild long after the extract does not
    # invent accrual months with no data behind them.
    end = ACCRUAL_END or con.execute("""
        SELECT strftime(greatest(max(effective_date), max(execution_date)), '%Y-%m')
        FROM kam_contracts
    """).fetchone()[0]
    log(f"  accruing monthly to {end}")

    con.execute(f"""
        CREATE OR REPLACE TABLE kam_month AS
        WITH spine AS (
            SELECT c.*,
                   date_trunc('month', c.effective_date)              AS m0,
                   date_trunc('month', CAST('{end}-01' AS DATE))       AS m1
            FROM kam_contracts c
        ), months AS (
            SELECT s.*, UNNEST(generate_series(s.m0, s.m1, INTERVAL '1' MONTH)) AS m
            FROM spine s
            WHERE s.m0 <= s.m1
        )
        SELECT strftime(m, '%Y-%m')          AS month,
               CAST(m AS DATE)               AS month_date,
               mfr_id, mfr, contract_id, contract_state,
               COALESCE(monthly_fee, 0)      AS fee,
               effective_date,
               (date_diff('month', date_trunc('month', effective_date), m) + 1) AS month_idx
        FROM months
        ORDER BY month, mfr
    """)
    con.execute("""
        CREATE OR REPLACE TABLE kam_month_total AS
        SELECT month, any_value(month_date) AS month_date,
               count(*)                                     AS contracts,
               round(sum(fee), 2)                            AS fee,
               round(sum(fee) FILTER (WHERE contract_state = 'APPROVED'), 2) AS fee_approved,
               count(*) FILTER (WHERE contract_state = 'APPROVED')           AS contracts_approved,
               count(*) FILTER (WHERE month_idx = 1)         AS new_contracts
        FROM kam_month GROUP BY month ORDER BY month
    """)

    # Contracts master gives the wider KAM picture: how many contracts opted
    # out of KAM support entirely. Optional -- skip quietly if absent.
    if contract_paths:
        con.execute(f"""
            CREATE OR REPLACE TABLE kam_coverage AS
            SELECT COALESCE(NULLIF(trim(kam_support), ''), 'Not set') AS kam_support,
                   contract_state,
                   count(*) AS contracts,
                   count(DISTINCT manufacturer_id) AS mfrs
            FROM read_parquet({sql_list(contract_paths)})
            GROUP BY ALL ORDER BY contracts DESC
        """)

    states = con.execute("""
        SELECT contract_state, count(*) FROM kam_contracts GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    pending = sum(c for s, c in states if s and s.upper() != "APPROVED")
    if pending:
        note("warn", "KAM support fees",
             f"{pending} of {n} KAM contracts are not APPROVED "
             f"({', '.join(f'{s}: {c}' for s, c in states)}). Accrual includes "
             f"them by default; use the contract-state filter to see the "
             f"APPROVED-only number, which is what is actually billable today.")
    return {"kam_contracts": n, "kam_end": end}


# --------------------------------------------------------------------------
# 5. recall assistance -- PRN recall-assistance actuals
# --------------------------------------------------------------------------

# The contractual recall-assistance schedule. Rates outside this set are legal
# (a contract can negotiate anything) but rare enough to be worth flagging, so a
# typo upstream shows up as a note instead of quietly becoming money.
RECALL_RATES = (2.5, 5.0)

# Blinkit's own packaging SKUs, which are returns in the extract but not vendor
# returns in any commercial sense. They carry no contract and therefore no fee,
# so dropping them changes the money by exactly Rs 0 -- but it changes the
# quantity ranking a lot: the carry bag alone is ~24% of all returned units and
# was the top bar on every quantity chart, pushing real vendor SKUs off it.
#
# (lower-case LIKE pattern, reason). `_` is escaped because it is LIKE's
# single-character wildcard. Dropped rows are kept in `rtv_excluded` and stated in
# a build note, so this is an auditable exclusion rather than a silent filter.
RECALL_EXCLUDE = [
    (r"new\_blinkit%bag%", "Blinkit's own carry bag — internal packaging, not a vendor return"),
]


def build_recall(con, paths):
    """PRN recall lines -> the month x manufacturer x item recall-fee cube.

    Source: `recall_assistance_fee*.parquet`, the corrected extract that
    supersedes the BCPL_RTV_Claim CSV drop. The correction is the whole point of
    this section: the CSV charged a recall fee on returns from every
    manufacturer with no check that a recall-assistance clause existed. This
    extract is joined to the contract, so a line is only chargeable when there
    is a rate behind it, and it carries that rate:

        recall_assistance_fee = chargeable_prn_qty * recall_charge_rs

    That identity holds row-exact on the extract, which makes this the rare fee
    that is both *reported* and *reconcilable*. We take the reported money and
    assert the identity (see the reconciliation note below) rather than
    recomputing -- if upstream ever changes how it charges, the note fires
    instead of the dashboard silently disagreeing with the claim.

    Three quantities, deliberately kept apart:
      qty          = prn_qty, everything that came back.
      qty_charged  = chargeable_prn_qty, the slice with a contract rate.
      qty_unrated  = returns on lines with no contract_id, whose fee is
                     UNKNOWN, not zero. Half the lines are in that state.

    Dedupe. (duration, manufacturer_id, item_id) is the grain. It is unique in
    the extract today; the row_number guard is what keeps a re-pulled part from
    doubling both quantity and fee, with the alphabetically-last part winning
    deterministically.
    """
    log(f"reading PRN recall assistance ({len(paths)} file(s))")
    con.execute(f"""
        CREATE OR REPLACE TABLE rtv_claim AS
        WITH raw AS (
            SELECT
                CAST(duration AS DATE)              AS claim_date,
                CAST(item_id AS VARCHAR)            AS item_id,
                trim(CAST(item_name AS VARCHAR))    AS item,
                CAST(manufacturer_id AS VARCHAR)    AS mfr_id,
                trim(CAST(manufacturer AS VARCHAR)) AS mfr,
                TRY_CAST(contract_id AS BIGINT)     AS contract_id,
                -- a RATE: never summed, only max'd/weighted within a group.
                TRY_CAST(recall_charge_rs AS DOUBLE)              AS rate_contract,
                COALESCE(TRY_CAST(prn_qty AS DOUBLE), 0)          AS qty,
                COALESCE(TRY_CAST(chargeable_prn_qty AS DOUBLE), 0) AS qty_charged,
                COALESCE(TRY_CAST(recall_assistance_fee AS DOUBLE), 0) AS fee,
                COALESCE(TRY_CAST(dmg_rtv_flag AS INTEGER), 0)    AS dmg_rtv,
                COALESCE(TRY_CAST(nte_rtv_flag AS INTEGER), 0)    AS nte_rtv,
                COALESCE(TRY_CAST(rtv_eligible AS INTEGER), 0)    AS rtv_eligible,
                filename                            AS src_file
            FROM read_parquet({sql_list(paths)}, filename=true, union_by_name=true)
            WHERE duration IS NOT NULL
        )
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, row_number() OVER (
                PARTITION BY claim_date, mfr_id, item_id
                ORDER BY src_file DESC) AS rn
            FROM raw
        ) WHERE rn = 1
    """)

    # Own-packaging exclusions. Split rather than deleted, so the Data tab can
    # say exactly what left the cube and what it was worth.
    excl = " OR ".join(f"lower(item) LIKE '{pat}' ESCAPE '\\'"
                       for pat, _reason in RECALL_EXCLUDE)
    con.execute(f"""
        CREATE OR REPLACE TABLE rtv_excluded AS
        SELECT * FROM rtv_claim WHERE {excl}
    """)
    con.execute(f"DELETE FROM rtv_claim WHERE {excl}")
    dropped = con.execute("""
        SELECT count(*), count(DISTINCT item_id), sum(qty), round(sum(fee), 2)
        FROM rtv_excluded
    """).fetchone()
    if dropped[0]:
        names = con.execute("""
            SELECT any_value(item), any_value(mfr), sum(qty)
            FROM rtv_excluded GROUP BY item_id ORDER BY sum(qty) DESC LIMIT 4
        """).fetchall()
        kept_qty = con.execute("SELECT sum(qty) FROM rtv_claim").fetchone()[0] or 0
        note("info", "Recall assistance (PRN)",
             f"Excluded {dropped[1]:,} of Blinkit's own packaging SKU(s) — "
             + "; ".join(f"<b>{i}</b> ({m}, {q:,.0f} units)" for i, m, q in names)
             + f" — covering {dropped[0]:,} line(s) and {dropped[2]:,.0f} returned "
             f"units. They are returns in the extract but not vendor returns, they "
             f"carry no contract and no fee, so this changes the recall charge by "
             f"<b>exactly Rs 0</b> while removing the largest bar from every "
             f"quantity chart. Returned units on the tab therefore total "
             f"{kept_qty:,.0f}, not {kept_qty + dropped[2]:,.0f}.")

    n = con.execute("SELECT count(*) FROM rtv_claim").fetchone()[0]
    if not n:
        note("warn", "Recall assistance (PRN)",
             "The recall extract parsed but contained no dated rows, so there is "
             "nothing to report.")
        con.execute("CREATE OR REPLACE TABLE rtv_month (month VARCHAR, "
                    "month_date DATE, mfr_id VARCHAR, mfr VARCHAR, "
                    "item_id VARCHAR, item VARCHAR, contract_id BIGINT, "
                    "rate_contract DOUBLE, qty DOUBLE, fee DOUBLE, "
                    "lines INTEGER, lines_charged INTEGER, qty_charged DOUBLE, "
                    "qty_unrated DOUBLE, fee_ineligible DOUBLE)")
        con.execute("CREATE OR REPLACE TABLE rtv_month_total (month VARCHAR, "
                    "month_date DATE, fee DOUBLE, qty DOUBLE, mfrs INTEGER, "
                    "items INTEGER, mfrs_charged INTEGER, items_charged INTEGER, "
                    "lines INTEGER, lines_charged INTEGER, "
                    "qty_charged DOUBLE, qty_unrated DOUBLE, "
                    "contracts INTEGER, fee_ineligible DOUBLE, rate DOUBLE)")
        con.execute("CREATE OR REPLACE TABLE rtv_rate (rate DOUBLE, lines INTEGER, "
                    "qty DOUBLE, fee DOUBLE, contracts INTEGER)")
        return {}

    # The cube. Grouped rather than taken row-for-row so a future extract at day
    # grain still rolls up to exactly one row per month x manufacturer x item.
    con.execute("""
        CREATE OR REPLACE TABLE rtv_month AS
        SELECT strftime(claim_date, '%Y-%m')              AS month,
               date_trunc('month', claim_date)            AS month_date,
               mfr_id, any_value(mfr)  AS mfr,
               item_id, any_value(item) AS item,
               any_value(contract_id)                     AS contract_id,
               max(rate_contract)                         AS rate_contract,
               sum(qty)                                   AS qty,
               round(sum(fee), 2)                         AS fee,
               count(*)                                   AS lines,
               -- "charged" is having a contract rate applied to units, not
               -- fee > 0: a genuine Rs 0 rate is charged-at-zero, which is a
               -- different fact from having no contract at all.
               count(*) FILTER (WHERE qty_charged > 0)     AS lines_charged,
               sum(qty_charged)                           AS qty_charged,
               -- COALESCE to 0 is right here, unlike on a fee: "no un-rated
               -- units in this group" is a known fact, not an unknown.
               COALESCE(sum(qty) FILTER (WHERE rate_contract IS NULL), 0) AS qty_unrated,
               COALESCE(round(sum(fee) FILTER (WHERE rtv_eligible = 0), 2), 0) AS fee_ineligible
        FROM rtv_claim
        GROUP BY month, month_date, mfr_id, item_id
    """)
    con.execute("""
        CREATE OR REPLACE TABLE rtv_month_total AS
        SELECT month, any_value(month_date) AS month_date,
               round(sum(fee), 2)              AS fee,
               sum(qty)                        AS qty,
               count(DISTINCT mfr_id)          AS mfrs,
               count(DISTINCT item_id)         AS items,
               -- charged counts live here rather than being computed from a
               -- filtered request, so the KPI tiles stay stable while the
               -- search box narrows the charts and tables below them.
               count(DISTINCT mfr_id)  FILTER (WHERE qty_charged > 0) AS mfrs_charged,
               count(DISTINCT item_id) FILTER (WHERE qty_charged > 0) AS items_charged,
               sum(lines)                      AS lines,
               sum(lines_charged)              AS lines_charged,
               sum(qty_charged)                AS qty_charged,
               sum(qty_unrated)                AS qty_unrated,
               count(DISTINCT contract_id)     AS contracts,
               round(sum(fee_ineligible), 2)   AS fee_ineligible,
               round(sum(fee) / NULLIF(sum(qty_charged), 0), 3) AS rate
        FROM rtv_month GROUP BY month ORDER BY month
    """)

    # Contractual rate distribution. Reported, not derived (the extract carries
    # recall_charge_rs), so this is the schedule the money was actually charged
    # on -- the audit is that it concentrates on the contractual Rs 5 / Rs 2.5.
    con.execute("""
        CREATE OR REPLACE TABLE rtv_rate AS
        SELECT rate_contract              AS rate,
               count(*)                   AS lines,
               sum(qty_charged)           AS qty,
               round(sum(fee), 2)         AS fee,
               count(DISTINCT contract_id) AS contracts
        FROM rtv_claim
        WHERE rate_contract IS NOT NULL
        GROUP BY 1 ORDER BY fee DESC
    """)

    # --- caveats the tab has to surface, not bury.
    st = con.execute("""
        SELECT count(*)                                   AS lines,
               count(*) FILTER (WHERE rate_contract IS NULL) AS unrated_lines,
               sum(qty) FILTER (WHERE rate_contract IS NULL) AS unrated_qty,
               sum(qty)                                   AS qty,
               sum(qty_charged)                           AS qty_charged,
               round(sum(fee), 2)                         AS fee,
               count(DISTINCT mfr_id)                     AS mfrs,
               count(DISTINCT mfr_id) FILTER (WHERE contract_id IS NOT NULL) AS mfrs_rated,
               count(DISTINCT contract_id)                AS contracts,
               count(*) FILTER (WHERE qty = 0)            AS noqty_lines,
               count(DISTINCT strftime(claim_date, '%Y-%m')) AS months
        FROM rtv_claim
    """).fetchone()
    (lines, unrated_lines, unrated_qty, qty, qty_charged, fee,
     mfrs, mfrs_rated, contracts, noqty, months) = st

    # 1. The reconciliation. This is the note that makes the reported fee
    #    trustworthy, so it is emitted whether it passes or fails.
    bad = con.execute("""
        SELECT count(*), round(max(abs(fee - qty_charged * rate_contract)), 4)
        FROM rtv_claim
        WHERE rate_contract IS NOT NULL
          AND abs(fee - qty_charged * rate_contract) >= 0.01
    """).fetchone()
    rated_lines = lines - unrated_lines
    if bad[0]:
        note("error", "Recall assistance (PRN)",
             f"{bad[0]:,} of {rated_lines:,} rated lines do NOT satisfy "
             f"recall_assistance_fee = chargeable_prn_qty x recall_charge_rs "
             f"(worst gap Rs {bad[1]:,.2f}). The reported fee no longer "
             f"reconciles to the contractual rate — treat the totals as "
             f"unverified until upstream is checked.")
    else:
        note("info", "Recall assistance (PRN)",
             f"Reconciled: all {rated_lines:,} rated lines satisfy "
             f"<code>recall_assistance_fee = chargeable_prn_qty × "
             f"recall_charge_rs</code> exactly. The fee is taken as reported and "
             f"lands on the contractual rate, so every figure on the tab can be "
             f"checked back to the contract.")

    # 2. Half the returns have no contract behind them. Unknown, not zero.
    if unrated_lines:
        note("warn", "Recall assistance (PRN)",
             f"{unrated_lines:,} of {lines:,} lines "
             f"({unrated_lines / lines * 100:.0f}%) carry no "
             f"<code>contract_id</code> and therefore no "
             f"<code>recall_charge_rs</code>, covering {unrated_qty:,.0f} of "
             f"{qty:,.0f} returned units. Their recall fee is <b>unknown, not "
             f"zero</b> — nothing here bills them at Rs 0, they are simply "
             f"outside the chargeable set. Only {mfrs_rated:,} of {mfrs:,} "
             f"manufacturers with returns are chargeable at all "
             f"({contracts:,} contracts), so the quantity ranking and the fee "
             f"ranking are genuinely different lists and the tab shows both.")

    # 3. Charged despite rtv_eligible = 0. Material share of the money, and not
    #    something the numbers explain on their own.
    inel = con.execute("""
        SELECT count(*), round(sum(fee), 2), sum(qty_charged),
               count(DISTINCT mfr_id)
        FROM rtv_claim WHERE rtv_eligible = 0 AND fee > 0
    """).fetchone()
    if inel[0]:
        share = (inel[1] or 0) / fee * 100 if fee else 0
        note("warn", "Recall assistance (PRN)",
             f"{inel[0]:,} lines are flagged <code>rtv_eligible = 0</code> "
             f"(neither <code>dmg_rtv_flag</code> nor <code>nte_rtv_flag</code> "
             f"set) yet still carry a fee — Rs {inel[1]:,.0f} across "
             f"{inel[3]:,} manufacturers, <b>{share:.0f}% of the total</b> on "
             f"{inel[2]:,.0f} chargeable units. Either the eligibility flags do "
             f"not gate chargeability the way the column name implies, or these "
             f"lines are charged in error. Worth confirming upstream before the "
             f"figure is billed.")

    # 4. Off-schedule rates: small money, but a rate typo is worth seeing.
    off = con.execute(f"""
        SELECT count(*), round(sum(fee), 2),
               string_agg(DISTINCT format('Rs {{:.2f}}', rate_contract), ', ')
        FROM rtv_claim
        WHERE rate_contract IS NOT NULL
          AND rate_contract NOT IN {RECALL_RATES}
    """).fetchone()
    if off[0]:
        note("info", "Recall assistance (PRN)",
             f"{off[0]:,} rated lines sit on a rate outside the contractual "
             f"Rs 5 / Rs 2.50 schedule ({off[2]}), worth Rs {off[1]:,.0f}. "
             f"Immaterial to the total, but they are either negotiated "
             f"exceptions or upstream typos.")

    if noqty:
        note("info", "Recall assistance (PRN)",
             f"{noqty:,} lines have prn_qty = 0 and so contribute no quantity "
             f"and no fee.")

    # Part month vs the month in the data. Worth stating explicitly, because a
    # dated filename invites the assumption that one file = one month.
    spread = con.execute("""
        SELECT regexp_replace(src_file, '^.*/', ''),
               string_agg(DISTINCT strftime(claim_date, '%Y-%m'), ', '
                          ORDER BY strftime(claim_date, '%Y-%m'))
        FROM rtv_claim GROUP BY 1 ORDER BY 1
    """).fetchall()
    # ...but only where the name actually makes a claim about a month. A part
    # called `recall_assistance_fee.parquet` promises nothing, so saying it holds
    # two months is noise; a `..._June 2026` part holding July is the caveat.
    dated = re.compile(r"\d{4}|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec",
                       re.I).search
    multi = [(f, ms) for f, ms in spread if "," in ms and dated(f)]
    if multi:
        note("info", "Recall assistance (PRN)",
             "A source part is not bounded by its own name — "
             + "; ".join(f"<b>{f}</b> contains {ms}" for f, ms in multi)
             + ". Every figure on this tab is bucketed by the row's own "
             "<code>duration</code> column, not by the filename.")

    # The superseded source. If the old claim CSV is still sitting in ../data,
    # say plainly that it is no longer read -- otherwise a reader who remembers
    # the Rs 5.66M headline has no way to know why the number moved.
    old_csv = sorted(glob.glob(os.path.join(
        os.path.dirname(os.path.abspath(paths[0])), "BCPL_RTV_Claim*.csv")))
    if old_csv:
        note("info", "Recall assistance (PRN)",
             "This tab now reads the corrected <code>recall_assistance_fee</code> "
             "extract. The earlier "
             + ", ".join(f"<code>{os.path.basename(p)}</code>" for p in old_csv)
             + " claim drop is <b>no longer read</b>: it applied a recall fee to "
             "returns regardless of whether the manufacturer had a "
             "recall-assistance clause, so it overstated the charge several-fold. "
             "Figures here are lower than that file by design.")

    dupes = con.execute("""
        SELECT count(*) FROM (
            SELECT 1 FROM rtv_month GROUP BY month, mfr_id, item_id HAVING count(*) > 1)
    """).fetchone()[0]
    if dupes:
        note("error", "Recall assistance (PRN)",
             f"{dupes} month/manufacturer/item combinations resolved to more "
             f"than one cube row; totals may be inflated.")

    log(f"  {lines:,} recall lines over {months} month(s), "
        f"{fee:,.0f} charged on {qty_charged:,.0f} of {qty:,.0f} units "
        f"({contracts:,} contracts)")
    return {"recall_lines": lines, "recall_months": months}


# --------------------------------------------------------------------------
# 6. contracts master -- insights + details
# --------------------------------------------------------------------------

# The commercial clause catalogue. Each entry is
#   (clause_key, label, group, enable_column, value_column, value_unit)
# `enable_column` is the Yes/No/null switch; `value_column` is the negotiated
# term that only means anything when the switch is on (None where the clause has
# no separate term). This one list drives the clause-adoption chart, the term
# distributions, and the column order of the contract detail drawer -- so a new
# clause upstream is one line here, not an edit in four places.
CLAUSES = [
    ("kam_support", "KAM support", "Support fees",
     "kam_support", "kam_charges", "Rs/month"),
    ("recall_assistance_fees", "Recall assistance", "Support fees",
     "recall_assistance_fees", "recall_charges", "Rs"),
    ("early_pay_enable", "Early payment discount", "Support fees",
     "early_pay_enable", None, None),
    ("advance_payment", "Advance payment", "Support fees",
     "advance_payment", None, None),

    ("defective_product_penalty", "Defective product penalty", "Penalties",
     "defective_product_penalty", None, None),
    ("fill_rate_penalty", "Fill-rate penalty", "Penalties",
     "fill_rate_penalty", "fill_rate_penalty_percent", "%"),
    ("po_clubbing", "PO clubbing", "Penalties",
     "po_clubbing", "po_clubbing_penalty", "%"),
    ("complaints_penalty_enable", "Complaints penalty", "Penalties",
     "complaints_penalty_enable", None, None),
    ("complaints_penalty_festive", "Complaints penalty (festive)", "Penalties",
     "complaints_penalty_festive", None, None),
    ("quality_check_penalty_festive_enable", "QC penalty (festive)", "Penalties",
     "quality_check_penalty_festive_enable", "quality_check_penalty_festive_percent", "%"),
    ("delivery_timeline_default_festive", "Delivery-timeline default (festive)", "Penalties",
     "delivery_timeline_default_festive", None, None),
    ("ullage_enable", "Ullage allowance", "Penalties",
     "ullage_enable", "ullage_threshold_percent", "%"),

    ("damages_lost_provision", "Damages & lost provision", "Risk & recovery",
     "damages_lost_provision", "damage_tolerance_percent", "%"),
    ("right_to_set_off", "Right to set off", "Risk & recovery",
     "right_to_set_off", None, None),
    ("pod_aligned", "POD aligned", "Risk & recovery",
     "pod_aligned", None, None),

    ("sp_benchmarking", "SP benchmarking", "Commercials",
     "sp_benchmarking", "sp_benchmark_aligned_rm_percent", "%"),
    ("min_ads_spend_enabled", "Minimum ads spend", "Commercials",
     "min_ads_spend_enabled", "min_ads_spend_value_percent", "%"),
    ("target_based_incentives", "Target-based incentives", "Commercials",
     "target_based_incentives", None, None),
    ("purchase_margin_off_invoice_enable", "Purchase margin off-invoice", "Commercials",
     "purchase_margin_off_invoice_enable", None, None),
]

# Small-cardinality descriptive columns worth a breakdown chart on the
# Contracts tab. (column, label)
CONTRACT_DIMS = [
    ("contract_state", "Contract state"),
    ("type_of_purchase", "Type of purchase"),
    ("company_buyer_name", "Buying entity"),
    ("purchase_margin_computation", "Purchase margin basis"),
    ("damage_price_based_on", "Damage price basis"),
    ("min_ads_basis", "Minimum ads-spend basis"),
    ("payout_frequency", "Payout frequency"),
    ("valid_until_cancelled", "Valid until cancelled"),
]

# Numeric negotiated terms whose distribution is interesting on its own.
CONTRACT_TERMS = [
    ("rtv_days", "RTV days", "days"),
    ("rtv_days_max", "RTV days (max)", "days"),
    ("credit_days_purchaser", "Credit days - purchaser", "days"),
    ("credit_days_brand_fund", "Credit days - brand fund", "days"),
    ("credit_days_ads_business", "Credit days - ads business", "days"),
    ("damage_tolerance_percent", "Damage tolerance", "%"),
    ("damage_percent_landing_price", "Damage % of landing price", "%"),
    ("fill_rate_threshold_percent", "Fill-rate threshold", "%"),
    ("fill_rate_penalty_percent", "Fill-rate penalty", "%"),
    ("sp_benchmark_aligned_rm_percent", "SP benchmark aligned RM", "%"),
    ("min_ads_spend_value_percent", "Minimum ads spend", "%"),
]


def build_contracts(con, paths):
    """Contracts master + the clause-adoption and term-distribution rollups."""
    log("reading contracts master")
    con.execute(f"""
        CREATE OR REPLACE TABLE contracts AS
        SELECT *, {norm('manufacturer')} AS mfr_key
        FROM read_parquet({sql_list(paths)})
    """)
    n = con.execute("SELECT count(*) FROM contracts").fetchone()[0]
    log(f"  contracts: {n} rows")

    cols = {r[0] for r in con.execute("DESCRIBE SELECT * FROM contracts").fetchall()}

    # A Yes/No switch stored as text arrives with 'NaN' from the pandas write
    # path as well as real NULLs; treat both as "not set" rather than as "No",
    # because "not set" and "explicitly declined" are different commercial facts.
    def yes(c):
        return f"upper(trim(COALESCE(CAST({c} AS VARCHAR), ''))) = 'YES'"

    def no(c):
        return f"upper(trim(COALESCE(CAST({c} AS VARCHAR), ''))) = 'NO'"

    # --- clause adoption, long format: one row per clause.
    log("  contract_clauses")
    con.execute("""
        CREATE OR REPLACE TABLE contract_clauses
        (clause VARCHAR, label VARCHAR, clause_group VARCHAR,
         enable_col VARCHAR, value_col VARCHAR, unit VARCHAR,
         enabled INTEGER, declined INTEGER, not_set INTEGER,
         contracts INTEGER, pct_enabled DOUBLE,
         enabled_approved INTEGER, modal_value VARCHAR)
    """)
    for key, label, grp, ecol, vcol, unit in CLAUSES:
        if ecol not in cols:
            note("warn", "Contracts master",
                 f"Clause column `{ecol}` is missing from the extract; "
                 f"'{label}' is omitted from the clause breakdown.")
            continue
        modal = "NULL"
        if vcol and vcol in cols:
            modal = (f"(SELECT CAST(mode({vcol}) AS VARCHAR) FROM contracts "
                     f"WHERE {yes(ecol)} AND {vcol} IS NOT NULL)")
        con.execute(f"""
            INSERT INTO contract_clauses
            SELECT ?, ?, ?, ?, ?, ?,
                   count(*) FILTER (WHERE {yes(ecol)}),
                   count(*) FILTER (WHERE {no(ecol)}),
                   count(*) FILTER (WHERE NOT {yes(ecol)} AND NOT {no(ecol)}),
                   count(*),
                   round(100.0 * count(*) FILTER (WHERE {yes(ecol)}) / nullif(count(*),0), 1),
                   count(*) FILTER (WHERE {yes(ecol)} AND contract_state = 'APPROVED'),
                   {modal}
            FROM contracts
        """, [key, label, grp, ecol, vcol or "", unit or ""])

    # --- descriptive dimension breakdowns, long format so the UI reads one table.
    log("  contract_dims")
    con.execute("""
        CREATE OR REPLACE TABLE contract_dims
        (dim VARCHAR, label VARCHAR, value VARCHAR,
         contracts INTEGER, mfrs INTEGER, approved INTEGER)
    """)
    for col, label in CONTRACT_DIMS:
        if col not in cols:
            continue
        con.execute(f"""
            INSERT INTO contract_dims
            SELECT ?, ?,
                   COALESCE(NULLIF(NULLIF(trim(CAST({col} AS VARCHAR)), ''), 'NaN'),
                            'Not set') AS value,
                   count(*), count(DISTINCT manufacturer_id),
                   count(*) FILTER (WHERE contract_state = 'APPROVED')
            FROM contracts GROUP BY 3 ORDER BY 4 DESC
        """, [col, label])

    # --- negotiated numeric terms. Kept as a value->count distribution rather
    # than mean/median: these are negotiated to round numbers, so the modal
    # values are the story and an average of them would be meaningless.
    log("  contract_terms")
    con.execute("""
        CREATE OR REPLACE TABLE contract_terms
        (term VARCHAR, label VARCHAR, unit VARCHAR, value DOUBLE,
         contracts INTEGER, approved INTEGER)
    """)
    for col, label, unit in CONTRACT_TERMS:
        if col not in cols:
            continue
        con.execute(f"""
            INSERT INTO contract_terms
            SELECT ?, ?, ?, TRY_CAST({col} AS DOUBLE),
                   count(*), count(*) FILTER (WHERE contract_state = 'APPROVED')
            FROM contracts
            WHERE TRY_CAST({col} AS DOUBLE) IS NOT NULL
            GROUP BY 4 ORDER BY 4
        """, [col, label, unit])

    # --- execution timeline. Contract signing is lumpy; a monthly count by
    # state shows both volume and how much of it ever got approved.
    log("  contract_timeline")
    con.execute("""
        CREATE OR REPLACE TABLE contract_timeline AS
        SELECT strftime(CAST(execution_date AS DATE), '%Y-%m') AS month,
               CAST(date_trunc('month', CAST(execution_date AS DATE)) AS DATE) AS month_date,
               contract_state,
               count(*) AS contracts,
               count(DISTINCT manufacturer_id) AS mfrs
        FROM contracts
        WHERE execution_date IS NOT NULL
        GROUP BY ALL ORDER BY month
    """)

    # --- per-contract clause density: how many commercial clauses each contract
    # actually switches on. Surfaces the thin contracts.
    enabled_sum = " + ".join(
        f"CASE WHEN {yes(e)} THEN 1 ELSE 0 END"
        for _k, _l, _g, e, _v, _u in CLAUSES if e in cols)
    log("  contract_list")
    con.execute(f"""
        CREATE OR REPLACE TABLE contract_list AS
        SELECT contract_id, manufacturer_id AS mfr_id, manufacturer AS mfr, mfr_key,
               company_buyer_name AS buyer,
               contract_state,
               CAST(execution_date AS DATE) AS execution_date,
               CAST(effective_date AS DATE) AS effective_date,
               CAST(updated_at AS DATE)     AS updated_at,
               type_of_purchase,
               valid_until_cancelled,
               payout_frequency,
               purchase_margin_computation,
               TRY_CAST(rtv_days AS DOUBLE)              AS rtv_days,
               TRY_CAST(credit_days_purchaser AS DOUBLE) AS credit_days,
               {yes('kam_support')}              AS has_kam,
               TRY_CAST(kam_charges AS DOUBLE)   AS kam_charges,
               {yes('defective_product_penalty')} AS has_defect_penalty,
               {yes('fill_rate_penalty')}         AS has_fill_rate_penalty,
               {yes('recall_assistance_fees')}    AS has_recall,
               {yes('sp_benchmarking')}           AS has_sp_benchmarking,
               {yes('min_ads_spend_enabled')}     AS has_min_ads,
               ({enabled_sum})                    AS clauses_enabled
        FROM contracts
    """)
    return {"contracts": n, "clause_count": len(CLAUSES)}


# --------------------------------------------------------------------------
# orchestration
# --------------------------------------------------------------------------

def _scalar(con, sql, default=None):
    """Single value, or `default` on any failure (missing table included)."""
    try:
        v = con.execute(sql).fetchone()
        return v[0] if v and v[0] is not None else default
    except Exception:
        return default


# Recall KPI keys, defined once so a full build and `--only recall` cannot drift.
def recall_kpis(con):
    return {
        "rtv_fee":         _scalar(con, "SELECT round(sum(fee),2) FROM rtv_month", 0),
        "rtv_qty":         _scalar(con, "SELECT sum(qty) FROM rtv_month", 0),
        "rtv_qty_charged": _scalar(con, "SELECT sum(qty_charged) FROM rtv_month", 0),
        # Returns with no contract rate behind them: unknown fee, NOT Rs 0.
        "rtv_qty_unrated": _scalar(con, "SELECT sum(qty_unrated) FROM rtv_month", 0),
        "rtv_lines":       _scalar(con, "SELECT sum(lines) FROM rtv_month", 0),
        "rtv_lines_charged": _scalar(con, "SELECT sum(lines_charged) FROM rtv_month", 0),
        "rtv_months":      _scalar(con, "SELECT count(*) FROM rtv_month_total", 0),
        "rtv_first_month": _scalar(con, "SELECT min(month) FROM rtv_month_total", ""),
        "rtv_last_month":  _scalar(con, "SELECT max(month) FROM rtv_month_total", ""),
        "rtv_mfrs":        _scalar(con, "SELECT count(DISTINCT mfr_id) FROM rtv_month", 0),
        "rtv_mfrs_charged": _scalar(con, "SELECT count(DISTINCT mfr_id) FROM rtv_month "
                                        "WHERE qty_charged > 0", 0),
        "rtv_items":       _scalar(con, "SELECT count(DISTINCT item_id) FROM rtv_month", 0),
        "rtv_items_charged": _scalar(con, "SELECT count(DISTINCT item_id) FROM rtv_month "
                                         "WHERE qty_charged > 0", 0),
        "rtv_contracts":   _scalar(con, "SELECT count(DISTINCT contract_id) FROM rtv_month", 0),
        # Fee on lines the extract flags as not RTV-eligible -- surfaced as a KPI
        # because it is a material share of the total, not a rounding footnote.
        "rtv_fee_ineligible": _scalar(con, "SELECT round(sum(fee_ineligible),2) "
                                           "FROM rtv_month", 0),
        # The modal contractual rate -- the one that explains most of the money.
        "rtv_rate_top":    _scalar(con, "SELECT rate FROM rtv_rate ORDER BY fee DESC LIMIT 1", ""),
        "rtv_rate_eff":    _scalar(con, "SELECT round(sum(fee)/NULLIF(sum(qty_charged),0),2) "
                                        "FROM rtv_month", 0),
    }


# KAM KPI keys, defined once so a full build and `--only kam` cannot drift.
def kam_kpis(con):
    return {
        "kam_contracts":   _scalar(con, "SELECT count(*) FROM kam_contracts", 0),
        "kam_approved":    _scalar(con, "SELECT count(*) FROM kam_contracts "
                                        "WHERE contract_state = 'APPROVED'", 0),
        "kam_fee_to_date": _scalar(con, "SELECT round(sum(fee),2) FROM kam_month", 0),
        "kam_fee_to_date_approved":
                           _scalar(con, "SELECT round(sum(fee_approved),2) "
                                        "FROM kam_month_total", 0),
        "kam_fee_latest":  _scalar(con, "SELECT round(sum(fee),2) FROM kam_month "
                                        "WHERE month = (SELECT max(month) FROM kam_month)", 0),
        "kam_months":      _scalar(con, "SELECT count(*) FROM kam_month_total", 0),
        "kam_first_month": _scalar(con, "SELECT min(month) FROM kam_month", ""),
        "kam_last_month":  _scalar(con, "SELECT max(month) FROM kam_month", ""),
    }


# Contract KPI keys, defined once so a full build and `--only contracts` cannot
# drift. `con_clauses_avg` is still written even though the tile that showed it was
# removed: it is one scalar, and a key the UI stops reading is cheaper than a key
# the UI starts wanting back.
def contract_kpis(con):
    return {
        "con_contracts":   _scalar(con, "SELECT count(*) FROM contract_list", 0),
        "con_mfrs":        _scalar(con, "SELECT count(DISTINCT mfr_id) FROM contract_list", 0),
        "con_approved":    _scalar(con, "SELECT count(*) FROM contract_list "
                                        "WHERE contract_state = 'APPROVED'", 0),
        "con_pending":     _scalar(con, "SELECT count(*) FROM contract_list "
                                        "WHERE contract_state = 'PENDING APPROVAL'", 0),
        "con_expired":     _scalar(con, "SELECT count(*) FROM contract_list "
                                        "WHERE contract_state = 'EXPIRED'", 0),
        "con_clauses_avg": _scalar(con, "SELECT round(avg(clauses_enabled),1) "
                                        "FROM contract_list", 0),
        "con_first_exec":  _scalar(con, "SELECT CAST(min(execution_date) AS VARCHAR) "
                                        "FROM contract_list", ""),
        "con_last_exec":   _scalar(con, "SELECT CAST(max(execution_date) AS VARCHAR) "
                                        "FROM contract_list", ""),
        "con_last_update": _scalar(con, "SELECT CAST(max(updated_at) AS VARCHAR) "
                                        "FROM contract_list", ""),
    }


def _refresh_kpis_extra(con, d):
    """Upsert one section's keys into an existing kpis_extra table."""
    con.execute("CREATE TABLE IF NOT EXISTS kpis_extra (key VARCHAR, value VARCHAR)")
    for k, v in d.items():
        con.execute("DELETE FROM kpis_extra WHERE key = ?", [k])
        con.execute("INSERT INTO kpis_extra VALUES (?, ?)", [k, str(v)])


def _refresh_recall_kpis(con):
    """Upsert just the recall keys into an existing kpis_extra table."""
    con.execute("CREATE TABLE IF NOT EXISTS kpis_extra (key VARCHAR, value VARCHAR)")
    for k, v in recall_kpis(con).items():
        con.execute("DELETE FROM kpis_extra WHERE key = ?", [k])
        con.execute("INSERT INTO kpis_extra VALUES (?, ?)", [k, str(v)])


def src_name(path, data_dir):
    """How a source file is named on the Data tab: relative to the data dir.

    Not basename: monthly folders restart part numbering, so two different months
    both hold a `_part001`, and a bare basename would make the provenance list
    look like a duplicate download. Files sitting in the data dir itself still
    render as a plain filename.
    """
    try:
        return os.path.relpath(path, data_dir)
    except ValueError:
        return os.path.basename(path)


def _refresh_files(con, key, paths, data_dir):
    """Keep the Data tab's provenance list honest after a partial rebuild."""
    try:
        con.execute("DELETE FROM build_files WHERE dataset = ?", [key])
    except Exception:
        return
    for p in paths:
        con.execute("INSERT INTO build_files VALUES (?, ?, ?, ?)",
                    [key, DATASETS[key][0], src_name(p, data_dir),
                     round(os.path.getsize(p) / 1e6, 2)])


# The wide single-row KPI table. Defined once so a full build and a partial
# rebuild of either input it reads (cities, sat fees) cannot drift apart.
def build_kpis(con):
    con.execute("""
        CREATE OR REPLACE TABLE kpis AS
        SELECT
            (SELECT count(*) FROM city_master)                                  AS master_cities,
            (SELECT count(*) FROM city_stats WHERE is_satellite AND has_fees)   AS satellite_cities_billing,
            (SELECT count(*) FROM city_stats WHERE NOT is_satellite)            AS offlist_cities,
            (SELECT round(min(distance_km),1) FROM city_master)                 AS min_distance,
            (SELECT round(max(distance_km),1) FROM city_master)                 AS max_distance,
            (SELECT round(avg(distance_km),1) FROM city_master)                 AS avg_distance,
            (SELECT round(sum(fee),2) FROM sat_cube)                            AS sat_fee_all,
            (SELECT round(sum(fee),2) FROM sat_cube WHERE is_satellite)         AS sat_fee_onlist,
            (SELECT round(sum(fee),2) FROM sat_cube WHERE NOT is_satellite)     AS sat_fee_offlist,
            (SELECT sum(net_qty) FROM sat_cube WHERE is_satellite)              AS sat_qty_onlist,
            (SELECT count(DISTINCT mfr_id) FROM sat_cube WHERE is_satellite)    AS sat_mfrs,
            (SELECT count(DISTINCT variant_id) FROM sat_cube WHERE is_satellite) AS sat_items,
            (SELECT min(day) FROM sat_daily)                                    AS sat_first_day,
            (SELECT max(day) FROM sat_daily)                                    AS sat_last_day
    """)


def compact_db(path):
    """Rewrite the DB into a fresh file so freed pages are actually released.

    `rebuild_part` replaces tables in place, and DuckDB does not return the old
    row groups' space to the OS -- rewriting the satellite section took the file
    from 377 MB to 986 MB with no extra data behind the growth beyond one month.
    That matters here in a way it would not for a scratch DB: this file is baked
    into the deploy image, so the bloat is paid on every `chef skaffold up` as
    docker context transfer. `COPY FROM DATABASE` costs ~16 s and gave 426 MB.

    Numbers cannot move: it is a table-for-table copy, verified on table count
    before the swap. On any failure the original is left exactly as it was.
    """
    tmp = path + ".compacting"
    before = os.path.getsize(path)
    for p in (tmp, tmp + ".wal"):
        if os.path.exists(p):
            os.remove(p)
    con = duckdb.connect()
    try:
        # ATTACH takes no bind parameters, so the paths are quoted by hand. Both
        # are ours (argv/derived), not user input, but escape anyway.
        def lit(p):
            return "'" + p.replace("'", "''") + "'"
        con.execute(f"ATTACH {lit(path)} AS src (READ_ONLY)")
        con.execute(f"ATTACH {lit(tmp)} AS dst")
        con.execute("COPY FROM DATABASE src TO dst")
        # duckdb_tables(), not information_schema: the latter is not addressable
        # across attached databases.
        n_src, n_dst = (con.execute(
            "SELECT count(*) FILTER (WHERE database_name = 'src'), "
            "       count(*) FILTER (WHERE database_name = 'dst') "
            "FROM duckdb_tables()").fetchone())
    finally:
        con.close()
    if n_src != n_dst:
        # Logged, not a build note: this is disk footprint, not a caveat about a
        # number, and the notes have already been written by this point anyway.
        os.remove(tmp)
        log(f"  WARNING: compaction copied {n_dst} of {n_src} tables and was "
            f"abandoned. The DB is intact at {before/1e6:.0f} MB, just larger "
            f"than it needs to be.")
        return
    for p in (path, path + ".wal"):
        if os.path.exists(p):
            os.remove(p)
    os.rename(tmp, path)
    log(f"  compacted {before/1e6:.0f} MB -> {os.path.getsize(path)/1e6:.0f} MB "
        f"({n_dst} tables)")


def rebuild_part(data_dir, out_path, part):
    """Rebuild ONE section in place, against the existing DB.

    The satellite pass reads 1.1 GB and takes ~3 minutes; iterating on the
    defect tokenizer or the KAM accrual should not pay that cost. This opens the
    live DB read-write and replaces only that section's tables. Build notes from
    the section are merged back into build_notes.

    Not a substitute for a full build: KPIs and provenance are refreshed for the
    part, but a section whose inputs changed shape still deserves `--only all`.
    """
    data_dir = os.path.abspath(data_dir)
    out_path = os.path.abspath(out_path)
    if not os.path.exists(out_path):
        raise SystemExit(f"{out_path} does not exist -- run a full build first.")
    _notes.clear()
    t0 = time.time()
    size_before = os.path.getsize(out_path)
    con = duckdb.connect(out_path)
    con.execute(f"SET memory_limit = '{duckdb_memory()}'")
    tmpdir = os.environ.get("DUCKDB_TMP", out_path + ".tmp")
    os.makedirs(tmpdir, exist_ok=True)
    con.execute(f"SET temp_directory = '{tmpdir}'")
    # `--only sat` spills as hard as a full build does, so give it the same room.
    con.execute("SET max_temp_directory_size = '20GB'")

    paths = {k: readable(resolve(data_dir, k))[0] for k in DATASETS}
    if part == "defects":
        build_defects(con, paths["defects"])
    elif part == "kam":
        # KAM reads the contracts extract too (that is where kam_charges lives), so
        # its provenance covers both files.
        stats = build_kam(con, paths["kam"], paths["contracts"]) or {}
        _refresh_kpis_extra(con, kam_kpis(con))
        _refresh_files(con, "kam", paths["kam"], data_dir)
        if stats.get("kam_end"):
            con.execute("DELETE FROM build_meta WHERE key = 'kam_accrual_end'")
            con.execute("INSERT INTO build_meta VALUES ('kam_accrual_end', ?)",
                        [str(stats["kam_end"])])
    elif part == "contracts":
        build_contracts(con, paths["contracts"])
        _refresh_kpis_extra(con, contract_kpis(con))
        _refresh_files(con, "contracts", paths["contracts"], data_dir)
    elif part == "cities":
        build_cities(con, paths["cities"])
    elif part == "recall":
        if not paths["recall"]:
            con.close()
            raise SystemExit(f"No file matching {DATASETS['recall'][1]} in {data_dir}")
        build_recall(con, paths["recall"])
        _refresh_recall_kpis(con)
        _refresh_files(con, "recall", paths["recall"], data_dir)
    elif part == "sat":
        # The one section that costs real time (~3 min, 2.2 GB read), which is
        # exactly why it needs a partial path of its own: a new month of fee
        # actuals must not force a rebuild of every other tab off whatever else
        # happens to be sitting in ../data today.
        if not paths["sat_fees"]:
            con.close()
            raise SystemExit(f"No file matching {DATASETS['sat_fees'][1]} in "
                             f"{data_dir} or its subfolders")
        build_sat(con, paths["sat_fees"])
        # sat_cube/sat_daily and city_stats both moved, and the headline row reads
        # all three, so it is rewritten in full from the shared definition.
        build_kpis(con)
        _refresh_files(con, "sat_fees", paths["sat_fees"], data_dir)
        con.execute("DELETE FROM build_meta WHERE key = 'source_bytes'")
        con.execute("INSERT INTO build_meta VALUES ('source_bytes', ?)", [str(
            _scalar(con, "SELECT CAST(round(sum(size_mb) * 1e6) AS BIGINT) "
                         "FROM build_files", 0))])
    else:
        con.close()
        raise SystemExit(f"--only {part} is not a partial-rebuildable section "
                         f"(use: sat, cities, defects, kam, contracts, recall, "
                         f"or all)")

    # Replace this section's notes only, so the other sections' caveats survive.
    labels = {"defects": ["Defective returns"],
              "kam": ["KAM support fees"],
              "contracts": ["Contracts master"],
              "cities": ["Satellite city master list"],
              # the fee extract owns the city-validation notes too: how much fee
              # sits off the master list is a property of this month's actuals.
              "sat": ["Satellite fee actuals", "Satellite city validation"],
              # both labels: a partial rebuild has to clear the notes left by
              # the superseded RTV-claim source as well as its own.
              "recall": ["Recall assistance (PRN)",
                         "Recall assistance (RTV claim)"]}[part]
    for lb in labels:
        con.execute("DELETE FROM build_notes WHERE label = ?", [lb])
    for lvl, label, txt in _notes:
        con.execute("INSERT INTO build_notes VALUES (?, ?, ?)", [lvl, label, txt])
    con.execute("DELETE FROM build_meta WHERE key = 'built_at'")
    con.execute("INSERT INTO build_meta VALUES ('built_at', ?)",
                [time.strftime("%Y-%m-%d %H:%M:%S") + f" ({part} rebuilt)"])
    con.close()

    # Only when the in-place rewrite actually bloated the file. Gating on measured
    # growth keeps `--only recall` at its advertised ~2 s instead of paying a
    # 16-second compaction to reclaim nothing.
    if os.path.getsize(out_path) > size_before * 1.2:
        log("  file grew materially -- compacting")
        compact_db(out_path)

    # Stamped last, and after the compaction, so it is the time this rebuild
    # actually took. Leaving the previous full build's value in place made the Data
    # tab footer read "(sat rebuilt) in 188.6s" for a 232-second rebuild.
    elapsed = time.time() - t0
    con = duckdb.connect(out_path)
    con.execute("DELETE FROM build_meta WHERE key = 'build_seconds'")
    con.execute("INSERT INTO build_meta VALUES ('build_seconds', ?)", [f"{elapsed:.1f}"])
    con.close()

    log(f"rebuilt '{part}' in {elapsed:.1f}s -- restart app.py to pick it up")


def build(data_dir, out_path):
    t0 = time.time()
    _notes.clear()
    data_dir = os.path.abspath(data_dir)
    out_path = os.path.abspath(out_path)
    log(f"data dir: {data_dir}")

    resolved, sizes = {}, {}
    for key, (label, _pat, _multi) in DATASETS.items():
        found = resolve(data_dir, key)
        ok, bad = readable(found)
        resolved[key] = ok
        sizes[key] = sum(os.path.getsize(p) for p in ok)
        for name, why in bad:
            note("error", label, f"{name} is unreadable and was skipped ({why}). "
                                 f"Totals for this dataset understate reality.")
        if not ok:
            lvl = "error" if key in REQUIRED else "warn"
            note(lvl, label, f"No file matching {_pat} in {data_dir}. "
                             f"{'This is required -- the build cannot continue.' if key in REQUIRED else 'That section will be empty.'}")
        else:
            log(f"  {label}: {len(ok)} file(s), {sizes[key]/1e6:.1f} MB")

    missing_required = [k for k in REQUIRED if not resolved[k]]
    if missing_required:
        raise SystemExit(
            "Cannot build: missing required dataset(s) "
            + ", ".join(DATASETS[k][0] for k in missing_required))

    # Build into a temp file and swap at the end, so a failed or interrupted
    # rebuild leaves the previously-served DB untouched.
    tmp = out_path + ".building"
    for p in (tmp, tmp + ".wal"):
        if os.path.exists(p):
            os.remove(p)

    con = duckdb.connect(tmp)
    # This build aggregates 42M rows into a 2.1M-row cube on a 8 GB box, so it
    # WILL spill. Give DuckDB a real temp directory and a limit well under
    # physical RAM -- set it at or above RAM and the OS OOM-killer wins the race
    # before DuckDB ever decides to spill.
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET memory_limit = '{duckdb_memory()}'")
    tmpdir = os.environ.get("DUCKDB_TMP", tmp + ".tmp")
    os.makedirs(tmpdir, exist_ok=True)
    con.execute(f"SET temp_directory = '{tmpdir}'")
    con.execute("SET max_temp_directory_size = '20GB'")

    stats = {}
    build_cities(con, resolved["cities"])
    stats.update(build_sat(con, resolved["sat_fees"]))

    if resolved["defects"]:
        try:
            build_defects(con, resolved["defects"])
        except Exception:
            note("error", "Defective returns",
                 "Rollup failed; the Defective Products tab will be empty. "
                 + traceback.format_exc(limit=1).strip().split("\n")[-1][:200])
    if resolved["kam"]:
        try:
            stats.update(build_kam(con, resolved["kam"], resolved["contracts"]))
        except Exception:
            note("error", "KAM support fees",
                 "Rollup failed; the KAM Fees tab will be empty. "
                 + traceback.format_exc(limit=1).strip().split("\n")[-1][:200])
    if resolved["contracts"]:
        try:
            stats.update(build_contracts(con, resolved["contracts"]))
        except Exception:
            note("error", "Contracts master",
                 "Rollup failed; the Contracts tab will be empty. "
                 + traceback.format_exc(limit=1).strip().split("\n")[-1][:200])
    if resolved["recall"]:
        try:
            stats.update(build_recall(con, resolved["recall"]))
        except Exception:
            note("error", "Recall assistance (RTV claim)",
                 "Rollup failed; the Recall Assistance tab will be empty. "
                 + traceback.format_exc(limit=1).strip().split("\n")[-1][:200])

    # --- headline KPIs, one row, so the UI's first paint is a single cheap read.
    log("building kpis")
    build_kpis(con)

    # Defect / KAM / contract KPIs live in their own single-row tables, keyed
    # name -> value, so a dataset that failed to build leaves its keys absent
    # instead of breaking the satellite KPI row. scalar() returns the default on
    # any failure (missing table included), which is the whole point.
    def scalar(sql, default=None):
        return _scalar(con, sql, default)

    con.execute("CREATE OR REPLACE TABLE kpis_extra (key VARCHAR, value VARCHAR)")
    extra = {
        # defects
        "def_qty":         scalar("SELECT sum(qty) FROM def_item", 0),
        "def_fee":         scalar("SELECT round(sum(fee),2) FROM def_item", 0),
        "def_complaints":  scalar("SELECT sum(complaints) FROM def_item", 0),
        "def_items":       scalar("SELECT count(*) FROM def_item", 0),
        "def_mfrs":        scalar("SELECT count(*) FROM def_mfr", 0),
        "def_types":       scalar("SELECT count(*) FROM def_type", 0),
        "def_rate":        DEFECT_RATE,
        "def_first_day":   scalar("SELECT CAST(min(day) AS VARCHAR) FROM def_daily", ""),
        "def_last_day":    scalar("SELECT CAST(max(day) AS VARCHAR) FROM def_daily", ""),
        "def_terms":       scalar("SELECT count(*) FROM def_terms", 0),
    }
    # Each section's keys come from the same helper its `--only` path uses, so a
    # partial rebuild can never leave a tile disagreeing with the table under it.
    extra.update(kam_kpis(con))
    extra.update(contract_kpis(con))
    extra.update(recall_kpis(con))
    for k, v in extra.items():
        con.execute("INSERT INTO kpis_extra VALUES (?, ?)", [k, str(v)])

    # --- provenance. Which file produced which number, so any figure on screen
    # can be traced back to a download.
    con.execute("CREATE OR REPLACE TABLE build_files "
                "(dataset VARCHAR, label VARCHAR, file VARCHAR, size_mb DOUBLE)")
    for key, paths in resolved.items():
        for p in paths:
            con.execute("INSERT INTO build_files VALUES (?, ?, ?, ?)",
                        [key, DATASETS[key][0], src_name(p, data_dir),
                         round(os.path.getsize(p) / 1e6, 2)])

    con.execute("CREATE OR REPLACE TABLE build_notes "
                "(level VARCHAR, label VARCHAR, note VARCHAR)")
    for lvl, label, txt in _notes:
        con.execute("INSERT INTO build_notes VALUES (?, ?, ?)", [lvl, label, txt])

    con.execute("CREATE OR REPLACE TABLE build_meta (key VARCHAR, value VARCHAR)")
    for k, v in [
        ("built_at", time.strftime("%Y-%m-%d %H:%M:%S")),
        ("build_seconds", f"{time.time() - t0:.1f}"),
        ("data_dir", data_dir),
        ("defect_rate", f"{DEFECT_RATE:g}"),
        ("kam_accrual_end", str(stats.get("kam_end", ""))),
        ("source_bytes", str(sum(sizes.values()))),
    ]:
        con.execute("INSERT INTO build_meta VALUES (?, ?)", [k, v])

    con.close()

    # Swap. Remove the stale WAL alongside the old DB or DuckDB will try to
    # replay it against the new file.
    for p in (out_path, out_path + ".wal"):
        if os.path.exists(p):
            os.remove(p)
    os.rename(tmp, out_path)
    shutil.rmtree(tmp + ".tmp", ignore_errors=True)

    log(f"wrote {out_path} ({os.path.getsize(out_path)/1e6:.1f} MB) "
        f"in {time.time() - t0:.1f}s")
    if _notes:
        log(f"{len(_notes)} build note(s) -- see the Data tab")
    return out_path


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--data", default=DEFAULT_DATA, help="directory of parquet extracts")
    ap.add_argument("--out", default=DEFAULT_OUT, help="output duckdb path")
    ap.add_argument("--only", default="all",
                    choices=["all", "sat", "cities", "defects", "kam",
                             "contracts", "recall"],
                    help="rebuild one section in place instead of everything. "
                         "Everything but 'sat' skips the ~3 min satellite pass; "
                         "'sat' is how you load a new month of fee actuals "
                         "without disturbing the other tabs")
    a = ap.parse_args()
    if a.only == "all":
        build(a.data, a.out)
    else:
        rebuild_part(a.data, a.out, a.only)


if __name__ == "__main__":
    main()
