"""
Build the analytics database for the Contracts & Fees dashboard.

Reads the raw parquet files in a data directory and pre-aggregates the large
fact tables (39.6M-row satellite fee actuals, 763K-row defective returns) into
compact rollup tables stored in a DuckDB file. The API server (app.py) then
serves filtered queries from these small tables instantly.

The raw parquet inputs are organised as a fixed set of logical *datasets*
(see DATASETS below). Each maps to a filename glob; large facts (satellite
fees, defective returns) are multi-part. The dashboard's "Manage Data" tab
uploads files into an uploads directory and calls build(uploads_dir, db_path)
to regenerate the DB — the same code path used by the CLI (which reads
../data).

Satellite fee semantics (confirmed from the source notebook):
  * fee_amt            -- the PER-UNIT satellite fee rate (₹/unit), from terms.
  * sat_fee_applicable -- net_qty_sold * fee_amt, but only when the item's MRP
                          meets the `bucket` threshold; else 0. i.e. the actual
                          total satellite fee for the line. This is the money.
  * It is computed for every row regardless of contract_state (PENDING
    APPROVAL / DRAFT / APPROVED) -- i.e. across all satellite sales, not only
    where the satellite clause is finalised/aligned.
So the headline "satellite fee" totals sum sat_fee_applicable; fee_amt is only
meaningful as an average per-unit rate.

Data (Apr–Jun 2026):
  * satellite_fees_actuals_part*.parquet  -- per-order satellite fee actuals.
  * satellite_cities_extract              -- satellite city -> min back-end
    distance (km). Joined on outlet_city; unmatched cities are local.
  * bcpl_defective_crbs   -- defective-return complaints, penalties & remarks.
  * recall_assistance_prn -- product-recall notices (purchase returns).
  * contracts_extract + term extracts (ads_spend, turnover_discount,
    sp_benchmarking).

Run:  python build_db.py            (builds from ../data)
"""
import os
import glob as _glob
import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.environ.get("DATA_DIR", os.path.join(HERE, "..", "data"))
DB = os.environ.get("DB_PATH", os.path.join(HERE, "insights.duckdb"))

# ----------------------------------------------------------------------
# Dataset registry — the single source of truth for what raw inputs exist,
# how their files are named, and which are multi-part. Shared with app.py so
# uploads are classified and validated against exactly these definitions.
#   key -> (label, filename glob, multi_part)
# `multi_part` datasets accept many files (e.g. satellite_fees_..._part001..N);
# single-part datasets keep exactly one file.
# ----------------------------------------------------------------------
DATASETS = {
    "satellite_fees":   ("Satellite fee actuals",       "satellite_fees_actuals_part*.parquet", True),
    "satellite_cities": ("Satellite cities (distance)", "satellite_cities_extract*.parquet",    False),
    "bcpl":             ("Defective returns (BCPL)",     "bcpl_defective_crbs*.parquet",         True),
    "recall":           ("Recall-assistance PRN",        "recall_assistance_prn*.parquet",       False),
    "contracts":        ("Contracts master",             "contracts_extract*.parquet",           False),
    "ads_spend":        ("Ads-spend extract",            "ads_spend_extract*.parquet",           False),
    "turnover":         ("Turnover-discount extract",    "turnover_discount_extract*.parquet",   False),
    "sp_benchmarking":  ("SP-benchmarking extract",      "sp_benchmarking_extract*.parquet",     False),
}


def resolve(data_dir, key):
    """Concrete list of parquet files for a dataset within data_dir.

    Multi-part datasets return every match (sorted); single-part datasets
    return just the first match so a stray duplicate in the directory can
    never double-read (which, for the cities join, would multiply fee rows).
    Returns [] when nothing matches.
    """
    _label, pat, multi = DATASETS[key]
    matches = sorted(_glob.glob(os.path.join(data_dir, pat)))
    if not matches:
        return []
    return matches if multi else [matches[0]]


def _rd(paths):
    """read_parquet(...) over an explicit list of files."""
    inner = ",".join("'" + p.replace("'", "''") + "'" for p in paths)
    return f"read_parquet([{inner}])"


# Scaffolding + generic stopwords stripped before building the defective-return
# comment word cloud. Genuine defect vocabulary (damaged, broken, leaking, …)
# is deliberately kept — only UI template text, politeness and filler is removed.
STOPWORDS = {
    "the","and","for","are","was","with","this","that","have","has","had","not",
    "but","you","your","yours","our","its","they","them","their","from","were",
    "will","would","can","cannot","cant","could","should","about","there","here",
    "what","when","which","who","why","how","into","out","off","too","very","just",
    "also","than","then","now","got","get","getting","give","given","want","wanted",
    "need","needed","please","pls","plz","ok","okay","okk","yes","yeah","yea","yep",
    "thanks","thank","thankyou","hi","hello","hey","sir","mam","madam","dear","team",
    "order","ordered","item","items","product","products","received","receive",
    "receiving","issue","issues","problem","problems","add","comment","comments",
    "open","camera","refund","refunded","refunds","replace","replacement","approved",
    "approve","pick","picked","pickup","picking","promo","code","payment","method",
    "original","money","rupees","rupee","upi","net","banking","hdfc","icici","card",
    "bank","amount","after","before","time","day","days","one","two","three","per",
    "each","none","null","nan","call","called","calling","chat","help","kindly",
    "soon","already","still","again","back","same","other","others","anymore","much",
    "many","more","most","some","any","all","only","even","such","been","being",
    "does","doing","done","make","made","know","told","said","say","see","seen",
    "com","www","http","https","the","and","was","use","using","due","per","via",
    "hai","nahi","nhi","kar","karo","karna","kiya","raha","rahi","rha","rhi","hota",
    "mera","meri","bhi","jab","fir","phir","koi","liye","hua","diya","kya","kyu",
    "kaise","accha","acha","aur","toh","hain","mai","main","mujhe","tha","thi",
    "sure","doesn","dont","don","didn","isn","wasn","couldn","wouldn","shouldn",
    "won","wont","aren","haven","hasn","doesnt","didnt","cant","its","thats",
    "theres","ive","youre","really","actually","properly","gonna","wanna",
}


# Allowlist of defect-signal words for the "what was the defect" word cloud.
# Only these (and close variants) are counted, so the cloud describes product
# defects rather than generic chatter.
DEFECT_KEYWORDS = {
    "damaged", "damage", "broken", "broke", "breakage", "cracked", "crack",
    "chipped", "shattered", "torn", "tear", "leaking", "leak", "leaked",
    "leakage", "spilled", "spill", "spillage", "missing", "defective", "defect",
    "faulty", "fault", "expired", "expiry", "stale", "spoiled", "spoilt",
    "rotten", "fungus", "fungal", "mould", "mold", "moldy", "smell", "smelly",
    "stinking", "stink", "taste", "tasteless", "bitter", "sour", "melted",
    "melt", "moisture", "soggy", "dented", "dent", "scratch", "scratched",
    "burst", "rusted", "rust", "insect", "insects", "worms", "fungused",
    "quality", "poor", "packaging", "packing", "seal", "unsealed", "dirty",
    "dusty", "crushed", "bent", "loose", "empty", "working", "functioning",
    "wrong", "incorrect", "mismatch", "duplicate", "fake", "counterfeit",
    "quantity", "weight", "useless", "punctured", "deflated", "contaminated",
    "adulterated", "half", "leaked", "leaky", "damages", "cut", "opened",
}


def _stopword_sql():
    return ",".join("'" + w.replace("'", "''") + "'" for w in sorted(STOPWORDS))


def _defect_sql():
    return ",".join("'" + w.replace("'", "''") + "'" for w in sorted(DEFECT_KEYWORDS))


def missing_datasets(data_dir):
    """Return [(key, label), …] for every required dataset with no file present."""
    return [(k, v[0]) for k, v in DATASETS.items() if not resolve(data_dir, k)]


def build(data_dir, db_path, log=print):
    """Build the analytics DB at db_path from raw parquet in data_dir.

    Raises ValueError if any required dataset is absent — the caller (CLI or
    the /api/data/rebuild endpoint) surfaces that to the user.
    """
    miss = missing_datasets(data_dir)
    if miss:
        names = ", ".join(f"{lbl} ({DATASETS[k][1]})" for k, lbl in miss)
        raise ValueError(f"Missing required dataset(s): {names}")

    SAT = resolve(data_dir, "satellite_fees")
    CITIES = resolve(data_dir, "satellite_cities")
    BCPL = resolve(data_dir, "bcpl")
    RECALL = resolve(data_dir, "recall")
    CONTRACTS = resolve(data_dir, "contracts")
    ADS = resolve(data_dir, "ads_spend")
    TURNOVER = resolve(data_dir, "turnover")
    SPBENCH = resolve(data_dir, "sp_benchmarking")

    def rd(paths):
        return _rd(paths)

    if os.path.exists(db_path):
        os.remove(db_path)
    con = duckdb.connect(db_path)
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA disable_progress_bar")
    # Constrained containers: honour a memory ceiling and spill to disk beside
    # the DB rather than OOM-killing the pod on the big satellite aggregation.
    mem = os.environ.get("DUCKDB_MEMORY_LIMIT")
    if mem:
        con.execute(f"PRAGMA memory_limit='{mem}'")
    tmp_dir = os.path.join(os.path.dirname(os.path.abspath(db_path)) or ".", ".duckdb_tmp")
    con.execute(f"PRAGMA temp_directory='{tmp_dir}'")

    # ------------------------------------------------------------------
    # 1. Satellite city fees (actuals)  -- the big fact (~39.6M rows)
    #    fee = sat_fee_applicable (the money); rate = fee_amt (per unit).
    # ------------------------------------------------------------------
    log("Building satellite-fee rollups...")
    con.execute(f"""
        CREATE TEMP VIEW sat AS
        SELECT
            date_trunc('day', f.insert_ds_ist)::DATE      AS day,
            f.outlet_city                                  AS city,
            f.manufacturer_name_x                          AS manufacturer,
            f.facility_name                                AS facility,
            f.contract_state                               AS contract_state,
            CASE WHEN f.product_type = 'Cold' THEN 'Cold chain'
                 ELSE 'Ambient' END                        AS chain,
            f.bucket                                       AS mrp_tier,
            c."BE Min. Distance (km)"                      AS distance_km,
            f.sat_fee_applicable                           AS fee,
            f.fee_amt                                      AS rate,
            f.qty_sold                                     AS qty,
            f.qty_returned                                 AS returned,
            f.net_qty_sold                                 AS net_qty
        FROM {rd(SAT)} f
        LEFT JOIN {rd(CITIES)} c
               ON lower(trim(f.outlet_city)) = lower(trim(c.City))
    """)

    con.execute("""
        CREATE TABLE sat_daily AS
        SELECT day,
               SUM(fee)      AS fee,
               SUM(qty)      AS qty,
               SUM(net_qty)  AS net_qty,
               SUM(returned) AS returned,
               COUNT(*)      AS rows
        FROM sat GROUP BY day ORDER BY day
    """)
    con.execute("""
        CREATE TABLE sat_city AS
        SELECT city,
               SUM(fee)         AS fee,
               SUM(qty)         AS qty,
               SUM(net_qty)     AS net_qty,
               SUM(returned)    AS returned,
               MAX(distance_km) AS distance_km,
               COUNT(*)         AS rows,
               SUM(fee)/NULLIF(SUM(net_qty),0) AS fee_per_unit
        FROM sat GROUP BY city ORDER BY fee DESC
    """)
    con.execute("""
        CREATE TABLE sat_mfr AS
        SELECT manufacturer,
               SUM(fee)     AS fee,
               SUM(qty)     AS qty,
               SUM(net_qty) AS net_qty,
               COUNT(*)     AS rows,
               SUM(fee)/NULLIF(SUM(net_qty),0) AS fee_per_unit
        FROM sat GROUP BY manufacturer ORDER BY fee DESC
    """)
    con.execute("""
        CREATE TABLE sat_facility AS
        SELECT facility,
               SUM(fee)     AS fee,
               SUM(qty)     AS qty,
               SUM(net_qty) AS net_qty,
               COUNT(*)     AS rows
        FROM sat GROUP BY facility ORDER BY fee DESC
    """)
    con.execute("""
        CREATE TABLE sat_split AS
        SELECT chain, mrp_tier,
               SUM(fee) AS fee, SUM(qty) AS qty, COUNT(*) AS rows
        FROM sat GROUP BY 1,2 ORDER BY fee DESC
    """)
    con.execute("""
        CREATE TABLE sat_state AS
        SELECT contract_state,
               SUM(fee) AS fee, SUM(qty) AS qty, COUNT(*) AS rows
        FROM sat GROUP BY 1 ORDER BY fee DESC
    """)

    # all satellite cities (from the master list) with their distance and any
    # fee they generated -- LEFT JOIN keeps cities that produced no fee rows.
    con.execute(f"""
        CREATE TABLE sat_cities_list AS
        SELECT c.City                        AS city,
               c."BE Min. Distance (km)"     AS distance_km,
               COALESCE(s.fee, 0)            AS fee,
               COALESCE(s.qty, 0)            AS qty,
               COALESCE(s.net_qty, 0)        AS net_qty,
               COALESCE(s.rows, 0)           AS rows
        FROM {rd(CITIES)} c
        LEFT JOIN sat_city s
               ON lower(trim(c.City)) = lower(trim(s.city))
        ORDER BY c."BE Min. Distance (km)" DESC
    """)

    # NB: the source pipeline already restricts sales to satellite cities
    # (back-end distance > 100 km), so every row here IS a satellite-city sale
    # and IS charged. Rows land in 'Distance not mapped' only when the city name
    # did not match the 227-row satellite-cities distance list -- not because
    # they are non-satellite / uncharged.
    dist_case = """
        CASE
            WHEN distance_km IS NULL   THEN 'Distance not mapped'
            WHEN distance_km < 150     THEN '100-150 km'
            WHEN distance_km < 200     THEN '150-200 km'
            WHEN distance_km < 250     THEN '200-250 km'
            WHEN distance_km < 350     THEN '250-350 km'
            WHEN distance_km < 500     THEN '350-500 km'
            ELSE '500+ km'
        END
    """
    con.execute(f"""
        CREATE TABLE sat_dist AS
        SELECT {dist_case} AS band,
               SUM(fee) AS fee, SUM(qty) AS qty, SUM(net_qty) AS net_qty,
               COUNT(*) AS rows, AVG(rate) AS avg_rate
        FROM sat GROUP BY 1
    """)

    # drilldown rollups
    con.execute("""
        CREATE TABLE sat_city_mfr AS
        SELECT city, manufacturer, SUM(fee) AS fee, SUM(qty) AS qty
        FROM sat GROUP BY 1,2
    """)
    con.execute("""
        CREATE TABLE sat_city_daily AS
        SELECT city, day, SUM(fee) AS fee, SUM(qty) AS qty
        FROM sat GROUP BY 1,2
    """)
    con.execute("""
        CREATE TABLE sat_mfr_daily AS
        SELECT manufacturer, day, SUM(fee) AS fee, SUM(qty) AS qty
        FROM sat GROUP BY 1,2
    """)

    # day-keyed dimensional rollups -- power the date-range raw-data exports.
    con.execute("""
        CREATE TABLE sat_split_daily AS
        SELECT day, chain, mrp_tier,
               SUM(fee) AS fee, SUM(qty) AS qty, COUNT(*) AS rows
        FROM sat GROUP BY 1,2,3 ORDER BY day
    """)
    con.execute(f"""
        CREATE TABLE sat_dist_daily AS
        SELECT day, {dist_case} AS band,
               SUM(fee) AS fee, SUM(qty) AS qty, COUNT(*) AS rows
        FROM sat GROUP BY 1,2 ORDER BY day
    """)
    con.execute("""
        CREATE TABLE sat_state_daily AS
        SELECT day, contract_state,
               SUM(fee) AS fee, SUM(qty) AS qty, COUNT(*) AS rows
        FROM sat GROUP BY 1,2 ORDER BY day
    """)

    # ------------------------------------------------------------------
    # 2. Defective returns / complaints (bcpl)  -- 763K rows
    # ------------------------------------------------------------------
    log("Building defective-return rollups...")
    con.execute(f"""
        CREATE TEMP VIEW bcpl AS
        SELECT
            date_trunc('day', duration)::DATE AS day,
            manufacturer,
            entity_name,
            item_name,
            complaint_type,
            bad_returns_qty,
            penalty,
            gmv
        FROM {rd(BCPL)}
    """)
    con.execute("""
        CREATE TABLE bcpl_daily AS
        SELECT day,
               SUM(penalty)          AS penalty,
               SUM(bad_returns_qty)  AS returns_qty,
               SUM(gmv)              AS gmv,
               COUNT(*)              AS complaints
        FROM bcpl GROUP BY day ORDER BY day
    """)
    con.execute("""
        CREATE TABLE bcpl_type AS
        SELECT complaint_type,
               SUM(penalty)         AS penalty,
               SUM(bad_returns_qty) AS returns_qty,
               SUM(gmv)             AS gmv,
               COUNT(*)             AS complaints
        FROM bcpl GROUP BY complaint_type ORDER BY penalty DESC
    """)
    con.execute("""
        CREATE TABLE bcpl_mfr AS
        SELECT manufacturer,
               SUM(penalty)         AS penalty,
               SUM(bad_returns_qty) AS returns_qty,
               SUM(gmv)             AS gmv,
               COUNT(*)             AS complaints
        FROM bcpl GROUP BY manufacturer ORDER BY penalty DESC
    """)
    con.execute("""
        CREATE TABLE bcpl_item AS
        SELECT item_name, manufacturer,
               SUM(penalty)         AS penalty,
               SUM(bad_returns_qty) AS returns_qty,
               COUNT(*)             AS complaints
        FROM bcpl GROUP BY item_name, manufacturer ORDER BY penalty DESC LIMIT 500
    """)
    con.execute("""
        CREATE TABLE bcpl_mfr_type AS
        SELECT manufacturer, complaint_type, SUM(penalty) AS penalty,
               SUM(bad_returns_qty) AS returns_qty, COUNT(*) AS complaints
        FROM bcpl GROUP BY 1,2
    """)
    # day-keyed dimensional rollups for the raw-data exports.
    con.execute("""
        CREATE TABLE bcpl_type_daily AS
        SELECT day, complaint_type,
               SUM(penalty)         AS penalty,
               SUM(bad_returns_qty) AS returns_qty,
               COUNT(*)             AS complaints
        FROM bcpl GROUP BY 1,2 ORDER BY day
    """)
    con.execute("""
        CREATE TABLE bcpl_mfr_daily AS
        SELECT day, manufacturer,
               SUM(penalty)         AS penalty,
               SUM(bad_returns_qty) AS returns_qty,
               COUNT(*)             AS complaints
        FROM bcpl GROUP BY 1,2 ORDER BY day
    """)
    con.execute("""
        CREATE TABLE bcpl_item_daily AS
        SELECT day, item_name, manufacturer,
               SUM(penalty)         AS penalty,
               SUM(bad_returns_qty) AS returns_qty,
               COUNT(*)             AS complaints
        FROM bcpl GROUP BY 1,2,3 ORDER BY day
    """)

    # comment word cloud -- token frequencies from free-text remarks.
    log("Building defective-return comment keywords...")
    con.execute(f"""
        CREATE TABLE bcpl_keywords AS
        WITH toks AS (
            SELECT unnest(
                string_split_regex(
                    lower(regexp_replace(remarks, '[^a-zA-Z]+', ' ', 'g')), ' '
                )) AS word
            FROM {rd(BCPL)}
            WHERE remarks IS NOT NULL AND length(trim(remarks)) > 0
        )
        SELECT word, COUNT(*) AS n
        FROM toks
        WHERE length(word) >= 3
          AND word NOT SIMILAR TO 'x+'
          AND word NOT IN ({_stopword_sql()})
        GROUP BY word
        ORDER BY n DESC
        LIMIT 120
    """)
    # defect-only keywords -- restricted to the defect allowlist so the cloud
    # answers "what was wrong with the product".
    con.execute(f"""
        CREATE TABLE bcpl_defect_keywords AS
        WITH toks AS (
            SELECT unnest(
                string_split_regex(
                    lower(regexp_replace(remarks, '[^a-zA-Z]+', ' ', 'g')), ' '
                )) AS word
            FROM {rd(BCPL)}
            WHERE remarks IS NOT NULL AND length(trim(remarks)) > 0
        )
        SELECT word, COUNT(*) AS n
        FROM toks
        WHERE word IN ({_defect_sql()})
        GROUP BY word
        ORDER BY n DESC
        LIMIT 60
    """)

    # ------------------------------------------------------------------
    # 3. Purchase returns -- recall-assistance PRN log
    # ------------------------------------------------------------------
    log("Loading purchase-return (recall) tables...")
    con.execute(f"CREATE TABLE recall_assistance AS SELECT * FROM {rd(RECALL)}")
    con.execute("""
        CREATE TABLE recall_by_mfr AS
        SELECT manufacturer,
               SUM(prn_qty)                    AS return_qty,
               SUM(COALESCE(recall_charges,0)) AS fee,
               COUNT(*)                        AS notices,
               COUNT(DISTINCT item_id)         AS items
        FROM recall_assistance GROUP BY 1 ORDER BY return_qty DESC
    """)

    # ------------------------------------------------------------------
    # 4. Contract master + term extracts (small -- store as-is)
    # ------------------------------------------------------------------
    log("Loading contract tables...")
    con.execute(f"CREATE TABLE contracts AS SELECT * FROM {rd(CONTRACTS)}")
    con.execute(f"CREATE TABLE ads_spend AS SELECT * FROM {rd(ADS)}")
    con.execute(f"CREATE TABLE turnover_discount AS SELECT * FROM {rd(TURNOVER)}")
    con.execute(f"CREATE TABLE sp_benchmarking AS SELECT * FROM {rd(SPBENCH)}")

    # ------------------------------------------------------------------
    # 5. Headline KPIs (single-row table)
    # ------------------------------------------------------------------
    log("Computing headline KPIs...")
    con.execute("""
        CREATE TABLE kpis AS
        SELECT
            (SELECT SUM(fee) FROM sat_daily)               AS total_satellite_fee,
            (SELECT SUM(qty) FROM sat_daily)               AS total_sale_qty,
            (SELECT SUM(net_qty) FROM sat_daily)           AS total_net_qty,
            (SELECT SUM(returned) FROM sat_daily)          AS total_sat_returned,
            (SELECT SUM(rows) FROM sat_daily)              AS sat_rows,
            (SELECT SUM(fee)/NULLIF(SUM(net_qty),0) FROM sat_daily) AS avg_fee_per_unit,
            (SELECT COUNT(*) FROM sat_city)                AS n_cities,
            (SELECT COUNT(*) FROM sat_cities_list)         AS n_satellite_cities,
            (SELECT COUNT(*) FROM sat_mfr)                 AS n_fee_manufacturers,
            (SELECT COUNT(*) FROM sat_facility)            AS n_facilities,
            (SELECT SUM(penalty) FROM bcpl_daily)          AS total_penalty,
            (SELECT SUM(returns_qty) FROM bcpl_daily)      AS total_returns,
            (SELECT SUM(complaints) FROM bcpl_daily)       AS total_complaints,
            (SELECT SUM(gmv) FROM bcpl_daily)              AS total_return_gmv,
            (SELECT COUNT(*) FROM contracts)               AS n_contracts,
            (SELECT COUNT(DISTINCT manufacturer) FROM contracts) AS n_contract_mfrs,
            (SELECT MIN(day) FROM sat_daily)               AS sat_start,
            (SELECT MAX(day) FROM sat_daily)               AS sat_end
    """)

    con.close()
    size = os.path.getsize(db_path) / 1e6
    log(f"Done. Wrote {db_path} ({size:.1f} MB)")
    return size


def main():
    if not os.path.isdir(DATA):
        raise SystemExit(f"Data directory not found: {DATA}")
    build(DATA, DB)


if __name__ == "__main__":
    main()
