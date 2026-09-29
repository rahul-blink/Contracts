"""
Load a Contract Details CSV export as the dashboard's contracts + KAM source.

The raw export (e.g. Contract_Details_3.0_Revised_2026_09_29.csv) carries the
same fields as the contracts_extract parquet build_db.py was written against,
but seven headers are spelled differently and one column is duplicated. This
maps it onto that schema, writes it out as BOTH the contracts master and the
KAM fee extract (the KAM extract is a column subset of the same contract
data; build_kam filters kam_support = 'Yes' itself), and rebuilds only those
two sections of an existing fees.duckdb in place via build_db.rebuild_part.

Satellite, defect, recall and city tables are not touched, and nothing here
reads or writes the exclusion / opt-in lists (they live on the state volume,
keyed by variant_id / mfr_id, not in fees.duckdb).

Run:  python load_contracts.py <export.csv> [--db fees.duckdb] [--tag 2026_09_29]
"""
import os
import sys
import shutil
import argparse
import tempfile
import duckdb

import build_db

# export header -> contracts_extract column
RENAME = {
    "Damages/ Lost provision": "damages_lost_provision",
    "Recall_Assistance_Fees": "recall_assistance_fees",
    "Purchase_Margin_Computation": "purchase_margin_computation",
    "For Complaints Penalty (Festive)": "complaints_penalty_festive",
    "puchase_margin_off_invoice_enable": "purchase_margin_off_invoice_enable",
    "Target based incentives": "target_based_incentives",
    "Payout_Frequency": "payout_frequency",
}
# Exported twice under two names; the build reads complaints_penalty_enable.
DROP = {"complaints_penalty_enable1"}
DATES = ("execution_date", "effective_date", "updated_at")
# In the previous extract but not in this export; kept (NULL) so the column
# set -- and the contract detail view that selects * -- is unchanged.
LEGACY = ("margin_comment", "non_margin_comment")


def lit(p):
    return "'" + p.replace("'", "''") + "'"


def to_parquet(csv_path, out_path):
    con = duckdb.connect()
    src = f"read_csv({lit(csv_path)}, all_varchar=true, header=true)"
    cols = [c[0] for c in con.execute(f"SELECT * FROM {src} LIMIT 0").description]
    if "contract_id" not in cols:
        raise SystemExit("not a contract export: no contract_id column")
    dupes = con.execute(f"SELECT count(*) - count(DISTINCT contract_id) FROM {src}").fetchone()[0]
    if dupes:
        raise SystemExit(f"{dupes} duplicate contract_id row(s) in {csv_path}")
    sel = []
    for c in cols:
        if c in DROP:
            continue
        name = RENAME.get(c, c)
        q = '"' + c.replace('"', '""') + '"'
        sel.append(f"CAST({q} AS TIMESTAMP) AS {name}" if name in DATES else f"{q} AS {name}")
    names = {RENAME.get(c, c) for c in cols}
    sel += [f"CAST(NULL AS VARCHAR) AS {c}" for c in LEGACY if c not in names]
    con.execute(f"COPY (SELECT {', '.join(sel)} FROM {src}) TO {lit(out_path)} (FORMAT parquet)")
    n = con.execute(f"SELECT count(*) FROM read_parquet({lit(out_path)})").fetchone()[0]
    con.close()
    return n


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("csv")
    ap.add_argument("--db", default=build_db.DEFAULT_OUT)
    ap.add_argument("--tag", default="", help="suffix for the provenance filenames")
    a = ap.parse_args()
    tag = f"_{a.tag}" if a.tag else ""
    work = tempfile.mkdtemp(prefix="contracts_")
    try:
        con_pq = os.path.join(work, f"contracts_extract{tag}.parquet")
        n = to_parquet(a.csv, con_pq)
        print(f"{n} contracts from {os.path.basename(a.csv)}")
        shutil.copy(con_pq, os.path.join(work, f"kam_fees_extract{tag}.parquet"))
        build_db.rebuild_part(work, a.db, "contracts")
        build_db.rebuild_part(work, a.db, "kam")
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
