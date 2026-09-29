# -*- coding: utf-8 -*-
"""Rebuild the exact Contract Details export from the live contracts table + a delta.

  python apply_delta.py <fees.duckdb> <delta.txt> <out.csv>

delta.txt (tab-separated, ASCII; '\\\\' '\\t' '\\uXXXX' escapes):
  H  <export header fields>
  S  <contract_id>                    row identical to the current table
  P  <contract_id>  <i>=<v> ...       current row with fields i replaced
  N  <i>=<v> ...                      new row; fields not listed are empty
Rows are written in delta order with csv.writer; the caller checks sha256.
"""
import csv, re, sys, duckdb
from delta_lib import old_rows


def unesc(s):
    return re.sub(r"\\(\\|t|u[0-9a-f]{4})",
                  lambda m: {"\\": "\\", "t": "\t"}.get(m.group(1)) or chr(int(m.group(1)[1:], 16)), s)


def fields(parts):
    out = {}
    for kv in parts:
        k, v = kv.split("=", 1)
        out[int(k)] = unesc(v)
    return out


lines = open(sys.argv[2], encoding="ascii").read().split("\n")[:-1]
assert lines[0].startswith("H\t")
header = [unesc(h) for h in lines[0].split("\t")[1:]]
con = duckdb.connect(sys.argv[1], read_only=True)
old = old_rows(con, header)
con.close()
w = csv.writer(open(sys.argv[3], "w", newline="", encoding="utf-8"), lineterminator="\n")
w.writerow(header)
for ln in lines[1:]:
    t = ln.split("\t")
    if t[0] == "S":
        w.writerow(old[t[1]])
    elif t[0] == "P":
        r = list(old[t[1]])
        for i, v in fields(t[2:]).items():
            r[i] = v
        w.writerow(r)
    elif t[0] == "N":
        r = [""] * len(header)
        for i, v in fields(t[1:]).items():
            r[i] = v
        w.writerow(r)
    else:
        raise SystemExit(f"bad delta line: {ln[:40]}")
