# -*- coding: utf-8 -*-
"""Shared by make/apply: render a contracts-table row as export CSV fields."""
EXPORT_TO_DB = {
    "Damages/ Lost provision": "damages_lost_provision",
    "Recall_Assistance_Fees": "recall_assistance_fees",
    "Purchase_Margin_Computation": "purchase_margin_computation",
    "For Complaints Penalty (Festive)": "complaints_penalty_festive",
    "puchase_margin_off_invoice_enable": "purchase_margin_off_invoice_enable",
    "Target based incentives": "target_based_incentives",
    "Payout_Frequency": "payout_frequency",
    "complaints_penalty_enable1": "complaints_penalty_enable",
}
DATES = ("execution_date", "effective_date", "updated_at")


def old_rows(con, header):
    """contract_id -> export-ordered field list, from the live contracts table."""
    cols = [EXPORT_TO_DB.get(h, h) for h in header]
    sel = ", ".join(
        (f"strftime({c}, '%Y-%m-%d')" if c in DATES else f"CAST({c} AS VARCHAR)")
        for c in cols)
    out = {}
    for r in con.execute(f"SELECT {sel} FROM contracts").fetchall():
        out[r[0]] = ["" if v is None else v for v in r]
    return out
