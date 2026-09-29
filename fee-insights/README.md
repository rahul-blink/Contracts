# fee-insights (special-quetzal.apps.blinkit.in)

Source of the live Satellite / Defects / KAM / Recall / Contracts dashboard,
extracted from the running image `fee-insights:dev-20260804-111006`.

`fees.duckdb` (429 MB) is **not** in this repo. It lives inside that image and
the raw Parquet needed to rebuild it is not here either, so the `Dockerfile`
builds `FROM` that image and swaps in only `app.py` and `static/index.html`.
The data served is byte-identical to the Aug 4 build.

## Satellite fee exclusion list

Satellite fees tab → *Exclusion list*: search an item (name, manufacturer or
variant id), exclude it with an optional reason, restore it any time. Untick
*Apply to figures below* to see the original numbers.

- Stored in `SAT_EXCLUSIONS_PATH` (`/state/sat_exclusions.json` on the
  `fee-insights-state` PVC), never in `fees.duckdb`, which stays read-only.
- Applied at query time on `sat_cube` (the only item-grain table): KPI tiles,
  breakdown table, top-12 chart, MRP bucket / product-type / contract-state
  splits, filter options and CSV exports.
- **Not** applied (no item grain in the rollups): daily trend, per-unit rate
  histogram, serving-facility table, Satellite cities tab, headline `kpis`.
  The tab shows a note saying so whenever the list is active.
- Anyone who can open the dashboard can edit the list (chef-server auth only).

## Manufacturer opt-in

Three states per manufacturer (Satellite fees tab, bottom right):

- **Confirmation pending** -- satellite clause is Yes in the contract, not yet
  verified. The default; never stored.
- **Confirmed** -- manually validated.
- **Not_opted** -- manually marked as opted out. The only state removed from
  the figures (tiles, breakdown, top-12, bucket/type splits, daily trend).

Stored in `SAT_OPTOUT_PATH` (`/state/sat_optouts.json`). Entries written
before the three-state model had no status; they were opt-outs and read as
Not_opted, so no migration was needed and totals did not move.

Bulk edit: *Download status* gives every manufacturer as CSV (`mfr_id,
manufacturer, workdesk_cl_approval, optin_status, note, ...`). Edit
`optin_status` / `note`, *Upload status*: the file is validated first
(unknown ids, bad statuses, duplicates reject the whole file), the changes are
shown for confirmation, then applied. Only manufacturers in the file change.

Breakdown *Manufacturers* dropdown: Confirmed + pending (counted, default),
Confirmed only, Confirmation pending only, Not_opted only, All with satellite
clause. The dashed trend line is "If every Not_opted opted in". The
manufacturer breakdown's contract column is labelled *Workdesk CL approval*.

## KAM fees

- Billable = **APPROVED + PENDING APPROVAL** (DRAFT is not). The state filter
  has a *Billable* option; tiles are computed live (`/api/kam/kpis`).
- **KAM addendum dates** (bottom of the KAM tab): per contract, stored in
  `KAM_ADDENDA_PATH` (`/state/kam_addenda.json`). A contract with one accrues
  from the addendum's month instead of its effective month. `kam_month` is
  never modified: with addenda present the monthly spine is regenerated at
  query time from `kam_contracts` (verified row-identical to `kam_month` when
  the addendum equals the effective date); with none, `kam_month` is read as-is.

## Refreshing contracts (and KAM) from a Contract Details export

`load_contracts.py <export.csv> --db fees.duckdb --tag YYYY_MM_DD` maps a
Contract Details CSV export (7 renamed headers, a duplicated
`complaints_penalty_enable1`) onto the contracts_extract schema and rebuilds
only the **contracts** and **KAM** sections in place via
`build_db.rebuild_part`. The same file feeds both: KAM filters
`kam_support = 'Yes'` itself, and accrual runs to the latest KAM
effective/execution month in the file. Satellite, defects, recall and cities
are untouched; the exclusion / opt-in lists on the state volume are keyed by
variant_id / mfr_id and are not read or written. The export is not committed.

2026-09-29: `Contract_Details_3.0_Revised_2026_09_29.csv` -- 1,387 contracts
(was 1,290: +218, -121, 55 PENDING->APPROVED), 188 KAM contracts, accrual to
2026-09. Live as image `01M3PCK4F8GTC2XK1M7AGEMA4N`.

Shipping it without the CLI: the connector only takes inline file content, so
the export went up as a delta against the contracts table already in the
image (`apply_delta.py` + `delta_lib.py`; 218 new, 833 patched, 336 unchanged
rows). The build rebuilt the exact export (CSV sha256 `b49bbadf...`, LF
endings) before running `load_contracts.py`. The delta files carry contract
data and are not committed. With the `chef` CLI, just COPY the CSV and run
`load_contracts.py` directly.

## Deploy

```
chef skaffold up        # from this folder
```

Rollback: point the Deployment back at image tag `dev-20260804-111006`.

Deployed 2026-09-29 as image tag `01M3NX5EB244A3S7GSXTE7VVVE` through the
Blinkit Apps connector (no local `chef` CLI in that session). That build
applied the diff to the base image's `app.py` / `static/index.html` with a
sha256 check on both sides, so the image holds exactly the files in this
folder (app.py `618bbe0b…`, index.html `7bd880df…`); `fees.duckdb` was
verified unchanged in the new image (`d2ecad32…`).

The state PVC is ReadWriteOnce, so the Deployment uses `strategy: Recreate`:
each deploy stops the old pod before starting the new one (~30-60 s of
downtime) so the volume can move between nodes. The volume and the lists on
it survive every deploy.
