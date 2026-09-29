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

The state PVC is ReadWriteOnce. If a future rollout schedules the new pod on
another node it waits on the volume while the old pod keeps serving
(`maxUnavailable: 0`); delete the old pod to let it proceed.
