# Contracts & Fees Insights

An interactive dashboard over Blinkit's contracts and fees parquet extracts.

## What it shows

- **Overview** — headline KPIs (total satellite fee, cities served, return
  penalties, defective units, contracts) plus fee and penalty daily trends.
- **Satellite City Fees** — daily fee/units, top cities & manufacturers,
  fee-vs-distance bands, purchase-mode / cold-chain split. Filter by city or
  manufacturer.
- **Defective Returns** — penalty & complaint trends, breakdown by complaint
  type, top manufacturers and SKUs by penalty. Filter by complaint type.
- **Contracts** — lifecycle states, purchase model, payout cadence, per-clause
  term adoption, execution timeline, top manufacturers.

## Architecture

The raw parquet in `../data` is large (~34M satellite-fee rows, 763K return
rows). `build_db.py` uses **DuckDB** to pre-aggregate it into compact rollup
tables stored in `insights.duckdb` (~6 MB). `app.py` (**FastAPI**) serves
filtered queries from those small tables, so the dashboard is instant. The
frontend is a single `static/index.html` using vendored **ECharts** — no
external calls at runtime.

The corrupt `city_warehouse_distance` extract (4 bytes) is skipped.

## Run locally

```bash
pip install -r requirements.txt
python build_db.py      # reads ../data/*.parquet -> insights.duckdb
python app.py           # serves on http://127.0.0.1:8000
```

## Deploy (Blinkit Apps / chef)

`insights.duckdb` is baked into the image; the raw parquet is **not** shipped.

```bash
python build_db.py      # ensure insights.duckdb is current
chef skaffold up        # builds image, pushes, deploys; prints the URL
```

Inside the pod the server binds `0.0.0.0` (via the `HOST` env var) so the
ClusterIP Service can reach it; chef-server owns the public route, TLS, and
auth. Locally it defaults to `127.0.0.1`.
