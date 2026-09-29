# Moving fee-insights to a local workflow

Goal: work in a local clone at
`/Users/saxena.rahul@grofers.com/Documents/Claude-Fee Insights`, push to GitHub
now and then as a backup, and deploy from the Mac to Blinkit Apps with the
`chef` CLI.

```
Mac (code + data)  ──git push──▶  GitHub (code only, private)
      │
      └──chef skaffold up──▶  Blinkit Apps (image = code + fees.duckdb)
                                  └─ state volume (opt-ins, exclusions, addenda)
```

The three places hold different things:
- **Git** holds code only.
- **The image** holds code plus `fees.duckdb`.
- **The state volume** holds the lists edited in the live UI. They survive
  every deploy and are **never overwritten from the Mac**.

## Quick start (does phases 0-1 for you)

```bash
git clone -b claude/inspiring-bardeen-t0ozmw https://github.com/rahul-blink/contracts.git \
  "/Users/saxena.rahul@grofers.com/Documents/Claude-Fee Insights" \
  && cd "/Users/saxena.rahul@grofers.com/Documents/Claude-Fee Insights" \
  && bash scripts/setup_local.sh
```

`scripts/setup_local.sh` checks tools, builds `.venv`, opens the live
dashboard for the two downloads if they aren't in ~/Downloads yet, verifies
the DB checksum, files the backup under `Data/`, and starts the app on
http://127.0.0.1:8000. Safe to re-run; it deletes nothing.

---

## Phase 0 — Back up the live data (do this first)

Open the live dashboard → **Data & definitions** → **Download data**.

1. **↓ fees.duckdb** (~430 MB). This is every number on the dashboard. The raw
   extracts it was built from are not in git, so this file is the only copy
   outside the cluster.
2. **↓ Saved lists (zip)**. This holds item exclusions, opt-in states and KAM
   addendum dates, plus a `manifest.json` with the DB checksum.
3. Optional: **↓ Download status** on the Satellite tab gives the opt-in list as
   an editable CSV.

Save them in the local folder under `Data/backup-YYYYMMDD/`. Then check the
download is complete:

```bash
shasum -a 256 "Data/backup-YYYYMMDD/fees_YYYYMMDD.duckdb"
# must equal the sha256 shown on the Download data card / in manifest.json
```

If the 430 MB download stalls, the per-table **Parquet** links on the same card
give every table separately.

## Phase 1 — Local repo

```bash
cd "/Users/saxena.rahul@grofers.com/Documents"
git clone https://github.com/rahul-blink/contracts.git "Claude-Fee Insights"
cd "Claude-Fee Insights"
git checkout claude/inspiring-bardeen-t0ozmw
```

Make `main` match what is live. Merge the branch through a PR on GitHub (or
`git checkout main && git merge claude/inspiring-bardeen-t0ozmw && git push`).
From then on, work on `main` or on short feature branches.

**Data layout.** Everything below is git-ignored by the root `.gitignore`:

```
Claude-Fee Insights/
  Data/                     # raw extracts + backups, never committed
  fee-insights/
    fees.duckdb             # copy of the downloaded DB
    state/                  # unzip "Saved lists" here for local testing
```

**Run it locally:**

```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r fee-insights/requirements.txt
cp Data/backup-YYYYMMDD/fees_YYYYMMDD.duckdb fee-insights/fees.duckdb
mkdir -p fee-insights/state && unzip Data/backup-YYYYMMDD/fee_insights_state_*.zip -d fee-insights/state
cd fee-insights && python app.py          # http://127.0.0.1:8000 (localhost only)
```

Edits you make locally to opt-ins or exclusions only change `fee-insights/state/`
on the Mac. They are for testing and are never deployed.

## Phase 2 — Make the repo deployable from the Mac

The current `fee-insights/Dockerfile` layers a patch on top of the previous
live image. That was only needed because the web connector can't upload the
430 MB DB. From the Mac the build can include the DB directly, so switch to a
self-contained Dockerfile:

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY fees.duckdb .               # before the code: code-only changes reuse this layer
COPY static ./static
COPY app.py build_db.py load_contracts.py ./
ENV PORT=8000
EXPOSE 8000
CMD ["python", "app.py"]
```

Also do the following:
- Add `fee-insights/.dockerignore` (`state/`, `.venv/`, `__pycache__/`, `*.csv`,
  `*.parquet`) so local test state and raw data never go into the image.
- Delete the connector-only files from the repo (`apply_delta.py`,
  `delta_lib.py`, and the patch notes in the README).
- **Apple Silicon:** the cluster runs `linux/amd64`. If `chef` doesn't set the
  platform itself, add `platforms: ["linux/amd64"]` under `build:` in
  `skaffold.yaml`. Symptom if missing: the pod crash-loops with
  `exec format error`.
- Keep `k8s/*.yaml` exactly as they are: `Recreate` strategy, the
  `fee-insights-state` PVC and `HOST=0.0.0.0` in the pod. The PVC is what
  preserves the live lists.

**One-time setup:**
1. Install Docker Desktop.
2. Install the `chef` CLI from https://apps.blinkit.in/?tab=cli.
3. Run `chef auth login`, then `chef auth status`.

**First deploy from the Mac:**

```bash
cd fee-insights && chef skaffold up
```

Then open **Data & definitions → Download data** on the live site. Confirm:
- the fees.duckdb sha256 matches your backup;
- the saved-list counts are unchanged (this proves the volume was kept).

After this, stop using the claude.ai web connector for deploys. Two deploy
paths building different Dockerfiles would fight each other.

## Phase 3 — Day to day

| Task | Steps |
|---|---|
| Code change | edit → `python app.py` locally → `git commit` → `chef skaffold up` |
| Back up to GitHub | `git push` whenever a change works (code only; data is ignored) |
| New data files | put them in `Data/` → rebuild just that section into `fees.duckdb` (`python load_contracts.py <csv> --db fees.duckdb --tag YYYY_MM_DD` for contracts/KAM; `build_db.rebuild_part` for others) → check locally → `chef skaffold up` |
| Back up live edits | download **Saved lists (zip)** before any big change; it's the only copy of opt-in states |
| Roll back | redeploy the previous commit, or point the Deployment at an older image tag (listed in `fee-insights/README.md`) |

Each deploy has about 30–60 s of downtime (`Recreate` strategy).

## Risks and open points

- **Confidentiality.**
  - The GitHub repo must stay private.
  - `Data/`, `*.duckdb`, `*.csv`, `*.parquet` and `state/` are git-ignored at
    the root. Run `git status` before every push anyway.
  - Six of the notebooks at the repo root have saved cell outputs. Check them
    for data and clear the outputs before the next push if they hold any.
- **Rebuilding from scratch** needs the original raw extracts (`DATA_DIR`,
  default `../data`), which only exist wherever they were first downloaded.
  Until they're in `Data/`, refresh one section at a time on top of the
  downloaded `fees.duckdb`.
- **The live lists are edited in production** and are not in git. A local
  deploy never touches them. Restoring them would be a manual step (upload the
  opt-in CSV through the UI).
- **Image size.** Every image carries the ~430 MB DB. Keeping the DB layer
  before the code layer means code-only deploys push only a few KB.
