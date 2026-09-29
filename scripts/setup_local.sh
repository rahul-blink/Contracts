#!/usr/bin/env bash
# One-shot local setup for fee-insights (LOCAL_WORKFLOW.md, phases 0-1).
#
#   bash scripts/setup_local.sh            # from the repo root
#
# Safe to re-run. It never deletes anything, never touches the live app, and
# keeps every data file out of git (see the root .gitignore).
#
# What it does:
#   1. checks git / python3 (and reports on docker + chef for deploys)
#   2. creates .venv and installs fee-insights/requirements.txt
#   3. finds the files you downloaded from the live dashboard's
#      "Download data" card (fees_*.duckdb, fee_insights_state_*.zip) in
#      ~/Downloads -- opening the page for you if they are not there yet
#   4. verifies the DB against the sha256 inside the state zip
#   5. files them under Data/backup-YYYYMMDD/, copies the DB to
#      fee-insights/fees.duckdb and unzips the saved lists to fee-insights/state/
#   6. starts the dashboard on http://127.0.0.1:8000 (localhost only)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
APP="$ROOT/fee-insights"
DL="${DOWNLOADS:-$HOME/Downloads}"
LIVE="https://special-quetzal.apps.blinkit.in/#data"
cd "$ROOT"

say()  { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[33m!!  %s\033[0m\n' "$*"; }
die()  { printf '\033[31mxx  %s\033[0m\n' "$*" >&2; exit 1; }
sha()  { if command -v shasum >/dev/null; then shasum -a 256 "$1" | cut -d' ' -f1
         else sha256sum "$1" | cut -d' ' -f1; fi; }
newest() { ls -t "$DL"/$1 2>/dev/null | head -1 || true; }

# -- 1. prerequisites --------------------------------------------------------
say "Checking tools"
command -v git >/dev/null || die "git missing: run  xcode-select --install"
PY=""
for c in python3.12 python3.13 python3.11 python3; do
  if command -v "$c" >/dev/null && "$c" -c 'import sys; sys.exit(sys.version_info < (3, 10))'; then
    PY="$c"; break
  fi
done
[ -n "$PY" ] || die "Python 3.10+ missing: run  brew install python@3.12"
echo "python: $($PY --version)"
command -v docker >/dev/null && echo "docker: ok" || warn "Docker Desktop not installed (needed only to deploy)"
command -v chef   >/dev/null && echo "chef:   ok" || warn "chef CLI not installed (needed only to deploy): https://apps.blinkit.in/?tab=cli"

# -- 2. python env -----------------------------------------------------------
say "Python environment (.venv)"
[ -d .venv ] || "$PY" -m venv .venv
./.venv/bin/pip install -q --upgrade pip
./.venv/bin/pip install -q -r "$APP/requirements.txt"
echo "installed: $(./.venv/bin/pip freeze | grep -iE '^(fastapi|duckdb|uvicorn)=' | tr '\n' ' ')"

# -- 3. find the downloads ---------------------------------------------------
say "Looking for the live-dashboard downloads in $DL"
DB_DL="$(newest 'fees_*.duckdb')"
ZIP_DL="$(newest 'fee_insights_state_*.zip')"
if [ -z "$DB_DL" ] || [ -z "$ZIP_DL" ]; then
  warn "Not found yet. Opening the dashboard: Data & definitions -> Download data."
  warn "Click  '↓ fees.duckdb'  and  '↓ Saved lists (zip)', wait for both to finish."
  command -v open >/dev/null && open "$LIVE" || echo "Open: $LIVE"
  while :; do
    read -r -p "Press Enter when both downloads are complete (Ctrl-C to stop)... " _
    DB_DL="$(newest 'fees_*.duckdb')"; ZIP_DL="$(newest 'fee_insights_state_*.zip')"
    [ -n "$DB_DL" ] && [ -n "$ZIP_DL" ] && break
    warn "Still missing: ${DB_DL:-fees_*.duckdb} ${ZIP_DL:-fee_insights_state_*.zip}"
  done
fi
echo "db:    $DB_DL"
echo "lists: $ZIP_DL"

# -- 4. verify ---------------------------------------------------------------
say "Verifying the DB checksum"
WANT="$(unzip -p "$ZIP_DL" manifest.json | ./.venv/bin/python -c 'import json,sys; print(json.load(sys.stdin)["db_sha256"])')"
GOT="$(sha "$DB_DL")"
echo "expected $WANT"
echo "got      $GOT"
[ "$WANT" = "$GOT" ] || die "Checksum mismatch -- the DB download is incomplete. Delete it, download again, re-run."
echo "OK: byte-identical to the live DB"

# -- 5. file them ------------------------------------------------------------
say "Saving the backup and wiring it into the local app"
BK="$ROOT/Data/backup-$(date +%Y%m%d)"
mkdir -p "$BK"
[ -f "$BK/$(basename "$DB_DL")" ]  || cp "$DB_DL"  "$BK/"
[ -f "$BK/$(basename "$ZIP_DL")" ] || cp "$ZIP_DL" "$BK/"
cp "$DB_DL" "$APP/fees.duckdb"
if [ -d "$APP/state" ] && [ -n "$(ls -A "$APP/state" 2>/dev/null)" ]; then
  keep="$APP/state.before-$(date +%Y%m%d%H%M%S)"
  mv "$APP/state" "$keep"; warn "Existing local state moved to $keep"
fi
mkdir -p "$APP/state"
unzip -q -o "$ZIP_DL" -x manifest.json -d "$APP/state"
echo "backup: $BK"; ls -la "$BK"
echo "state:  $(ls "$APP/state" | tr '\n' ' ')"

# Belt and braces: nothing under Data/, no DB and no state may be tracked.
if [ -n "$(git ls-files -- Data "$APP/fees.duckdb" "$APP/state")" ]; then
  die "Data files are tracked by git -- stop and fix .gitignore before pushing"
fi
echo "git: data files are ignored"

# -- 6. run ------------------------------------------------------------------
say "Starting the dashboard on http://127.0.0.1:8000  (Ctrl-C to stop)"
( sleep 4; command -v open >/dev/null && open "http://127.0.0.1:8000" ) &
cd "$APP" && exec ../.venv/bin/python app.py
