#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TARGET_PATH="${1:-/stock_monitor}"
DAILY_REPO_PATH="${2:-/daily_stock_analysis}"

cd "$PROJECT_ROOT"

echo "[preflight] project_root=$PROJECT_ROOT"
echo "[preflight] target_path=$TARGET_PATH"
echo "[preflight] daily_repo_path=$DAILY_REPO_PATH"

if [[ ! -f guardian.py || ! -d modules || ! -d config ]]; then
  echo "[error] invalid project structure"
  exit 1
fi

if [[ ! -f config/.env ]]; then
  echo "[error] missing config/.env"
  exit 1
fi

if [[ ! -d "$DAILY_REPO_PATH" ]]; then
  echo "[warn] daily_stock_analysis path not found: $DAILY_REPO_PATH"
else
  if git -C "$DAILY_REPO_PATH" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "[ok] daily_stock_analysis git repo detected"
  else
    echo "[warn] daily_stock_analysis path exists but not git repo"
  fi
fi

mkdir -p "$TARGET_PATH"

rsync -a --delete \
  --exclude '.git/' \
  --exclude '.venv/' \
  --exclude '__pycache__/' \
  --exclude 'modules/__pycache__/' \
  --exclude 'logs/' \
  --exclude '*.pyc' \
  "$PROJECT_ROOT/" "$TARGET_PATH/"

find "$TARGET_PATH" -type d -name "__pycache__" -prune -exec rm -rf {} +

echo "[ok] synced to $TARGET_PATH"
