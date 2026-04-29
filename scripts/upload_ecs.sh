#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <user@host> <ssh_key_path> [remote_dir]"
  exit 1
fi

REMOTE="$1"
KEY_PATH="$2"
REMOTE_DIR="${3:-/stock_monitor}"
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

cd "$PROJECT_ROOT"

rsync -az --delete \
  -e "ssh -i $KEY_PATH" \
  --exclude '.git/' \
  --exclude '.venv/' \
  --exclude '__pycache__/' \
  --exclude 'modules/__pycache__/' \
  --exclude 'scripts/__pycache__/' \
  --exclude 'logs/' \
  --exclude 'data/' \
  --exclude '*.pyc' \
  "$PROJECT_ROOT/" "$REMOTE:$REMOTE_DIR/"

echo "[ok] uploaded to $REMOTE:$REMOTE_DIR"
