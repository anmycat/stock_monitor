#!/bin/bash
# B9：本地 vs ECS 代码漂移巡检（每周一次）
# 用法（仓库根）：bash deploy/inspect_drift.sh
# 产出：逐文件 md5 对比，漂移清单打印到 stdout；无漂移 exit 0，有漂移 exit 2
# 说明：排除 __pycache__/.pyc/.env/logs（与 update_ecs.rsync 排除规则一致），
#       只巡检会上传到 ECS 的文件，避免噪声。
set -uo pipefail

ECS=root@8.134.89.55
REMOTE=/stock_monitor
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=15 -o BatchMode=yes"
if [ -f "${HOME}/.ssh/id_ed25519" ]; then
    SSH_OPTS="$SSH_OPTS -i ${HOME}/.ssh/id_ed25519"
fi

# 用 python3 生成 md5 清单（macOS 无 md5sum，python3 两侧都有，口径统一）
gen_manifest() {
    python3 - "$1" << 'PY'
import hashlib, sys
from pathlib import Path

root = Path(sys.argv[1])
EXCLUDE_DIRS = {'__pycache__', '.git', '.ruff_cache', 'logs', 'node_modules'}
EXCLUDE_SUFFIX = {'.pyc', '.log', '.env'}
EXCLUDE_NAMES = {'.DS_Store', '.env'}
rows = []
for p in sorted(root.rglob('*')):
    if not p.is_file():
        continue
    if any(part in EXCLUDE_DIRS for part in p.parts):
        continue
    if p.suffix in EXCLUDE_SUFFIX or p.name in EXCLUDE_NAMES:
        continue
    try:
        digest = hashlib.md5(p.read_bytes()).hexdigest()
    except OSError:
        continue
    rows.append(f'{digest}  {p.relative_to(root)}')
print('\n'.join(rows))
PY
}

echo "== B9 漂移巡检 $(date '+%F %T') =="
echo "-- 本地清单生成"
LOCAL_MANIFEST=$(gen_manifest quant_engine)
LOCAL_COUNT=$(printf '%s\n' "$LOCAL_MANIFEST" | grep -c . || true)
echo "  本地 quant_engine: $LOCAL_COUNT 个文件"

echo "-- ECS 清单生成"
REMOTE_MANIFEST=$(ssh $SSH_OPTS $ECS "cd $REMOTE && python3 - \"$REMOTE/quant_engine\" << 'PY'
import hashlib, sys
from pathlib import Path

root = Path(sys.argv[1])
EXCLUDE_DIRS = {'__pycache__', '.git', '.ruff_cache', 'logs', 'node_modules'}
EXCLUDE_SUFFIX = {'.pyc', '.log', '.env'}
EXCLUDE_NAMES = {'.DS_Store', '.env'}
rows = []
for p in sorted(root.rglob('*')):
    if not p.is_file():
        continue
    if any(part in EXCLUDE_DIRS for part in p.parts):
        continue
    if p.suffix in EXCLUDE_SUFFIX or p.name in EXCLUDE_NAMES:
        continue
    try:
        digest = hashlib.md5(p.read_bytes()).hexdigest()
    except OSError:
        continue
    rows.append(f'{digest}  {p.relative_to(root)}')
print('\n'.join(rows))
PY" 2>/dev/null)

if [ -z "$REMOTE_MANIFEST" ]; then
    echo "❌ ECS 清单获取失败（SSH 或远端 python3 异常）"
    exit 1
fi
REMOTE_COUNT=$(printf '%s\n' "$REMOTE_MANIFEST" | grep -c . || true)
echo "  ECS quant_engine: $REMOTE_COUNT 个文件"

# 按相对路径对齐 diff（忽略 md5 列排序差异）
printf '%s\n' "$LOCAL_MANIFEST" | sort -k2 > /tmp/_drift_local_$$.txt
printf '%s\n' "$REMOTE_MANIFEST" | sort -k2 > /tmp/_drift_remote_$$.txt

echo "-- 对比结果"
if diff -u /tmp/_drift_local_$$.txt /tmp/_drift_remote_$$.txt > /tmp/_drift_diff_$$.txt; then
    echo "✅ 无漂移：两侧 $LOCAL_COUNT 个文件 md5 全一致"
    rm -f /tmp/_drift_local_$$.txt /tmp/_drift_remote_$$.txt /tmp/_drift_diff_$$.txt
    exit 0
fi

echo "⚠️ 检测到代码漂移："
grep -E '^[+-]' /tmp/_drift_diff_$$.txt | grep -vE '^[+-]{3}' | sed 's/^/    /' | head -40
DRIFT_N=$(grep -cE '^[+-][^+-]' /tmp/_drift_diff_$$.txt || true)
echo "  共 $DRIFT_N 行差异（本地-/远端+；含本地新增远端缺、远端多出两类）"
echo "  完整 diff: /tmp/_drift_diff_$$.txt"
echo "  处置：确认漂移是有意（ECS 热更）则同步回本地或重跑 update_ecs.sh；"
echo "        无意则立即重跑 bash deploy/update_ecs.sh 拉齐。"
rm -f /tmp/_drift_local_$$.txt /tmp/_drift_remote_$$.txt
exit 2
