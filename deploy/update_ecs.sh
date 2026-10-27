#!/bin/bash
# ECS 生产更新脚本（SSH 密钥认证，无需密码交互）
# 用法（仓库根）：bash deploy/update_ecs.sh
# 流程：备份现网 → 上传代码（排除 .env/缓存/日志）→ 装依赖 → 导入冒烟 → 重启三服务 → 端点验证
# 注意：.env 永不上传（gitignore 凭据，ECS 保留自身配置）；--delete 受 --exclude 保护不会删 ECS 侧日志/配置
# 认证：显式 -i ~/.ssh/id_ed25519 + BatchMode（agent 不可见时仍走密钥，失败即报错不挂起等密码）
set -euo pipefail

ECS=root@8.134.89.55
REMOTE=/stock_monitor
TS=$(date +%Y%m%d_%H%M%S)
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=15 -o BatchMode=yes"
# 显式指定身份文件：沙箱/CI 环境下 ssh-agent 可能不可见，默认身份探测会被拦导致回退密码交互挂起
if [ -f "${HOME}/.ssh/id_ed25519" ]; then
    SSH_OPTS="$SSH_OPTS -i ${HOME}/.ssh/id_ed25519"
    RSYNC_RSH="ssh $SSH_OPTS"
else
    RSYNC_RSH="ssh $SSH_OPTS"
fi

echo "== 0/6 连通性与现网状态"
ssh $SSH_OPTS $ECS "echo CONNECTED; systemctl is-active quant-api quant-worker quant-beat; cd $REMOTE/quant_engine 2>/dev/null && (git rev-parse --short HEAD 2>/dev/null || echo 'no-git-repo')"

echo "== 1/6 备份 ECS 现有生产代码（本次迭代前备份）"
ssh $SSH_OPTS $ECS "tar czf /root/backup_pre_refactor_$TS.tar.gz --exclude='__pycache__' --exclude='.ruff_cache' -C $REMOTE quant_engine UX 2>/dev/null; ls -lh /root/backup_pre_refactor_$TS.tar.gz; (cd $REMOTE/quant_engine && git rev-parse HEAD 2>/dev/null || echo no-git) > /root/ecs_version_$TS.txt; cat /root/ecs_version_$TS.txt"

echo "== 2/6 上传代码（quant_engine + UX；排除 .env/__pycache__/logs）"
RSYNC_EXCLUDES=(--exclude='__pycache__' --exclude='*.pyc' --exclude='.env' --exclude='.env.*'
                --exclude='logs/' --exclude='*.log' --exclude='.ruff_cache' --exclude='.DS_Store')
if ssh $SSH_OPTS $ECS "command -v rsync >/dev/null"; then
    rsync -az --delete -e "$RSYNC_RSH" "${RSYNC_EXCLUDES[@]}" quant_engine/ "$ECS:$REMOTE/quant_engine/"
    rsync -az --delete -e "$RSYNC_RSH" --exclude='__pycache__' --exclude='.DS_Store' UX/ "$ECS:$REMOTE/UX/"
else
    echo "ECS 无 rsync，走 tar 管道"
    tar czf - --exclude='__pycache__' --exclude='*.pyc' --exclude='.env' --exclude='.env.*' \
        --exclude='logs' --exclude='*.log' --exclude='.ruff_cache' --exclude='.DS_Store' \
        quant_engine UX | ssh $SSH_OPTS $ECS "tar xzf - -C $REMOTE"
fi

echo "== 3/6 依赖探针（只装缺失的；ECS 现网版本优先，不按钉版降级）"
ssh $SSH_OPTS $ECS "cd $REMOTE && python3 - <<'PY'
import importlib.util, subprocess, sys
mods = ['fastapi','pydantic','pytdx','requests','pandas','numpy','loguru','httpx','apscheduler','redis','yaml','websocket']
missing = [m for m in mods if importlib.util.find_spec(m) is None]
if missing:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-q',
                           '--break-system-packages', *missing])
    print('installed missing:', missing)
else:
    print('all deps present -> skip install (现网版本优先，不按钉版降级)')
PY"

echo "== 4/6 ECS 侧导入冒烟"
ssh $SSH_OPTS $ECS "cd $REMOTE && python3 -c 'import quant_engine.main; print(\"import OK\")'"

echo "== 5/6 重启三服务并确认 active"
ssh $SSH_OPTS $ECS "systemctl restart quant-api quant-worker quant-beat && sleep 8 && systemctl is-active quant-api quant-worker quant-beat"

echo "== 6/6 端点验证（本机 8000）"
ssh $SSH_OPTS $ECS 'for ep in health market signals; do curl -s -o /dev/null -w "$ep %{http_code}\n" http://localhost:8000/api/$ep; done'

echo "==================================="
echo "部署完成。备份：/root/backup_pre_refactor_$TS.tar.gz（ECS 上）"
echo "版本记录：/root/ecs_version_$TS.txt"
echo "==================================="
