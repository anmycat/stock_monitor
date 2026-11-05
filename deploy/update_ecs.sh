#!/bin/bash
# 生产服务器更新脚本（SSH 密钥认证，无需密码交互）
# 用法（仓库根）：export DEPLOY_HOST=<host>; bash deploy/update_ecs.sh
# 流程：备份现网 → 上传代码（排除 .env/缓存/日志）→ 装依赖 → 导入冒烟 → 重启三服务 → 端点验证
# 注意：.env 永不上传（gitignore 凭据，ECS 保留自身配置）；--delete 受 --exclude 保护不会删 ECS 侧日志/配置
# 认证：显式 -i ~/.ssh/id_ed25519 + BatchMode（agent 不可见时仍走密钥，失败即报错不挂起等密码）
set -euo pipefail

# ===== 目标主机（凭据与地址一律经环境变量传入，不入库）=====
# 用法：export DEPLOY_HOST=<host> DEPLOY_PATH=<path> DEPLOY_USER=<user>
#      未设置 DEPLOY_HOST 时直接报错退出 —— 避免误连默认主机
: "${DEPLOY_HOST:?必须设置 DEPLOY_HOST 环境变量（目标主机地址）}"
DEPLOY_USER="${DEPLOY_USER:-root}"
DEPLOY_PATH="${DEPLOY_PATH:-/stock_monitor}"
ECS="${DEPLOY_USER}@${DEPLOY_HOST}"
REMOTE="$DEPLOY_PATH"
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

echo "== 0.5/6 P1-1 安全检查：API_AUTH_TOKEN 配置探测"
# D61：.env 实际位于部署根目录（systemd WorkingDirectory），不是 quant_engine/ 下。
# 旧路径导致该检查恒失败（假阴性），已修正；两处路径都探测，兼容不同部署布局。
if ssh $SSH_OPTS $ECS "grep -E '^API_AUTH_TOKEN=.+' $REMOTE/.env $REMOTE/quant_engine/.env 2>/dev/null | grep -vE '^API_AUTH_TOKEN=\s*$'" | grep -q .; then
    echo "  API_AUTH_TOKEN 已配置 ✅"
else
    echo "  ⚠️ [SECURITY] API_AUTH_TOKEN 未配置——ECS 所有 API/WS 端点无认证开放。"
    echo "     内网可信环境可忽略；公网暴露请在 $REMOTE/.env 配置后重启 quant-api。"
fi

echo "== 0.7/6 敏感信息扫描（D59：防止密钥误入 git / 部署包）"
if [ -f research/scripts/scan_secrets.py ]; then
  if python3 research/scripts/scan_secrets.py; then
    echo "  ✅ 未发现敏感信息"
  else
    echo "  ❌ 检测到敏感信息 —— 部署中止！"
    echo "     请先吊销该密钥并清理（详见 research/scripts/scan_secrets.py 输出）"
    exit 1
  fi
fi

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

echo "== 3.5/6 node 探针（问财 Hexin-V 自动生成需要；可选依赖，缺失不阻塞部署）"
ssh $SSH_OPTS $ECS "if command -v node >/dev/null 2>&1; then echo '  node 已装：\$(node -v)'; else echo '  node 缺失 → 安装（问财选股需要）'; DEBIAN_FRONTEND=noninteractive apt-get install -y -q nodejs >/dev/null 2>&1 && echo '  node 安装完成：\$(node -v)' || echo '  ⚠️ node 安装失败（问财功能将降级，其余服务不受影响）'; fi"

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
