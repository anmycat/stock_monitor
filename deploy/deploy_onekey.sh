#!/bin/bash
# 一键部署入口（I-07，2026-11-07 修正形态漂移）
#
# 旧版本是 **单服务形态**：写 quant-engine.service、uvicorn --workers 2、
# 排除 UX/test 目录、健康检查打 /health，且引用了未定义的 REMOTE_USER。
# 现网自 D52 起是 quant-api / quant-worker / quant-beat **三服务 + Redis**，
# 继续按旧脚本部署会覆盖 systemd 单元并打断 worker/beat 定时链路。
#
# 因此本入口不再自己部署，统一委托 `deploy/update_ecs.sh`（SSH 密钥认证、
# 备份、三服务重启、端点验证、部署前门禁）。保留入口名避免旧文档/习惯失效。
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"

echo "======================================"
echo "量化交易系统部署（三服务形态）"
echo "======================================"
echo "⚠️  deploy_onekey.sh 已由旧单服务脚本改为 update_ecs.sh 的委托入口。"
echo "   旧脚本的 quant-engine.service / --workers 2 与现网三服务不一致，已停用。"

if [ -z "${DEPLOY_HOST:-}" ]; then
    echo ""
    echo "❌ 缺少 DEPLOY_HOST。正确用法："
    echo "   export DEPLOY_HOST=<目标主机>"
    echo "   bash deploy/update_ecs.sh [--skip-tests]"
    echo ""
    echo "   说明：现网使用 SSH 密钥认证（~/.ssh/id_ed25519），不再需要 SSHPASS。"
    exit 2
fi

if [ -n "${SSHPASS:-}" ]; then
    echo "ℹ️  检测到 SSHPASS，但新链路使用 SSH 密钥，忽略该变量。"
fi

echo ""
echo "→ 转发至 deploy/update_ecs.sh $*"
exec bash "$HERE/update_ecs.sh" "$@"
