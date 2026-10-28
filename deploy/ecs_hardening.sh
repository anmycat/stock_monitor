#!/bin/bash
# ECS 服务器加固（B2/B3/B6，prd/07 §15）——幂等，可重复执行
#
# B2：quant-worker/api/beat 设 systemd MemoryMax 上限（防单服务泄漏拖垮
#     整机触发内核 OOM 随机杀进程；systemd 杀可预期、Restart=always 自恢复）
#     定值依据（2026-10-28 实测）：worker 重启后峰值 ~163MB、api ~227MB、
#     beat ~151MB；历史峰值记录 689MB（审计期）。取 2-4 倍余量封顶：
#     worker 700M / api 500M / beat 300M——正常峰值远低于限值，失控时
#     由 systemd 提前处决而非内核随机 OOM。
# B3：guardian（v2 旧服务，.venv 已删仍 Restart=always，崩溃循环 40 万+ 次）
#     → stop + disable + mask（mask 后任何方式无法再拉起）。
# B6：journald 磁盘占用 2.8G → SystemMaxUse=500M；/stock_monitor/logs/*.log
#     logrotate（50M/5 份，missingok——当前活动 unit 均走 journald，
#     此为面向未来的文件日志保护）；Redis maxmemory 256mb + allkeys-lru
#     （对齐 REDIS_INSTALL_REPORT 既定设计；当前实际用量仅 ~7MB，零影响）。
# 附带：清理旧 quant-engine.service unit（uvicorn 旧入口，inactive+disabled，
#     StandardOutput 指向不存在的 logs 目录）→ mv /root/trash 可恢复。
#
# 用法（在 ECS 上执行）：bash ecs_hardening.sh
set -euo pipefail

echo "== B3: guardian 旧服务封杀 =="
systemctl stop guardian 2>/dev/null || true
systemctl disable guardian 2>/dev/null || true
# mask 要求 unit 文件不存在：先把遗留 unit 移入废纸篓（可恢复），再建
# /dev/null 符号链接——此后任何方式（含手动 start / 依赖触发）均无法拉起。
if [ -f /etc/systemd/system/guardian.service ]; then
    mv /etc/systemd/system/guardian.service /root/trash/guardian.service.$(date +%Y%m%d)
    echo "  guardian.service -> /root/trash/"
fi
systemctl daemon-reload
systemctl mask guardian 2>/dev/null || true
systemctl show guardian -p ActiveState -p UnitFileState | sed 's/^/  /'

echo "== 附带: 旧 quant-engine unit 入废纸篓 =="
mkdir -p /root/trash
if [ -f /etc/systemd/system/quant-engine.service ]; then
    mv /etc/systemd/system/quant-engine.service /root/trash/quant-engine.service.$(date +%Y%m%d)
    echo "  quant-engine.service -> /root/trash/"
else
    echo "  已不存在，跳过"
fi
systemctl daemon-reload

echo "== B2: systemd MemoryMax 上限 =="
set_limit() {  # $1=unit $2=limit
    mkdir -p "/etc/systemd/system/$1.service.d"
    cat > "/etc/systemd/system/$1.service.d/90-memory.conf" <<EOF
# B2（prd/07 §15）：内存上限——失控时 systemd 处决 + Restart=always 自恢复，
# 避免内核 OOM 随机杀进程（含 redis/api）的级联故障。2026-10-28 定值。
[Service]
MemoryMax=$2
EOF
    echo "  $1 -> MemoryMax=$2"
}
set_limit quant-worker 700M
set_limit quant-api 500M
set_limit quant-beat 300M
systemctl daemon-reload
systemctl restart quant-worker quant-api quant-beat
sleep 2
for s in quant-api quant-worker quant-beat; do
    echo "  $s: $(systemctl is-active $s) / $(systemctl show $s -p MemoryMax)"
done

echo "== B6: journald 限界 500M =="
if ! grep -q '^SystemMaxUse=500M' /etc/systemd/journald.conf; then
    sed -i 's/^#\?SystemMaxUse=.*/SystemMaxUse=500M/' /etc/systemd/system/../journald.conf 2>/dev/null \
        || sed -i 's/^#\?SystemMaxUse=.*/SystemMaxUse=500M/' /etc/systemd/journald.conf
    grep -q '^SystemMaxUse=500M' /etc/systemd/journald.conf \
        || echo 'SystemMaxUse=500M' >> /etc/systemd/journald.conf
fi
systemctl restart systemd-journald
journalctl --disk-usage

echo "== B6: logrotate（文件日志保护，missingok 面向未来） =="
mkdir -p /stock_monitor/logs
cat > /etc/logrotate.d/stock_monitor <<'EOF'
/stock_monitor/logs/*.log {
    daily
    size 50M
    rotate 5
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
}
EOF
echo "  /etc/logrotate.d/stock_monitor 已写入"

echo "== 附带: Redis maxmemory 256mb + allkeys-lru（对齐安装报告设计） =="
redis-cli config set maxmemory 256mb >/dev/null
redis-cli config set maxmemory-policy allkeys-lru >/dev/null
redis-cli config rewrite >/dev/null 2>&1 || true
redis-cli config get maxmemory maxmemory-policy

echo "== 加固完成 =="
