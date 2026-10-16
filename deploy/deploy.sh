#!/bin/bash
# 量化扫描系统 V5 - 一键部署脚本
# 使用方法: bash deploy.sh

set -e

echo "==================================="
echo "量化扫描系统 V5 - 自动部署脚本"
echo "==================================="

# 配置变量
PROJECT_DIR="/root/stock_monitor"
REPO_URL=""  # 如果使用git，填入仓库地址

# 1. 安装系统依赖
echo "[1/6] 安装系统依赖..."
apt-get update
apt-get install -y redis-server python3-pip python3-venv

# 2. 启动Redis
echo "[2/6] 启动Redis服务..."
systemctl start redis-server
systemctl enable redis-server

# 测试Redis
if redis-cli ping | grep -q "PONG"; then
    echo "Redis启动成功"
else
    echo "Redis启动失败，请检查"
    exit 1
fi

# 3. 创建项目目录
echo "[3/6] 创建项目目录..."
mkdir -p $PROJECT_DIR/quant_engine

# 4. 创建环境配置
echo "[4/6] 创建环境配置..."
cat > $PROJECT_DIR/quant_engine/.env << 'EOF'
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
API_HOST=0.0.0.0
API_PORT=8000
DATA_SOURCE=tushare
TUSHARE_TOKEN=
DINGTALK_WEBHOOK=<redacted>
DINGTALK_SECRET=REDACTED
EOF

# 5. 安装Python依赖
echo "[5/6] 安装Python依赖..."
cd $PROJECT_DIR
pip3 install -r quant_engine/requirements.txt

# 6. 创建systemd服务
echo "[6/6] 创建系统服务..."

# API服务
cat > /etc/systemd/system/quant-api.service << EOF
[Unit]
Description=Quant Engine API
After=network.target redis.service

[Service]
Type=simple
User=root
WorkingDirectory=$PROJECT_DIR
ExecStart=/usr/bin/python3 -m quant_engine.main
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# Worker服务
cat > /etc/systemd/system/quant-worker.service << EOF
[Unit]
Description=Quant Engine Worker
After=network.target redis.service

[Service]
Type=simple
User=root
WorkingDirectory=$PROJECT_DIR
ExecStart=/usr/bin/python3 -m quant_engine.worker
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# Beat服务
cat > /etc/systemd/system/quant-beat.service << EOF
[Unit]
Description=Quant Engine Beat
After=network.target redis.service

[Service]
Type=simple
User=root
WorkingDirectory=$PROJECT_DIR
ExecStart=/usr/bin/python3 -m quant_engine.beat
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# 重载systemd
systemctl daemon-reload

echo "==================================="
echo "部署完成！"
echo "==================================="
echo ""
echo "请先上传项目文件到: $PROJECT_DIR/quant_engine/"
echo ""
echo "启动服务命令:"
echo "  systemctl start quant-api"
echo "  systemctl start quant-worker"
echo "  systemctl start quant-beat"
echo ""
echo "查看状态命令:"
echo "  systemctl status quant-api"
echo "  systemctl status quant-worker"
echo "  systemctl status quant-beat"
echo ""
echo "查看日志命令:"
echo "  journalctl -u quant-api -f"
echo "  journalctl -u quant-worker -f"
echo "  journalctl -u quant-beat -f"
echo ""
echo "测试API:"
echo "  curl http://localhost:8000/api/health"
echo "  curl http://localhost:8000/api/signals"
echo "==================================="
