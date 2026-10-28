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
# G14 修复：原 heredoc 内 DINGTALK_WEBHOOK=<redacted> / DINGTALK_SECRET=REDACTED
# 为字面占位符，一旦执行会把现网 .env 写成废值导致钉钉推送全废。
# 现规则：已存在 .env 则不覆盖（保护凭据）；敏感字段一律留空 + 注释，
# 由运维手工填写。取数路径由 SourceManager 多源降级链决定（prd/02 §3），
# 不依赖 DATA_SOURCE——该字段为遗留零引用项（见 prd/02 §4 注），此处不写入。
if [ -f "$PROJECT_DIR/quant_engine/.env" ]; then
    echo "  .env 已存在，保留不动（保护现网凭据）"
else
    cat > $PROJECT_DIR/quant_engine/.env << 'EOF'
# ===== 基础 =====
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
API_HOST=0.0.0.0
API_PORT=8000

# ===== 可选凭据（留空则对应功能关闭，勿填占位符）=====
# Tushare token（兜底线，免费源全不可用时才轮到）：https://tushare.pro 注册获取
TUSHARE_TOKEN=
# 钉钉机器人 webhook + 加签 secret：机器人管理页获取，两者需成对
DINGTALK_WEBHOOK=
DINGTALK_SECRET=
# API 认证 token（P1-1）：留空则所有端点无认证开放，公网暴露必须配置
API_AUTH_TOKEN=
# AI 摘要（DeepSeek）：留空则 AI 摘要关闭，不影响主链
DEEPSEEK_API_KEY=
OPENAI_BASE_URL=https://api.deepseek.com/v1
EOF
    echo "  已生成 .env 模板（敏感字段留空，请按需填写后重启服务）"
fi

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
