#!/bin/bash
# 一键部署脚本 - 自动处理SSH确认和密码输入

: "${SSHPASS:?请先设置SSHPASS环境变量}"

REMOTE_USER="root"
REMOTE_HOST="8.134.89.55"
REMOTE_DIR="/stock_monitor"
LOCAL_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "======================================"
echo "量化交易系统一键部署"
echo "======================================"
echo "目标: ${REMOTE_HOST}:${REMOTE_DIR}"
echo "======================================"
echo ""

# 检查sshpass是否安装
if ! command -v sshpass &> /dev/null; then
    echo "❌ 未找到sshpass，正在安装..."
    if command -v brew &> /dev/null; then
        brew install sshpass
    elif command -v apt-get &> /dev/null; then
        sudo apt-get install -y sshpass
    else
        echo "请手动安装sshpass: brew install sshpass"
        exit 1
    fi
fi

# Step 1: 创建远程目录（自动处理SSH密钥确认）
echo "📁 步骤 1/6: 创建远程目录..."
sshpass -p "${SSHPASS}" ssh -o StrictHostKeyChecking=no ${REMOTE_USER}@${REMOTE_HOST} "mkdir -p ${REMOTE_DIR} && echo '✅ 目录创建成功'"
echo ""

# Step 2: 同步代码
echo "📦 步骤 2/6: 同步代码文件..."
rsync -avz --progress \
    --exclude '__pycache__' \
    --exclude '*.pyc' \
    --exclude '.DS_Store' \
    --exclude '*.md' \
    --exclude 'venv' \
    --exclude '.venv' \
    --exclude 'test' \
    --exclude 'UX' \
    --exclude 'deploy' \
    -e "sshpass -p ${SSHPASS} ssh -o StrictHostKeyChecking=no" \
    ${LOCAL_DIR}/ ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/
echo "✅ 代码同步完成"
echo ""

# Step 3: 安装依赖
echo "🔧 步骤 3/6: 安装Python依赖..."
sshpass -p "${SSHPASS}" ssh -o StrictHostKeyChecking=no ${REMOTE_USER}@${REMOTE_HOST} << 'ENDSSH'
cd /stock_monitor
echo "Python版本:"
python3 --version
echo ""
echo "安装依赖（这可能需要几分钟）..."
pip3 install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
echo ""
echo "安装额外依赖..."
pip3 install python-multipart -i https://pypi.tuna.tsinghua.edu.cn/simple
echo "✅ 依赖安装完成"
ENDSSH
echo ""

# Step 4: 创建配置文件
echo "⚙️ 步骤 4/6: 创建配置文件..."
sshpass -p "${SSHPASS}" ssh -o StrictHostKeyChecking=no ${REMOTE_USER}@${REMOTE_HOST} << 'ENDSSH'
cd /stock_monitor
if [ ! -f .env ]; then
cat > .env << 'EOF'
# 基础配置
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_PASSWORD=
REDIS_SSL=false
API_HOST=0.0.0.0
API_PORT=8000
API_AUTH_TOKEN=
DATA_SOURCE=akshare

# 日志配置
LOG_LEVEL=INFO
LOG_FILE=logs/quant_engine.log
EOF
echo "✅ 配置文件已创建"
else
echo "✅ 配置文件已存在"
fi
mkdir -p logs
ENDSSH
echo ""

# Step 5: 配置systemd服务
echo "📝 步骤 5/6: 配置systemd服务..."
sshpass -p "${SSHPASS}" ssh -o StrictHostKeyChecking=no ${REMOTE_USER}@${REMOTE_HOST} << 'ENDSSH'
# 创建systemd服务文件
cat > /etc/systemd/system/quant-engine.service << 'SERVICEEOF'
[Unit]
Description=Quant Engine - 量化交易系统
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/stock_monitor
Environment=PATH=/usr/local/bin:/usr/bin:/bin
ExecStart=/usr/bin/python3 -m uvicorn quant_engine.main:app --host 0.0.0.0 --port 8000 --workers 2
Restart=always
RestartSec=10
StandardOutput=append:/stock_monitor/logs/quant-engine.log
StandardError=append:/stock_monitor/logs/quant-engine-error.log

[Install]
WantedBy=multi-user.target
SERVICEEOF

# 重新加载systemd
systemctl daemon-reload
systemctl enable quant-engine
echo "✅ systemd服务配置完成"
ENDSSH
echo ""

# Step 6: 验证部署
echo "🔍 步骤 6/6: 验证部署状态..."
sshpass -p "${SSHPASS}" ssh -o StrictHostKeyChecking=no ${REMOTE_USER}@${REMOTE_HOST} << 'ENDSSH'
cd /stock_monitor

echo "📁 文件检查:"
ls -la | head -15
echo ""

echo "📦 Python包验证:"
python3 -c "import fastapi; print(f'✅ FastAPI {fastapi.__version__}')"
python3 -c "import uvicorn; print(f'✅ Uvicorn {uvicorn.__version__}')"
python3 -c "import pandas; print(f'✅ Pandas {pandas.__version__}')"
python3 -c "import numpy; print(f'✅ NumPy {numpy.__version__}')"
python3 -c "import redis; print(f'✅ Redis {redis.__version__}')"
python3 -c "import loguru; print(f'✅ Loguru {loguru.__version__}')"
echo ""

echo "✅ 部署验证完成"
ENDSSH

echo ""
echo "======================================"
echo "🎉 部署成功完成！"
echo "======================================"
echo ""
echo "📋 下一步操作:"
echo ""
echo "1️⃣  启动服务（请选择一种方式）:"
echo ""
echo "   方式A - systemd服务（推荐）:"
echo "   sshpass -p \"\$SSHPASS\" ssh -o StrictHostKeyChecking=no root@8.134.89.55"
echo "   systemctl start quant-engine"
echo "   systemctl status quant-engine"
echo ""
echo "   方式B - 直接运行（调试用）:"
echo "   sshpass -p \"\$SSHPASS\" ssh -o StrictHostKeyChecking=no root@8.134.89.55"
echo "   cd /stock_monitor"
echo "   python3 -m uvicorn quant_engine.main:app --host 0.0.0.0 --port 8000"
echo ""
echo "2️⃣  访问服务:"
echo "   🌐 Web界面: http://8.134.89.55:8000"
echo "   📖 API文档: http://8.134.89.55:8000/docs"
echo "   💚 健康检查: http://8.134.89.55:8000/health"
echo ""
echo "3️⃣  测试API（启动服务后执行）:"
echo "   curl http://8.134.89.55:8000/health"
echo "   curl http://8.134.89.55:8000/api/signals"
echo ""
echo "4️⃣  查看日志:"
echo "   sshpass -p \"\$SSHPASS\" ssh -o StrictHostKeyChecking=no root@8.134.89.55"
echo "   journalctl -u quant-engine -f"
echo "   # 或"
echo "   tail -f /stock_monitor/logs/quant-engine.log"
echo ""
echo "5️⃣  配置API认证（推荐）:"
echo "   # 生成安全token"
echo "   openssl rand -hex 32"
echo "   # 编辑 /stock_monitor/.env"
echo "   # 添加: API_AUTH_TOKEN=生成的token"
echo "   # 重启: systemctl restart quant-engine"
echo ""
echo "======================================"
echo "💡 快速启动命令:"
echo "======================================"
echo ""
echo "# 启动服务"
echo "sshpass -p \"\$SSHPASS\" ssh -o StrictHostKeyChecking=no root@8.134.89.55 'systemctl start quant-engine'"
echo ""
echo "# 检查状态"
echo "sshpass -p \"\$SSHPASS\" ssh -o StrictHostKeyChecking=no root@8.134.89.55 'systemctl status quant-engine'"
echo ""
echo "# 测试访问"
echo "curl http://8.134.89.55:8000/health"
echo ""
echo "======================================"
