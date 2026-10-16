#!/usr/bin/env python3
"""
量化交易系统自动部署脚本
使用paramiko库处理SSH连接
"""
import paramiko
import subprocess
import sys
import os
from pathlib import Path

# 配置
REMOTE_USER = "root"
REMOTE_HOST = "8.134.89.55"
REMOTE_PASSWORD = os.getenv("REMOTE_PASSWORD", "")
REMOTE_DIR = "/stock_monitor"
LOCAL_DIR = Path(__file__).resolve().parent.parent

def print_header(text):
    print("\n" + "=" * 60)
    print(text)
    print("=" * 60 + "\n")

def print_step(step, total, text):
    print(f"\n{'='*60}")
    print(f"步骤 {step}/{total}: {text}")
    print(f"{'='*60}")

def check_dependencies():
    """检查并安装依赖"""
    try:
        import paramiko
        print("✅ paramiko已安装")
    except ImportError:
        print("❌ 正在安装paramiko...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "paramiko"])
        import paramiko

def create_ssh_client():
    """创建SSH客户端"""
    if not REMOTE_PASSWORD:
        raise RuntimeError("missing REMOTE_PASSWORD")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=REMOTE_HOST,
        username=REMOTE_USER,
        password=REMOTE_PASSWORD,
        timeout=30
    )
    return client

def execute_ssh_command(client, command, verbose=True):
    """执行SSH命令"""
    stdin, stdout, stderr = client.exec_command(command)
    
    if verbose:
        output = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')
        
        if output:
            print(output)
        if error and 'Warning' not in error:
            print(f"❌ 错误: {error}")
        
        return stdout.channel.recv_exit_status() == 0
    return True

def upload_files(client):
    """上传文件到服务器"""
    print_step(1, 6, "创建远程目录")
    execute_ssh_command(client, f"mkdir -p {REMOTE_DIR}")
    print("✅ 目录创建成功")
    
    print_step(2, 6, "同步代码文件")
    # 使用scp上传
    transport = client.get_transport()
    sftp = paramiko.SFTPClient.from_transport(transport)
    
    # 排除的文件/目录
    exclude_patterns = {
        '__pycache__', '*.pyc', '.DS_Store', '*.md',
        'venv', '.venv', '.git', 'node_modules',
        'test', 'UX', 'deploy',
        'deploy.sh', 'deploy_auto.sh', 'deploy_step.sh',
        'deploy_onekey.sh', 'deploy_with_python.py'
    }
    
    def upload_dir(local_path, remote_path):
        """递归上传目录"""
        try:
            sftp.mkdir(remote_path)
        except:
            pass
        
        for item in os.listdir(local_path):
            # 简单字符串匹配
            if item in exclude_patterns or item.endswith('.pyc') or item.startswith('.'):
                continue
            
            local_item = local_path / item
            remote_item = f"{remote_path}/{item}"
            
            if local_item.is_dir():
                upload_dir(local_item, remote_item)
            else:
                try:
                    sftp.put(str(local_item), remote_item)
                    print(f"  ⬆️  {item}")
                except Exception as e:
                    print(f"  ❌ 上传失败 {item}: {e}")
    
    upload_dir(LOCAL_DIR, REMOTE_DIR)
    sftp.close()
    print("✅ 代码上传完成")

def install_dependencies(client):
    """安装Python依赖"""
    print_step(3, 6, "安装Python依赖")
    
    commands = [
        ("cd /stock_monitor && python3 --version", True),
        ("cd /stock_monitor && pip3 install --break-system-packages -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple", True),
        ("cd /stock_monitor && pip3 install --break-system-packages python-multipart -i https://pypi.tuna.tsinghua.edu.cn/simple", True),
    ]
    
    for cmd, verbose in commands:
        execute_ssh_command(client, cmd, verbose)
    
    print("✅ 依赖安装完成")

def create_config(client):
    """创建配置文件"""
    print_step(4, 6, "创建配置文件")
    
    env_content = """# 基础配置
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
"""
    
    # 检查.env是否存在
    stdin, stdout, stderr = client.exec_command(f"test -f {REMOTE_DIR}/.env && echo 'exists' || echo 'not_exists'")
    result = stdout.read().decode('utf-8').strip()
    
    if result == 'not_exists':
        # 创建.env文件
        stdin, stdout, stderr = client.exec_command(f"cat > {REMOTE_DIR}/.env << 'EOF'\n{env_content}\nEOF")
        print("✅ 配置文件已创建")
    else:
        print("✅ 配置文件已存在")
    
    # 创建日志目录
    execute_ssh_command(client, f"mkdir -p {REMOTE_DIR}/logs")

def setup_systemd(client):
    """配置systemd服务"""
    print_step(5, 6, "配置systemd服务")
    
    service_content = """[Unit]
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
"""
    
    # 写入service文件
    command = f"""cat > /etc/systemd/system/quant-engine.service << 'SERVICEEOF'
{service_content}
SERVICEEOF
"""
    execute_ssh_command(client, command)
    
    # 重新加载和启用
    execute_ssh_command(client, "systemctl daemon-reload")
    execute_ssh_command(client, "systemctl enable quant-engine")
    
    print("✅ systemd服务配置完成")

def verify_deployment(client):
    """验证部署"""
    print_step(6, 6, "验证部署状态")
    
    print("\n📁 文件检查:")
    execute_ssh_command(client, f"cd {REMOTE_DIR} && ls -la | head -20")
    
    print("\n📦 Python包验证:")
    packages = [
        "import fastapi; print(f'✅ FastAPI {fastapi.__version__}')",
        "import uvicorn; print(f'✅ Uvicorn {uvicorn.__version__}')",
        "import pandas; print(f'✅ Pandas {pandas.__version__}')",
        "import numpy; print(f'✅ NumPy {numpy.__version__}')",
        "import redis; print(f'✅ Redis {redis.__version__}')",
        "import loguru; print(f'✅ Loguru {loguru.__version__}')",
    ]
    
    for pkg in packages:
        execute_ssh_command(client, f"python3 -c \"{pkg}\"")
    
    print("\n✅ 部署验证完成")

def print_summary():
    """打印总结"""
    print_header("🎉 部署成功完成！")
    
    print("📋 下一步操作:\n")
    
    print("1️⃣  启动服务（请选择一种方式）:\n")
    print("   方式A - systemd服务（推荐）:")
    print(f"   ssh {REMOTE_USER}@{REMOTE_HOST}")
    print("   systemctl start quant-engine")
    print("   systemctl status quant-engine\n")
    
    print("   方式B - 直接运行（调试用）:")
    print(f"   ssh {REMOTE_USER}@{REMOTE_HOST}")
    print("   cd /stock_monitor")
    print("   python3 -m uvicorn quant_engine.main:app --host 0.0.0.0 --port 8000\n")
    
    print("2️⃣  访问服务:")
    print(f"   🌐 Web界面: http://{REMOTE_HOST}:8000")
    print(f"   📖 API文档: http://{REMOTE_HOST}:8000/docs")
    print(f"   💚 健康检查: http://{REMOTE_HOST}:8000/health\n")
    
    print("3️⃣  测试API（启动服务后执行）:")
    print(f"   curl http://{REMOTE_HOST}:8000/health")
    print(f"   curl http://{REMOTE_HOST}:8000/api/signals\n")
    
    print("4️⃣  查看日志:")
    print(f"   ssh {REMOTE_USER}@{REMOTE_HOST}")
    print("   journalctl -u quant-engine -f")
    print("   # 或")
    print("   tail -f /stock_monitor/logs/quant-engine.log\n")
    
    print("5️⃣  配置API认证（推荐）:")
    print("   # 生成安全token")
    print("   openssl rand -hex 32")
    print("   # 编辑 /stock_monitor/.env")
    print("   # 添加: API_AUTH_TOKEN=生成的token")
    print("   # 重启: systemctl restart quant-engine\n")
    
    print("=" * 60)
    print("💡 快速启动命令:")
    print("=" * 60)
    print()
    print("# 启动服务")
    print(f"ssh {REMOTE_USER}@{REMOTE_HOST} 'systemctl start quant-engine'")
    print()
    print("# 检查状态")
    print(f"ssh {REMOTE_USER}@{REMOTE_HOST} 'systemctl status quant-engine'")
    print()
    print("# 测试访问")
    print(f"curl http://{REMOTE_HOST}:8000/health")
    print()
    print("=" * 60)

def main():
    """主函数"""
    print_header("量化交易系统自动部署")
    
    # 检查依赖
    check_dependencies()
    
    # 创建SSH连接
    print(f"\n🔗 正在连接到 {REMOTE_HOST}...")
    client = create_ssh_client()
    print("✅ SSH连接成功\n")
    
    try:
        # 执行部署步骤
        upload_files(client)
        install_dependencies(client)
        create_config(client)
        setup_systemd(client)
        verify_deployment(client)
        
        # 打印总结
        print_summary()
        
    except Exception as e:
        print(f"\n❌ 部署失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()
        print("\n✅ SSH连接已关闭")

if __name__ == "__main__":
    main()
