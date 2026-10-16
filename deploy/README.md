# deploy · 部署目录

全部部署相关脚本（本地执行的上传脚本 + 服务器端 provisioning 脚本）。2026-10-16 从 `coding/` 根与 `quant_engine/` 内归集到本目录；同日仓库二次重组（撤销 `coding/` 中间层），本目录随 `quant_engine/test/UX` 一并提升到仓库根，同步逻辑改为**仓库根 = 远程 `/stock_monitor`** 同构映射。

## 文件清单与分工

| 脚本 | 执行位置 | 机制 | 用途 |
|---|---|---|---|
| `deploy_onekey.sh` | 本地 | sshpass + rsync + systemd 六步 | 一键上传部署（单服务版） |
| `deploy_with_python.py` | 本地 | paramiko SFTP + systemd | 一键上传部署的 Python 平行实现 |
| `deploy.sh` | **服务器上** | bash provisioning | 装 Redis + 创建 `/stock_monitor/quant_engine/.env` + 三个 systemd 服务（api/worker/beat） |

## 目录结构与同步逻辑

```
本地仓库根（stock_monitor/）      服务器 /stock_monitor/
├── quant_engine/          →     ├── quant_engine/     （uvicorn quant_engine.main:app）
├── test/          （排除）       ├── test/      不上传
├── UX/            （排除）       ├── UX/        不上传
├── prd/           （排除）       ├── prd/       不上传
├── research/      （排除）       ├── research/  不上传
└── deploy/        （排除）       └── deploy/    不上传
```

两个上传脚本的 `LOCAL_DIR` 均指向**仓库根**（本目录的上一级；2026-10-16 二次重组后脚本位置从 `coding/deploy/` 变为 `deploy/`，`parent.parent` 语义不变、代码零改动），远程 `/stock_monitor/quant_engine/` 包子目录正确生成；排除列表已包含 `test`、`UX`、`deploy`、`prd`、`research`、`.venv`、`__pycache__`。

## 运行

```bash
# 方式一：bash 版（需 sshpass：brew install sshpass）
cd deploy
SSHPASS='<服务器密码>' bash deploy_onekey.sh

# 方式二：Python 版（需 paramiko）
cd deploy
REMOTE_PASSWORD='<服务器密码>' python3 deploy_with_python.py

# 服务器端 provisioning（传到服务器后执行）
bash deploy.sh
```

目标服务器：`root@8.134.89.55`，目录 `/stock_monitor`，端口 8000。

## 本次修复（2026-10-16）

1. **LOCAL_DIR 错位**：两个上传脚本原位于 `quant_engine/` 内，`LOCAL_DIR` 取自身目录，会把 quant_engine 的内容直接同步成 `/stock_monitor/` 根（缺 `quant_engine/` 包子目录，远程 `uvicorn quant_engine.main:app` 必失败）。已改为取上一级目录
2. **排除列表**：新增 `test`、`UX`、`deploy`、`prd`、`research`；修正 `deploy_with_python.py` 排除列表里的笔误（原写 `deploy_with_python.sh`，实际文件是 `.py`，导致自身会被上传）
3. **配置漂移**：`deploy.sh` heredoc 中 `DINGTALK_WEBHOOK=<redacted>`、`DINGTALK_SECRET=REDACTED` 仍是脱敏占位符——重跑 provisioning 会覆盖真实值导致钉钉推送静默失效（prd/10 风险 G14）。**重跑前必须先改 heredoc 或改用服务器 .env 手工补填**

## 二次重组适配（2026-10-16 晚）

仓库撤销 `coding/` 中间层，本目录从 `coding/deploy/` 提升到仓库根 `deploy/`。两个上传脚本的 `LOCAL_DIR`（`$(dirname $0)/..` 与 `Path(__file__).parent.parent`）语义不变、**代码零改动**，同步逻辑变为仓库根 = 远程 `/stock_monitor` 同构映射（见上方结构图）。`deploy.sh` 内 provisioning 路径 `/root/stock_monitor` 与实际 `/stock_monitor` 存在表述差异（服务器上二者均为有效路径，待统一）。

## 已知局限

- `deploy_onekey.sh` / `deploy_with_python.py` 创建的是**单服务** `quant-engine.service`；现行生产是 deploy.sh 的**三服务**（quant-api/worker/beat）形态。一键脚本未跟进三服务拆分，重部署时以 deploy.sh + 手工 systemd 配置为准
- 两个上传脚本都会跳过点文件（`.env` 不上传），服务器 .env 由 provisioning 或手工创建——这正是 G14 漂移风险的根源，见 prd/07 §5.4
