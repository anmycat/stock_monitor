# stock_monitor

A股智能投研系统：**产品线**（quant_engine v5.0 盘中决策引擎）+ **调研线**（交易规则方法论与半年实盘验证）双轨并行。

## 目录布局

```
stock_monitor/                  ← 本仓库根 = 服务器 /stock_monitor
├── quant_engine/    生产代码：FastAPI + Redis + Celery beat/worker 三服务
├── test/            测试（3 个，unittest 直跑）
├── UX/              前端原型（Apple Design System，尚未接入后端）
├── deploy/          部署脚本（本地上传 + 服务器 provisioning）
├── prd/             产品文档：15 个按功能模块命名的文档（00-12 + README + manifest）
├── research/        调研线资产：rules v1.4 + 16 份调研原文 + 验证数据与脚本
├── coding/          仅含本地 .venv 虚拟环境（不入 git，可随时重建）
└── .git/
```

## 快速开始

### 本地运行

```bash
# 虚拟环境（首次：python3 -m venv coding/.venv && coding/.venv/bin/pip install -r quant_engine/requirements.txt）
coding/.venv/bin/python -m uvicorn quant_engine.main:app --reload --port 8000
# 浏览器打开 http://localhost:8000 （API 文档 /docs）
```

### 测试

```bash
cd test && ../coding/.venv/bin/python -m unittest discover -v
```

### 部署（ECS：root@8.134.89.55，/stock_monitor，端口 8000）

```bash
cd deploy
SSHPASS='<服务器密码>' bash deploy_onekey.sh     # bash 版上传（需 sshpass）
# 或：REMOTE_PASSWORD='<服务器密码>' python3 deploy_with_python.py
# 服务器端 provisioning（三服务 api/worker/beat）：bash deploy.sh
```

生产环境：三个 systemd 服务 `quant-api` / `quant-engine-worker` / `quant-engine-beat`。

## 文档入口

| 想了解什么 | 去这里 |
|---|---|
| 项目全貌、架构、设计哲学 | `prd/00_总体设计与系统边界.md` |
| 任何功能模块的规格与实现 | `prd/01`-`prd/12`（文件名即功能模块） |
| 调研规则 v1.4 与验证数据 | `research/README.md` → `research/rules/methodology_rules.json` |
| 已知风险与遗留问题 | `prd/10_风险登记.md`、`prd/12_审计清单.md` |

## 当前状态（2026-10-20）

- **生产**：ECS 8.134.89.55:8000 在线，Redis healthy，三服务运行中；数据源优先级已重组（腾讯/新浪一线、东财备路，akshare 移出主路）——**本地已提交待推送部署**（领先 origin/main 16 个提交）
- **策略**：trend / dip / defense + 三个 YAML 策略（trend_breakout_v1 / dip_reversal_v1 / balanced_alpha_v1）+ 八因子评分 + 三层漏斗
- **当日新增（F 线·数据源重组）**：修复两条断裂链路——stock_list（原仅 akshare 未安装 → 退化为 5 只兜底；新浪 `get_all_stocks` 实测 5574 只，含科创 617/北交所 351/创业板 936）、daily（原仅 akshare 实现 → signals 恒空；腾讯 fqkline + 新浪 getKLineData 双第一线）；ETF 监控改按目标代码直查实时路。详见 `prd/02 §3.8`
- **当日落地（TDX minute）**：通达信 pytdx 分钟线接入（`sources/tdx.py`，`minute` 第一线）——补齐 gates 分钟断链（接入前无任何源实现 `get_minute_data`，gates 恒放行降级）；5/15/30/60 分钟K，服务器池探活 + 掉线重试 + 全失败降级不断链。详见 `prd/02 §3.9`
- **当日调研（P2 待确认）**：同花顺涨停池/日K/分时实测可用，拟作 daily 第三线 + 信号归因素材
- **待办**：① M1 清偿部署债：推送 16 个提交 + ECS 部署验收（交易时段验证 `/api/market` 覆盖数、`/api/signals` 非空、`/api/etf` 实时价、**TDX 117.34.114.x 可达性**）② THS daily 第三线接入（P2，待确认）③ `/api/evaluation/runs` 500 错误 ④ 回测池扩容 100 只/12 个月

## 双轨说明（重要）

产品线（quant_engine）与调研线（research/rules）是**两套独立的策略体系**，文档中并列呈现、不强行调和：调研验证结论（如 R13 变体 +0.34%/笔）只对调研口径成立，不可外推为产品盘中信号的收益承诺。

## 纪律

1. **凭据绝不入 git**：真实配置只在 `quant_engine/.env`（已被 .gitignore 排除）；`.env.example` 是唯一允许提交的模板
2. **调研规则单一事实源**：改规则必须走 `research/rules/methodology_rules.json`，并同步 prd 相关章节，不允许只改一边
3. **ECS 密码不落盘**：部署时通过环境变量临时注入（见上方部署命令）
