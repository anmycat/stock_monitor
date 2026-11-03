# stock_monitor

A股智能投研系统：**产品线**（`quant_engine` 盘中决策引擎）+ **调研线**（交易规则方法论与实盘验证）双轨并行。

## 目录布局

```
stock_monitor/
├── quant_engine/    生产代码：FastAPI + Redis + Celery beat/worker 三服务
├── test/            测试（unittest 直跑）
├── UX/              前端原型（Apple Design System）
├── deploy/          部署脚本（参数化：主机/端口经环境变量传入）
├── research/        调研线资产：rules + 调研原文归档 + 验证数据与脚本
├── coding/          仅含本地 .venv 虚拟环境（不入 git，可随时重建）
└── .git/
```

> 私有目录（需求规格 / 数据源 / 数据模型 / 执行与验收 / 风险登记 / 审计清单等研究文档）不在本仓库公开，属未公开研究成果，仅保留于本地与部署环境。

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

### 部署

部署脚本不内置任何主机地址与凭据，目标主机通过环境变量传入：

```bash
export DEPLOY_HOST=<目标主机>          # 必填
export DEPLOY_PORT=22                  # 可选，默认 22
export DEPLOY_PATH=<部署目录>          # 可选
export DEPLOY_USER=<登录用户>          # 可选，默认 root
# 凭据仅通过环境变量临时注入，不落盘
bash deploy/update_ecs.sh
```

生产环境为三个 systemd 服务：`quant-api` / `quant-worker` / `quant-beat`。

## 文档入口

| 想了解什么 | 去这里 |
|---|---|
| 调研规则与验证数据 | `research/README.md` → `research/rules/methodology_rules.json` |
| 调研原文与验证报告 | `research/docs/` |
| 数据源适配与降级链 | `quant_engine/data/sources/`（各源 docstring 即规格） |
| 接口清单 | 启动后 `/docs`（FastAPI 自动生成） |

## 架构要点

- **五层依赖宪法**（L0 api → L1 services → L2 strategies/backtest → L3 data/models → L4 utils），由 `research/scripts/arch_audit.py` 机器校验
- **多源降级链**：每个能力声明优先级，逐级降级；源级冷却与熔断，空结果与真实错误分离计数
- **数据源契约层**（`quant_engine/data/spec.py`）：统一能力声明 + 字段对齐 + 源口径声明（成交量单位/复权方式），失败以结构化结果返回而非抛异常
- **策略与提示分层**：`strategies/`（打分与漏斗）与 `services/ai/`（多模型融合总结）互不耦合

## 双轨说明（重要）

产品线（`quant_engine`）与调研线（`research/rules`）是**两套独立的策略体系**，文档中并列呈现、不强行调和：调研验证结论只对调研口径成立，不可外推为产品盘中信号的收益承诺。

## 纪律

1. **凭据绝不入 git**：真实配置只在 `.env`（已被 `.gitignore` 排除）；`.env.example` 是唯一允许提交的模板
2. **调研规则单一事实源**：改规则必须走 `research/rules/methodology_rules.json`，不允许只改一边
3. **部署凭据不落盘**：通过环境变量临时注入
4. **敏感信息扫描**：提交前自动扫描（`research/scripts/scan_secrets.py`，含 pre-commit 钩子）
