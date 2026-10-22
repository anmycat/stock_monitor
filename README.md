# stock_monitor

A股智能投研系统：**产品线**（quant_engine v5.0 盘中决策引擎）+ **调研线**（交易规则方法论与半年实盘验证）双轨并行。

## 目录布局

```
stock_monitor/                  ← 本仓库根 = 服务器 /stock_monitor
├── quant_engine/    生产代码：FastAPI + Redis + Celery beat/worker 三服务
├── test/            测试（10 个，unittest 直跑）
├── UX/              前端原型（Apple Design System，尚未接入后端）
├── deploy/          部署脚本（本地上传 + 服务器 provisioning）
├── prd/             产品文档：15 个按功能模块命名的文档（00-14）+ README + manifest
├── research/        调研线资产：rules v1.4 + 20 份调研原文（00-19）+ 验证数据与脚本
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

## 当前状态（2026-10-21）

- **生产**：ECS 8.134.89.55:8000 在线，Redis healthy，三服务运行中；数据源优先级已重组（腾讯/新浪一线、东财备路，akshare 移出主路）——**git 侧已结（21 个提交已推送、本地与 origin/main 同步），剩余 ECS 一键部署 + 首日验收（部署债 = 战略第一待办 M1）**
- **策略**：trend / dip / defense + 三个 YAML 策略（trend_breakout_v1 / dip_reversal_v1 / balanced_alpha_v1）+ 八因子评分 + 三层漏斗
- **F 线·数据源重组（10-20）**：修复两条断裂链路——stock_list（原仅 akshare 未安装 → 退化为 5 只兜底；新浪 `get_all_stocks` 实测 5574 只，含科创 617/北交所 351/创业板 936）、daily（原仅 akshare 实现 → signals 恒空；腾讯 fqkline + 新浪 getKLineData 双第一线）；ETF 监控改按目标代码直查实时路。详见 `prd/02 §3.8`
- **TDX minute（10-20）**：通达信 pytdx 分钟线接入（`sources/tdx.py`，`minute` 第一线）——补齐 gates 分钟断链（接入前无任何源实现 `get_minute_data`，gates 恒放行降级）；5/15/30/60 分钟K，服务器池探活 + 掉线重试 + 全失败降级不断链。详见 `prd/02 §3.9`
- **四份调研消化（10-20，归档 research/docs/16-19）**：全链审计 D26（P1×3/P2×6 全处置）；ECS 性能实测（每轮 5.8s/300s、CPU 1.9%、并发无排队 → **不换语言不重构**，修数据链路）；DSA 参照系（可借鉴 P0×4/P1×4）；三层战略路线图（修命 M1-M4 → 补骨 B1-B5 → 增肌 A1-A4，见 `prd/09 §7`）
- **PRD 已修订至最新状态（10-20 晚，D27）**：09/14/07/08/10/12/13/README/manifest 全部显式升版登记；内容审计发现 realtime 市场概览池仍走腾讯硬编码 ~406 只（M2 收尾项，腾讯失败时新浪兜底 5574）
- **四大K线形态3个月实盘验证（10-20 深夜，docs/20，提交 707f989）**：4,229 只全市场票上四形态 5 日超额全负、三重检验全败（P1 -6.43%/P2 -0.86%/P3 -2.51%/P4 -1.83%），不纳入买入规则库；沉淀验证标准三件套（聚类显著性+条件分解+全市场基准，已入 docs/09 §11）；P3 的"回避信号"假设经牛熊分段复验**否决**（见下条）
- **结合评估（10-20 深夜，D28）**：L1 方法论（docs/09 §11 三件套）+ L2 取数设施已落地；**P3 牛熊分段复验完成，裁定否决**（docs/21）——3 个月的负Edge全部来自 2026H2 单一深熊 episode（-4.79%，聚类 t=-3.11），2026H1 熊市日反向 +2.17%；单 episode 不论证，rules JSON 不动；二次复验触发条件已登记（第二个独立深熊 episode 且负Edge重现）
- **PRD 内容审计（10-21，D29，12 §3.16）**：16 份文档 + manifest + README 全量核对，8 项发现全部修复——cooldown TTL 86400→14400s（4h）、stock_list TTL 300→86400s、Python 文件数 82→87、R12 半年数字误植三处订正、部署债表述 9 处更新（git 已同步）；Kelly/样本数/采纳配置一致性 7/7 通过
- **3M 第二窗口验真（10-21，D30，报告 research/docs/22）**：数据延伸至 10-20（neodata 同源 + 通达信独立源双校验 + 回归零破坏）后重跑 07-21~10-20 窗口——**v1.5 定稿复现**（196 笔/44.4%/+0.74%/+145.63%，8/9 月与 PRD 声明逐字段相同）；**R02 转负 −0.12%**（v1 的 +1.90% 由 5 笔早期暴利单驱动，重叠窗口 19 笔分毫不差——"最强规则"声明撤销）；R05 转负 −0.18%（尾部截断 7 笔 −19.48%，非尾部仍为正）；**rules JSON 已升 v1.6（用户 10-21 确认）：Kelly R02/R05→0、R04→2.84%、混合→1.92%、R01 维持 2.46%，R06 avoidance 与 R12 默认开启一并落库**
- **迭代后 3M 复验（10-22，D33，报告 research/docs/23）**：五阶段第五阶段——① **工程改动零破坏**：回归复跑非 R06 信号 225 笔零差异（284→295 增量全归因 rules v1.6 R06 avoidance）；生产链路交叉校验 M2' 池 27/27 + 报价 27/27 一致 + B5 零误剔 ② **第三窗口**（07-22~10-21）：定稿口径 **195 笔/44.1%/+0.68%/+131.72%**，三窗口稳定（201→196→195）；8/9 月月度数字三窗口逐字段相同；R02 19 笔 −2.32% 与 docs/22 归因分毫不差 ③ **调整**：验证脚本补 avoidance 跳过（v1.6 落库遗漏的消费方）；数据延伸至 10-21（TDX+腾讯双源三重校验 + vol 单位逐票标定 + 指数 vol 单位勘误）；**Kelly v1.7 建议（R04→2.36%/混合→1.69%）与"连续两月 ≤0 降仓"条款待用户确认，rules JSON 保持 v1.6 不动**
- **全量架构重构（10-22，P0-P3 已提交，设计+验收见 prd/15）**：六阶段绞杀者迁移——**P0** `6c99c9b3`（correlation_manager 384 行零调用切除 + akshare 半死链切除 + F/E9 73 项真问题清零，hot_money 降级行为保持）；**P1** `cf206978`（DataCenter 上帝对象 → `data/repositories/market_data.py`，15 消费方切换，git 识别 84% rename，缓存 key/TTL/线程锁/B5 断言逐一对齐）；**P2** `b81b5f8b`（services 30 模块 → 6 子包 DAG 宪法，51 文件全 rename 保历史）；**P3**（`api/routes.py` 1122 行 → 12 域 router，36 端点 URL/方法/响应模型对照表全绿，前端零改动）。每阶段验收：ruff F/E9=0 + 导入冒烟 + 172 测试全绿 + API 冒烟 4 端点 200 + 架构审计（模块级零环维持）+ 独立 commit。**P4/P5 进行中**（违宪边参数注入净化 + BLE001 分级 + 风格现代化分批）

- **待办**：① M1 清偿部署债：ECS 一键部署 + 验收（交易时段验证 `/api/market` 覆盖数、`/api/signals` 非空、`/api/etf` 实时价、TDX 117.34.114.x 可达性）② M2 收尾：realtime 池动态化（`/api/market` ≥5000 稳定达成）③ THS daily 第三线（P2，待确认）④ `/api/evaluation/runs` 500 错误 ⑤ 回测池扩容 100 只/12 个月（取数设施已就绪：`fetch_pattern_pool.py --bars/--out`）⑥ P3 二次复验触发条件：第二个独立深熊 episode 且负Edge重现才考虑深熊条件性回避闸门（docs/21 §7.2）⑦ 11-01 月度复验（`validate_3m_recent_v2.py` 滚动）

## 双轨说明（重要）

产品线（quant_engine）与调研线（research/rules）是**两套独立的策略体系**，文档中并列呈现、不强行调和：调研验证结论（如 R13 变体 +0.34%/笔）只对调研口径成立，不可外推为产品盘中信号的收益承诺。

## 纪律

1. **凭据绝不入 git**：真实配置只在 `quant_engine/.env`（已被 .gitignore 排除）；`.env.example` 是唯一允许提交的模板
2. **调研规则单一事实源**：改规则必须走 `research/rules/methodology_rules.json`，并同步 prd 相关章节，不允许只改一边
3. **ECS 密码不落盘**：部署时通过环境变量临时注入（见上方部署命令）
