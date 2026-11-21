# 同花顺官方数据源能力评估与 market_alert 接入（D71，2026-11-21）

> 触发：用户「同花顺官方数据源能力评估（新发现 Hithink-Tech-Org/Financial-API），可以考虑接入提升项目 market_alert 能力」
> 承接：`docs/39` §3/§4 N-10（当时裁定「只评估不接入」）、`docs/40` §4（N-10 未做）
> 官方仓库：`Hithink-Tech-Org/Financial-API`（MIT，2026-11-12 快照 2026.11.12.5）
> 官方文档：`https://fuyao.aicubes.cn/docs/`、REST 契约、`llms-full.txt`
> **代码侧已完成一个默认关闭的只读适配层；未取得 API Key、未做真实请求、未部署 ECS。**

---

## 1. 结论

**可以接入，但只接「盘面证据」，不接行情主链。** 官方 API 的差异化能力（封板率/炸板/连板/异动原因）正好补上本项目 `market_alert` 算不出来的维度；它明确**不提供分钟 K / tick / Level-2 / 港美股**，所以不能替代腾讯/新浪/东财任何一路日 K 或实时报价。

落地形态：**默认关闭 + 无 Key 零请求 + 只写告警展示字段**。开关 `THS_MARKET_ALERT_ENRICH=false` 时运行时与改动前完全一致；启用后每 120s 最多取一次快照，失败静默降级。

---

## 2. 官方能力清单（与 market_alert 相关部分）

统一响应 `ApiResponse`：`code`（0 成功）/`message`/`request_id`/`data`；**HTTP 恒 200**，业务错误必须看 `code`。鉴权头 `X-api-key`，缺失或无效 `code=2001`。时间戳毫秒、时区 `Asia/Shanghai`。

| 接口 | 关键字段 | 对 market_alert 的价值 |
|---|---|---|
| `GET /api/a-share/special-data/limit-up-statistics` | `seal_rate`（封板率，小数）、`open_stock_count`（涨停打开家数）、`limit_up_stock_count`、`touched_limit_up_stock_count`、`page_boards[].promotion_rate`（晋级率）、`exact_boards[].board_num/stock_count`（精确连板） | **最高**：现有告警只能数涨跌停家数，无法判断「封住没有 / 炸了多少 / 高标断层」 |
| `GET /api/a-share/special-data/limit-break-pool` | `pagination.total`、`open_times`、`first_seal_time`/`last_seal_time`、`turnover` | 炸板家数；`size=1` 即可拿到 total，无需拉全量 |
| `GET /api/a-share/special-data/anomaly-analysis-list?tag_codes=LIMIT_UP,SHARP_RISE` | `stock_name`、`tag_name`、`analysis_content`、`keyword_list` | 异动**原因**（项目目前只有「涨了多少」，没有「为什么涨」） |
| `limit-up-pool` / `limit-down-pool` / `limit-up-ladder` | 涨停时间、封单额、连板天数；近 30 交易日梯队矩阵 | 暂不接：首版证据已够，避免多请求；留作后续 |
| `skyrocket-list` / `hot-stock-list*` | 飙升榜、热股榜（默认 30 / 最多 100，日榜与小时榜） | 暂不接：热度属个股维度，不是大盘告警 |
| `dragon-tiger-list` / `*-institution` / `*-department-*` | 龙虎榜、机构/游资席位 | 暂不接：对应遗留 M5-1「龙虎榜日报」，不在本次 market_alert 范围 |
| `GET /api/a-share/prices/snapshot` / 历史 K（日/周/月，前/后复权） | 标准行情 | **不接**：主链已有腾讯/新浪/东财/THS 网页四路，且官方明确无分钟 K |
| 交易日历、公司行动复权、财务、指数板块、公募基金、期货期权 | — | 不接：与本次目标无关 |

**明确不提供**：分钟 K、tick、Level-2、港股/美股行情、宏观数据、新闻公告原文、研报原文、基金申赎、投资建议。端内专用能力（资金流向/高频动向/筹码形态）只在同花顺 AI 客户端，公开 API 不提供；`docs/27` 的「暗盘资金拆分」仍不能靠它解决。

---

## 3. 与现状的缺口对照

`quant_engine/services/market/market_alert.py` 现有 5 类检测：涨跌停家数（D63 逐股口径 + 宽度交叉判定）、成交量突增/萎缩、市场状态/评分变更、市场宽度、板块异动（注入 provider）。

缺口与补法：

| 缺口 | 补法 | 是否改信号 |
|---|---|---|
| 只看「涨停多少家」，不看封板率/炸板 | 告警挂 `evidence`：`seal_rate` / `open_stock_count` / `touched_limit_up_stock_count` / `break_stock_count` | 否（只展示） |
| 无连板梯队概念 | `evidence.top_boards`（精确板数 top3） | 否 |
| 无「为什么异动」 | `evidence.anomaly_reasons`（前 5 条 tag + 关键词 + 解读） | 否 |
| 封板转弱无独立提示 | 新增 `SEAL_RATE_WEAK` 告警（默认关闭开关下不产生） | 否（告警不是交易信号） |
| 陈旧行情解释（docs/37 2.7~4.4h） | `evidence.source/as_of/trade_date` 明确「谁的数据、什么时刻」 | 否 |

**零信号影响论证**：新增字段只在告警 dict 与推送文案；`MarketAlert` 不接 `SignalGenerator`，`detect_alerts()` 的返回只进通知与日报输入，不进评分/门控/Kelly/风控/回测；`quant_engine/backtest/` 与 `strategies/` 零改动。因此不需要 D41 双跑（D41 只覆盖决策核心 6 字段白名单），但本次仍跑了全量测试确认无回归。

---

## 4. 代码落地（2026-11-21，本地完成）

| 文件 | 改动 |
|---|---|
| `quant_engine/data/sources/ths_financial.py`（新增） | 只读 REST 薄客户端：`to_thscode`、`_get`（统一信封校验 + 串行 0.5s）、`limit_up_statistics`、`limit_break_pool_size`、`anomaly_reasons`、`market_alert_context`、`build_ths_context_provider` |
| `quant_engine/config.py` | 新增 `THS_FINANCIAL_API_KEY` / `THS_FINANCIAL_BASE_URL` / `THS_FINANCIAL_TIMEOUT` / `THS_MARKET_ALERT_ENRICH=False` / `THS_SEAL_RATE_WEAK=0.6` / `THS_LIMIT_BREAK_MIN=20` |
| `quant_engine/services/market/market_alert.py` | 注入 `ths_context_provider`（默认 None）；120s 证据缓存；涨跌停告警挂 `evidence`；`SEAL_RATE_WEAK`；推送文案附来源行 |
| `quant_engine/worker.py`、`quant_engine/api/dependencies.py` | 用工厂注入 provider；默认关闭/无 Key 时传 None |
| `quant_engine/.env.example` | 新增 4 行配置占位，Key 留空 |
| `test/test_ths_financial_d71.py`（新增） | 10 例：码转换、无 Key 零请求、信封/HTTP/JSON 异常降级、请求契约与字段、工厂双关闭条件、证据只挂展示、默认关闭无新告警、封板转弱 + 冷却、文案兼容 |

**失败语义**：无 Key → 工厂直接 None；HTTP≠200 / `code≠0` / JSON 异常 / 网络异常 → `None` 并告警日志；provider 抛异常 → `MarketAlert` 捕获、不影响主循环；证据缓存 120s，失败不缓存以便下一轮重试。

**回退**：`.env` 设 `THS_MARKET_ALERT_ENRICH=false` 或清空 `THS_FINANCIAL_API_KEY` 并重启三服务；两者任一即可回到零请求原行为。

---

## 5. 授权、成本与合规（启用前必须确认）

1. API Key 在 `https://fuyao.aicubes.cn/admin/` 签发；REST/MCP/Python/CLI 共用同一 Key。Key 只写 gitignored 的 `.env`，仓库/文档/日志绝不落值；`scan_secrets.py` 目前识别 `sk-`/`bs-`/`tp-`，**新增 Key 前缀需先补规则再写入**。
2. 官方声明「不设置累计调用次数上限」，但要求避免短时集中请求或过高并发、触发动态限流后降频重试。本项目串行 + 0.5s + 120s 缓存（交易时段约 3 请求/2 分钟/进程）满足该约束；注意 worker 与 api 是两进程，各自独立，总频率翻倍仍远低于限制。
3. 仓库 MIT **不等于数据许可**。数据使用条款、是否允许商用/再分发，以官网与账号授权为准；本次只做能力评估，未做真实请求，未在现网启用。
4. 官方原则「真实数据不可用时说明原因，不使用模拟/静态数据替代」——适配层严格遵守：失败返回 `None`，测试 fixture 只在测试内使用。

---

## 6. 启用步骤（待用户裁定，未执行）

1. 用户确认数据条款与成本 → 签发 Key；
2. `scan_secrets.py` 若需，补新 Key 前缀识别；
3. ECS `/stock_monitor/.env` 写 `THS_FINANCIAL_API_KEY=<key>`、`THS_MARKET_ALERT_ENRICH=true`；
4. 随 D69/D70 一起一次性部署（本清单并入 `docs/38` A1 表）；
5. 验证：日志出现 `[THS-FIN]` 仅在失败时；人工核对一条涨停告警的「数据来源：同花顺官方」行与 `seal_rate` 数值；确认无 `code=2001`；
6. 观察一个交易日：无动态限流告警、无推送量异常膨胀（`SEAL_RATE_WEAK` 受 300s 冷却约束）。

---

## 7. 边界与未做

- 未取得/未验证任何 API Key；**无真实请求证据**，全部接口行为依据官方文档，不代表线上权限与延迟。
- 热榜、龙虎榜、连板天梯、集合竞价机会、成交额预测、筹码形态未接入；后两者是端内专用契约，公开 API 不可用。
- 不替换任何行情源；`docs/39` 对 eltdx / TDX 的结论不变。
- 本次代码只本地提交，未部署 ECS；现网动作按 `docs/38` + 本文 §6 一次性执行。
