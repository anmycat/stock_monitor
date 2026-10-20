# daily_stock_analysis 项目深度分析（侧重盘中监控与舆情收集）

> 分析日期：2026-10-20
> 对象：https://github.com/ZhuLinsen/daily_stock_analysis（66.7k star / 55.4k fork / Python / 2026-01 创建，10 月仍在高频更新）
> 方法：GitHub API 全量文件树（1217 文件）+ 关键模块源码级阅读（search_service 4961 行 / alert_service 1352 行 / alert_worker 910 行 / intelligence_service / events 566 行）+ README 与 .env.example 配置面分析
> 参照系：本项目 stock_monitor（quant_engine 15448 行 / 108 测试 / 规则引擎 + 监控推送）

---

## 1. 项目定位与整体架构

**一句话**：LLM 驱动的多市场（A股/港股/美股/日韩台）股票分析报告生成器 + 告警监控 + Web/桌面工作台。GitHub Actions 零成本定时运行是其主打场景。

### 1.1 规模与分层

| 层 | 模块 | 规模 | 职责 |
|---|---|---|---|
| 数据层 | `data_provider/` | 19 个 fetcher | akshare(105KB)/tushare/baostock/pytdx/yfinance/longbridge/futu/efinance/tickflow/miaoxiang/alphavantage/finnhub + fundamental_adapter 基本面适配 |
| 情报层 | `src/search_service.py` | **194KB / 4961 行** | 6+ 搜索引擎适配、新闻质量分级、相关性评分（本次分析重点） |
| 情报层 | `src/services/intelligence_service.py` | 36KB | RSS/Atom/NewsNow 可配置资讯源（含 SSRF 防护） |
| 舆情层 | `src/services/social_sentiment_service.py` | 14KB | Reddit/X/Polymarket 社交情绪（adanos.org，仅美股） |
| 分析层 | `src/analyzer.py` | 251KB | LLM 决策报告生成（评分/趋势/买卖点/风险/催化） |
| 监控层 | `src/services/alert_*.py` | 55+39+20KB | 告警规则 + worker + 技术指标（本次分析重点） |
| 决策层 | `decision_signal_*.py` | ~110KB | 从 LLM 报告提取结构化决策信号（用户截图的"决策仪表盘"） |
| 调度层 | `runtime_scheduler.py` | 33KB | 后台任务编排（分析任务 + 事件监控 + 看门狗超时） |
| 服务层 | `api/` + `apps/` | 41 + 335 文件 | FastAPI + Web/桌面客户端 |

**架构判断**：这是一个**重 LLM、重 Web**的项目。盘中监控只是其体系的一小块（告警），且设计目标是"辅助 LLM 分析"而非独立监控系统——与我们的定位（规则引擎 + 数据监控 + 推送）有本质差异。

---

## 2. 盘中监控策略深度分析

### 2.1 三层告警体系

```
配置层（声明式 JSON）
  AGENT_EVENT_ALERT_RULES_JSON=[{"stock_code":"600519","alert_type":"price_cross",
    "direction":"above","price":1800}, {...volume_spike, multiplier:2.5}]
        ↓
EventMonitor（src/agent/events.py，566 行）—— 轻量内存规则引擎
  PriceAlert 价格穿越（above/below）
  PriceChangeAlert 涨跌幅（up/down %）
  VolumeAlert 放量（> N × 20 日均量）
        ↓
AlertService（alert_service.py，1352 行）—— DB 持久化规则 CRUD
  + 技术指标规则（alert_indicators.py，514 行）：
    MA 交叉 / RSI 超买超卖 / MACD 金叉死叉零轴 / KDJ / CCI（方向+阈值+周期）
  + 组合风险（portfolio_alerts.py）：集中度 35% / 回撤 15% / 止损 10%
        ↓
AlertWorker（alert_worker.py，910 行）—— 后台执行器
```

### 2.2 AlertWorker 的工程细节（最值得学的部分）

| 机制 | 实现 | 解决什么问题 |
|---|---|---|
| **双层去重** | ① `_semantic_key`（scope+target+type+parameters 哈希）② 内存指纹 TTL ③ DB 冷却表 `_check_db_cooldown`/`_upsert_db_cooldown_safely` | 重启后不重复推送；DB 冷却读失败时降级内存并记录 suppression |
| **触发记录** | AlertTriggerRecord + 通知 attempts（`_record_notification_attempts`，含 `_synthetic_attempt_for_dispatch` 合成失败记录） | 通知链路可观测，失败可追 |
| **决策信号关联** | `_attach_decision_signal_summary_safely`——触发告警时附上该股最新 LLM 决策摘要 | 告警不只是"价格到了"，还带上下文 |
| **诊断信息** | `_diagnostics_for_status` / `_build_analysis_visibility` / `_alert_market_phase_summary` | 每条告警带分析可见性与市场时段 |
| **优雅降级** | 全部 `_safely` 后缀方法（通知失败/冷却写失败不阻断主循环） | worker 长活稳定性 |

### 2.3 调度模型

`runtime_scheduler.py`：后台任务表驱动（task + interval_seconds + run_immediately），事件监控默认 **5 分钟轮询**（`AGENT_EVENT_MONITOR_INTERVAL_MINUTES=5`），配置热重载（`reconcile_from_config`），分析任务看门狗超时（`_run_analysis_with_watchdog` + 超时降级部分结果 `analysis_timeout_partial.py`）。

### 2.4 与我们 burst_monitor 的对比

| 维度 | daily_stock_analysis | 我们的 stock_monitor |
|---|---|---|
| 触发模型 | **规则声明式**（JSON/DB，用户可配任意股票+阈值） | 参数硬编码（`burst_monitor.py` 类属性：300 只池、涨速 0.4%、涨幅 2-7.5%） |
| 告警类型 | 价格/涨跌幅/放量/MA/RSI/MACD/KDJ/CCI/组合风险 | 仅 burst（涨速+涨幅区间+成交额） |
| 去重 | 语义键+指纹 TTL+DB 冷却三层 | Redis check_date + 单股 300s 冷却 |
| 持久化 | SQLAlchemy DB（规则/触发/通知全落库） | Redis（无规则持久化） |
| 技术指标告警 | 有（P5 规则） | **无**（有 indicator 计算但未接入告警） |
| 盘中数据 | DataFetcherManager 多源容错 + 20 日日线均量 | 腾讯批量 + Redis 缓存（当前池子残缺，见性能调研报告） |

**结论**：他们的盘中监控是"**可配置告警平台**"（重规则管理、轻实时性——5 分钟轮询），我们的是"**异动雷达**"（重实时性、轻规则）。两者互补：**我们应该抄他们的规则化与技术指标告警，他们没有我们的涨速检测**。

---

## 3. 舆情收集与搜索补强深度分析（用户重点关注）

### 3.1 搜索服务架构（`search_service.py` 4961 行）

```
BaseSearchProvider（抽象基类）
  ├─ 多 Key 负载均衡：_get_next_key 轮换 + _record_error/_record_success 计数
  ├─ _execute_search：重试 + 统一 SearchResponse
  └─ 7 个具体 Provider：
       Tavily（新闻模式 tavily_topic=news）
       SerpAPI（organic 结果富摘要提取 _extract_rich_snippet_extensions）
       Bocha（博查，国内）
       Anspire（国内）
       MiniMax（带 _parse_http_error 与时间提示 _time_hint）
       Brave（locale 适配 _brave_search_locale）
       SearXNG（公共实例池，见 3.4）
```

**多市场查询构造**（`SearchService` 类方法）：
- `_is_foreign_stock`：美股识别（`^[A-Za-z]{1,5}(\.[A-Za-z])?$`）
- `_foreign_english_query_terms`：从 `STOCK_ENGLISH_NAME_MAP` 取英文公司名（AAPL→"Apple Inc."，剥法律后缀得 "Apple"）——**英文引擎必须喂英文名，否则漏检**
- `_should_prefer_chinese_news`：中文用户看美股，优先中文报道（但查询词用英文）
- `ENHANCED_SEARCH_KEYWORDS` / `_EN`：增强查询词表（中英双版）

### 3.2 新闻质量分级与相关性评分（核心资产，177 行的 `_score_news_relevance`）

**三级分类 + 优先级**：
```
_DIRECT_NEWS_CATEGORY   direct_company_news   直接公司新闻（优先级 0）
_SECTOR_NEWS_CATEGORY   sector_related_news   板块相关（1）
_MACRO_NEWS_CATEGORY    macro_market_news     宏观市场（2）
```

**身份词加权**（防"苹果"匹配到水果新闻）：
| 命中位置 | 股票代码身份词 | 公司名（明确） | 公司名（歧义英文） |
|---|---|---|---|
| 标题 | +55 | +45 | **+26**（apple/meta/square/target/gap 降权） |
| 摘要 | +34 | +28 | +16 |
| URL | +18 | — | — |

**可解释性**：每个评分带 `reasons`（≤5 条，如"标题命中股票代码 600519"），`direct_signal > 0` 才给公司事件词 +12 加成。

**垃圾过滤**（三层正则/词表）：
- 低质量下载页：`_LOW_QUALITY_URL_RE`（download/apk/exe/dmg…）+ `_LOW_QUALITY_FILE_SIZE_RE` + `_LOW_QUALITY_RATING_RE` + APP 元数据信号
- **成人服务垃圾**：强词 + 歧义词 + 上下文词 + 联系方式正则（`_ADULT_SERVICE_SPAM_*`）——搜索引擎搜 A 股必踩的坑
- 官方源白名单：`_OFFICIAL_SOURCE_TERMS/HOSTS/LABELS`（交易所/巨潮/主流财经媒体提权）

**时间窗**：`NEWS_STRATEGY_PROFILE`（ultra_short 1天/short 3天/medium 7天/long 30天）× `NEWS_MAX_AGE_DAYS` 取小；`_parse_relative_news_date`（"3天前"→date）；未来日期容忍 `FUTURE_TOLERANCE_DAYS=1`；分析型情报回看 180 天（`ANALYTICAL_INTEL_LOOKBACK_DAYS`）。

### 3.3 综合情报多维搜索（`search_comprehensive_intel`，253 行）

每只股票并行搜 4 个维度：**最新消息**（strict_freshness 强时效）/ **机构分析**（评级/目标价）/ **风险排查**（减持/处罚/利空）/ **业绩预期**（预告/快报）。美股走英文维度（effective_name 用英文别名），指数/ETF 走 `is_index_or_etf` 分支。结果 `format_intel_report` 结构化后注入 LLM context。

### 3.4 两项关键工程设计（直接可抄）

**① 子进程隔离搜索（防卡死）**
```python
_call_topic_news_in_subprocess:
  _SEARCH_TIMEOUT_WORKER_SLOTS.acquire(blocking=False)   # 信号量限并发
  ctx = multiprocessing.get_context(start_method)
  parent_conn, child_conn = ctx.Pipe(duplex=False)
  process = ctx.Process(target=worker, daemon=True)       # daemon 进程
  if not parent_conn.poll(wait_seconds):
      _terminate_search_process(process)                  # 硬超时 KILL
      raise TimeoutError(...)
```
搜索引擎 SDK 卡死（不响应 timeout）是定时任务第一杀手——子进程 + Pipe + 硬 kill 是唯一可靠解。我们 D 线新闻抓取目前是裸 requests，同样暴露此风险。

**② SearXNG 公共实例池**
- `PUBLIC_INSTANCES_URL` 拉实例列表，按 `_search_latency_seconds`（timing.search.all.mean/median）**排序选最快**
- 缓存 TTL + 冷启动退避（`_public_instances_stale_retry_after`，刷新失败继续用过期缓存）
- `_rotate_candidates` 多实例轮换尝试

**③ 缓存预留防击穿**（`_get_cached_or_reserve`）：一个查询只允许一个线程回源，其他线程等 `threading.Event`——我们 news 缓存无此机制（并发触发 9:15 窗口时可能重复抓三源）。

### 3.5 RSS/NewsNow 情报源与社交舆情

- `IntelligenceService`：用户可配置 RSS/Atom 源（CRUD + 模板 + `_builtin_source_templates`），NewsNow 聚合 API（公开实例风险已在 .env.example 明示，建议自建）
- **SSRF 防护**：`_is_blocked_ip` + `_get_with_validated_dns` + `_validate_addrinfos`（DNS 解析后校验非内网 IP）+ `_read_limited_response` 限流读取——用户自定义 URL 场景的必修课
- `SocialSentimentService`：adanos.org 一个 API 拿 Reddit/X/Polymarket 三个平台的 ticker 情绪 + trending 榜（仅美股，`SOCIAL_SENTIMENT_API_KEY` 可选）。**结构化程度高但市场单一，对 A 股场景无价值，可跳过**

---

## 4. 对 stock_monitor 的可借鉴项（分级）

### 4.1 P0 立即实施（低成本高收益，均为我们已暴露的风险）

| # | 借鉴项 | 我们的现状 | 实施 |
|---|---|---|---|
| 1 | **子进程隔离抓取** | D 线 news_sources 裸 requests，引擎卡死会拖垮 worker 主循环 | 给 6 个新闻源适配器加"子进程 + Pipe + 硬超时"包装（可先只包东财/同花顺两个主力源） |
| 2 | **新闻三级分类 + 身份词加权** | D 线关键词板块分类（25 行业词），已被验证有噪声（"全球"误吸农业） | 引入 direct/sector/macro 三级 + 股票代码身份词匹配（我们监控的是 ETF 成分股，代码命中比公司名可靠得多） |
| 3 | **缓存预留防击穿** | news/etf_holdings 缓存无预留，9:15 窗口并发触发重复抓取 | Redis 加 reservation event（或简单版：抓取锁 SETNX + TTL） |
| 4 | **垃圾内容过滤** | 无（我们的源是白名单接口，风险低但 RSS 扩展时会踩） | 记入设计文档，RSS 源上线前必须带过滤 |

### 4.2 P1 近期实施（补齐监控能力短板）

| # | 借鉴项 | 价值 |
|---|---|---|
| 5 | **告警规则声明化** | burst 参数（300 池/0.4%/2-7.5%/800万）从类属性提到 YAML/Redis 规则，用户可配；参照其 `AGENT_EVENT_ALERT_RULES_JSON` 模式 |
| 6 | **技术指标告警** | 我们已有 indicator 计算（MA/MACD/RSI/KDJ 等），未接告警；抄 `alert_indicators.py` 的 evaluate 模式（交叉/阈值/方向 + `compute_required_bars` 自动算所需 K 线数） |
| 7 | **多搜索引擎适配层** | 国内可接博查/Anspire（我们只有直连接口）；其 BaseSearchProvider 多 Key 容错模式成熟 |
| 8 | **三层去重升级** | 我们 burst 是单股 300s 冷却；引入其语义键 + DB/Redis 冷却双层，重启不重复推 |

### 4.3 P2 演进（视需求）

- 可配置 RSS/NewsNow 情报源（替代 D 线硬编码源列表）+ SSRF 防护
- 触发告警附"上下文摘要"（其 `_attach_decision_signal_summary` 的轻量版：附该股最近一次信号/持仓变化）
- adanos 社交舆情——**仅美股，本项目不做美股，明确跳过**

### 4.4 明确不借鉴

| 项 | 原因 |
|---|---|
| LLM 决策报告（analyzer.py 251KB） | 我们是规则引擎定位，token 成本与不确定性违背"监控"本质；其决策仪表盘（截图）正是 LLM 报告结构化产物 |
| Web/桌面工作台（apps/ 335 文件） | 我们已有 FastAPI + 前端，单人项目无需双端 |
| 多市场 fetcher（futu/longbridge/yfinance…） | 我们专注 A 股 + ETF，数据源在精不在多 |
| GitHub Actions 零成本定时 | 我们有 ECS + systemd 三服务，更可控 |

---

## 5. 结论

1. **它的盘中监控不"重"**：5 分钟轮询 + 内存/DB 规则引擎，技术上没有实时性魔法。真正的差异化是**规则管理**（声明式 JSON/DB CRUD/技术指标五件套/组合风险）和**工程化细节**（三层去重、通知 attempts 记录、决策信号关联）。对我们最有价值的是 **P0 四件套**（子进程隔离/三级分类+身份词/缓存预留/垃圾过滤）和 **P1 的规则声明化 + 指标告警**。
2. **它的舆情收集是全项目最重的资产**（194KB）：6+ 引擎容错、可解释评分、多市场查询构造、质量过滤——这套体系直接服务于 LLM 报告质量。我们 D 线已有多源+去重+板块分类的骨架，缺的正是**质量分级与身份词匹配**（这正是我们关键词分类被噪声困扰的对症解药）。
3. **定位差异决定取舍**：它是"LLM 投研报告平台"，我们是"规则监控推送系统"——抄工程模式，不抄 LLM 依赖。
4. 建议落地顺序：P0-1 子进程隔离（防 worker 卡死，1 天）→ P0-2/3 分类与预留（0.5 天）→ P1-5 规则声明化（1 天）→ P1-6 指标告警（1-2 天）。全部本地实施 + 测试，不动 ECS。
