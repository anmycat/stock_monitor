# stock_monitor

A股盘前/盘中/盘后监控与复盘系统：专业交易员级别的智能投研助手。

## 系统概述

本系统基于专业交易员决策框架构建，具备：
- **多源数据容错**：qtimg为主力源，sina/eastmoney/yfinance为备用
- **并行化处理**：盘中扫描响应时间降低90%
- **专业决策引擎**：结合市场环境、时间模式、资金流向综合判断
- **智能推送**：去重冷却机制，避免通知风暴

本次ETF增强参考了 GitHub 高 star 项目的可复用思路（以本系统为主，外部项目为辅）：
- [OpenBB](https://github.com/OpenBB-finance/OpenBB)：多数据源统一抽象与“connect once, consume everywhere”接口理念
- [AkShare](https://github.com/akfamily/akshare)：A股/ETF数据覆盖与字段标准化
- [FinanceDatabase](https://github.com/JerBouma/FinanceDatabase)：ETF分类与可扩展标的池思路
- [invest-alchemy](https://github.com/bmpi-dev/invest-alchemy)：ETF组合跟踪、策略清单、交易建议面板
- [microsoft/qlib](https://github.com/microsoft/qlib)：研究到生产的流程化与模型/因子可扩展设计

上游版本快照（核验日期：2026-04-29）：
- `daily_stock_analysis`: `c5ac36e73114ace58ec983ea415f3f67dc25df75`
- `AkShare`: `release-v1.18.58`（`19c47bdb76247404c496e13037faa52c666b5356`）
- `Kronos`: `67b630e67f6a18c9e9be918d9b4337c960db1e9a`（仓库：[shiyu-coder/Kronos](https://github.com/shiyu-coder/Kronos)，论文：[arXiv:2508.02739](https://arxiv.org/abs/2508.02739)）

---

## 1. 业务逻辑流程

### 盘前 (09:25)
| 任务 | 模块 | 状态 |
|------|------|------|
| 集合竞价+新闻(交易日) | `guardian.py:_job_morning_call_auction_and_news()` | ✅ 启用 |
| 新闻速览(非交易日) | `guardian.py:_job_morning_call_auction_and_news()` | ✅ 启用 |
| 指数开盘情况 | `etf_tracker.py` | ✅ 启用 |
| ETF状态Top3（可买/观望/X不买） | `etf_tracker.py:get_etf_trade_states()` | ✅ 启用 |
| ETF持仓变化扫描 | `etf_tracker.py:scan_etf_holdings_changes()` | ✅ 启用 |
| 财经新闻获取 | `weekly_ops.py:fetch_finance_news()` | ✅ 启用 |
| 新闻情感分析 | `sentiment.py:analyze_news_sentiment()` | ✅ 启用 |

### 盘中 (每30分钟)
| 任务 | 模块 | 状态 |
|------|------|------|
| A段 起爆资金（3秒双快照） | `stock_scanner.py:scan_burst_fund_signals()` | ✅ 启用 |
| B段 小盘股监控 | `stock_scanner.py:scan_small_cap_monitor()` | ✅ 启用 |
| C段 现有策略扫盘（决策卡） | `stock_scanner.py:scan_market_trade_candidates()` | ✅ 启用 |
| 市场环境评估 | `trader_brain.py` | ✅ 启用 |
| 热门板块发现 | `sector_analysis.py:auto_discover_hot_sectors()` | ✅ 启用 |
| 游资关注股 | `fund_flow.py:get_famous_trader_stocks()` | ✅ 启用 |
| 多因子选股 | `factors.py:score_stocks_by_factors()` | ✅ 启用 |
| ETF状态Top3（可买/观望/X不买） | `etf_tracker.py:evaluate_etf_trade_state()` | ✅ 启用 |
| ETF成分股警报 | `etf_tracker.py:get_etf_stock_alerts()` | ✅ 启用 |

### 盘后 (11:35 / 15:10)
| 任务 | 模块 | 状态 |
|------|------|------|
| 午间复盘 | `guardian.py:_job_noon_recap()` | ✅ 启用 |
| 全日复盘 | `guardian.py:_job_close_recap()` | ✅ 启用 |
| ETF状态Top3（可买/观望/X不买） | `etf_tracker.py:get_etf_trade_states()` | ✅ 启用 |
| 市场宽度统计 | `sector_analysis.py:get_market_breadth()` | ✅ 启用 |
| AI市场摘要 | `briefing.py:generate_market_brief()` | ✅ 启用 |

---

## 2. 文件树

```
stock_monitor/
├── guardian.py                 # 核心调度入口 (APScheduler)
├── logger.py                   # 轮转日志
├── requirements.txt             # Python 依赖
├── README.md                   # 本文档
├── config/
│   ├── .env.example           # 环境变量模板
│   └── watchlist.json         # 自选股+ETF配置
├── logs/                       # 运行日志目录
├── data/                       # 数据存储目录
├── scripts/
│   ├── guardian.sh            # 启动管理脚本
│   ├── guardian.service        # systemd 服务配置
│   ├── smoke_test.py          # 冒烟测试
│   ├── simulate_recent_days.py # 最近交易日历史回放
│   └── ecs_test.py             # ECS测试脚本
└── modules/
    ├── market.py              # 交易日历 + 行情数据 ⭐
    ├── sector_analysis.py      # 板块分析 + 热门板块发现 ⭐
    ├── stock_scanner.py       # 盘中扫描 + 决策卡 ⭐
    ├── decision_support.py     # 决策卡 + 检查清单 ⭐
    ├── factors.py             # 多因子评分 ⭐
    ├── fund_flow.py           # 资金流向 + 龙虎榜 ⭐
    ├── etf_tracker.py          # ETF持仓追踪 + 决策 ⭐
    ├── trader_brain.py        # 专业交易员决策引擎 ⭐
    ├── human_thinking.py      # 市场上下文 + 时间模式
    ├── ai_engine.py           # Gemini/DeepSeek AI摘要
    ├── sentiment.py           # 新闻情感分析
    ├── weekly_ops.py          # 周常任务 + 新闻采集
    ├── notifier.py            # DingTalk + PushDeer通知
    ├── utils.py               # 共享工具函数 ⭐
    ├── briefing.py            # AI市场摘要生成
    ├── analysis.py            # 策略 + 风控 + ETF流
    └── auction_engine.py      # 集合竞价分析引擎 ⭐
```

⭐ = 核心模块

---

## 3. UI输出规范

### 3.1 推送格式
```
【模块标题】 + Emoji + 中文

├─ 分节标题 【】
├─ 表格化数据（带序号）
├─ Emoji指示器 (🟢/🔴/⚪/📈/📉/🔥/💧)
└─ 风险/建议提示
```

### 3.2 推送字段规范
- 股票格式：`名称(代码)—换手率—推送理由`
- 涨跌幅：不使用"="号，使用"涨跌"或"%"

### 3.3 通知去重
- 冷却时间：`NOTIFY_COOLDOWN_SECONDS=300`
- 去重窗口：`NOTIFY_DEDUPE_SECONDS=1800`

---

## 4. 配置要点

### 4.1 必需配置
```bash
QUOTE_SOURCE=qtimg                    # 主力数据源
QUOTE_SOURCE_POOL=qtimg,eastmoney,sina,akshare  # auto/random可用源白名单
DINGDING_WEBHOOK=https://oapi.dingtalk.com/robot/send?access_token=你的token
DINGDING_SECRET=xxx
PUSHDEER_PUSHKEY=xxx
```

### 4.2 可选配置
```bash
GEMINI_API_KEY=xxx                   # AI摘要
TUSHARE_TOKEN=xxx                    # 增强数据
DEEPSEEK_API_KEY=xxx                 # 备选AI

# 盘中扫描
INTRADAY_SCAN_ENABLED=true
SCAN_MIN_PCT_CHANGE=3.0             # 最低涨幅%
SCAN_MAX_PCT_CHANGE=9.5              # 最高涨幅%
SCAN_MIN_TURNOVER_RATE=3.0          # 最低换手率%
SCAN_MIN_SCORE=45                    # 最低评分
SCAN_ADAPTIVE_ENABLED=true           # 无候选时自动放宽一档
SCAN_RELAX_SECTOR_THRESHOLD=4        # 热门板块数量达到阈值触发放宽
SCAN_RELAX_MIN_PCT_CHANGE=1.2
SCAN_RELAX_MIN_TURNOVER_RATE=1.8
SCAN_RELAX_MIN_SCORE=38
SCAN_MAX_EVAL_ROWS=24              # C段最大评估股票数（提速防卡顿）
INTRADAY_CANDIDATE_TOPN=10           # 输出候选数
INTRADAY_SCAN_UNIVERSE=180           # 扫描范围

# A段 起爆资金（3秒双快照）
BURST_CANDIDATE_TOPN=10
BURST_SCAN_UNIVERSE=300
BURST_SCAN_INTERVAL_SECONDS=3
BURST_MIN_3S_PCT_CHANGE=0.4         # 3秒涨幅阈值(%)
BURST_MIN_3S_AMOUNT=6000000         # 3秒成交额阈值(元)
BURST_MIN_PCT_CHANGE=2.0            # 当前涨幅下限(%)
BURST_MAX_PCT_CHANGE=7.5            # 当前涨幅上限(%)

# B段 小盘股监控
SMALL_CAP_TOPN=10
SMALL_CAP_SCAN_UNIVERSE=300
SMALL_CAP_MIN_MV_YI=30              # 最小市值(亿)
SMALL_CAP_MAX_MV_YI=300             # 最大市值(亿)
SMALL_CAP_MIN_PCT_CHANGE=1.5
SMALL_CAP_MAX_PCT_CHANGE=9.5
SMALL_CAP_MIN_TURNOVER_RATE=3.0
SMALL_CAP_MIN_AMOUNT_WAN=3000       # 最小成交额(万)

# 热门板块
HOT_SECTOR_SCAN_LIMIT=12
HOT_SECTOR_MIN_PCT=2.0
HOT_SECTOR_MIN_STOCKS=5
MARKET_BREADTH_SAMPLE_SIZE=5000      # 全市场样本规模
MARKET_BREADTH_MIN_SAMPLE=800        # 低于该样本数触发降级
MARKET_CONTEXT_MODEL=percentile       # 可选 percentile / amount_weighted

# 数据源稳健性
QUOTE_ENRICH_MAX_SOURCES=3            # 缺失字段最多补齐来源数
SOURCE_CIRCUIT_FAILURE_THRESHOLD=3    # 连续失败熔断阈值
SOURCE_CIRCUIT_COOLDOWN_SECONDS=900   # 熔断冷却时间(秒)
QUOTE_SKIP_SLOW_DURING_SESSION=true   # 交易时段跳过慢源(如yfinance/pytdx)
QUOTE_SLOW_SOURCES=yfinance,pytdx     # 慢源名单
YFINANCE_SKIP_DURING_SESSION=true     # 交易时段禁用yfinance
YFINANCE_TIMEOUT_SECONDS=5            # yfinance超时
ETF_HOLDCAP_404_COOLDOWN_SECONDS=21600 # ETF持仓接口404冷却(秒)
ETF_HOLDINGS_MAX_RETRIES=1            # ETF持仓接口重试次数
ETF_HOLDINGS_TIMEOUT_SECONDS=8         # ETF持仓接口超时
ETF_HOLDINGS_SCAN_WORKERS=2            # ETF持仓并发抓取worker
ETF_HOLDINGS_SCAN_MAX_CODES=20         # 每轮最多扫描ETF数量
ETF_HOLDINGS_DIFF_MIN_PCT=0.02         # 持仓权重变化最小阈值
ETF_HOLDINGS_NEW_MIN_PCT=0.50          # 新入成分股最小权重阈值
ETF_COMPONENT_QUOTE_SOURCE=qtimg       # ETF成分股默认行情源

# 定时任务
AUCTION_ALERT_TIME=09:25          # 集合竞价+新闻推送
NOON_RECAP_TIME=11:35             # 午间复盘
CLOSE_RECAP_TIME=15:10            # 收盘复盘
INTRADAY_SCAN_ENABLED=true        # 盘中扫描
INTRADAY_SCAN_INTERVAL_MINUTES=15 # 盘中扫描间隔（15分钟整齐）
MORNING_MAX_RUNTIME_SECONDS=90    # 09:25任务最大耗时预算(秒)
MORNING_NEWS_LIMIT=10             # 早盘新闻条数
MORNING_ETF_STATE_ENABLED=false   # 09:25默认关闭重型ETF状态计算
MORNING_ETF_STATE_MAX_CODES=0     # 开启时：早盘ETF状态最多抓取ETF数量
MORNING_ETF_PANEL_ENABLED=false   # 09:25默认关闭重型ETF成分股变化
MORNING_ETF_PANEL_MAX_CODES=3     # 开启时仅抓取前N个ETF
MORNING_ETF_PANEL_TOPN=3          # 每类ETF成分股推送条数
MORNING_ETF_QUOTE_SOURCE=qtimg    # 早盘ETF成分股行情源
```

### 4.3 daily_stock_analysis集成
```bash
DAILY_STOCK_ANALYSIS_REPO_PATH=/path/to/daily_stock_analysis
DAILY_STOCK_ANALYSIS_QUOTE_PATH=data/daily_stock_analysis/latest_quote.json
DAILY_STOCK_ANALYSIS_MAX_STALENESS_SECONDS=900
DAILY_STOCK_ANALYSIS_ALLOW_STALE=false
BETTAFISH_REPORT_PATH=data/bettafish/latest_report.json
```

### 4.4 新闻源配置
```bash
# JSON新闻源（推荐）- 新浪财经快讯
FINANCE_NEWS_JSON_URLS=https://feed.mix.sina.com.cn/api/roll/get?pageid=153&lid=2516&k=%E8%82%A1%E7%A5%A8&num=10&page=1

# RSS新闻源（备用）
FINANCE_NEWS_RSS_URLS=https://feed.eastmoney.com/market.xml,https://rss.sina.com.cn/news/china/focus15.xml

# Tushare新闻（每天限制2次）
TUSHARE_TOKEN=your_token

# 新闻过滤
NEWS_REQUIRE_CHINESE=true    # 仅中文新闻
NEWS_REQUIRE_A_SHARE=true    # 仅A股相关新闻
NEWS_RELAX_TO_CN_ONLY=true   # 无A股新闻时允许中文通用新闻
```

---

## 5. 性能优化

### 5.1 并行化处理
| 模块 | 优化前 | 优化后 | 提升 |
|------|--------|--------|------|
| 盘中扫描 | 216秒 | ~22秒 | 10× |
| 多因子评分 | 60秒 | ~6秒 | 10× |
| 游资追踪 | 24秒 | ~5秒 | 5× |

### 5.2 缓存策略
- 行情缓存：30秒
- K线缓存：300秒
- ETF持仓：3600秒
- 因子数据：300秒

---

## 6. 启动方式

### 本地
```bash
pip install -r requirements.txt
cp config/.env.example config/.env
python guardian.py
```

### ECS
```bash
systemctl enable guardian
systemctl start guardian
systemctl status guardian

# 如使用 scripts/*.exp 自动化脚本，建议仅在当前终端会话临时注入凭据（不写入文件）
read -s ECS_PASSWORD && export ECS_PASSWORD
```

### 历史回放
```bash
python scripts/simulate_recent_days.py --days 4 --topn 10
```

---

## 7. daily_stock_analysis 融合方案

### 7.1 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                    
│                                                             │
│  ┌───────────────────┐      ┌───────────────────────────┐  │
│  │ stock_monitor     │      │ daily_stock_analysis     │  │
│  │ (盘中监控推送)     │ ←──→ │ (AI选股分析报告)          │  │
│  │                   │      │                           │  │
│  │ • 实时行情        │      │ • 多因子选股              │  │
│  │ • 盘中扫描        │      │ • 市场宽度                │  │
│  │ • 资金流向        │      │ • 情绪分析                │  │
│  │ • ETF追踪         │      │ • AI摘要                  │  │
│  │ • 推送通知        │      │ • 每日报告                │  │
│  └───────────────────┘      └───────────────────────────┘  │
│            ↑                         ↑                     │
│            │    ┌─────────────────┐  │                     │
│            └──→ │  数据共享层     │ ←┘                     │
│                 │  latest_quote.json                     │
│                 │  weekly_ops_state.json                  │
│                 └─────────────────┘                       │
└─────────────────────────────────────────────────────────────┘
```

### 7.2 融合模式

| 模式 | 功能 | 配置 |
|------|------|------|
| **方案1: 独立运行** | daily_stock_analysis独立运行AI选股分析 | `DSA_INTEGRATION_MODE=standalone` |
| **方案2: 数据共享** | stock_monitor读取daily_stock_analysis的行情数据 | `DSA_DATA_SHARE_ENABLED=true` |
| **方案3: 融合模式(推荐)** | 同时启用方案1和2 | `DSA_INTEGRATION_MODE=both` |

### 7.3 配置项

```bash
# 仓库路径
DAILY_STOCK_ANALYSIS_REPO_PATH=/daily_stock_analysis
DAILY_STOCK_ANALYSIS_BRANCH=main

# 自动同步（每周一03:00检查）
DAILY_STOCK_ANALYSIS_AUTO_UPGRADE=true
DAILY_STOCK_ANALYSIS_SYNC_MODE=ff_only  # ff_only / rebase / hard_reset

# 数据共享
DAILY_STOCK_ANALYSIS_QUOTE_PATH=data/daily_stock_analysis/latest_quote.json
DSA_DATA_SHARE_ENABLED=true
DSA_REPORT_PATH=/daily_stock_analysis/output

# 融合模式
DSA_INTEGRATION_MODE=both
```

### 7.4 定时任务

| 时间 | 任务 | 说明 |
|------|------|------|
| 周一03:00 | Git同步检查 | 检查并同步上游仓库 |
| 周一08:30 | 周早新闻 | 版本状态 + 财经汇总 |
| 盘中每30分钟 | 盘中扫描 | 实时监控推送 |

### 7.5 数据流

```
daily_stock_analysis                    stock_monitor
      │                                      │
      ├─ latest_quote.json ────────────────→ 行情数据
      │                                      │
      ├─ output/daily_report.json ────────→ AI选股信号
      │                                      │
      └─────────────────── Git Pull ──────── 每周同步
```

### 7.6 维护命令

```bash
# 手动检查更新
cd /daily_stock_analysis && git status && git log --oneline -3

# 手动同步
cd /daily_stock_analysis && git pull origin main

# 查看同步历史
cat logs/weekly_ops_state.json

# 测试数据共享
python -c "from modules.market import get_quote; print(get_quote('sh600519', source='daily'))"
```

---

## 8. 扫描/选股逻辑详解

### 8.1 盘中扫描三段式 (`stock_scanner.py`)

**A段：起爆资金 (`scan_burst_fund_signals`)**

**B段：小盘股监控 (`scan_small_cap_monitor`)**


**C段：现有策略扫盘 (`scan_market_trade_candidates`)**




## 14. 更新日志

### v2.13 (2026-04-24)
- ✅ **盘中扫描升级为 A/B/C 三段式**：
  - A段起爆资金：3秒双快照（涨幅+成交额增量）
  - B段小盘股监控：市值/换手/成交额联合过滤
  - C段现有决策卡扫描保留，并增加 `SCAN_MAX_EVAL_ROWS` 限流
- ✅ **ETF持仓变化机制增强**：
  - 新增 `change_mode` 贯通（`amount` / `hold_pct_delta` / `hold_pct_new`）
  - 推送端按模式切换“成交变化”或“权重变化”展示，避免误读
- ✅ **回放脚本容错修复**：
  - `simulate_recent_days.py` 对 Tushare 权限不足进入 fallback
  - fallback 分支增加单股 K 线异常跳过，避免整轮中断
- ✅ **上游版本核验完成**：
  - `daily_stock_analysis` / `AkShare` / `Kronos` 最新提交已记录于文档

### v2.12 (2026-04-09)
- ✅ **基于本周 ECS 日志的稳定性优化**：
  - `get_quote(auto/random)` 改为可配置源池（`QUOTE_SOURCE_POOL`），避免无效源反复探测
  - 默认跳过未配置密钥的数据源（`tushare/mkts`），降低健康告警噪音
  - `source_health_report` 仅统计当前启用源，不再被历史失败源持续干扰
- ✅ **ETF链路降噪与提速**：
  - ETF持仓主接口出现404时进入冷却（`ETF_HOLDCAP_404_COOLDOWN_SECONDS`），避免每轮先报错
  - 无真实调仓数据时改为 `neutral` 观察列表，不再伪造“增持/减持”信号
- ✅ **盘中扫描命中率优化**：
  - 新增自适应二次扫描（`SCAN_ADAPTIVE_ENABLED`），在热点明显但首轮无候选时自动放宽阈值
  - `intraday_scan` 日志增加 `relaxed` 计数，便于复盘阈值效果

### v2.11 (2026-04-08)
- ✅ **ETF决策面板增强（参考高 star ETF 项目能力）**：
  - 新增 `get_etf_trade_states()` / `evaluate_etf_trade_state()`，输出 `可买/观望/X不买`
  - 决策维度加入：支撑位距离、均线结构、资金流方向、追高约束、综合评分
  - 新面板已接入：盘前、盘中、午间复盘、全日复盘、独立ETF监控
- ✅ **推送可读性优化**：
  - ETF状态输出统一为“状态 + 指令 + 支撑距离 + 资金流 + 评分 + 理由”
  - 保持原有钉钉/PushDeer通道、去重与冷却机制不变
- ✅ **文档补齐**：
  - README 增补“ETF高 star 项目来源说明”与“ETF状态面板规则”

### v2.10 (2026-04-08)
- ✅ **对齐 daily_stock_analysis 稳健性架构**（不改变推送通道）：
  - 数据源新增熔断/半开探测机制（连续失败后冷却，冷却后单次探测恢复）
  - 健康报告新增 `cfail/circuit/open_remaining` 字段，问题定位更直观
  - 修复 `source_health_report` 运行错误（`snapshot.items()` -> 正确遍历列表）
- ✅ **行情质量增强**：
  - `daily_stock_analysis` 本地行情增加新鲜度校验（交易时段拒绝过期文件）
  - 缺失字段补齐支持多源补齐（`QUOTE_ENRICH_MAX_SOURCES`）
  - 全市场宽度改为分页拉取，减少单页样本失真
  - 市场上下文增加最小样本保护，样本不足自动降级
- ✅ **换手率逻辑修正**：
  - 修复扫描模块百分比误乘 `*100`
  - 推送展示精度优化，低换手不再被显示为 `0.0%`
- ✅ **文档与安全**：
  - `.env.example` 中 `MX_APIKEY` 改为占位符
  - README 明确新增稳定性配置项
  - 保持通知链路不变：`DingTalk + PushDeer`

### v2.9 (2026-04-07)
- ✅ **ETF持仓变化格式统一**：
  - 第一部分：🔥红利低波ETF成份股持仓变化（买入/卖出）
  - 第二部分：📈行业ETF成份股持仓变化（买入/卖出）
  - 格式：`🟢股票名称(代码) 持仓:X.XX% 价格:XX.XX 买入`
  - Emoji统一：🟢买入/🔴卖出
  - 合并为单条通知，避免重复打扰
- ✅ **代码清理**：
  - 删除未使用的测试文件（test_*.py, smoke_test.py等）
  - 删除重复的stock_scanner.py
  - 删除旧backup目录和env备份文件
  - 删除Mac临时文件(._*)
  - 优化目录结构

### v2.8 (2026-04-06)
- ✅ **ETF持仓优化**：
  - 动态缓存策略：盘中5分钟缓存，盘后1小时缓存
  - 增加重试机制：API失败时最多重试2次
- ✅ **数据源健康报告优化**：
  - 阈值从0.8提升到0.5，只在严重问题时发送通知
  - 添加问题数据源详细信息显示
- ✅ **股票名称Fallback机制**：
  - 添加`_ETF_FALLBACK_STOCK_NAMES`名称映射
  - API失败时使用预设名称，避免股票名称丢失
- ✅ **DSA集成**：
  - 新增`modules/dsa_integration.py`模块
  - 支持从DSA获取行情数据
  - 支持配置合并
  - DSA行情生成增加ETF数据

### v2.7 (2026-04-02)
- ✅ **新增红利低波ETF追踪**：
  - 新增 `etf_codes_low_vol` 配置组 (515180, 512890, 515300, 561590)
  - 盘前/盘后分开显示：🔥红利低波ETF + 📈行业ETF
- ✅ **修复ETF代码解析**：EastMoney返回的7-8位数字代码正确转换为标准格式
- ✅ **修复相对导入问题**：scheduler调用时使用绝对导入
- ✅ **同步ECS配置**：
  - watchlist.json 已更新
  - etf_tracker.py 已更新
  - guardian.py 已更新

### v2.6 (2026-03-27)
- ✅ **修复集合竞价股票代码生成**：`_build_top_codes()` 修复代码格式
  - 修正沪市代码生成 (sh600000-sh603xxx)
  - 修正深市主板代码格式 (sz000001-sz000999)
  - 修正中小板代码格式 (sz002xxx)
  - 修正创业板代码格式 (sz300xxx)
- ✅ 修复 `_build_top_codes()` 范围问题：确保生成足够数量的股票代码
- ✅ 测试验证：ECS上成功获取真实A股股票数据（浦发银行、邯郸钢铁等）
- ✅ **修复 `analyze_sector_momentum()` NoneType错误**：`pct_change`可能为None导致比较失败
- ✅ **优化扫描阈值**（本周复盘后）：
  - SCAN_MIN_PCT_CHANGE: 2.0 → 1.5
  - SCAN_MIN_TURNOVER_RATE: 2.0 → 1.5
  - SCAN_MIN_SCORE: 40 → 35
  - HOT_SECTOR_MIN_PCT: 2.0 → 1.5
  - HOT_SECTOR_MIN_STOCKS: 5 → 3
- ✅ 修复配置：QUOTE_SOURCE从random改为qtimg
- ✅ **增加ETF追踪范围**：从8个增加到15个ETF（覆盖宽基、金融、科技、创新药、消费、光伏、医疗、新能源、AI、科创50等）
- ✅ **ETF持仓数据源升级**：
  - 新增 `_get_etf_holdings_akshare()` - 从EastMoney获取实时成分股
  - 获取Top10成分股（按持仓权重排序）
  - 失败时自动降级到预设成分股列表
  - 添加fallback缓存避免重复请求
- ✅ **DSA数据缓存优化**：添加DSA可用性缓存，避免重复尝试读取不存在的文件（5分钟缓存）
- ✅ **同步daily_stock_analysis**：已更新到最新版本（6 commits）
- ✅ **新增DSA行情生成任务**：每小时自动生成DSA兼容的latest_quote.json文件
- ✅ **视觉优化**：推送消息添加Emoji指示器
  - 🟢 上涨/买入/增持
  - 🔴 下跌/卖出/减持
  - 🟡 持有
  - ⚪ 持平
- ✅ **ETF推送优化**：
  - 盘中ETF显示数量从5只增加到20只
  - 新增独立ETF持仓变化推送任务（09:35/10:30/14:00）
  - 独立发送，更清晰的格式
  - 新增ETF指数行情显示
  - 明确标注"跟买"/"跟卖"建议
- ✅ **盘中扫描优化**：
  - 降低扫描阈值：涨幅1.5%、换手1.5%、评分35
  - 移除不支持的ETF（sz159941, sz159003）
- ✅ **新增市场状态Token系统** (`market_state.py`)
  - 7种市场状态：震荡/上涨/下跌/恐慌/观望/强势/弱势
  - 基于涨跌幅、上涨股占比、热门板块数自动分类

### v2.5 (2026-03-24)
- ✅ **集合竞价数据源优化**：添加多数据源轮询（qtimg→EastMoney→AkShare）
- ✅ 添加 `_get_call_auction_top10_qtimg()` 函数 - GT咪数据
- ✅ 添加 `_get_call_auction_top10_akshare()` 函数
- ✅ 添加 `_get_call_auction_top10_daily_analysis()` 预留接口
- ✅ **新增 `auction_engine.py` 专业版**：
  - `AuctionAnalyzer` 简化版（当前使用，基于qtimg）
  - `AuctionStrategySystem` 专业版（预留，需Level-2数据）
  - `AdaptiveParamEngine` 自适应参数（5种市场状态）
  - `MarketRegime` 市场状态枚举
  - `StockType` 个股类型枚举（9种）
- ✅ 多因子评分系统：涨幅、换手率、量能、时间因子
- ✅ 推送优化：显示竞价评分和标签
- ⚠️ 注意：ECS云服务器存在到国内金融API的网络限制

### v2.4 (2026-03-23)
- ✅ **修复定时任务不执行问题**：重置损坏的apscheduler.sqlite数据库
- ✅ 手动触发验证：morning_alert和intraday_scan均可正常执行
- ✅ 更新README 9.5节：补充不可用新闻源（tonghuashun、policy_json、tushare）
- ✅ 代码审查通过：所有模块均可正常导入
- ✅ 更新README 1.1节：ETF成分股警报（替换ETF决策面板）
- ✅ 更新README 4.2节：补充定时任务配置项
- ✅ 新增README第13章：定时任务详解+故障排查

### v2.3 (2026-03-23)
- ✅ 新闻推送时间调整：每日9:25推送（含集合竞价+新闻）
- ✅ 非交易日仅推送新闻（"非交易日"标题）
- ✅ 交易日推送完整内容（集合竞价情况+新闻速览）
- ✅ `_job_morning_call_auction_and_news()` 逻辑重构
- ✅ `_format_morning_alert()` 格式更新
- ✅ README第8章重构：分为"扫描/选股逻辑详解"和"暂停/废弃业务逻辑"两大部分
- ✅ **ETF持仓追踪逻辑重构**：从"ETF产品买卖"改为"成分股操作建议"
- ✅ 新增 `get_etf_stock_alerts()` 函数：ETF增持→关注成分股，减持→减仓提示
- ✅ 移除废弃的 `evaluate_etf_trade()` 和 `build_etf_trade_panel()` 函数
- ✅ 更新 `_append_etf_panel()` 输出格式为成分股信息
- ✅ **修复新闻获取**：新增Sina JSON财经快讯源
- ✅ 优化 `_fetch_news_from_json()` 支持嵌套JSON格式
- ✅ 添加 `ctime` 时间戳解析支持
- ✅ 更新 README 第4.4节新闻源配置文档

### v2.2 (2026-03-22)
- ✅ 修复 `stock_scanner.py` 缺失函数
- ✅ `scan_watchlist_stocks()` 已添加到 ECS
- ✅ `format_stock_alert()` 已添加到 ECS
- ✅ `detect_turnover_alerts()` 已添加
- ✅ `detect_fund_flow_proxy()` 已添加
- ✅ 验证同步机制（cron已设置）

### v2.1 (2026-03-22)
- ✅ daily_stock_analysis融合方案（独立运行+数据共享）
- ✅ 每周Git自动同步配置
- ✅ 更新README文档结构

### v2.0 (2026-03-22)
- ✅ 并行化API调用（ThreadPoolExecutor）
- ✅ 统一缓存管理（CacheManager）
- ✅ 修复东财API价格BUG
- ✅ 修复ETF开盘价逻辑
- ✅ 统一工具函数
- ✅ 清理重复代码
- ✅ 添加暂停业务逻辑章节
- ✅ qtimg设为主要数据源
