from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # 基础配置
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str = ""
    REDIS_SSL: bool = False
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    API_RELOAD: bool = False
    CORS_ORIGINS: str = "http://localhost:3000"
    API_AUTH_TOKEN: str = ""
    DATA_SOURCE: str = "tushare"
    TUSHARE_TOKEN: str = ""
    
    # 钉钉配置
    DINGTALK_WEBHOOK: str = ""
    DINGTALK_SECRET: str = ""

    # 通知流量控制（D44：钉钉自定义机器人 20 条/分钟硬限，超限限流 10 分钟）
    # 最小发送间隔（秒）：低于间隔的消息暂存聚合缓冲，轮末合并为摘要发出
    NOTIFY_MIN_INTERVAL_SECONDS: float = 3.0
    # 每分钟发送上限（留余量给市场告警）：窗口满时消息暂存，flush 延到下一轮
    NOTIFY_MAX_PER_MINUTE: int = 18
    
    # 盘中日K 实时修正（D65，2026-11-06）
    # 背景：日K 主源（腾讯 fqkline）盘中含当日未完成 bar，但降级源（新浪 240 分钟日K）
    #       盘中**不含** → 指标全部基于 T-1，跳空完全不在评分视野（实测：当日 12 个 BUY 中
    #       4 个是「跳空 ≥3% 下行的票」）。开启后用实时报价把日K 最后一根对齐到当下。
    # 关闭则退回旧口径（仅保留 D64 的现价/参考价分离展示）。
    INTRADAY_KLINE_CORRECTION: bool = True
    # A/B 影子对比：同时用「修正前 / 修正后」两个口径各算一次信号，把差异写入
    # Redis（intraday_kline:ab）供复盘——这是本次改动唯一的真实证据来源
    # （历史完成日上该修正是 no-op，半年窗口无法直接验证）。
    INTRADAY_KLINE_SHADOW: bool = True
    # S4（D64 跳空否决，2026-11-21 用户裁定启用）：盘中现价相对日K基准跳空
    # ≤ 该阈值时，把 BUY 降级为 WATCH（不改评分/仓位，只否决「追跌买入」）。
    # 实测背景：当日 12 个 BUY 中 4 个是跳空 ≥3% 下行票，属追跌。设 0 表示关闭。
    GAP_VETO_BUY_PCT: float = -0.03

    # D66 日K 数据防误用（2026-11-06）
    # 背景：日志发现降级源把 T-1 收盘当今日使用；另有 daily 优先级里
    # eastmoney 无 get_daily_data 的死配置、跨日缓存/快照仍可读等隐患。
    # ① 疑似降级时用独立数据源针对性复核（只在 lagging 时触发，正常零请求）
    KLINE_CROSS_CHECK: bool = True
    # ② 收盘后把当日完成 bar 打包封存到 SQLite（带日期/来源/复核标签）
    KLINE_EOD_SEAL: bool = True
    # ③ 缓存 key 带交易日 + 隔夜 stale 快照作废（防止次日误读昨日数据）
    KLINE_DAY_BOUNDARY_GUARD: bool = True

    # I-04 日K 整帧完整性（2026-11-07）
    # ① 标注层：逐根校验交易日/单调/价格，结论只写 df.attrs（零信号影响）
    KLINE_FRAME_VALIDATION: bool = True
    # ② 拒绝层（S5/I-04，2026-11-21 用户裁定启用）：整帧异常且非当日的数据
    #    不再出厂 → 会改变信号（该轮可能少出信号），已按纪律 D41 双跑记录变化量。
    KLINE_REJECT_STALE_FRAME: bool = True

    # I-05 数据源失败路径效率（2026-11-07）
    # 超时/连接类失败不再盲重试（原本每次 ~10s × 2 重试拖垮全市场扫描），
    # 并在抑制窗口内跳过同源重复请求；窗口内其它源仍正常参与 fallback。
    SOURCE_TIMEOUT_SUPPRESSION: bool = True
    SOURCE_TIMEOUT_SUPPRESS_SECONDS: float = 20.0
    # 连续多少次同类超时才进入抑制（避免偶发抖动误伤）
    SOURCE_TIMEOUT_SUPPRESS_STREAK: int = 2

    # S7/N-03 半开探测状态机（2026-11-21 用户裁定启用）
    # 冷却/熔断窗口到期后不直接全量放行，而是放行「唯一一次试探请求」：
    #   · 试探成功 → 关闭熔断（CLOSED），恢复常规取数；
    #   · 试探失败 → 立即重新熔断（不再等 error_count>=3 / 连续 3 轮冷却）。
    # 目的：避免冷却刚过的一瞬间把全部丢包打在仍不健康的源上；探测期间只打
    # 一次（强制 max_retries=1），不放大。回退：设 false 恢复旧「到期即全放」。
    SOURCE_HALF_OPEN: bool = True
    # 半开探测窗口（秒）：探测在途超过该时长仍未决 → 视为卡死，重新熔断。
    SOURCE_HALF_OPEN_PROBE_SECONDS: float = 60.0

    # D69 陈旧报价硬拒绝层（2026-11-21，ECS 体检实证）
    # 0 = 关闭（默认，保持既有「降级优于无数据」行为与零信号漂移）；
    # >0 = 实时报价快照超过该秒数则不再返回旧价（返回 None 触发源链继续降级）。
    # S6（D69 陈旧报价硬拒绝，2026-11-21 用户裁定启用）：实时报价快照年龄
    # 超过 4h(14400s) 视为陈旧、不再返回旧价 → 该轮该票不出信号（接受少出信号）。
    # 实测背景：ECS 体检见 age 2.7~4.4h 仍被当实时用；4h 线只拒绝对确定陈旧者。
    STALE_QUOTE_REJECT_SECONDS: int = 14400

    # I-11 AI 叙述失败时仍推送已完成的部分结果（附降级标记）
    AI_PARTIAL_NOTIFY: bool = True

    # S1/I-12 回测成交时点锚定（2026-11-21 用户裁定启用）
    # True（默认）＝信号基于「截至今日收盘」的数据生成，成交锚定到「次日开盘价」
    #   （挂单次日成交）——修正原「用当日收盘决策 + 同价当日收盘成交」的未来函数
    #   （同一根 bar 既参与决策又当场成交 → 系统性乐观偏差）。
    # False ＝ 保留旧的「当日收盘即时成交」用于 A/B 对比乐观偏差量（不影响实盘信号）。
    # 说明：回测是纯分析工具，不改任何实时信号/仓位，故 D41 实盘零漂移不受影响。
    BACKTEST_FILL_NEXT_OPEN: bool = True

    # Rust 加速扩展（D78，2026-11-22 用户要求「本项目自身 Rust 化改造」）：
    # 指标模块三处 Python 级逐元素循环（Wilder 平滑 / RSI / 平台天数 / 连续涨跌）
    # 下沉到自有 PyO3 扩展 `qerust`（源码 rust_accel/），其余仍 pandas/numpy 向量化。
    # True（默认）＝可用就用；扩展缺失/构建失败/平台不匹配时自动回退纯 Python（逐位等价）。
    # False ＝ 强制纯 Python（A/B 对比用）。
    RUST_ACCEL_ENABLED: bool = True

    # Kelly v1.8（2026-11-21 用户裁定落库，docs/44 复核 8-22~11-20 窗口全规则 f*≤0）：
    # Kelly 判定无正期望时（has_edge=False）的建议仓位。
    # 0.0＝归零口径（v1.8：停止按 Kelly 建议开仓）；回退 legacy 设 0.05（旧「无 edge→最小仓位 5%」地板）。
    KELLY_NO_EDGE_MIN_POSITION: float = 0.0

    # I-08 日报「过程纪律」第六维（R3，2026-11-21 用户裁定推进，默认启用）；
    # 由代码从真实信号/评估历史算分，绝不交给模型发挥（D61 铁律）。
    # 零信号影响：只加日报展示分区，不进门控 5 维综合分、不改仓位建议。
    DAILY_REPORT_DISCIPLINE_DIMENSION: bool = True

    # D68 东财反封禁「三招」跨进程加固（2026-11-07，用户截图复核）
    # 原三招只在单进程内生效，而现网 quant-api / quant-worker / quant-beat
    # 是三个独立进程 → 各自限速，对东财实际 QPS 可达 3 倍。开启后用 Redis
    # 原子预约下一个允许时刻，保证全局任意两次请求间隔 ≥1s + 抖动。
    # Redis 不可用 / fork 子进程 / 关闭开关时自动降级回原进程内锁（零行为变化）。
    EM_GLOBAL_THROTTLE: bool = True

    # D71 / N-10 同花顺官方金融数据服务（Hithink-Tech-Org/Financial-API）
    # 只做 market_alert 的旁路证据增强（封板率/炸板/异动原因），不进信号、
    # 不进评分/风控/Kelly/回测。默认关闭；无 API Key 时工厂直接返回 None。
    # Key 从 https://fuyao.aicubes.cn/admin/ 签发，只写 gitignored 的 .env。
    THS_FINANCIAL_API_KEY: str = ""
    THS_FINANCIAL_BASE_URL: str = "https://fuyao.aicubes.cn"
    THS_FINANCIAL_TIMEOUT: float = 10.0
    THS_MARKET_ALERT_ENRICH: bool = False
    # 「封板转弱」告警阈值：封板率低于该值且炸板家数达到下限时才提示。
    # 来源是 THS 官方字段语义（seal_rate 为小数比例、open_stock_count 为家数），
    # 不是策略参数；仅影响展示层告警，默认关闭开关下不生效。
    THS_SEAL_RATE_WEAK: float = 0.6
    THS_LIMIT_BREAK_MIN: int = 20

    # D73/D74 eltdx / AxData 只读适配层（research/docs/45、docs/46）
    # 实证：eltdx 的 Rust 7709 实现可真实取到日/分钟K、分时、逐笔、原生资金流，
    # 而本项目 pytdx 客户端在新旧节点均返回空（根因是客户端，非上游停供）。
    # 许可证：两上游为 Research-Only/Non-Commercial；用户 2026-11-21 裁定本项目为
    # 个人自用非商业、不对外提供服务，「能用上的全部用上」→ 默认启用（D74）。
    # 安全边界：K 线只进 _PRIORITY 末位兜底（主链成功永不触发）；未安装 eltdx 包时
    # EltdxFallbackSource 返回 None，降级链无缝跳过 → 本地 py3.9 无包时零影响。
    # 旁路（分时/逐笔/资金流/连板/榜单）只挂 market_alert 证据，不改任何信号口径。
    ELTDX_ENABLED: bool = True
    ELTDX_HOSTS: str = ""        # 逗号分隔 ip:port；空=用 eltdx 内置 43 节点列表
    ELTDX_TIMEOUT: float = 3.0   # 单次请求超时（秒）
    ELTDX_PROBE_HOSTS: int = 0   # >0 时探活前 N 台；0=不主动探测
    ELTDX_ADJUST: str = "qfq"    # 日K 前复权，对齐主链腾讯 fqkline 口径
    ELTDX_MARKET_ALERT_ENRICH: bool = True  # 旁路证据注入 market_alert（不改判定）

    # AI配置
    DEEPSEEK_API_KEY: str = ""
    OPENAI_API_KEY: str = ""
    OPENAI_BASE_URL: str = ""

    # 云知声 U2 配置（D56：替代小米 MiMo，作为第一主力模型）
    # 端点：OpenAI 兼容 https://maas-api.unisound.com/v1
    #       Anthropic 原生 https://maas-api.unisound.com/anthropic/v1
    #       两者实测均可用，鉴权头 x-api-key 或 Authorization: Bearer 都接受
    UNISOUND_API_KEY: str = ""
    UNISOUND_BASE_URL: str = "https://maas-api.unisound.com/v1"
    # D70（2026-11-21）：主力模型显式指定 u2-flash。此前写 "u2" 由服务端隐式
    # 路由到 u2-flash，响应 model 字段回显 u2-flash，日志/健康面板难以核对；
    # 显式写出后请求与回显一致。可选：u2-flash / u2 / u2-med。
    UNISOUND_MODEL: str = "u2-flash"
    # 思考型模型（u2 系列）思考 token 也会计入 max_tokens；
    # max_tokens 过小会导致 content=null + finish_reason=length。
    # D57 实测：典型 output 261~1256 tokens，偶发思考超预算。
    # D69（2026-11-21）：ECS 近 7 天出现 3 次 finish_reason=length 空正文，
    # 其中一次两轮重试仍为空 → 级联降级，实证 2500 已不足。
    # 上调至 6000 留足 thinking + 正文余量。
    # 注意：ECS 的 .env 若仍显式写 2500 会覆盖本默认值，部署时必须同步改。
    UNISOUND_MAX_TOKENS: int = 6000

    # 小米MiMo配置（D56：已下线，保留字段以兼容旧 .env，不再注册模型）
    MIMO_API_KEY: str = ""
    MIMO_BASE_URL: str = "https://api.xiaomimimo.com/v1"
    
    # 企业微信配置
    WECHAT_WEBHOOK_URL: str = ""
    
    # 飞书配置
    FEISHU_WEBHOOK_URL: str = ""
    
    # Telegram配置
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID: str = ""
    
    # 新闻搜索配置
    SERPAPI_API_KEY: str = ""
    TAVILY_API_KEY: str = ""

    # SQLite 持久层（2026-10-25，research/docs/25 §5.1）：信号/评估/总结耐久归档
    SQLITE_DB_PATH: str = "data/history.db"   # 相对路径按仓库根解析
    SIGNAL_RETENTION_DAYS: int = 400          # 信号归档保留天数
    EVAL_RETENTION_DAYS: int = 200            # 回测评估归档保留天数
    SUMMARY_RETENTION_COUNT: int = 500        # 总结归档保留条数（按 id 取最新）

    # 早间财经新闻推送配置（每日9:15，按板块，去重）
    NEWS_WINDOW_HOURS: int = 20          # 无上次推送记录时的回看窗口（小时）
    NEWS_MAX_PER_SECTOR: int = 6         # 每板块最多展示条数
    NEWS_MAX_SECTORS: int = 8            # 文案最多展示板块数
    NEWS_MAX_TOTAL: int = 120            # 单次抓取总量上限
    NEWS_DEDUP_THRESHOLD: float = 0.85   # 标题近似去重相似度阈值
    NEWS_SEEN_TTL_HOURS: int = 48        # 去重指纹保留时长（跨推送去重）
    NEWS_X_BEARER_TOKEN: str = ""        # X(Twitter) API v2 Bearer Token（留空关闭）
    NEWS_X_QUERY: str = "$SPX OR $DJIA OR $NDX OR #stocks -is:retweet"
    NEWS_TDX_MCP_ENDPOINT: str = ""      # 通达信 MCP HTTP 端点（留空关闭）
    NEWS_TDX_MCP_API_KEY: str = ""       # 通达信 MCP API Key
    NEWS_RSS_FEEDS: str = ""             # 额外RSS源，格式 "名称|url,名称2|url2"

    class Config:
        env_file = ".env"

    @property
    def NEWS_RSS_FEEDS_LIST(self) -> list:
        if not self.NEWS_RSS_FEEDS:
            return []
        return [s.strip() for s in self.NEWS_RSS_FEEDS.split(',') if s.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
