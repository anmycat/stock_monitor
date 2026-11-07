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
    # ② 拒绝层：**默认关闭**——开启后「整帧异常且非当日」的数据不再出厂，
    #    会改变信号。启用前必须按项目纪律做 D41 双跑 + 半年窗口复验 + 用户裁定。
    KLINE_REJECT_STALE_FRAME: bool = False

    # I-05 数据源失败路径效率（2026-11-07）
    # 超时/连接类失败不再盲重试（原本每次 ~10s × 2 重试拖垮全市场扫描），
    # 并在抑制窗口内跳过同源重复请求；窗口内其它源仍正常参与 fallback。
    SOURCE_TIMEOUT_SUPPRESSION: bool = True
    SOURCE_TIMEOUT_SUPPRESS_SECONDS: float = 20.0
    # 连续多少次同类超时才进入抑制（避免偶发抖动误伤）
    SOURCE_TIMEOUT_SUPPRESS_STREAK: int = 2

    # I-11 AI 叙述失败时仍推送已完成的部分结果（附降级标记）
    AI_PARTIAL_NOTIFY: bool = True

    # I-08 日报「过程纪律」第六维：默认关闭，口径待用户裁定；
    # 开启后由代码从真实信号/评估历史算分，绝不交给模型发挥（D61 铁律）。
    DAILY_REPORT_DISCIPLINE_DIMENSION: bool = False

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
    UNISOUND_MODEL: str = "u2"          # 可选：u2 / u2-flash / u2-med
    # 思考型模型（u2 系列）思考 token 也会计入 max_tokens；
    # max_tokens 过小会导致 content=null + finish_reason=length。
    # D57 实测：典型 output 261~1256 tokens，但**偶发**思考超预算
    # （生产 morning_review 出现过一次 finish_reason=length 空正文）。
    # 取 2500 留足余量（实测延迟 11~16s，余量充足）。
    UNISOUND_MAX_TOKENS: int = 2500

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
