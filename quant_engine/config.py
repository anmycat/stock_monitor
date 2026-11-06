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
