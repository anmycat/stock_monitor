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
    
    # AI配置
    DEEPSEEK_API_KEY: str = ""
    OPENAI_API_KEY: str = ""
    OPENAI_BASE_URL: str = ""
    
    # 小米MiMo配置
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


@lru_cache()
def get_settings() -> Settings:
    return Settings()
