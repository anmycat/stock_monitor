from pydantic_settings import BaseSettings
from functools import lru_cache


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

    class Config:
        env_file = ".env"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
