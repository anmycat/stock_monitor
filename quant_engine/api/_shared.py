"""API 层共享依赖（P3 重构从 routes.py 提取）

域 router 统一经本模块取服务实例/校验器/正则，避免重复构造。
实例构造顺序与原 routes.py 模块级一致（单例语义不变）。
"""
import re

from ..backtest.post_evaluation import PostEvaluation
from ..services.ai.ai_summary import AISummary
from ..services.market.data_validator import DataValidator
from ..services.market.market_analyzer import MarketAnalyzer
from ..services.portfolio import PortfolioManager
from ..utils.stock_resolver import resolver
from .auth import verify_api_key
from .dependencies import (
    backtest_engine,
    code_guard,
    cooldown_manager,
    datacenter,
    etf_holdings_monitor,
    etf_monitor,
    hot_money_monitor,
    hotspot_service,
    market_alert,
    news_monitor,
    sector_monitor,
    signal_generator,
    strategy_registry,
)

# 域 router 的公共导出面（转手导出，勿删；__all__ 同时消除 F401）
__all__ = [
    '_CODE_RE',
    '_DATE_RE',
    '_RUN_ID_RE',
    '_SUMMARY_TYPES',
    'ai_summary',
    'backtest_engine',
    'code_guard',
    'cooldown_manager',
    'data_validator',
    'datacenter',
    'etf_holdings_monitor',
    'etf_monitor',
    'hot_money_monitor',
    'hotspot_service',
    'market_alert',
    'market_analyzer',
    'news_monitor',
    'portfolio_manager',
    'post_evaluation',
    'resolver',
    'sector_monitor',
    'signal_generator',
    'strategy_registry',
    'verify_api_key',
]

# 入参校验正则
_CODE_RE = re.compile(r'^\d{6}\.(SZ|SH|BJ)$')
_SUMMARY_TYPES = {'general', 'morning_review', 'daily_review', 'call_auction'}
_DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
_RUN_ID_RE = re.compile(r'^[A-Za-z0-9_-]{1,64}$')

# 模块级服务实例（原 routes.py L28-32，构造顺序一致）
ai_summary = AISummary()
market_analyzer = MarketAnalyzer(datacenter)
data_validator = DataValidator(datacenter.cache)
portfolio_manager = PortfolioManager(datacenter.cache)
post_evaluation = PostEvaluation(datacenter.cache, datacenter)
