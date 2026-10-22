from ..backtest.engine import BacktestEngine
from ..data.repositories import MarketDataRepository
from ..services.ai.llm_ranker import LLMRanker
from ..services.code_guard import CodeGuard
from ..services.market.market_alert import MarketAlert
from ..services.monitors.etf_holdings_monitor import ETFHoldingsMonitor
from ..services.monitors.etf_monitor import ETFMonitor
from ..services.monitors.hot_money_monitor import HotMoneyMonitor
from ..services.monitors.hotspot import HotspotService
from ..services.monitors.news_monitor import MorningNewsMonitor
from ..services.monitors.sector_monitor import SectorMonitor
from ..services.risk.cooldown import CooldownManager
from ..services.signals.candidate_context import CandidateContext
from ..services.signals.signal_generator import SignalGenerator
from ..strategies.registry import StrategyRegistry

datacenter = MarketDataRepository()
signal_generator = SignalGenerator(datacenter)
backtest_engine = BacktestEngine(datacenter)
etf_monitor = ETFMonitor(datacenter)
etf_holdings_monitor = ETFHoldingsMonitor(datacenter)
news_monitor = MorningNewsMonitor(datacenter)
code_guard = CodeGuard(datacenter)
cooldown_manager = CooldownManager(datacenter.cache)
sector_monitor = SectorMonitor(datacenter)
market_alert = MarketAlert(datacenter)
hot_money_monitor = HotMoneyMonitor(datacenter)
# P4：funnel 增强层由 L0 注入 L1 实例（L2 strategies 不再反向 import L1 services）
strategy_registry = StrategyRegistry(
    datacenter, llm_ranker=LLMRanker(), context_enricher=CandidateContext()
)
hotspot_service = HotspotService(datacenter)