from ..data.repositories import MarketDataRepository
from ..services.signals.signal_generator import SignalGenerator
from ..services.monitors.etf_monitor import ETFMonitor
from ..services.monitors.etf_holdings_monitor import ETFHoldingsMonitor
from ..services.monitors.news_monitor import MorningNewsMonitor
from ..services.code_guard import CodeGuard
from ..services.risk.cooldown import CooldownManager
from ..services.monitors.sector_monitor import SectorMonitor
from ..services.market.market_alert import MarketAlert
from ..services.monitors.hot_money_monitor import HotMoneyMonitor
from ..backtest.engine import BacktestEngine
from ..strategies.registry import StrategyRegistry
from ..services.monitors.hotspot import HotspotService

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
strategy_registry = StrategyRegistry(datacenter)
hotspot_service = HotspotService(datacenter)