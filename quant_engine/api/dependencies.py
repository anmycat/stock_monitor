from ..data.datacenter import DataCenter
from ..services.signal_generator import SignalGenerator
from ..services.etf_monitor import ETFMonitor
from ..services.cooldown import CooldownManager
from ..services.sector_monitor import SectorMonitor
from ..services.market_alert import MarketAlert
from ..services.hot_money_monitor import HotMoneyMonitor
from ..backtest.engine import BacktestEngine
from ..strategies.registry import StrategyRegistry
from ..services.hotspot import HotspotService

datacenter = DataCenter()
signal_generator = SignalGenerator(datacenter)
backtest_engine = BacktestEngine(datacenter)
etf_monitor = ETFMonitor(datacenter)
cooldown_manager = CooldownManager(datacenter.cache)
sector_monitor = SectorMonitor(datacenter)
market_alert = MarketAlert(datacenter)
hot_money_monitor = HotMoneyMonitor(datacenter)
strategy_registry = StrategyRegistry(datacenter)
hotspot_service = HotspotService(datacenter)