"""服务层聚合导出（P2 重构：30 模块按职责分 6 子包，prd/15 §2.3）

子包：monitors（监控族）/ market（市场分析族）/ risk（风控族）/
signals（信号族）/ notifications（通知族）/ ai（AI 族）。
本文件保持历史 import 路径（from ..services import X）可用。
"""
from .ai.ai_summary import AISummary
from .market.market_analyzer import MarketAnalyzer
from .monitors.burst_monitor import BurstMonitor
from .monitors.hot_money_monitor import HotMoneyMonitor
from .notifications.manager import DingTalkNotifier
from .portfolio import PortfolioManager
from .risk.cooldown import CooldownManager
from .risk.position import PositionManager
from .risk.risk import RiskManager
from .signals.candidate_context import CandidateContext
from .signals.signal_generator import SignalGenerator

__all__ = [
    "AISummary",
    "BurstMonitor",
    "CandidateContext",
    "CooldownManager",
    "DingTalkNotifier",
    "HotMoneyMonitor",
    "MarketAnalyzer",
    "PortfolioManager",
    "PositionManager",
    "RiskManager",
    "SignalGenerator",
]
