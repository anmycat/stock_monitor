"""
Market State Tokenization System
基于Kronos思想，将市场状态量化为Token
"""

from enum import Enum
from typing import Dict, Optional
import logging

logger = logging.getLogger("guardian")


class MarketState(Enum):
    """市场状态Token"""

    CONSOLIDATION = "震荡"
    UPTREND = "上涨"
    DOWNTREND = "下跌"
    PANIC = "恐慌"
    WATCH = "观望"
    BULL_WEAK = "强势"
    BEAR_WEAK = "弱势"


class MarketStateAnalyzer:
    """市场状态分析器"""

    def __init__(self):
        self._cache = {}
        self._cache_ttl = 300

    def analyze_market_state(self) -> Dict:
        """分析当前市场状态"""
        import time

        now = time.time()

        if (
            self._cache.get("state")
            and (now - self._cache.get("ts", 0)) < self._cache_ttl
        ):
            return self._cache.get("data", {})

        from .sector_analysis import get_market_breadth, auto_discover_hot_sectors
        from .market import get_quote, is_trading_day

        if not is_trading_day():
            return {
                "state": MarketState.WATCH.value,
                "confidence": 0.5,
                "reason": "非交易日",
            }

        try:
            index_code = "sh000001"
            index_q = get_quote(index_code)
            price = index_q.get("price", 0)
            pct = index_q.get("pct_change", 0) or 0

            breadth = get_market_breadth(sample_size=1000)
            rising_pct = breadth.get("rising_pct", 0) * 100 if breadth else 0

            hot_sectors = auto_discover_hot_sectors(
                min_stocks_rising=3, min_avg_pct=1.0
            )
            hot_count = len(hot_sectors) if hot_sectors else 0

            state, confidence, reason = self._classify_state(
                price, pct, rising_pct, hot_count
            )

            result = {
                "state": state.value,
                "confidence": confidence,
                "reason": reason,
                "index_price": price,
                "index_pct": pct,
                "rising_pct": rising_pct,
                "hot_sectors": hot_count,
            }

            self._cache = {"data": result, "ts": now}

        except Exception as e:
            logger.warning(f"market_state_analysis_failed: {e}")
            return {
                "state": MarketState.WATCH.value,
                "confidence": 0.3,
                "reason": f"分析失败: {e}",
            }

        return result

    def _classify_state(
        self, price: float, pct: float, rising_pct: float, hot_count: int
    ):
        """分类市场状态"""
        if pct > 2.5 and rising_pct > 60:
            return MarketState.PANIC, 0.9, "大幅上涨+市场过热"

        if pct > 1.5 and rising_pct > 40:
            return MarketState.BULL_WEAK, 0.8, "上涨趋势但非极端"

        if pct > 0.5 and rising_pct > 30:
            return MarketState.UPTREND, 0.7, "温和上涨"

        if pct < -2.5 and rising_pct < 25:
            return MarketState.PANIC, 0.9, "大幅下跌+市场恐慌"

        if pct < -1.5 and rising_pct < 35:
            return MarketState.BEAR_WEAK, 0.8, "下跌趋势但非极端"

        if pct < -0.5 and rising_pct < 45:
            return MarketState.DOWNTREND, 0.7, "温和下跌"

        if hot_count >= 5 and rising_pct > 25:
            return MarketState.CONSOLIDATION, 0.6, "板块活跃但指数平稳"

        return MarketState.WATCH, 0.5, "市场观望"


def get_market_state_token() -> str:
    """获取当前市场状态Token"""
    analyzer = MarketStateAnalyzer()
    result = analyzer.analyze_market_state()
    return result.get("state", "观望")


def get_market_state_info() -> Dict:
    """获取完整市场状态信息"""
    analyzer = MarketStateAnalyzer()
    return analyzer.analyze_market_state()
