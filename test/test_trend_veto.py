"""趋势策略风控闸门测试（D44，docs/26 M3-1/M3-5/M3-6）

覆盖：5日乖离率>9% 禁BUY降WATCH / 高位剧烈放量(量比≥5)禁BUY /
低位温和放量加分 / 高位明显放量降分 / 量增价平降分 / 量比<0.5扣分 /
正常多头排列基准仍BUY。fixture：内存DataFrame，不打网络。
"""
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.models.market import MarketState, MarketType
from quant_engine.strategies.trend import TrendStrategy


def _market() -> MarketState:
    return MarketState(
        type=MarketType.BULL, score=75.0, trend=0.5, volume_ratio=1.2,
        breadth=0.6, timestamp=datetime.now(),
    )


def _df(closes, volumes=None, highs=None, lows=None) -> pd.DataFrame:
    """构造日K DataFrame（closes 驱动，其余列默认由 close 派生）"""
    n = len(closes)
    if volumes is None:
        volumes = [1_000_000] * n
    if highs is None:
        highs = [c * 1.01 for c in closes]
    if lows is None:
        lows = [c * 0.99 for c in closes]
    dates = [datetime(2026, 8, 1) + timedelta(days=i) for i in range(n)]
    return pd.DataFrame({
        'date': dates, 'open': closes, 'high': highs, 'low': lows,
        'close': closes, 'volume': volumes,
    })


def _indicators(ma5, ma10, ma20, rsi=55, vol_ratio=1.0) -> dict:
    return {
        'ma5': ma5, 'ma10': ma10, 'ma20': ma20, 'rsi': rsi,
        'macd': 0.5, 'macd_signal': 0.2, 'volume_ratio': vol_ratio,
    }


class TestTrendVeto(unittest.TestCase):
    """D44 风控闸门：分数达标但触发硬条件 → 强制 WATCH 不推 BUY"""

    def setUp(self):
        self.strategy = TrendStrategy()
        self.market = _market()

    def test_bias_overbought_blocks_buy(self):
        """5日乖离 >9%：即使其余指标全优也降 WATCH"""
        closes = [10.0] * 55 + [11.0, 11.5, 12.0, 12.5, 13.0]  # 末价远离ma5
        df = _df(closes)
        ind = _indicators(ma5=11.5, ma10=11.0, ma20=10.6, vol_ratio=2.0)
        # 乖离 = 13/11.5-1 = 13% > 9%
        signal = self.strategy.analyze(df, ind, self.market)
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action.value, 'WATCH')
        self.assertTrue(any('不追高' in r for r in signal.reason))

    def test_high_position_extreme_volume_blocks_buy(self):
        """20日高位 + 量比≥5（剧烈放量）：防派发禁BUY"""
        closes = list(range(10, 60))  # 单边上行，末价=20日高位
        df = _df([float(c) for c in closes])
        last = closes[-1]
        ind = _indicators(ma5=last * 0.98, ma10=last * 0.96, ma20=last * 0.93,
                          vol_ratio=6.0)
        signal = self.strategy.analyze(df, ind, self.market)
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action.value, 'WATCH')
        self.assertTrue(any('防派发' in r for r in signal.reason))

    def test_low_position_moderate_volume_bonus(self):
        """20日低位 + 温和放量：低位放量形态产生信号且 reason 标注低位"""
        # 先跌后横盘微升：末价处于20日低位（low_20 由早期高点决定）
        closes = [20.0] * 40 + [10.0] * 12 + [10.2, 10.4, 10.6, 10.8, 11.0, 11.1]
        df = _df([float(c) for c in closes])
        ind = _indicators(ma5=10.6, ma10=10.4, ma20=10.9, vol_ratio=2.0)
        signal = self.strategy.analyze(df, ind, self.market)
        self.assertIsNotNone(signal)
        self.assertTrue(any('20日低位' in r for r in signal.reason))

    def test_high_position_moderate_volume_penalized_not_blocked(self):
        """高位 + 明显放量(2.5-5)：较非高位同放量降 10 分，但不硬禁
        （主升浪加速也呈此形态，一刀切会误杀）"""
        # 高位场景：单边上行，末价=20日窗口顶
        closes_high = [float(c) for c in range(10, 60)]
        df_high = _df(closes_high)
        ind = _indicators(ma5=59 * 0.99, ma10=59 * 0.98, ma20=59 * 0.96,
                          vol_ratio=3.0)
        score_high = self.strategy._volume_position_score(df_high, ind)
        # 非高位场景：区间震荡，末价处于 20 日窗口中部（pos≈0.75）
        closes_mid = (list(range(10, 21)) +
                      [15, 16, 14, 15, 13, 15, 17, 14, 16, 15] * 5 + [16])
        df_mid = _df([float(c) for c in closes_mid])
        score_mid = self.strategy._volume_position_score(df_mid, ind)
        # 同样放量：高位较非高位少 10 分（位置派发惩罚）
        self.assertEqual(score_mid - score_high, 10)
        # 且不触发硬 veto（量比<5）
        veto, _ = self.strategy._check_veto(df_high, ind)
        self.assertFalse(veto)

    def test_volume_up_price_flat_penalty(self):
        """量增价平：放量但涨幅<1% → 较同样放量价涨少 5 分"""
        base = [10.0] * 59
        df_flat = _df([float(c) for c in base + [10.0005]])   # +0.005% 价平
        df_up = _df([float(c) for c in base + [10.2]])        # +2% 价涨
        ind = _indicators(ma5=10.4, ma10=10.4, ma20=10.4, vol_ratio=3.0)
        score_flat = self.strategy._volume_position_score(df_flat, ind)
        score_up = self.strategy._volume_position_score(df_up, ind)
        self.assertEqual(score_up - score_flat, 5)

    def test_normal_bull_alignment_buy(self):
        """基准：多头排列 + 健康指标 → BUY（修复不误杀正常信号）"""
        closes = [10.0] * 50 + [10.2, 10.5, 10.8, 11.0, 11.2, 11.5, 11.8,
                                12.0, 12.2, 12.5]
        df = _df([float(c) for c in closes])
        last = closes[-1]
        ind = _indicators(ma5=last * 0.97, ma10=last * 0.95, ma20=last * 0.90,
                          vol_ratio=2.0)
        signal = self.strategy.analyze(df, ind, self.market)
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action.value, 'BUY')

    def test_tiny_volume_penalty(self):
        """量比<0.5（无人问津）：扣5分"""
        closes = [float(c) for c in range(10, 60)]
        df = _df(closes)
        last = closes[-1]
        ind = _indicators(ma5=last * 0.99, ma10=last * 0.98, ma20=last * 0.96,
                          vol_ratio=0.3)
        score = self.strategy._volume_position_score(df, ind)
        ind_ref = dict(ind, volume_ratio=1.0)
        self.assertLess(score, self.strategy._volume_position_score(df, ind_ref))

    def test_bear_market_returns_none(self):
        """熊市门保持有效（不被 D44 逻辑影响）"""
        closes = [float(c) for c in range(60, 10, -1)]
        df = _df(closes)
        ind = _indicators(ma5=11.0, ma10=12.0, ma20=13.0, vol_ratio=2.0)
        bear = MarketState(
            type=MarketType.BEAR, score=20.0, trend=-0.5, volume_ratio=1.2,
            breadth=0.3, timestamp=datetime.now(),
        )
        self.assertIsNone(self.strategy.analyze(df, ind, bear))


if __name__ == '__main__':
    unittest.main()
