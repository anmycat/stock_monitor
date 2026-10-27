"""因子融合后级别重算测试（D47）

覆盖：blended = 策略分×0.7 + 因子分×0.3 替换 score 后，level 按
同一阈值（S80/A70/B60）重算——修复"63.5 分挂 B 级"的级别与分数
不同源问题；WATCH 降级信号不挂级；因子分<40 直接拒绝。
"""
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.models.signal import Signal, SignalAction, SignalLevel
from quant_engine.strategies.dispatcher import StrategyDispatcher


def _signal(score=65.0, action=SignalAction.BUY, level=SignalLevel.B):
    return Signal(
        code='601512.SH', name='中新集团', action=action, level=level,
        score=score, position=0.1, reason=['RSI超卖'], strategy='低吸反转',
        price=7.74, timestamp=pd.Timestamp.now(),
    )


def _dispatcher_with_factor(factor_score):
    """构造仅含 factor_scorer 的 dispatcher 实例（绕过 __init__ 依赖）"""
    d = StrategyDispatcher.__new__(StrategyDispatcher)
    d.factor_scorer = MagicMock()
    d.factor_scorer.calculate_factors.return_value = {
        'composite_score': factor_score,
        'liquidity_score': 80, 'stability_score': 75,
    }
    return d


class TestFactorBlendLevel(unittest.TestCase):
    def test_level_recomputed_from_blended_score(self):
        """策略分65×0.7 + 因子60×0.3 = 63.5 → B级（≥60，与YAML B阈值一致）"""
        d = _dispatcher_with_factor(60.0)
        sig = _signal(score=65.0, level=SignalLevel.B)
        out = d._apply_factor_scoring(sig, pd.DataFrame(), {})
        self.assertIsNotNone(out)
        self.assertEqual(out.score, 63.5)
        self.assertEqual(out.level, SignalLevel.B)

    def test_high_blended_gets_a(self):
        """策略分70×0.7 + 因子90×0.3 = 76 → A级"""
        d = _dispatcher_with_factor(90.0)
        sig = _signal(score=70.0, level=SignalLevel.B)
        out = d._apply_factor_scoring(sig, pd.DataFrame(), {})
        self.assertEqual(out.score, 76.0)
        self.assertEqual(out.level, SignalLevel.A)

    def test_watch_downgrade_keeps_no_level(self):
        """因子分50<55 → BUY 降级 WATCH 且无级（blended 60.5 也不挂级）"""
        d = _dispatcher_with_factor(50.0)
        sig = _signal(score=65.0, level=SignalLevel.B)
        out = d._apply_factor_scoring(sig, pd.DataFrame(), {})
        self.assertEqual(out.action, SignalAction.WATCH)
        self.assertIsNone(out.level)

    def test_watch_signal_not_given_level(self):
        """策略侧 WATCH 信号（level=None）不因融合分高而挂级"""
        d = _dispatcher_with_factor(80.0)
        sig = _signal(score=50.0, action=SignalAction.WATCH, level=None)
        out = d._apply_factor_scoring(sig, pd.DataFrame(), {})
        self.assertIsNone(out.level)

    def test_factor_reject_below_40(self):
        """因子分<40 → BUY 直接拒绝（None）"""
        d = _dispatcher_with_factor(35.0)
        sig = _signal(score=90.0, level=SignalLevel.S)
        out = d._apply_factor_scoring(sig, pd.DataFrame(), {})
        self.assertIsNone(out)

    def test_factor_highlights_appended(self):
        """因子亮点（≥70 的前二名）追加到 reason"""
        d = _dispatcher_with_factor(60.0)
        sig = _signal(score=65.0)
        out = d._apply_factor_scoring(sig, pd.DataFrame(), {})
        self.assertTrue(any('因子:' in r for r in out.reason))


if __name__ == '__main__':
    unittest.main()
