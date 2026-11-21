"""S1/I-12 回测时间锚点测试（2026-11-21 用户裁定）

验证两件事：
1. 成交锚点：默认（BACKTEST_FILL_NEXT_OPEN=True）下，信号基于「截至当日收盘」
   的数据生成，成交于「次日开盘价」——修复原「当日收盘决策 + 同日收盘成交」
   的未来函数（决策价与成交价重合 = 乐观偏差）。
2. 日期列归一：真实日K列名是 ``date``，原实现硬用 ``trade_date`` 会 KeyError；
   现两种列名都兼容。
"""
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.backtest.engine import BacktestEngine
from quant_engine.config import get_settings


class _BuySignal:
    class _Action:
        value = 'BUY'

    def __init__(self, position: float = 0.5):
        self.action = _BuySignal._Action()
        self.position = position


class _AlwaysBuyDispatcher:
    def dispatch(self, df, market):
        return _BuySignal()


def _make_df(n: int = 80, date_col: str = 'date') -> pd.DataFrame:
    rows = []
    for i in range(n):
        d = f"2024-{(i // 28) + 1:02d}-{(i % 28) + 1:02d}"
        o = 10 + i * 0.1
        c = o + 0.05
        rows.append({date_col: d, 'open': o, 'close': c,
                     'high': c, 'low': o, 'volume': 100})
    df = pd.DataFrame(rows)
    df['code'] = '000001.SZ'
    return df


def _fresh_engine() -> BacktestEngine:
    """绕开构造器（避免真实 StrategyDispatcher 初始化 IO），直接注入桩。"""
    engine = BacktestEngine.__new__(BacktestEngine)
    engine.dispatcher = _AlwaysBuyDispatcher()
    engine.initial_capital = 100000.0
    engine.commission_rate = 0.0003
    engine.stop_loss = 0.05
    engine.take_profit = 0.15
    return engine


class TestBacktestAnchor(unittest.TestCase):
    def setUp(self):
        self._saved = get_settings().BACKTEST_FILL_NEXT_OPEN

    def tearDown(self):
        get_settings().BACKTEST_FILL_NEXT_OPEN = self._saved

    def test_next_open_fill_anchor(self):
        """默认：成交价应取至某根「开盘价」，且晚于信号日一日。"""
        get_settings().BACKTEST_FILL_NEXT_OPEN = True
        engine = _fresh_engine()
        df = _make_df(80)
        trades, _ = engine._backtest_stock(df, '2024-01-01', '2024-12-31', 100000)
        self.assertTrue(trades, '应产生至少一笔完整交易')
        first = trades[0]
        opens = {round(float(v), 4) for v in df['open']}
        self.assertIn(round(float(first['entry_price']), 4), opens,
                      'next_open 模式成交价必须是开盘价（次日开盘成交）')
        # window=60 → 首个可决策日索引 60，成交在次日索引 61 的开盘
        self.assertAlmostEqual(float(first['entry_price']),
                               round(float(df['open'].iloc[61]), 4), places=4)

    def test_legacy_same_bar_fill(self):
        """legacy：保留旧「当日收盘即时成交」，用于 A/B 对比乐观偏差量。"""
        get_settings().BACKTEST_FILL_NEXT_OPEN = False
        engine = _fresh_engine()
        df = _make_df(80)
        trades, _ = engine._backtest_stock(df, '2024-01-01', '2024-12-31', 100000)
        self.assertTrue(trades)
        first = trades[0]
        closes = {round(float(v), 4) for v in df['close']}
        self.assertIn(round(float(first['entry_price']), 4), closes,
                      'legacy 模式成交价必须是收盘价（同日收盘成交）')

    def test_date_column_accepted(self):
        """真实列名 'date' 不应 KeyError（原实现用 trade_date）。"""
        get_settings().BACKTEST_FILL_NEXT_OPEN = True
        engine = _fresh_engine()
        df = _make_df(70, date_col='date')
        _trades, equity = engine._backtest_stock(df, '2024-01-01', '2024-12-31', 100000)
        self.assertIsInstance(equity, list)

    def test_trade_date_column_still_works(self):
        """历史夹具列名 'trade_date' 仍兼容。"""
        get_settings().BACKTEST_FILL_NEXT_OPEN = True
        engine = _fresh_engine()
        df = _make_df(70, date_col='trade_date')
        _trades, equity = engine._backtest_stock(df, '2024-01-01', '2024-12-31', 100000)
        self.assertIsInstance(equity, list)


if __name__ == '__main__':
    unittest.main()
