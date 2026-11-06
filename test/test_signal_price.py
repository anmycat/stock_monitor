"""信号现价与日K参考价分离测试（D64）

背景（2026-11-06 实测）：用户截图显示大晟文化（600892.SH）信号卡「信号价 5.14」，
但当日实际最高仅 4.88、收盘跌停 4.63。根因：日K 主源（腾讯）盘中返回**含当日未完成
bar**，`df['close'].iloc[-1]` 即盘中实时价；但腾讯 501（限流，实测 15+ 只/日）时降级到
新浪，而**新浪的 240 分钟日K 盘中不含当日未完成 bar** → 最后一根是昨日 → 信号价成昨收。

本测试覆盖：
1. `_extract_prices`：从本轮全市场实时报价提取 {code: price}
2. `_fill_current_price`：填 current_price + price_ref；本轮报价优先，按票缓存兜底
3. 卡片：两者偏离 >0.5% 时并列给出现价；现价大幅低于参考价时给跳空提示
4. **信号计算口径不变**：price 字段仍是日K收盘（D47 语义，与 signals 表 price 列一致）
"""
import os
import sys
import unittest
from datetime import datetime
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from quant_engine.models.signal import Signal, SignalAction
from quant_engine.services.notifications.manager import NotificationManager
from quant_engine.services.signals.signal_generator import SignalGenerator


def _signal(code='600892.SH', name='大晟文化', price=5.14, action=SignalAction.BUY):
    return Signal(
        code=code, name=name, action=action, level=None, score=82.0,
        position=0.0525, reason=['均线多头排列'], strategy='趋势主升', price=price,
        timestamp=datetime(2026, 11, 6, 10, 52, 39),
    )


def _gen(round_quotes=None, cached=None):
    """最小 SignalGenerator 替身（__new__ 绕开 __init__ 的外部依赖）"""
    g = SignalGenerator.__new__(SignalGenerator)
    g._round_quotes = round_quotes or {}
    dc = MagicMock()
    if cached is None:
        dc.get_cached_quote.return_value = None
    else:
        dc.get_cached_quote.return_value = cached
    g.datacenter = dc
    return g


class TestExtractQuotes(unittest.TestCase):
    """D65：提取的是**整行** quote（修正需要 open/high/low/volume）"""

    def test_extracts_full_quote_rows(self):
        df = pd.DataFrame([
            {'code': '600892.SH', 'price': 4.83, 'open': 4.90,
             'high': 4.95, 'low': 4.80, 'volume': 100, 'change_pct': -6.0},
            {'code': '000001.SZ', 'price': 12.3, 'open': 12.0,
             'high': 12.5, 'low': 11.9, 'volume': 200, 'change_pct': 1.2},
        ])
        out = SignalGenerator._extract_quotes(df)
        self.assertEqual(len(out), 2)
        self.assertEqual(out['600892.SH']['price'], 4.83)
        self.assertEqual(out['600892.SH']['open'], 4.90)     # 修正需要的字段
        self.assertEqual(out['600892.SH']['high'], 4.95)
        self.assertEqual(out['600892.SH']['low'], 4.80)
        self.assertEqual(out['000001.SZ']['price'], 12.3)

    def test_skips_invalid_prices(self):
        df = pd.DataFrame([
            {'code': 'A.SH', 'price': 0, 'open': 1},
            {'code': 'B.SH', 'price': None, 'open': 1},
            {'code': 'C.SH', 'price': 3.5, 'open': 3.4},
        ])
        out = SignalGenerator._extract_quotes(df)
        self.assertEqual(list(out.keys()), ['C.SH'])
        self.assertEqual(out['C.SH']['price'], 3.5)

    def test_empty_and_missing_columns(self):
        self.assertEqual(SignalGenerator._extract_quotes(None), {})
        self.assertEqual(SignalGenerator._extract_quotes(pd.DataFrame()), {})
        self.assertEqual(
            SignalGenerator._extract_quotes(pd.DataFrame([{'code': 'A'}])),
            {})


class TestFillCurrentPrice(unittest.TestCase):
    def test_uses_round_quotes(self):
        g = _gen(round_quotes={'600892.SH': {'price': 4.83}})
        sig = _signal()
        g._fill_current_price(sig)
        self.assertEqual(sig.current_price, 4.83)
        self.assertEqual(sig.price_ref, 5.14)
        # price 字段语义不变（D47）：仍是日K最后一根收盘
        self.assertEqual(sig.price, 5.14)

    def test_falls_back_to_cached_quote(self):
        g = _gen(round_quotes={}, cached={'price': 4.8})
        sig = _signal()
        g._fill_current_price(sig)
        self.assertEqual(sig.current_price, 4.8)
        self.assertEqual(sig.price_ref, 5.14)

    def test_no_price_available_keeps_none(self):
        g = _gen(round_quotes={}, cached=None)
        sig = _signal()
        g._fill_current_price(sig)
        self.assertIsNone(sig.current_price)
        self.assertEqual(sig.price_ref, 5.14)

    def test_cached_quote_error_does_not_raise(self):
        g = _gen(round_quotes={}, cached=None)
        g.datacenter.get_cached_quote.side_effect = RuntimeError('redis down')
        sig = _signal()
        g._fill_current_price(sig)   # 不应抛异常
        self.assertIsNone(sig.current_price)


class TestSignalCardPrice(unittest.TestCase):
    def setUp(self):
        self.manager = NotificationManager()
        self.channel = MagicMock()
        self.channel.name = 'MockChannel'
        self.channel.send = MagicMock(return_value=True)
        self.manager.notifiers = [self.channel]
        self.manager._min_interval = 0.0
        self.manager._max_per_minute = 100

    def _content(self, signal):
        self.manager.send_signal(signal)
        return self.channel.send.call_args[0][1]

    def test_price_line_always_present(self):
        content = self._content(_signal())
        self.assertIn('**信号价**', content)
        self.assertIn('5.14', content)

    def test_current_price_shown_when_deviates(self):
        sig = _signal()
        sig.current_price = 4.63      # 跌停价
        content = self._content(sig)
        self.assertIn('**现价**', content)
        self.assertIn('4.63', content)
        self.assertIn('跳空提示', content)

    def test_current_price_hidden_when_identical(self):
        sig = _signal()
        sig.current_price = 5.14      # 与信号价一致（腾讯主源路径）
        content = self._content(sig)
        self.assertIn('**信号价**', content)
        self.assertNotIn('**现价**', content)
        self.assertNotIn('跳空提示', content)

    def test_small_deviation_no_extra_line(self):
        sig = _signal()
        sig.current_price = 5.12      # 偏离 0.4% < 0.5% 阈值
        content = self._content(sig)
        self.assertNotIn('**现价**', content)

    def test_upward_deviation_no_gap_warning(self):
        sig = _signal()
        sig.current_price = 5.80      # 涨 7.6%：不是跳空风险
        content = self._content(sig)
        self.assertIn('**现价**', content)
        self.assertNotIn('跳空提示', content)


if __name__ == '__main__':
    unittest.main()
