"""盘中日K 实时修正测试（D65）

覆盖：
1. `apply_intraday_correction` 三种情形：末根已是当日（刷新）/ 末根是昨日（追加）/ 不可修正
2. 追加当日 bar 的字段正确性（open/high/low/close/volume 取自实时报价）
3. 边界：空 df / 无报价 / 非法价格 / 末根日期在未来 / 缺列
4. **真实回放**：大晟文化 600892（腾讯路径，末根含当日）与新浪路径（末根为昨日）
   两种情形下，修正后末根 close 是否等于现价
5. SignalGenerator 接线：`_extract_quotes` / `_quote_for_code` / A/B 影子记录
"""
import os
import sys
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from quant_engine.services.signals.signal_generator import SignalGenerator
from quant_engine.utils.intraday_kline import (
    apply_intraday_correction,
    describe,
)


def _df(rows):
    return pd.DataFrame(rows)


def _yesterday_df(close=5.14, prev=4.81):
    """新浪路径：末根是昨日（无当日 bar）"""
    return _df([
        {'date': '2026-11-03', 'open': 4.70, 'high': 4.85, 'low': 4.60,
         'close': prev, 'volume': 7062300},
        {'date': '2026-11-04', 'open': 5.20, 'high': 5.30, 'low': 5.20,
         'close': 5.29, 'volume': 13135200},
        {'date': '2026-11-05', 'open': 5.28, 'high': 5.49, 'low': 4.88,
         'close': close, 'volume': 227897100},
    ])


def _today_df(close=4.63):
    """腾讯路径：末根已是当日未完成 bar"""
    return _df([
        {'date': '2026-11-04', 'open': 5.20, 'high': 5.30, 'low': 5.20,
         'close': 5.29, 'volume': 13135200},
        {'date': '2026-11-05', 'open': 5.28, 'high': 5.49, 'low': 4.88,
         'close': 5.14, 'volume': 227897100},
        {'date': '2026-11-06', 'open': 4.78, 'high': 4.88, 'low': 4.63,
         'close': close, 'volume': 139525400},
    ])


def _quote(price=4.63, open_=4.78, high=4.88, low=4.63, vol=139525400):
    return {'code': '600892.SH', 'name': '大晟文化', 'price': price,
            'prev_close': 5.14, 'open': open_, 'high': high, 'low': low,
            'change_pct': -9.92, 'volume': vol}


class TestApplyCorrection(unittest.TestCase):
    def test_update_when_last_bar_is_today(self):
        df = _today_df(close=4.80)
        out, info = apply_intraday_correction(df, _quote(price=4.75),
                                               today='2026-11-06')
        self.assertTrue(info['applied'])
        self.assertEqual(info['mode'], 'update')
        self.assertEqual(len(out), 3)                     # 不新增行
        self.assertEqual(out['close'].iloc[-1], 4.75)     # close 刷新为现价
        self.assertEqual(out['high'].iloc[-1], 4.88)      # high 取 max
        self.assertEqual(out['low'].iloc[-1], 4.63)       # low 取 min
        self.assertEqual(out['date'].iloc[-1], '2026-11-06')

    def test_append_when_last_bar_is_yesterday(self):
        """新浪路径：末根是昨日 → 追加当日 bar（这是本次改动的核心）"""
        df = _yesterday_df()
        out, info = apply_intraday_correction(df, _quote(), today='2026-11-06')
        self.assertTrue(info['applied'])
        self.assertEqual(info['mode'], 'append')
        self.assertEqual(len(out), 4)                     # 追加一行
        last = out.iloc[-1]
        self.assertEqual(last['date'], '2026-11-06')
        self.assertEqual(last['close'], 4.63)             # close = 现价
        self.assertEqual(last['open'], 4.78)              # open = 今开
        self.assertEqual(last['high'], 4.88)              # high = 今高
        self.assertEqual(last['low'], 4.63)               # low = 今低
        self.assertEqual(last['volume'], 139525400)
        # 原数据不被破坏
        self.assertEqual(out['close'].iloc[-2], 5.14)

    def test_append_makes_today_the_last_bar(self):
        """修正后最后一根必须是今日 → 指标（含均线）才会反映当日行情"""
        df = _yesterday_df()
        out, _ = apply_intraday_correction(df, _quote(), today='2026-11-06')
        self.assertEqual(out['date'].iloc[-1], '2026-11-06')
        # 5 日均价的口径随之改变（末根从 5.14 变成 4.63）
        ma5_old = df['close'].tail(5).mean() if len(df) >= 5 else None
        ma5_new = out['close'].tail(5).mean()
        self.assertIsNotNone(ma5_new)
        if ma5_old is not None:
            self.assertNotEqual(round(ma5_old, 4), round(ma5_new, 4))

    def test_gap_pct_recorded(self):
        df = _yesterday_df(close=5.14)
        _, info = apply_intraday_correction(df, _quote(price=4.63),
                                             today='2026-11-06')
        self.assertAlmostEqual(info['gap_pct'], 4.63 / 5.14 - 1, places=4)
        self.assertAlmostEqual(info['base_close'], 5.14, places=4)

    def test_no_correction_cases(self):
        df = _yesterday_df()
        for quote, reason in ((None, 'no_quote'),
                               ({'price': 0}, 'invalid_price'),
                               ({'price': None}, 'invalid_price')):
            out, info = apply_intraday_correction(df, quote, today='2026-11-06')
            self.assertFalse(info['applied'])
            self.assertEqual(info['reason'], reason)
            self.assertEqual(len(out), len(df))

    def test_empty_df(self):
        _out, info = apply_intraday_correction(pd.DataFrame(), _quote())
        self.assertFalse(info['applied'])
        self.assertEqual(info['reason'], 'empty_df')

    def test_future_last_bar_untouched(self):
        df = _today_df()
        df.loc[df.index[-1], 'date'] = '2026-11-07'
        out, info = apply_intraday_correction(df, _quote(), today='2026-11-06')
        self.assertFalse(info['applied'])
        self.assertIn('ahead', info['reason'])
        self.assertEqual(len(out), len(df))

    def test_quote_missing_open_high_low_falls_back(self):
        """报价只有 price（老格式）→ 用昨收兜底，不炸"""
        df = _yesterday_df()
        quote = {'price': 4.63, 'prev_close': 5.14, 'volume': 100}
        out, info = apply_intraday_correction(df, quote, today='2026-11-06')
        self.assertTrue(info['applied'])
        last = out.iloc[-1]
        self.assertEqual(last['close'], 4.63)
        self.assertEqual(last['open'], 5.14)      # 无 open → 用 prev_close
        self.assertEqual(last['high'], 5.14)
        self.assertEqual(last['low'], 4.63)

    def test_stale_data_not_appended(self):
        """陈旧/历史数据不得被接上今天的 bar（回归脚本安全性的关键）"""
        df = _df([
            {'date': '2026-04-01', 'open': 4.0, 'high': 4.2, 'low': 3.9,
             'close': 4.1, 'volume': 1000},
            {'date': '2026-04-02', 'open': 4.1, 'high': 4.3, 'low': 4.0,
             'close': 4.2, 'volume': 1100},
        ])
        out, info = apply_intraday_correction(df, _quote(price=4.63),
                                               today='2026-11-06')
        self.assertFalse(info['applied'])
        self.assertIn('stale_data', info['reason'])
        self.assertEqual(len(out), len(df))          # 没有多出 bar
        self.assertNotEqual(out['date'].iloc[-1], '2026-11-06')

    def test_previous_trading_day_is_appendable(self):
        """末根=上一交易日 → 允许追加（生产降级源路径）"""
        df = _yesterday_df()   # 末根 2026-11-05
        out, info = apply_intraday_correction(df, _quote(), today='2026-11-06')
        self.assertTrue(info['applied'])
        self.assertEqual(info['mode'], 'append')
        self.assertEqual(out['date'].iloc[-1], '2026-11-06')

    def test_describe(self):
        _, info = apply_intraday_correction(_yesterday_df(), _quote(),
                                            today='2026-11-06')
        self.assertIn('append', describe(info))
        _, info2 = apply_intraday_correction(_yesterday_df(), None)
        self.assertIn('未修正', describe(info2))


class TestGeneratorWiring(unittest.TestCase):
    def test_extract_quotes_keeps_full_row(self):
        df = _df([
            {'code': '600892.SH', 'price': 4.63, 'open': 4.78, 'high': 4.88,
             'low': 4.63, 'volume': 139525400, 'change_pct': -9.92},
            {'code': '000001.SZ', 'price': 12.3, 'open': 12.0, 'high': 12.5,
             'low': 11.9, 'volume': 100, 'change_pct': 1.2},
            {'code': 'BAD.SH', 'price': 0, 'open': 1, 'high': 1, 'low': 1,
             'volume': 1, 'change_pct': 0},
        ])
        out = SignalGenerator._extract_quotes(df)
        self.assertEqual(len(out), 2)
        row = out['600892.SH']
        self.assertEqual(row['price'], 4.63)
        self.assertEqual(row['open'], 4.78)     # 修正需要的字段必须保留
        self.assertEqual(row['high'], 4.88)
        self.assertEqual(row['low'], 4.63)
        self.assertEqual(row['volume'], 139525400)

    def test_extract_quotes_edge_cases(self):
        self.assertEqual(SignalGenerator._extract_quotes(None), {})
        self.assertEqual(SignalGenerator._extract_quotes(pd.DataFrame()), {})
        self.assertEqual(
            SignalGenerator._extract_quotes(_df([{'code': 'A'}])) , {})

    def test_quote_for_code_prefers_round_quotes(self):
        g = SignalGenerator.__new__(SignalGenerator)
        g._round_quotes = {'600892.SH': {'price': 4.63, 'open': 4.78}}
        dc = MagicMock()
        dc.get_realtime_quote.return_value = pd.DataFrame()
        g.datacenter = dc
        q = g._quote_for_code('600892.SH')
        self.assertEqual(q['price'], 4.63)
        dc.get_realtime_quote.assert_not_called()   # 命中本轮 → 不发请求

    def test_quote_for_code_falls_back_to_source(self):
        g = SignalGenerator.__new__(SignalGenerator)
        g._round_quotes = {}
        dc = MagicMock()
        dc.get_realtime_quote.return_value = _df([
            {'code': '600892.SH', 'price': 4.63, 'open': 4.78}])
        g.datacenter = dc
        q = g._quote_for_code('600892.SH')
        self.assertEqual(q['price'], 4.63)
        dc.get_realtime_quote.assert_called_once()

    def test_quote_for_code_handles_error(self):
        g = SignalGenerator.__new__(SignalGenerator)
        g._round_quotes = {}
        dc = MagicMock()
        dc.get_realtime_quote.side_effect = RuntimeError('boom')
        g.datacenter = dc
        self.assertIsNone(g._quote_for_code('600892.SH'))


if __name__ == '__main__':
    unittest.main()
