"""D73 eltdx / AxData 只读适配层测试

纪律：只验证「默认关闭 + 优雅降级 + 代码/周期转换 + 列对齐」，
不触碰任何信号/评分/门控/Kelly/回测口径。注入假客户端，无需安装 eltdx。
"""
from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from quant_engine.config import Settings
from quant_engine.data.sources.eltdx_client import (
    EltdxClient,
    map_period,
    to_eltdx_code,
)

_TZ = timezone(timedelta(hours=8))


def _bar(date_str, o, h, l, c, lots, amount):
    return SimpleNamespace(
        time=datetime.strptime(date_str, '%Y-%m-%d %H:%M').replace(tzinfo=_TZ),
        open=o, high=h, low=l, close=c, volume_lots=lots, amount=amount,
    )


class FakeBars:
    def __init__(self, bars):
        self.bars = tuple(bars)

    def get(self, code, *, period, count, adjust):
        self.last_call = (code, period, count, adjust)
        return self if False else FakeBars([
            _bar('2026-11-20 15:00', 11.99, 12.25, 11.91, 12.22, 2134988, 2581567488.0),
            _bar('2026-11-19 15:00', 12.20, 12.26, 11.95, 12.04, 1800000, 2160000000.0),
        ])


class FakeClient:
    def __init__(self, hosts=None):
        self.hosts = hosts
        self.bars = FakeBars([])


class TestCodeAndPeriodConversion(unittest.TestCase):
    def test_code_with_suffix(self):
        self.assertEqual(to_eltdx_code('600519.SH'), ('sh600519', 'SH'))
        self.assertEqual(to_eltdx_code('000001.SZ'), ('sz000001', 'SZ'))

    def test_code_bare(self):
        self.assertEqual(to_eltdx_code('600519'), ('sh600519', 'SH'))
        self.assertEqual(to_eltdx_code('000001'), ('sz000001', 'SZ'))

    def test_code_bj(self):
        self.assertEqual(to_eltdx_code('430047.BJ'), ('bj430047', 'BJ'))

    def test_code_invalid(self):
        for bad in ['', 'abc', '60051', '000001.US']:
            self.assertIsNone(to_eltdx_code(bad))

    def test_period_map(self):
        self.assertEqual(map_period('5'), '5m')
        self.assertEqual(map_period('1'), '1m')
        self.assertEqual(map_period('DAY'), 'day')
        self.assertIsNone(map_period('min5'))


class TestDefaultOffAndDegrade(unittest.TestCase):
    def test_disabled_returns_empty_no_request(self):
        """默认关闭时零请求：factory 一旦被调用即失败。"""
        def boom(_hosts):
            raise AssertionError('关闭时不应初始化客户端')

        cfg = Settings(ELTDX_ENABLED=False)
        c = EltdxClient(settings=cfg, client_factory=boom)
        self.assertTrue(c.get_minute_data('000001.SZ', '5').empty)
        self.assertEqual(c.probe(), [])

    def test_factory_error_degrades(self):
        def boom(_hosts):
            raise RuntimeError('network down')

        cfg = Settings(ELTDX_ENABLED=True)
        c = EltdxClient(settings=cfg, client_factory=boom)
        self.assertTrue(c.get_minute_data('000001.SZ', '5').empty)
        self.assertEqual(c.probe(), [])

    def test_invalid_code_or_period_no_request(self):
        cfg = Settings(ELTDX_ENABLED=True)
        c = EltdxClient(settings=cfg, client_factory=lambda h: FakeClient(h))
        self.assertTrue(c.get_minute_data('bad-code', '5').empty)
        self.assertTrue(c.get_minute_data('000001.SZ', 'min5').empty)


class TestOutputContract(unittest.TestCase):
    def test_bars_to_df_columns_and_units(self):
        """输出列严格对齐，volume=手×100=股，按日期升序。"""
        cfg = Settings(ELTDX_ENABLED=True)
        c = EltdxClient(settings=cfg, client_factory=lambda h: FakeClient(h))
        df = c.get_minute_data('000001.SZ', '5', count=2)
        self.assertEqual(list(df.columns),
                         ['date', 'open', 'high', 'low', 'close', 'volume', 'amount'])
        self.assertEqual(len(df), 2)
        self.assertEqual(df.iloc[0]['date'], '2026-11-19 15:00')  # 升序
        self.assertEqual(df.iloc[1]['close'], 12.22)
        self.assertEqual(df.iloc[1]['volume'], 2134988 * 100)

    def test_period_forwarded_to_client(self):
        """周期正确转发给 eltdx（'1' → 1m，验证 1 分钟新增能力路径）。"""
        cfg = Settings(ELTDX_ENABLED=True)
        fake = FakeClient()
        c = EltdxClient(settings=cfg, client_factory=lambda h: fake)
        c.get_minute_data('600519.SH', '1', count=10)
        code, period, count, adjust = fake.bars.last_call
        self.assertEqual((code, period, adjust), ('sh600519', '1m', None))
        self.assertEqual(count, 10)

    def test_empty_bars_returns_empty_df(self):
        class EmptyBars(FakeBars):
            def get(self, *a, **k):
                return FakeBars([])

        class C(FakeClient):
            def __init__(self, hosts=None):
                super().__init__(hosts)
                self.bars = EmptyBars([])

        cfg = Settings(ELTDX_ENABLED=True)
        c = EltdxClient(settings=cfg, client_factory=lambda h: C(h))
        df = c.get_minute_data('000001.SZ', '5')
        self.assertTrue(df.empty)
        self.assertEqual(list(df.columns),
                         ['date', 'open', 'high', 'low', 'close', 'volume', 'amount'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
