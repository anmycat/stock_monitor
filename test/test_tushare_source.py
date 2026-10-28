"""B7 TushareSource 适配器测试（mock tushare 模块，不依赖真实安装）"""

import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd


class _FakeProApi:
    """模拟 tushare pro_api 的调用面"""

    def __init__(self, token):
        self.token = token
        self.calls = []

    def daily(self, ts_code, start_date, end_date):
        self.calls.append(('daily', ts_code, start_date, end_date))
        return pd.DataFrame({
            'ts_code': [ts_code, ts_code],
            'trade_date': ['20261027', '20261026'],
            'open': [10.0, 9.8],
            'high': [10.5, 10.1],
            'low': [9.9, 9.7],
            'close': [10.3, 10.0],
            'vol': [1000000, 900000],
            'amount': [10300000.0, 9000000.0],
        })

    def stock_basic(self, exchange, list_status, fields):
        self.calls.append(('stock_basic', exchange, list_status, fields))
        return pd.DataFrame({
            'ts_code': ['000001.SZ', '600000.SH'],
            'name': ['平安银行', '浦发银行'],
            'industry': ['银行', '银行'],
        })


def _install_fake_tushare():
    fake = types.ModuleType('tushare')
    holder = {}

    def pro_api(token):
        holder['api'] = _FakeProApi(token)
        return holder['api']

    fake.pro_api = pro_api
    fake._holder = holder  # type: ignore[attr-defined]
    return fake, holder


class TestTushareSource(unittest.TestCase):

    def setUp(self):
        self.fake_mod, self.holder = _install_fake_tushare()
        self._patcher = patch.dict(sys.modules, {'tushare': self.fake_mod})
        self._patcher.start()

    def tearDown(self):
        self._patcher.stop()

    def _patch_settings(self, token='fake-token'):
        from quant_engine.config import get_settings
        settings = get_settings()
        return patch.object(settings, 'TUSHARE_TOKEN', token, create=True)

    def test_daily_data_conversion(self):
        from quant_engine.data.sources.tushare import TushareSource
        with self._patch_settings():
            df = TushareSource.get_daily_data('000001.SZ', days=5)
        self.assertIsNotNone(df)
        self.assertEqual(list(df.columns),
                         ['date', 'open', 'high', 'low', 'close', 'volume', 'amount'])
        # 日期转内部格式（YYYYMMDD -> YYYY-MM-DD）+ 统一正序（最新在最后）+ days 截断
        self.assertEqual(df.iloc[-1]['date'], '2026-10-27')
        self.assertEqual(df.iloc[-1]['close'], 10.3)
        self.assertEqual(df.iloc[-1]['volume'], 1000000)
        self.assertEqual(len(df), 2)
        self.assertEqual(self.holder['api'].calls[0][0], 'daily')
        self.assertEqual(self.holder['api'].token, 'fake-token')

    def test_stock_list_conversion(self):
        from quant_engine.data.sources.tushare import TushareSource
        with self._patch_settings():
            df = TushareSource.get_stock_list()
        self.assertIsNotNone(df)
        self.assertEqual(list(df.columns), ['code', 'name', 'industry'])
        self.assertEqual(df.iloc[0]['code'], '000001.SZ')
        self.assertEqual(df.iloc[1]['name'], '浦发银行')
        self.assertEqual(self.holder['api'].calls[0][0], 'stock_basic')
        self.assertEqual(self.holder['api'].calls[0][2], 'L')

    def test_realtime_quote_not_supported(self):
        from quant_engine.data.sources.tushare import TushareSource
        with self._patch_settings():
            result = TushareSource.get_realtime_quote(['000001.SZ'])
        self.assertIsNone(result)

    def test_no_token_returns_none(self):
        """token 未配置：所有方法返 None（不抛异常）"""
        from quant_engine.data.sources.tushare import TushareSource
        with self._patch_settings(token=''):
            self.assertIsNone(TushareSource.get_daily_data('000001.SZ'))
            self.assertIsNone(TushareSource.get_stock_list())
            self.assertIsNone(TushareSource.get_realtime_quote(['000001.SZ']))

    def test_missing_package_returns_none(self):
        """tushare 未安装：返 None（降级链跳过）"""
        from quant_engine.data.sources.tushare import TushareSource
        with patch.dict(sys.modules, {'tushare': None}), self._patch_settings():
            self.assertIsNone(TushareSource.get_daily_data('000001.SZ'))
            self.assertIsNone(TushareSource.get_stock_list())

    def test_api_error_returns_none(self):
        """接口抛异常：warning + 返 None"""
        from quant_engine.data.sources.tushare import TushareSource

        class _Boom(_FakeProApi):
            def daily(self, *a, **kw):
                raise RuntimeError('quota exceeded')

        boom = _Boom('fake-token')
        # 预置 pro_api 返回 Boom 实例（避免 _get_api 重建标准 mock 覆盖）
        self.fake_mod.pro_api = lambda token: boom
        with self._patch_settings():
            self.assertIsNone(TushareSource.get_daily_data('000001.SZ'))

    def test_composite_source_registers_tushare(self):
        """CompositeSource 注册 tushare，且 daily 优先级排最后（兜底）"""
        from quant_engine.data.source import _PRIORITY, CompositeSource
        self.assertIn('tushare', _PRIORITY['daily'])
        self.assertEqual(_PRIORITY['daily'][-1], 'tushare')
        self.assertIn('tushare', _PRIORITY['stock_list'])
        # 初始化不炸（tushare 未真实安装）
        cs = CompositeSource()
        self.assertIn('tushare', cs.sources)


if __name__ == '__main__':
    unittest.main(verbosity=2)
