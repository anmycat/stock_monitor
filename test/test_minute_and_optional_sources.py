"""新增数据源与分钟线替代测试（D53 遗留修复 #3）

覆盖：
1. 新浪分钟 K（getKLineData scale=5/15/30/60）——TDX 公共节点停供行情后的替代
2. AKShare / 巨潮 / 问财 三源的可选依赖与凭据缺失降级（不抛异常、返回空）
3. registry 能力声明与 minute 优先级（sina → tdx）
"""
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.data.registry import SOURCES, sources_for
from quant_engine.data.source import _PRIORITY
from quant_engine.data.sources.akshare_source import AkshareSource
from quant_engine.data.sources.cninfo_source import CninfoSource
from quant_engine.data.sources.iwencai_source import IwencaiSource
from quant_engine.data.sources.sina import SinaSource
from quant_engine.data.spec import Capability

# 新浪 getKLineData 实测响应（截断 2 根）
SINA_KLINE = [
    {"day": "2026-11-03 10:50:00", "open": "1444.900", "high": "1449.950",
     "low": "1444.000", "close": "1449.950", "volume": "64100",
     "amount": "92716994.0313"},
    {"day": "2026-11-03 10:55:00", "open": "1449.950", "high": "1452.000",
     "low": "1448.100", "close": "1451.000", "volume": "52800",
     "amount": "76541280.0000"},
]


class TestSinaMinute(unittest.TestCase):
    """新浪分钟 K（TDX 替代）"""

    @patch('quant_engine.data.sources.sina.requests.get')
    def test_minute_parsing(self, mock_get):
        resp = MagicMock(status_code=200)
        resp.text = json.dumps(SINA_KLINE)
        mock_get.return_value = resp

        df = SinaSource.get_minute_data('600519.SH', period='5')
        self.assertEqual(len(df), 2)
        row = df.iloc[-1]
        self.assertEqual(row['date'], '2026-11-03 10:55:00')
        self.assertAlmostEqual(row['open'], 1449.95)
        self.assertAlmostEqual(row['high'], 1452.0)
        self.assertAlmostEqual(row['low'], 1448.1)
        self.assertAlmostEqual(row['close'], 1451.0)
        self.assertAlmostEqual(row['volume'], 52800.0)
        self.assertAlmostEqual(row['amount'], 76541280.0)

    @patch('quant_engine.data.sources.sina.requests.get')
    def test_minute_requests_correct_scale(self, mock_get):
        resp = MagicMock(status_code=200)
        resp.text = json.dumps(SINA_KLINE)
        mock_get.return_value = resp
        SinaSource.get_minute_data('600519.SH', period='30')
        params = mock_get.call_args.kwargs['params']
        self.assertEqual(params['scale'], 30)
        self.assertEqual(params['symbol'], 'sh600519')

    @patch('quant_engine.data.sources.sina.requests.get')
    def test_minute_unsupported_period_returns_empty(self, mock_get):
        df = SinaSource.get_minute_data('600519.SH', period='1')
        self.assertEqual(len(df), 0)
        mock_get.assert_not_called()

    @patch('quant_engine.data.sources.sina.requests.get')
    def test_minute_http_error_returns_empty(self, mock_get):
        mock_get.return_value = MagicMock(status_code=503)
        df = SinaSource.get_minute_data('600519.SH', period='5')
        self.assertEqual(len(df), 0)

    def test_minute_columns_align_with_tdx(self):
        """列名与 TdxSource.get_minute_data 对齐（下游 gates 分钟断链检查依赖）"""
        import inspect
        src = inspect.getsource(SinaSource.get_minute_data)
        for col in ('date', 'open', 'high', 'low', 'close', 'volume', 'amount'):
            self.assertIn(f"'{col}'", src)


class TestOptionalSourcesDegrade(unittest.TestCase):
    """可选依赖 / 凭据缺失 → 明确降级，不抛异常"""

    def test_akshare_without_install_returns_empty(self):
        """未装 akshare → 空 DataFrame（不抛异常）"""
        if AkshareSource.available():
            self.skipTest('本地已装 akshare')
        self.assertEqual(len(AkshareSource.get_limit_up_pool()), 0)
        self.assertEqual(len(AkshareSource.get_all_stocks()), 0)

    def test_akshare_call_failure_returns_none(self):
        """akshare 接口异常 → None（不抛异常）"""
        with patch.object(AkshareSource, '_call', side_effect=lambda *a, **k: None):
            self.assertEqual(len(AkshareSource.get_limit_up_pool()), 0)

    def test_cninfo_invalid_code_returns_empty(self):
        df = CninfoSource.get_announcements('不是代码')
        self.assertEqual(len(df), 0)

    def test_cninfo_missing_org_id_returns_empty(self):
        """取不到 orgId → 空（不抛异常）"""
        with patch.object(CninfoSource, 'get_org_id', return_value=''):
            df = CninfoSource.get_announcements('600519')
        self.assertEqual(len(df), 0)

    def test_cninfo_announcement_parsing(self):
        """公告解析（mock HTTP）"""
        payload = {'announcements': [{
            'announcementId': '123', 'secCode': '600519', 'secName': '贵州茅台',
            'announcementTitle': '关于回购公司股份的进展公告',
            'announcementTime': 1767225600000,
            'adjunctUrl': 'finalpage/2026-01-01/123.PDF',
        }]}
        with patch.object(CninfoSource, 'get_org_id', return_value='9900019512'), \
                patch('quant_engine.data.sources.cninfo_source._SESSION.post') as mock_post:
            resp = MagicMock(status_code=200)
            resp.json.return_value = payload
            mock_post.return_value = resp
            df = CninfoSource.get_announcements('600519.SH')
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['title'], '关于回购公司股份的进展公告')
        self.assertTrue(df.iloc[0]['pdf_url'].endswith('123.PDF'))

    def test_iwencai_missing_credentials_degrades(self):
        """缺 HEXIN_V / IWENCAI_COOKIE → 空 DataFrame + 一次性告警"""
        import os
        old_v, old_c = os.environ.pop('HEXIN_V', None), os.environ.pop('IWENCAI_COOKIE', None)
        try:
            self.assertFalse(IwencaiSource.available())
            df = IwencaiSource.select('今日涨停')
            self.assertEqual(len(df), 0)
            self.assertEqual(len(IwencaiSource.select_codes('今日涨停')), 0)
        finally:
            if old_v:
                os.environ['HEXIN_V'] = old_v
            if old_c:
                os.environ['IWENCAI_COOKIE'] = old_c

    def test_iwencai_select_codes_normalizes_suffix(self):
        """问财返回裸代码 → 归一化为标准代码"""
        df = pd.DataFrame([{'code': '600519'}, {'code': '000001'}, {'code': '920819'}])
        with patch.object(IwencaiSource, 'select', return_value=df):
            codes = IwencaiSource.select_codes('x')
        self.assertEqual(codes, ['600519.SH', '000001.SZ', '920819.BJ'])


import pandas as pd  # noqa: E402  （供上面的 fixture 使用）


class TestRegistryUpdates(unittest.TestCase):
    def test_sina_has_minute_capability(self):
        spec = SOURCES['sina']
        self.assertTrue(spec.supports(Capability.MINUTE))
        self.assertEqual(spec.rank(Capability.MINUTE), 0)

    def test_minute_priority_sina_first(self):
        self.assertEqual(_PRIORITY['minute'], ['sina', 'tdx'])

    def test_new_sources_registered(self):
        for name in ('akshare', 'cninfo', 'iwencai'):
            self.assertIn(name, SOURCES)
        self.assertTrue(SOURCES['akshare'].supports(Capability.POOL))
        self.assertTrue(SOURCES['cninfo'].supports(Capability.FUNDAMENTAL))
        self.assertTrue(SOURCES['iwencai'].supports(Capability.LIST))

    def test_iwencai_declares_key_requirement(self):
        self.assertTrue(SOURCES['iwencai'].needs_key)
        self.assertEqual(SOURCES['iwencai'].key_env, 'HEXIN_V')

    def test_pool_sources_ordered(self):
        names = [s.name for s in sources_for(Capability.POOL)]
        self.assertEqual(names[0], 'em_pool')
        self.assertIn('akshare', names)


if __name__ == '__main__':
    unittest.main()
