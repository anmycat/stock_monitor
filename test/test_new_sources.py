"""新增数据源解析测试（D53：同花顺网页端 + 东财 push2ex 盘面池）

用真实 API 响应 fixture 验证解析逻辑（不联网）：
- 同花顺 JSONP 包裹剥离 + 日K 12 列解析 + 日期格式化 + volume 单位（股）
- 市场前缀映射（hs_/sz_/bj_）
- 东财 push2ex pool 元素解析（价格 ×1000 还原、连板数、封板资金、炸板次数、行业）
"""
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.data.sources.em_pool import EastMoneyPoolSource
from quant_engine.data.sources.ths_web import ThsWebSource

# 同花顺日K JSONP（实测截断，保留 2 根）
THS_LINE_JSONP = (
    'quotebridge_v6_line_hs_600519_01_last({"year":{"2026":198},"total":"6032",'
    '"num":140,"rt":"0930-1130,1300-1500","start":"20010827",'
    '"name":"\\u8d35\\u5dde\\u8305\\u53f0",'
    '"data":"20261029,1349.99,1356.00,1330.01,1354.00,4299231,'
    '5797980300.00,0.344,,,1200.00,1624800;'
    '20261030,1363.00,1393.37,1352.98,1392.05,5992729,'
    '8257381800.00,0.479,,,1800.00,2505690"});'
)

# 同花顺快照 JSONP（实测截断）
THS_HEAD_JSONP = (
    'quotebridge_v2_realhead_hs_600519_last({"items":{'
    '"5":"600519","6":"1354.00","7":"1363.00","8":"1393.37","9":"1352.98",'
    '"10":"1392.05","13":"5992729.00","19":"8257381800.00",'
    '"69":"1489.40","70":"1218.60"}})'
)

# 东财 push2ex 涨停池（实测截断）
EM_POOL = {
    'rc': 0, 'data': {'tc': 2, 'qdate': 20261030, 'pool': [
        {'c': '603703', 'm': 1, 'n': '盛洋科技', 'p': 11950,
         'zdp': 10.0368, 'amount': 91126637, 'ltsz': 4959036334.0,
         'hs': 1.8375, 'lbc': 2, 'fbt': 92500, 'lbt': 92500,
         'fund': 242852202, 'zbc': 0, 'hybk': '光学光电',
         'zttj': {'days': 2, 'ct': 2}},
        {'c': '300750', 'm': 0, 'n': '宁德时代', 'p': 250000,
         'zdp': 9.9, 'amount': 12345678, 'hs': 1.2, 'lbc': 1,
         'fbt': 93000, 'lbt': 93000, 'fund': 1000, 'zbc': 2,
         'hybk': '电池'},
    ]},
}


class _FakeResp:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code


class TestThsParsing(unittest.TestCase):
    def setUp(self):
        self._orig = ThsWebSource._get_jsonp
        ThsWebSource._get_jsonp = staticmethod(lambda url: json.loads(
            THS_LINE_JSONP[THS_LINE_JSONP.index('(') + 1:THS_LINE_JSONP.rindex(')')]))

    def tearDown(self):
        ThsWebSource._get_jsonp = self._orig

    def test_daily_parsing_columns(self):
        df = ThsWebSource.get_daily_data('600519.SH', days=10)
        self.assertEqual(len(df), 2)
        row = df.iloc[-1]
        self.assertEqual(row['date'], '2026-10-30')
        self.assertAlmostEqual(row['open'], 1363.00)
        self.assertAlmostEqual(row['high'], 1393.37)
        self.assertAlmostEqual(row['low'], 1352.98)
        self.assertAlmostEqual(row['close'], 1392.05)
        # volume 单位是「股」（腾讯同日为 59,927 手 × 100）
        self.assertAlmostEqual(row['volume'], 5992729.0)
        self.assertAlmostEqual(row['amount'], 8257381800.0)
        self.assertAlmostEqual(row['turnover_rate'], 0.479)

    def test_daily_sorted_ascending(self):
        df = ThsWebSource.get_daily_data('600519.SH')
        self.assertTrue(df['date'].is_monotonic_increasing)

    def test_days_truncation(self):
        df = ThsWebSource.get_daily_data('600519.SH', days=1)
        self.assertEqual(len(df), 1)

    def test_market_prefix(self):
        self.assertEqual(ThsWebSource._market_prefix('600519.SH'), 'hs_')
        self.assertEqual(ThsWebSource._market_prefix('000001.SZ'), 'sz_')
        self.assertEqual(ThsWebSource._market_prefix('300750.SZ'), 'sz_')
        self.assertEqual(ThsWebSource._market_prefix('920819.BJ'), 'bj_')
        self.assertEqual(ThsWebSource._market_prefix('830799.BJ'), 'bj_')

    def test_market_suffix(self):
        self.assertEqual(ThsWebSource._market_suffix('600519.SH'), 'SH')
        self.assertEqual(ThsWebSource._market_suffix('000001.SZ'), 'SZ')
        self.assertEqual(ThsWebSource._market_suffix('920819.BJ'), 'BJ')

    def test_empty_on_failure(self):
        ThsWebSource._get_jsonp = staticmethod(lambda url: None)
        df = ThsWebSource.get_daily_data('600519.SH')
        self.assertEqual(len(df), 0)

    def test_stock_list_empty(self):
        """同花顺网页端无全市场列表接口"""
        self.assertEqual(len(ThsWebSource.get_stock_list()), 0)


class TestThsSnapshot(unittest.TestCase):
    def setUp(self):
        self._orig = ThsWebSource._get_jsonp
        ThsWebSource._get_jsonp = staticmethod(lambda url: json.loads(
            THS_HEAD_JSONP[THS_HEAD_JSONP.index('(') + 1:THS_HEAD_JSONP.rindex(')')]))

    def tearDown(self):
        ThsWebSource._get_jsonp = self._orig

    def test_snapshot_parsing(self):
        df = ThsWebSource.get_realtime_quote(['600519.SH'])
        self.assertEqual(len(df), 1)
        row = df.iloc[0]
        self.assertEqual(row['code'], '600519.SH')
        self.assertAlmostEqual(row['price'], 1392.05)
        self.assertAlmostEqual(row['prev_close'], 1363.00)
        self.assertAlmostEqual(row['volume'], 5992729.0)
        self.assertAlmostEqual(row['change_pct'], round((1392.05 - 1363.0) / 1363.0 * 100, 4))


class TestEmPoolParsing(unittest.TestCase):
    def setUp(self):
        self._orig = EastMoneyPoolSource._fetch_pool
        EastMoneyPoolSource._fetch_pool = classmethod(
            lambda cls, endpoint, date_str, page_size=200: EM_POOL['data']['pool'])

    def tearDown(self):
        EastMoneyPoolSource._fetch_pool = self._orig

    def test_limit_up_pool_parsing(self):
        df = EastMoneyPoolSource.get_limit_up_pool('20261030')
        self.assertEqual(len(df), 2)
        row = df.iloc[0]
        self.assertEqual(row['code'], '603703.SH')
        self.assertEqual(row['name'], '盛洋科技')
        self.assertAlmostEqual(row['price'], 11.95)      # p=11950 → /1000
        self.assertAlmostEqual(row['change_pct'], 10.0368, places=3)
        self.assertEqual(row['limit_up_times'], 2)
        self.assertEqual(row['first_seal_time'], 92500)
        self.assertAlmostEqual(row['seal_fund'], 242852202.0)
        self.assertEqual(row['broken_times'], 0)
        self.assertEqual(row['industry'], '光学光电')

    def test_deep_market_suffix(self):
        df = EastMoneyPoolSource.get_limit_up_pool('20261030')
        self.assertEqual(df.iloc[1]['code'], '300750.SZ')

    def test_broken_and_down_pool_same_parser(self):
        self.assertEqual(len(EastMoneyPoolSource.get_broken_board_pool('20261030')), 2)
        self.assertEqual(len(EastMoneyPoolSource.get_limit_down_pool('20261030')), 2)


if __name__ == '__main__':
    unittest.main()
