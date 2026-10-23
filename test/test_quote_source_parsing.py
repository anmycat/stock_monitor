"""行情数据源代码解析回归测试

用真实 API 响应 fixture 验证三个源的代码解析修复：
- 腾讯：行首 v_sh/sz/bj 前缀取市场（旧 bug：startswith('6') 把
  510300 ETF 错判 .SZ）；GBK 中文名解码
- 新浪：bj 前缀映射 BJ（旧 bug：else 一律判 SZ）；GBK 解码
- 东财：f13+前缀组合规则（旧 bug：920819 北交所错判 .SH）；
  secid 支持 BJ
"""
import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from quant_engine.data.sources.eastmoney import EastMoneySource
from quant_engine.data.sources.sina import SinaSource
from quant_engine.data.sources.tencent import TencentSource

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures')


def _load_bytes(name):
    with open(os.path.join(FIXTURES, name), 'rb') as f:
        return f.read()


def _mock_response(content_bytes, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.content = content_bytes
    # requests .text 按 encoding 解码；源码中先设置 response.encoding
    resp.text = content_bytes.decode('gbk', errors='replace')
    return resp


class TestTencentParsing(unittest.TestCase):
    """腾讯源：行首市场前缀 + GBK 名称"""

    @patch('quant_engine.data.sources.tencent.requests.get')
    def test_market_from_line_prefix(self, mock_get):
        """ETF(510300)/北交所(920819)/科创(688981) 市场全部正确"""
        mock_get.return_value = _mock_response(_load_bytes('quote_tencent_raw.txt'))
        df = TencentSource.get_realtime_quote(
            ['510300.SH', '000001.SZ', '920819.BJ', '688981.SH',
             '300750.SZ', '600000.SH', '518880.SH', '588000.SH'])
        codes = set(df['code'])
        self.assertEqual(codes, {
            '510300.SH', '000001.SZ', '920819.BJ', '688981.SH',
            '300750.SZ', '600000.SH', '518880.SH', '588000.SH'})
        # 事故回归：ETF 与北交所不得错判
        self.assertNotIn('510300.SZ', codes)
        self.assertNotIn('920819.SH', codes)
        self.assertNotIn('920819.SZ', codes)

    @patch('quant_engine.data.sources.tencent.requests.get')
    def test_chinese_name_decoded(self, mock_get):
        """GBK 中文名称正常解码（非乱码）"""
        mock_get.return_value = _mock_response(_load_bytes('quote_tencent_raw.txt'))
        df = TencentSource.get_realtime_quote(['510300.SH', '000001.SZ'])
        names = dict(zip(df['code'], df['name']))
        self.assertEqual(names['510300.SH'], '沪深300ETF华泰柏瑞')
        self.assertEqual(names['000001.SZ'], '平安银行')

    @patch('quant_engine.data.sources.tencent.requests.get')
    def test_price_and_change(self, mock_get):
        mock_get.return_value = _mock_response(_load_bytes('quote_tencent_raw.txt'))
        df = TencentSource.get_realtime_quote(['510300.SH'])
        row = df.iloc[0]
        self.assertAlmostEqual(row['price'], 4.236, places=3)
        self.assertAlmostEqual(row['change_pct'], -0.14, places=2)

    def test_convert_code(self):
        self.assertEqual(TencentSource._convert_code('510300.SH'), 'sh510300')
        self.assertEqual(TencentSource._convert_code('920819.BJ'), 'bj920819')
        self.assertEqual(TencentSource._convert_code('000001.SZ'), 'sz000001')


class TestSinaParsing(unittest.TestCase):
    """新浪源：bj 前缀映射 BJ + GBK 解码"""

    @patch('quant_engine.data.sources.sina.requests.get')
    def test_bj_prefix_mapped(self, mock_get):
        """北交所 bj920819 → 920819.BJ（旧 bug：错判 .SZ）"""
        mock_get.return_value = _mock_response(_load_bytes('quote_sina_raw.txt'))
        df = SinaSource.get_realtime_quote(['510300.SH', '000001.SZ', '920819.BJ'])
        codes = set(df['code'])
        self.assertEqual(codes, {'510300.SH', '000001.SZ', '920819.BJ'})
        self.assertNotIn('920819.SZ', codes)

    @patch('quant_engine.data.sources.sina.requests.get')
    def test_chinese_name_decoded(self, mock_get):
        mock_get.return_value = _mock_response(_load_bytes('quote_sina_raw.txt'))
        df = SinaSource.get_realtime_quote(['510300.SH', '920819.BJ'])
        names = dict(zip(df['code'], df['name']))
        self.assertEqual(names['510300.SH'], '沪深300ETF华泰柏瑞')
        self.assertEqual(names['920819.BJ'], '颖泰生物')

    def test_convert_code(self):
        self.assertEqual(SinaSource._convert_code('920819.BJ'), 'bj920819')
        self.assertEqual(SinaSource._convert_code('510300.SH'), 'sh510300')


class TestEastMoneyParsing(unittest.TestCase):
    """东财源：f13+前缀组合规则 + secid 适配"""

    @patch('quant_engine.data.sources.eastmoney.requests.get')
    def test_ulist_market_from_f13_and_prefix(self, mock_get):
        """510300(ETF/f13=1)→SH；920819(北交所/f13=0)→BJ"""
        with open(os.path.join(FIXTURES, 'quote_em_ulist.json'),
                  encoding='utf-8') as f:
            payload = f.read()
        resp = MagicMock(status_code=200)
        resp.json.return_value = json.loads(payload)
        mock_get.return_value = resp

        df = EastMoneySource.get_realtime_quote(
            ['510300.SH', '000001.SZ', '920819.BJ', '688981.SH',
             '300750.SZ', '600000.SH'])
        codes = set(df['code'])
        self.assertEqual(codes, {
            '510300.SH', '000001.SZ', '920819.BJ', '688981.SH',
            '300750.SZ', '600000.SH'})
        self.assertNotIn('920819.SH', codes)  # 旧 bug 回归
        self.assertNotIn('510300.SZ', codes)

    def test_secid_conversion(self):
        """to_secid：沪市 1. / 深北 0.（经 requests URL 参数验证）"""
        with patch('quant_engine.data.sources.eastmoney.requests.get') as mock_get:
            resp = MagicMock(status_code=200)
            resp.json.return_value = {'data': None}
            mock_get.return_value = resp
            EastMoneySource.get_realtime_quote(['510300.SH', '920819.BJ'])
            _, kwargs = mock_get.call_args
            secids = kwargs['params']['secids']
            self.assertEqual(secids, '1.510300,0.920819')

    def test_market_from_em_helper(self):
        """_market_from_em 组合规则单测"""
        f = EastMoneySource._market_from_em
        self.assertEqual(f('600000', 1), 'SH')
        self.assertEqual(f('688981', 1), 'SH')
        self.assertEqual(f('000001', 0), 'SZ')
        self.assertEqual(f('920819', 0), 'BJ')   # f13=0 但北交所
        self.assertEqual(f('830799', 0), 'BJ')
        self.assertEqual(f('510300', None), 'SH')  # f13 缺失兜底
        self.assertIsNone(f('abc', 1))

    @patch('quant_engine.data.sources.eastmoney.requests.get')
    def test_clist_get_all_stocks_parsing(self, mock_get):
        """get_all_stocks（clist 全市场列表，M2' 备路依赖）fixture 解析回归"""
        with open(os.path.join(FIXTURES, 'quote_em_clist.json'),
                  encoding='utf-8') as f:
            payload = json.loads(f.read())
        resp = MagicMock(status_code=200)
        resp.json.return_value = payload
        mock_get.return_value = resp

        df = EastMoneySource.get_all_stocks()
        self.assertEqual(len(df), 6)
        self.assertEqual(set(df['code']), {
            '510300.SH', '000001.SZ', '920819.BJ', '688981.SH',
            '300750.SZ', '600000.SH'})
        self.assertAlmostEqual(df.iloc[0]['price'], 4.236, places=3)
        self.assertAlmostEqual(df.iloc[0]['change_pct'], -0.14, places=2)
        # 请求参数带重试封装后不变（fs/pz 关键项）
        _, kwargs = mock_get.call_args
        self.assertEqual(kwargs['params']['pz'], '5000')
        self.assertEqual(kwargs['params']['fs'], EastMoneySource.CLIST_FS)

    @patch('quant_engine.data.sources.eastmoney.requests.get')
    def test_clist_retry_then_success(self, mock_get):
        """_get_json 重试：前两次 RemoteDisconnected，第三次成功"""
        with open(os.path.join(FIXTURES, 'quote_em_clist.json'),
                  encoding='utf-8') as f:
            payload = json.loads(f.read())
        ok = MagicMock(status_code=200)
        ok.json.return_value = payload
        mock_get.side_effect = [ConnectionError('boom'), ConnectionError('boom2'), ok]

        df = EastMoneySource.get_all_stocks()
        self.assertEqual(len(df), 6)
        self.assertEqual(mock_get.call_count, 3)

    @patch('quant_engine.data.sources.eastmoney.requests.get')
    def test_clist_all_retries_fail_returns_empty(self, mock_get):
        """重试耗尽不抛异常，返回空 df（上层自然降级）"""
        mock_get.side_effect = ConnectionError('down')
        df = EastMoneySource.get_all_stocks()
        self.assertTrue(df.empty)
        self.assertEqual(mock_get.call_count, 3)


if __name__ == '__main__':
    unittest.main()
