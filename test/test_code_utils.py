"""code_utils 单元测试 - 全市场代码归一化与数据源适配

覆盖：
- normalize_code：任意格式（裸码/标准码/腾讯前缀/新浪前缀/东财secid）→ 标准码
- market_by_symbol：SH(6/5/9) / SZ(0/1/2/3) / BJ(4/8/92) 全代码段
- to_secid / to_tencent_code / to_sina_code：数据源适配
- is_valid_code / code_key / split_code / market_of_code
- 事故回归：510300（ETF）、920819（北交所新段）不得错判市场
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from quant_engine.utils.code_utils import (
    MARKET_BJ,
    MARKET_SH,
    MARKET_SZ,
    code_key,
    is_valid_code,
    market_by_symbol,
    market_of_code,
    normalize_code,
    split_code,
    to_secid,
    to_sina_code,
    to_tencent_code,
)


class TestNormalizeCode(unittest.TestCase):
    """任意格式 → 标准码"""

    def test_bare_symbol(self):
        self.assertEqual(normalize_code('510300'), '510300.SH')
        self.assertEqual(normalize_code('000001'), '000001.SZ')
        self.assertEqual(normalize_code('920819'), '920819.BJ')

    def test_standard_code(self):
        self.assertEqual(normalize_code('510300.SH'), '510300.SH')
        self.assertEqual(normalize_code('000001.SZ'), '000001.SZ')
        self.assertEqual(normalize_code('920819.BJ'), '920819.BJ')

    def test_lowercase_suffix(self):
        self.assertEqual(normalize_code('510300.sh'), '510300.SH')
        self.assertEqual(normalize_code('000001.sz'), '000001.SZ')

    def test_tencent_prefix(self):
        self.assertEqual(normalize_code('sh510300'), '510300.SH')
        self.assertEqual(normalize_code('sz000001'), '000001.SZ')
        self.assertEqual(normalize_code('bj920819'), '920819.BJ')

    def test_sina_prefix(self):
        self.assertEqual(normalize_code('sh600000'), '600000.SH')
        self.assertEqual(normalize_code('bj830799'), '830799.BJ')

    def test_eastmoney_secid(self):
        self.assertEqual(normalize_code('1.510300'), '510300.SH')
        # 东财把北交所归 0 组（f13=0），secid 0. 前缀需按代码段细分
        self.assertEqual(normalize_code('0.920819'), '920819.BJ')
        self.assertEqual(normalize_code('0.000001'), '000001.SZ')

    def test_whitespace(self):
        self.assertEqual(normalize_code(' 510300 '), '510300.SH')
        self.assertEqual(normalize_code(' sh510300 '), '510300.SH')

    def test_invalid(self):
        self.assertIsNone(normalize_code('abc'))
        self.assertIsNone(normalize_code('12345'))      # 5 位
        self.assertIsNone(normalize_code('1234567'))    # 7 位
        self.assertIsNone(normalize_code(''))
        self.assertIsNone(normalize_code(None))
        self.assertIsNone(normalize_code('600000.XX'))  # 未知市场后缀


class TestMarketBySymbol(unittest.TestCase):
    """静态市场规则（兜底推断）"""

    def test_sh_segments(self):
        # 6 开头主板/科创、5 开头基金、9 开头 B 股
        for symbol in ('600000', '601888', '605199', '688981',
                       '510300', '518880', '588000', '900901'):
            self.assertEqual(market_by_symbol(symbol), MARKET_SH, symbol)

    def test_sz_segments(self):
        # 0 主板、3 创业板、2 B股、1 可转债
        for symbol in ('000001', '001979', '300750', '301301',
                       '200012', '113050', '127045'):
            self.assertEqual(market_by_symbol(symbol), MARKET_SZ, symbol)

    def test_bj_segments(self):
        # 4/8 老三板转板、92 北交所新段
        for symbol in ('430047', '830799', '872925', '920819', '920002'):
            self.assertEqual(market_by_symbol(symbol), MARKET_BJ, symbol)

    def test_strips_suffix(self):
        self.assertEqual(market_by_symbol('510300.SH'), MARKET_SH)
        self.assertEqual(market_by_symbol('920819.bj'), MARKET_BJ)

    def test_invalid(self):
        self.assertIsNone(market_by_symbol('abc'))
        self.assertIsNone(market_by_symbol('12345'))
        self.assertIsNone(market_by_symbol(''))
        self.assertIsNone(market_by_symbol(None))


class TestAccidentRegression(unittest.TestCase):
    """事故回归：原「代码与实际股票不匹配」的核心场景"""

    def test_etf_510300_must_be_sh(self):
        """腾讯源旧 bug：startswith('6') 推断把 510300 错判 .SZ"""
        self.assertEqual(normalize_code('510300'), '510300.SH')
        self.assertEqual(market_by_symbol('510300'), MARKET_SH)

    def test_bj_920819_must_be_bj(self):
        """东财/新浪旧 bug：920819 被错判 .SH/.SZ"""
        self.assertEqual(normalize_code('920819'), '920819.BJ')
        self.assertEqual(normalize_code('0.920819'), '920819.BJ')

    def test_cross_source_consistency(self):
        """同一股票经不同源格式归一化后必须一致"""
        variants = ['510300', '510300.SH', 'sh510300', 'SH510300', '1.510300']
        self.assertEqual(len({normalize_code(v) for v in variants}), 1)
        variants_bj = ['920819', '920819.BJ', 'bj920819', '0.920819']
        self.assertEqual(len({normalize_code(v) for v in variants_bj}), 1)


class TestSourceAdapters(unittest.TestCase):
    """数据源适配函数"""

    def test_to_secid(self):
        self.assertEqual(to_secid('510300.SH'), '1.510300')
        self.assertEqual(to_secid('000001.SZ'), '0.000001')
        self.assertEqual(to_secid('920819.BJ'), '0.920819')  # BJ 与 SZ 共用 0 前缀

    def test_to_tencent_code(self):
        self.assertEqual(to_tencent_code('510300.SH'), 'sh510300')
        self.assertEqual(to_tencent_code('000001.SZ'), 'sz000001')
        self.assertEqual(to_tencent_code('920819.BJ'), 'bj920819')

    def test_to_sina_code(self):
        self.assertEqual(to_sina_code('510300.SH'), 'sh510300')
        self.assertEqual(to_sina_code('920819.BJ'), 'bj920819')

    def test_adapters_invalid(self):
        self.assertIsNone(to_secid('abc'))
        self.assertIsNone(to_tencent_code('abc'))
        self.assertIsNone(to_sina_code('abc'))


class TestValidityAndKey(unittest.TestCase):
    """合法性与 key 提取"""

    def test_is_valid(self):
        self.assertTrue(is_valid_code('510300.SH'))
        self.assertTrue(is_valid_code('510300'))       # 可推断
        self.assertTrue(is_valid_code('bj920819'))
        self.assertFalse(is_valid_code('510300.XX'))
        self.assertFalse(is_valid_code('12345'))
        self.assertFalse(is_valid_code(''))

    def test_code_key(self):
        """快照/diff 匹配键与市场后缀解耦"""
        self.assertEqual(code_key('510300.SH'), '510300')
        self.assertEqual(code_key('bj920819'), '920819')
        self.assertIsNone(code_key('abc'))

    def test_split_code(self):
        self.assertEqual(split_code('510300.SH'), ('510300', 'SH'))
        self.assertEqual(split_code('920819'), ('920819', 'BJ'))
        self.assertIsNone(split_code('12345'))

    def test_market_of_code(self):
        self.assertEqual(market_of_code('510300.SH'), MARKET_SH)
        self.assertEqual(market_of_code('000001'), MARKET_SZ)
        self.assertIsNone(market_of_code('600000.XX'))


if __name__ == '__main__':
    unittest.main()
