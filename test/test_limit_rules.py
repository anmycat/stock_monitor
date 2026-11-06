"""D63 涨跌停口径与异动文案测试

覆盖：
1. `utils/limit_rules` 逐股名义幅度（ST/主板/创业板科创/北交所）
2. 涨停/跌停判定（含四舍五入容差与低价股自适应）
3. `market_sanity.change_pct_limit` 行为不变（物理上限 ST 不敏感）
4. `MarketAlert` 涨停/跌停文案**用市场宽度交叉判定**——普跌日不得断言「极度亢奋」
5. `MarketAlert._count_at_limit` 与 2026-11-06 实测样本一致
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from quant_engine.services.market.market_alert import MarketAlert
from quant_engine.utils.limit_rules import (
    is_at_limit_down,
    is_at_limit_up,
    limit_pct_nominal,
    limit_pct_physical,
)
from quant_engine.utils.market_sanity import change_pct_limit


class TestLimitNominal(unittest.TestCase):
    """逐股名义涨跌停幅度"""

    def test_main_board_10(self):
        for code in ('600000.SH', '000001.SZ', '001979.SZ', '605118.SH',
                     '002415.SZ', '603959.SH'):
            self.assertEqual(limit_pct_nominal(code), 10.0, code)

    def test_growth_board_20(self):
        for code in ('688981.SH', '689008.SH', '300750.SZ', '301102.SZ',
                     '302132.SZ'):
            self.assertEqual(limit_pct_nominal(code), 20.0, code)

    def test_bj_board_30(self):
        for code in ('920819.BJ', '830799.BJ', '872925.BJ'):
            self.assertEqual(limit_pct_nominal(code), 30.0, code)

    def test_st_main_board_is_5(self):
        for name in ('ST百利', '*ST天首', 'SST前锋'):
            self.assertEqual(limit_pct_nominal('600001.SH', name), 5.0, name)

    def test_st_on_growth_board_is_20(self):
        """注册制板块 ST 股与普通股同幅度：创业板/科创板 20%（易错点）"""
        self.assertEqual(limit_pct_nominal('300831.SZ', 'ST派瑞'), 20.0)
        self.assertEqual(limit_pct_nominal('688001.SH', '*ST测试'), 20.0)

    def test_st_on_bj_board_is_30(self):
        self.assertEqual(limit_pct_nominal('920819.BJ', 'ST测试'), 30.0)

    def test_non_st_name_not_matched(self):
        """名字里含 ST 但不在前缀位置 → 不按 ST 处理"""
        self.assertEqual(limit_pct_nominal('600001.SH', '东方STONE'), 10.0)

    def test_physical_limit_is_st_insensitive(self):
        """物理上限对 ST 不敏感（改动会影响 breadth → 信号链，必须保持）"""
        self.assertEqual(limit_pct_physical('600001.SH'), 10.5)
        self.assertEqual(limit_pct_physical('300750.SZ'), 20.5)
        self.assertEqual(limit_pct_physical('920819.BJ'), 30.5)

    def test_change_pct_limit_delegates_and_behavior_unchanged(self):
        """market_sanity.change_pct_limit 行为必须与 D63 之前完全一致"""
        cases = {
            '600000.SH': 10.5, '000001.SZ': 10.5, '001979.SZ': 10.5,
            '605118.SH': 10.5, '688981.SH': 20.5, '689008.SH': 20.5,
            '300750.SZ': 20.5, '301102.SZ': 20.5, '302132.SZ': 20.5,
            '920819.BJ': 30.5, '830799.BJ': 30.5,
        }
        for code, expected in cases.items():
            self.assertEqual(change_pct_limit(code), expected, code)


class TestAtLimit(unittest.TestCase):
    """涨停/跌停判定"""

    def test_main_board_limit_up(self):
        self.assertTrue(is_at_limit_up(10.0, '600000.SH'))
        self.assertTrue(is_at_limit_up(10.16, '600000.SH'))

    def test_main_board_not_limit(self):
        self.assertFalse(is_at_limit_up(9.4, '600000.SH'))
        self.assertFalse(is_at_limit_up(5.0, '600000.SH'))

    def test_growth_board_12pct_is_not_limit_up(self):
        """D63 核心：创业板 +12% 不是涨停（旧口径会误算）"""
        self.assertFalse(is_at_limit_up(12.0, '300118.SZ'))
        self.assertFalse(is_at_limit_up(15.15, '300118.SZ'))

    def test_growth_board_20pct_is_limit_up(self):
        self.assertTrue(is_at_limit_up(20.0, '300750.SZ'))
        self.assertTrue(is_at_limit_up(20.04, '300750.SZ'))

    def test_bj_board_25pct_is_not_limit_up(self):
        self.assertFalse(is_at_limit_up(25.0, '920819.BJ'))
        self.assertTrue(is_at_limit_up(30.0, '920819.BJ'))

    def test_st_main_board_limit_up(self):
        """主板 ST 名义 5% → 4.68% 即涨停（实测 603959 ST百利 口径）"""
        self.assertTrue(is_at_limit_up(4.68, '603959.SH', 'ST百利'))
        self.assertFalse(is_at_limit_up(4.2, '603959.SH', 'ST百利'))

    def test_st_growth_board_4_68pct_is_not_limit_up(self):
        """创业板 ST 名义 20% → +4.68% 只是普通涨幅（实测 300831 ST派瑞）"""
        self.assertFalse(is_at_limit_up(4.68, '300831.SZ', 'ST派瑞'))

    def test_limit_down_symmetric(self):
        self.assertTrue(is_at_limit_down(-10.0, '600000.SH'))
        self.assertTrue(is_at_limit_down(-20.0, '300750.SZ'))
        self.assertFalse(is_at_limit_down(-12.0, '300118.SZ'))

    def test_low_price_rounding_tolerance(self):
        """低价股涨停价四舍五入可低于名义值 → 需自适应容差

        实测：600278 东方创业 +9.74% 是真涨停（10% 名义，前收低价的舍入所致）
        """
        self.assertTrue(is_at_limit_up(9.74, '600278.SH', '东方创业', price=1.63))

    def test_high_price_tight_tolerance(self):
        """高价股容差收紧：+9.9% 不是涨停"""
        self.assertFalse(is_at_limit_up(9.9, '600000.SH', '测试', price=100.0))

    def test_none_and_invalid_inputs(self):
        self.assertFalse(is_at_limit_up(None, '600000.SH'))
        self.assertFalse(is_at_limit_up(float('nan'), '600000.SH'))
        self.assertFalse(is_at_limit_down(None, '600000.SH'))


class TestAlertWording(unittest.TestCase):
    """文案交叉判定（D63 核心修复）"""

    def test_broad_decline_does_not_claim_euphoria(self):
        """83% 下跌的普跌日 → 不得出现「极度亢奋」"""
        detail = MarketAlert._limit_up_detail(32, breadth=0.17)
        self.assertNotIn('亢奋', detail)
        self.assertIn('结构性分化', detail)
        self.assertIn('83%', detail)

    def test_bull_market_keeps_euphoria_wording(self):
        detail = MarketAlert._limit_up_detail(32, breadth=0.72)
        self.assertIn('极度亢奋', detail)

    def test_mid_breadth_neutral(self):
        detail = MarketAlert._limit_up_detail(32, breadth=0.45)
        self.assertNotIn('亢奋', detail)
        self.assertIn('局部情绪偏热', detail)

    def test_medium_tier_has_no_emotion_claim(self):
        detail = MarketAlert._limit_up_detail(6, breadth=0.17)
        self.assertNotIn('亢奋', detail)

    def test_no_breadth_falls_back_to_plain(self):
        detail = MarketAlert._limit_up_detail(32, breadth=None)
        self.assertEqual(detail, '当前32只股票涨停')
        self.assertNotIn('亢奋', detail)

    def test_limit_down_bull_market_is_not_panic(self):
        detail = MarketAlert._limit_down_detail(12, breadth=0.78)
        self.assertNotIn('恐慌蔓延', detail)
        self.assertIn('结构性分化', detail)

    def test_limit_down_bear_market_is_panic(self):
        detail = MarketAlert._limit_down_detail(12, breadth=0.15)
        self.assertIn('恐慌', detail)

    def test_title_also_cross_checked(self):
        """普跌日不得称「涨停潮」（实测 5213 只里 35 只涨停、86% 下跌）"""
        self.assertIn('结构性涨停', MarketAlert._limit_up_title(35, breadth=0.137))
        self.assertNotIn('涨停潮', MarketAlert._limit_up_title(35, breadth=0.137))
        self.assertIn('涨停潮', MarketAlert._limit_up_title(35, breadth=0.72))
        self.assertIn('涨停潮', MarketAlert._limit_up_title(35, breadth=None))

    def test_breadth_consistency_between_two_messages(self):
        """同屏两条消息引用同一宽度 → 语义自洽"""
        breadth = 0.17
        up = MarketAlert._limit_up_detail(32, breadth)
        down = MarketAlert._limit_down_detail(5, breadth)
        self.assertIn('83%', up)
        self.assertIn('83%', down)


class TestCountAtLimit(unittest.TestCase):
    """家数统计（D63 用 2026-11-06 实测样本）"""

    @staticmethod
    def _df(rows):
        return pd.DataFrame(rows)

    def test_counts_with_board_adaptation(self):
        rows = [
            # 主板真涨停 3 只
            {'code': '600000.SH', 'name': '甲', 'price': 10.0, 'change_pct': 10.0},
            {'code': '000001.SZ', 'name': '乙', 'price': 12.0, 'change_pct': 10.05},
            {'code': '601398.SH', 'name': '丙', 'price': 5.0, 'change_pct': 10.0},
            # 创业板非涨停 2 只（旧口径会误算）
            {'code': '300118.SZ', 'name': '丁', 'price': 10.26, 'change_pct': 15.15},
            {'code': '688203.SH', 'name': '戊', 'price': 30.0, 'change_pct': 12.06},
            # 创业板真涨停 1 只
            {'code': '300750.SZ', 'name': '己', 'price': 200.0, 'change_pct': 20.0},
            # 北交所非涨停 1 只 + 真涨停 1 只
            {'code': '920819.BJ', 'name': '庚', 'price': 30.0, 'change_pct': 25.0},
            {'code': '830799.BJ', 'name': '辛', 'price': 5.0, 'change_pct': 30.0},
            # 主板 ST 真涨停 1 只（名义 5%）
            {'code': '603959.SH', 'name': 'ST百利', 'price': 1.9, 'change_pct': 4.68},
            # 创业板 ST +4.68% → 名义 20%，不是涨停
            {'code': '300831.SZ', 'name': 'ST派瑞', 'price': 9.0, 'change_pct': 4.68},
            # 非涨停若干
            {'code': '000002.SZ', 'name': '壬', 'price': 8.0, 'change_pct': -3.0},
            {'code': '600519.SH', 'name': '癸', 'price': 100.0, 'change_pct': 1.2},
            # 主板真跌停 1 只
            {'code': '600028.SH', 'name': '子', 'price': 8.8, 'change_pct': -10.0},
        ]
        df = self._df(rows)
        self.assertEqual(MarketAlert._count_at_limit(df, up=True), 6)
        self.assertEqual(MarketAlert._count_at_limit(df, up=False), 1)

    def test_old_caliber_would_overcount(self):
        """对照：旧口径（统一 >=9.9）会把创业板非涨停算进去"""
        rows = [
            {'code': '300118.SZ', 'name': '丁', 'price': 10.26, 'change_pct': 15.15},
            {'code': '688203.SH', 'name': '戊', 'price': 30.0, 'change_pct': 12.06},
            {'code': '600000.SH', 'name': '甲', 'price': 10.0, 'change_pct': 10.0},
        ]
        df = self._df(rows)
        old = len(df[df['change_pct'] >= 9.9])
        self.assertEqual(old, 3)
        self.assertEqual(MarketAlert._count_at_limit(df, up=True), 1)

    def test_empty_and_missing_columns(self):
        self.assertEqual(MarketAlert._count_at_limit(pd.DataFrame(), up=True), 0)
        self.assertEqual(
            MarketAlert._count_at_limit(self._df([{'code': 'x'}]), up=True), 0)


if __name__ == '__main__':
    unittest.main()
