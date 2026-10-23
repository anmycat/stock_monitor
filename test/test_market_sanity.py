"""B5 回归：实时行情出口数据卫生断言（utils/market_sanity）

背景（13 报告 §4.4 + 14 §3 B5）：东财冷却期 change_pct 脏数据曾致
"/api/market 100 只全涨 11.35%、文案 >100%"。本测试冻结：
① 按代码段的涨跌幅物理上限判定；② 超限/非正价/非有限行剔除；
③ overview 出口一致性断言；④ 高剔除率 suspect 路径。
"""
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.utils.market_sanity import (
    DROP_RATIO_SUSPECT,
    change_pct_limit,
    is_overview_sane,
    sanitize_quotes,
)


def _df(rows):
    return pd.DataFrame(rows)


class ChangePctLimitTests(unittest.TestCase):
    def test_limits_by_segment(self):
        self.assertEqual(change_pct_limit("600000.SH"), 10.5)   # 沪主板
        self.assertEqual(change_pct_limit("000001.SZ"), 10.5)   # 深主板
        self.assertEqual(change_pct_limit("001979.SZ"), 10.5)   # 深主板新段
        self.assertEqual(change_pct_limit("605118.SH"), 10.5)   # 沪主板 605 段
        self.assertEqual(change_pct_limit("688981.SH"), 20.5)   # 科创
        self.assertEqual(change_pct_limit("689008.SH"), 20.5)   # 科创 CDR 段
        self.assertEqual(change_pct_limit("300750.SZ"), 20.5)   # 创业板 300 段
        self.assertEqual(change_pct_limit("301102.SZ"), 20.5)   # 创业板 301 段（D32 实测补）
        self.assertEqual(change_pct_limit("302132.SZ"), 20.5)   # 创业板 302 段（已启用）
        self.assertEqual(change_pct_limit("920819.BJ"), 30.5)   # 北交所
        self.assertEqual(change_pct_limit("830799.BJ"), 30.5)   # 北交所老段


class SanitizeQuotesTests(unittest.TestCase):
    def test_dirty_rows_dropped(self):
        df = _df([
            {"code": "600000.SH", "name": "A", "price": 8.1, "change_pct": 1.2},
            {"code": "600001.SH", "name": "B", "price": 0, "change_pct": 3.0},      # price<=0
            {"code": "600002.SH", "name": "C", "price": 5.0, "change_pct": 1135.0},  # 脏值
            {"code": "600003.SH", "name": "D", "price": 5.0, "change_pct": float("nan")},
            {"code": "600004.SH", "name": "E", "price": 5.0, "change_pct": -9.9},    # 合法跌停附近
        ])
        clean, stats = sanitize_quotes(df)
        self.assertEqual(len(clean), 2)  # A 与 E 保留
        self.assertEqual(stats["dropped"], 3)
        self.assertAlmostEqual(stats["drop_ratio"], 0.6)
        self.assertEqual(set(clean["code"]), {"600000.SH", "600004.SH"})

    def test_segment_boundaries_kept(self):
        df = _df([
            {"code": "688111.SH", "price": 10.0, "change_pct": 20.0},   # 科创一字板合法
            {"code": "300059.SZ", "price": 10.0, "change_pct": -20.0},  # 创业地板合法
            {"code": "688222.SH", "price": 10.0, "change_pct": 21.0},   # 科创超限剔除
            {"code": "600000.SH", "price": 10.0, "change_pct": 10.5},   # 主板容差界合法
            {"code": "600001.SH", "price": 10.0, "change_pct": 10.6},   # 主板超限剔除
        ])
        clean, _stats = sanitize_quotes(df)
        self.assertEqual(set(clean["code"]), {"688111.SH", "300059.SZ", "600000.SH"})

    def test_crema_301_segment_not_dropped(self):
        """D32 回归（实测抓出）：创业板 301 段曾漏覆盖被误按主板 10.5% 剔除——
        301159 三维天地 +20.00%、301313 凡拓数创 +11.54% 均应保留"""
        df = _df([
            {"code": "301159.SZ", "name": "三维天地", "price": 38.76, "change_pct": 20.00},
            {"code": "301313.SZ", "name": "凡拓数创", "price": 52.20, "change_pct": 11.54},
            {"code": "301486.SZ", "name": "致尚科技", "price": 209.72, "change_pct": 17.12},
            {"code": "300296.SZ", "name": "利亚德", "price": 5.63, "change_pct": 20.04},  # 涨停四舍五入
            {"code": "301999.SZ", "name": "脏数据", "price": 10.0, "change_pct": 45.0},   # 真超限照剔
        ])
        clean, stats = sanitize_quotes(df)
        self.assertEqual(set(clean["code"]),
                         {"301159.SZ", "301313.SZ", "301486.SZ", "300296.SZ"})
        self.assertEqual(stats["reasons"]["over_limit"], 1)

    def test_empty_and_missing_column(self):
        clean, stats = sanitize_quotes(pd.DataFrame())
        self.assertTrue(clean.empty)
        self.assertEqual(stats["total"], 0)
        df = _df([{"code": "600000.SH", "price": 8.0}])
        clean, stats = sanitize_quotes(df)
        self.assertTrue(clean.empty)
        self.assertEqual(stats["reasons"].get("no_change_pct_column"), 1)


class OverviewAssertionTests(unittest.TestCase):
    def test_sane_overview_passes(self):
        ok = {"total": 100, "up": 60, "down": 30, "flat": 10, "avg_change": 1.23}
        self.assertTrue(is_overview_sane(ok))

    def test_all_up_big_avg_is_structurally_consistent(self):
        """全涨+均涨 11.35% 结构自洽（涨停潮理论可能）——不误报；
        防 11.35% 这种超物理上限脏值是 sanitize_quotes 的职责"""
        self.assertTrue(is_overview_sane({"total": 100, "up": 100, "down": 0, "flat": 0, "avg_change": 11.35}))

    def test_distribution_contradictions_fail(self):
        # 全涨却均跌（硬矛盾）
        self.assertFalse(is_overview_sane({"total": 100, "up": 100, "down": 0, "flat": 0, "avg_change": -1.0}))
        # 全跌却均涨（硬矛盾）
        self.assertFalse(is_overview_sane({"total": 100, "up": 0, "down": 100, "flat": 0, "avg_change": 1.0}))
        # 全平却均非零（硬矛盾）
        self.assertFalse(is_overview_sane({"total": 10, "up": 0, "down": 0, "flat": 10, "avg_change": 0.5}))

    def test_other_invalid_overviews_fail(self):
        # up+down+flat != total
        self.assertFalse(is_overview_sane({"total": 100, "up": 60, "down": 30, "flat": 5, "avg_change": 0.0}))
        # avg_change 超理论界
        self.assertFalse(is_overview_sane({"total": 10, "up": 5, "down": 5, "flat": 0, "avg_change": 99.0}))
        # total 为 0
        self.assertFalse(is_overview_sane({"total": 0, "up": 0, "down": 0, "flat": 0, "avg_change": 0.0}))


class SuspectThresholdTests(unittest.TestCase):
    def test_drop_ratio_constant(self):
        self.assertEqual(DROP_RATIO_SUSPECT, 0.20)

    def test_systematic_corruption_detected(self):
        rows = [{"code": f"60000{i}.SH", "price": 1.0, "change_pct": 999.0} for i in range(10)]
        rows.append({"code": "600010.SH", "price": 1.0, "change_pct": 0.5})
        _clean, stats = sanitize_quotes(_df(rows))
        self.assertGreater(stats["drop_ratio"], DROP_RATIO_SUSPECT)  # 触发 suspect 回退


class NewListingExemptTests(unittest.TestCase):
    """N/C 前缀新股（上市首五日不设涨跌幅）豁免上限判定

    实测依据（2026-10-21 M2'）：920296.BJ "N文峰" 上市首日 +217.03%，
    北交所新股首日无涨跌幅限制，合法；按号段上限剔除会误丢。
    """

    def test_n_prefixed_new_stock_exempt(self):
        rows = [{"code": "920296.BJ", "name": "N文峰", "price": 36.3,
                 "change_pct": 217.03}]
        clean, stats = sanitize_quotes(_df(rows))
        self.assertEqual(len(clean), 1)
        self.assertEqual(stats["reasons"].get("new_listing_exempt"), 1)
        self.assertEqual(stats["dropped"], 0)

    def test_c_prefixed_new_stock_exempt(self):
        rows = [{"code": "688001.SH", "name": "C华兴", "price": 10.0,
                 "change_pct": 150.0}]
        clean, _stats = sanitize_quotes(_df(rows))
        self.assertEqual(len(clean), 1)

    def test_dirty_row_still_dropped(self):
        """豁免不放松其他约束：脏价行/非新股超限行照剔"""
        rows = [
            {"code": "920296.BJ", "name": "N文峰", "price": 36.3, "change_pct": 217.03},
            {"code": "600000.SH", "name": "浦发银行", "price": 9.66, "change_pct": 999.0},
            {"code": "600001.SH", "name": "N假冒", "price": 0.0, "change_pct": 50.0},  # price<=0 仍剔
        ]
        clean, _stats = sanitize_quotes(_df(rows))
        self.assertEqual(len(clean), 1)
        self.assertEqual(clean.iloc[0]["name"], "N文峰")

    def test_non_new_listing_with_n_name_not_exempt(self):
        """名称以 N 开头但非汉字简称（异常数据）不豁免"""
        rows = [{"code": "600000.SH", "name": "NX123", "price": 9.66,
                 "change_pct": 999.0}]
        clean, _stats = sanitize_quotes(_df(rows))
        self.assertEqual(len(clean), 0)

    def test_missing_name_column_no_exemption(self):
        """无 name 列时保守处理（不豁免）"""
        rows = [{"code": "920296.BJ", "price": 36.3, "change_pct": 217.03}]
        clean, _stats = sanitize_quotes(_df(rows))
        self.assertEqual(len(clean), 0)


if __name__ == "__main__":
    unittest.main()
