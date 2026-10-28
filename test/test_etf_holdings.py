"""ETF持仓成分股监控 - 单元测试（离线，不打网络）

覆盖：
1. 天天基金持仓接口HTML解析（多季度/字段清洗/代码规范化/脏数据）
2. 季度环比diff（新进/退出/增持/减持/持平 + 净值异常检测）
3. 阈值过滤（剔除ETF份额申酬导致的整体缩放噪声）
4. 监控服务流程（快照存储/幂等/变化记录/总览）
5. 推送文案生成
"""
import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.data.sources.fund_holdings import EastMoneyFundHoldingsSource
from quant_engine.services.monitors.etf_holdings_monitor import ETFHoldingsMonitor

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def load_content(fixture_name: str) -> str:
    """从保存的原始响应中提取 apidata.content（与源类同一正则）"""
    raw = (FIXTURES / fixture_name).read_text(encoding="utf-8")
    m = re.search(r'content:"(.*)",arryear', raw, re.DOTALL)
    assert m, "fixture格式不符"
    return m.group(1)


def fixture_df(fixture_name: str):
    return EastMoneyFundHoldingsSource()._parse(load_content(fixture_name))


class FakeCache:
    """内存桩缓存，接口与 RedisCache 对齐"""

    def __init__(self):
        self._data = {}

    def get(self, key):
        return self._data.get(key)

    def set(self, key, value, ttl=0):
        self._data[key] = value
        return True


class FakeDataCenter:
    def __init__(self):
        self.cache = FakeCache()


class TestFundHoldingsParser(unittest.TestCase):
    """天天基金持仓接口解析（fixture为2026-10真实响应）"""

    @classmethod
    def setUpClass(cls):
        cls.df2026 = fixture_df("jjcc_510300_2026.txt")
        cls.df2025 = fixture_df("jjcc_510300_2025.txt")

    def test_2026_two_quarters_100_rows_each(self):
        counts = self.df2026.groupby("quarter").size().to_dict()
        self.assertEqual(counts, {"2026Q1": 100, "2026Q2": 100})

    def test_2025_four_quarters_100_rows_each(self):
        counts = self.df2025.groupby("quarter").size().to_dict()
        self.assertEqual(
            counts,
            {"2025Q1": 100, "2025Q2": 100, "2025Q3": 100, "2025Q4": 100},
        )

    def test_report_dates(self):
        dates = self.df2026.groupby("quarter")["report_date"].first().to_dict()
        self.assertEqual(dates["2026Q1"], "2026-03-31")
        self.assertEqual(dates["2026Q2"], "2026-06-30")

    def test_field_values_ningde(self):
        row = self.df2026[
            (self.df2026["quarter"] == "2026Q1")
            & (self.df2026["code"] == "300750.SZ")
        ].iloc[0]
        self.assertEqual(row["name"], "宁德时代")
        self.assertAlmostEqual(row["weight"], 4.27, places=2)
        self.assertAlmostEqual(row["shares"], 2126.97, places=2)
        self.assertAlmostEqual(row["market_value"], 854404.57, places=2)

    def test_code_normalization(self):
        codes = set(self.df2026["code"])
        self.assertIn("300750.SZ", codes)   # 0.前缀 -> 深市
        self.assertIn("600519.SH", codes)   # 1.前缀 -> 沪市
        for c in codes:
            self.assertRegex(c, r"^\d{6}\.(SZ|SH|BJ)$")

    def test_latest_quarter_price_spans_ignored(self):
        # 最新季度的行含两个JS占位span（最新价/涨跌幅），解析应取最后三个有效值
        row = self.df2026[
            (self.df2026["quarter"] == "2026Q2")
            & (self.df2026["code"] == "300308.SZ")
        ].iloc[0]
        self.assertAlmostEqual(row["weight"], 4.75, places=2)
        self.assertAlmostEqual(row["shares"], 354.65, places=2)
        self.assertAlmostEqual(row["market_value"], 450401.94, places=2)

    def test_empty_content_returns_empty_frame(self):
        df = EastMoneyFundHoldingsSource()._parse("")
        self.assertTrue(df.empty)
        self.assertIn("quarter", df.columns)

    def test_to_float_robustness(self):
        f = EastMoneyFundHoldingsSource._to_float
        self.assertEqual(f("4.75%"), 4.75)
        self.assertEqual(f("1,605,368.09"), 1605368.09)
        self.assertEqual(f("--"), 0.0)
        self.assertEqual(f(""), 0.0)


class TestDiffQuarters(unittest.TestCase):
    """季度环比diff逻辑（合成数据）"""

    def _rec(self, code, name, weight, shares, mv, seq=1):
        return {"code": code, "name": name, "weight": weight,
                "shares": shares, "market_value": mv, "seq": seq,
                "report_date": "2026-06-30"}

    def test_classification(self):
        prev = [self._rec("A.SZ", "甲", 2.0, 100, 2000),
                self._rec("B.SZ", "乙", 1.0, 50, 1000),
                self._rec("C.SZ", "丙", 0.5, 20, 400)]
        curr = [self._rec("A.SZ", "甲", 2.2, 110, 2200),
                self._rec("B.SZ", "乙", 0.4, 20, 400),
                self._rec("D.SH", "丁", 1.5, 60, 3000)]
        d = ETFHoldingsMonitor.diff_quarters("2026Q1", "2026Q2", prev, curr)
        self.assertEqual([i["code"] for i in d["new_entries"]], ["D.SH"])
        self.assertEqual([i["code"] for i in d["exits"]], ["C.SZ"])
        self.assertEqual([i["code"] for i in d["increased"]], ["A.SZ"])
        self.assertEqual([i["code"] for i in d["decreased"]], ["B.SZ"])
        self.assertEqual(d["unchanged_count"], 0)
        # 反推净值 = 持仓市值和/权重和*100；期望按公式实算
        nav_prev = (2000 + 1000 + 400) / (2.0 + 1.0 + 0.5) * 100
        nav_curr = (2200 + 400 + 3000) / (2.2 + 0.4 + 1.5) * 100
        self.assertAlmostEqual(d["nav_change_pct"],
                               (nav_curr - nav_prev) / nav_prev, places=3)

    def test_threshold_filter_rules(self):
        m = ETFHoldingsMonitor()
        changes = {
            "new_entries": [
                {"code": "N1.SZ", "name": "大新", "direction": "new",
                 "weight": 1.0, "market_value": 5000},
                {"code": "N2.SZ", "name": "小新", "direction": "new",
                 "weight": 0.1, "market_value": 500},
            ],
            "exits": [
                {"code": "E1.SZ", "name": "大退", "direction": "exit",
                 "weight": 0.8, "market_value": 8000},
            ],
            "increased": [
                # 持股+15%且权重同向+0.2pct -> 保留
                {"code": "U1.SZ", "name": "真增", "direction": "up",
                 "weight": 1.2, "market_value": 6000,
                 "shares_change_pct": 0.15, "weight_delta": 0.2},
            ],
            "decreased": [
                # 持股+15%但权重几乎不动 -> ETF份额申赎缩放，过滤
                {"code": "D1.SZ", "name": "缩放", "direction": "down",
                 "weight": 1.0, "market_value": 5000,
                 "shares_change_pct": 0.15, "weight_delta": 0.05},
                # 权重变化0.5pct -> 保留
                {"code": "D2.SZ", "name": "真减", "direction": "down",
                 "weight": 0.9, "market_value": 5000,
                 "shares_change_pct": 0.02, "weight_delta": -0.5},
            ],
            "unchanged_count": 90,
        }
        out = m.filter_alertable(changes)
        codes = {i["code"] for k in ("new_entries", "exits", "increased", "decreased")
                 for i in out[k]}
        self.assertEqual(codes, {"N1.SZ", "E1.SZ", "U1.SZ", "D2.SZ"})
        self.assertEqual(out["alert_count"], 4)

    def test_rel_change_edges(self):
        self.assertEqual(ETFHoldingsMonitor._rel_change(0, 10), 1.0)
        self.assertEqual(ETFHoldingsMonitor._rel_change(0, 0), 0.0)
        self.assertAlmostEqual(ETFHoldingsMonitor._rel_change(100, 110), 0.1, places=6)

    def test_implied_nav(self):
        recs = [{"market_value": 450401.94, "weight": 4.75}]
        self.assertAlmostEqual(ETFHoldingsMonitor._implied_nav(recs),
                               9482146.1, places=0)


class TestMonitorFlow(unittest.TestCase):
    """监控服务流程（桩缓存 + fixture数据，无网络）"""

    def setUp(self):
        self.monitor = ETFHoldingsMonitor(FakeDataCenter())
        df = fixture_df("jjcc_510300_2026.txt")
        self.monitor.source.get_holdings = lambda code, topline=100: df

    def test_first_run_stores_and_diffs(self):
        changes = self.monitor.refresh_etf("510300.SH", "沪深300ETF")
        self.assertIsNotNone(changes)
        self.assertEqual(changes["previous_quarter"], "2026Q1")
        self.assertEqual(changes["current_quarter"], "2026Q2")
        self.assertIn("alert_count", changes)
        self.assertGreater(changes["alert_count"], 0)
        # 快照与摘要已落库
        self.assertIsNotNone(self.monitor.get_latest("510300.SH"))
        self.assertIsNotNone(self.monitor.get_changes("510300.SH"))
        snap = self.monitor.datacenter.cache.get("etf_holdings:510300:2026Q2")
        self.assertEqual(len(snap), 100)

    def test_second_call_idempotent(self):
        self.monitor.refresh_etf("510300.SH", "沪深300ETF")
        self.assertIsNone(self.monitor.refresh_etf("510300.SH", "沪深300ETF"))

    def test_overview(self):
        self.monitor.refresh_etf("510300.SH", "沪深300ETF")
        ov = self.monitor.get_overview()
        self.assertEqual(len(ov), 14)  # 与HOT_ETFS一致
        item = next(o for o in ov if o["code"] == "510300.SH")
        self.assertTrue(item["tracked"])
        self.assertEqual(item["quarter"], "2026Q2")
        self.assertEqual(item["holdings_count"], 100)
        untracked = next(o for o in ov if o["code"] == "510880.SH")
        self.assertFalse(untracked["tracked"])


class TestMessageFormat(unittest.TestCase):
    """推送文案生成"""

    def test_message_sections_and_none_safety(self):
        m = ETFHoldingsMonitor()
        changes = {
            "previous_quarter": "2026Q1", "current_quarter": "2026Q2",
            "current_report_date": "2026-06-30",
            "nav_change_pct": -0.53,
            "unchanged_count": 46,
            "new_entries": [{"code": "688256.SH", "name": "寒武纪",
                             "direction": "new", "weight": 1.80,
                             "market_value": 170821.66}],
            "exits": [{"code": "000425.SZ", "name": "徐工机械",
                       "direction": "exit", "weight": 0.38,
                       "prev_weight": 0.38, "market_value": 76303.2}],
            "increased": [], "decreased": [],
        }
        msg = m.format_changes_message(changes)
        self.assertIn("2026Q1 → 2026Q2", msg)
        self.assertIn("新进", msg)
        self.assertIn("退出", msg)
        self.assertIn("寒武纪(688256)", msg)
        self.assertIn("原权重0.38%", msg)
        self.assertIn("-53.0%", msg)  # 净值异常提示
        self.assertIn("持平/未达阈值: 46只", msg)

    def test_message_empty_changes(self):
        m = ETFHoldingsMonitor()
        msg = m.format_changes_message({
            "previous_quarter": "2026Q1", "current_quarter": "2026Q2",
            "current_report_date": "2026-06-30", "nav_change_pct": None,
            "unchanged_count": 100, "new_entries": [], "exits": [],
            "increased": [], "decreased": [],
        })
        self.assertIn("持平/未达阈值: 100只", msg)
        self.assertNotIn("新进", msg)


class TestGetHoldingsLazyImport(unittest.TestCase):
    """回归：get_holdings 懒加载的 fetch_guard 相对导入必须可解析

    D50 同款事故（与 news_sources 一并修复，D52）：`from ...utils.fetch_guard`
    曾少一层点；既有 TestMonitorFlow 桩掉 source.get_holdings 整体，
    从未触发该方法内部的懒加载导入。本测试走真实方法，仅桩 _fetch_year 断网。
    """

    def test_get_holdings_import_chain(self):
        from quant_engine.data.sources.fund_holdings import (
            EastMoneyFundHoldingsSource,
        )
        src = EastMoneyFundHoldingsSource()
        df = fixture_df("jjcc_510300_2026.txt")
        src._fetch_year = lambda code, year, topline=100: df  # 不联网
        # 旧代码（少一层点）在此抛 ModuleNotFoundError
        out = src.get_holdings("510300", years=[2026])
        self.assertFalse(out.empty)
        self.assertIn("quarter", out.columns)


if __name__ == "__main__":
    unittest.main(verbosity=2)
