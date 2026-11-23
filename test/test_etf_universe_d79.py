"""D79 优势 ETF 榜专项测试（旁路功能，零趋势信号影响）。"""
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.data.sources import etf_universe as eu


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class FakeCache:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, ttl=None):
        self.data[key] = value


def _row(code, name, scale_yi, amount_yi=2.0, change=1.0):
    return {"f12": code, "f14": name, "f2": 1256, "f3": int(change * 100),
            "f5": 100000, "f6": amount_yi * 1e8, "f8": 250, "f20": scale_yi * 1e8}


class TestParseRows(unittest.TestCase):
    @staticmethod
    def _parsed(*specs):
        return eu._parse_rows({str(i): raw for i, raw in enumerate(specs)})

    def test_fields_and_scaling(self):
        rows = self._parsed(_row("512890", "红利低波ETF", 300, 7.5, -1.18),
                            _row("510300", "沪深300ETF", 500))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["code"], "512890")
        self.assertAlmostEqual(rows[0]["price"], 1.256)
        self.assertAlmostEqual(rows[0]["change_pct"], -1.18)
        self.assertAlmostEqual(rows[0]["amount"], 7.5e8)
        self.assertAlmostEqual(rows[0]["turnover_pct"], 2.5)
        self.assertEqual(rows[0]["scale"], 300e8)

    def test_bad_values_become_zero(self):
        raw = _row("510880", "红利ETF", 100)
        raw.update({"f2": "-", "f3": None, "f6": "bad", "f20": ""})
        out = self._parsed(raw)[0]
        self.assertEqual((out["price"], out["change_pct"], out["amount"], out["scale"]),
                         (0.0, 0.0, 0.0, 0.0))

    def test_placeholder_code_skipped(self):
        raw = _row("-", "占位", 100)
        self.assertEqual(self._parsed(raw), [])


class TestSelectLeaders(unittest.TestCase):
    ROWS = eu._parse_rows({str(i): raw for i, raw in enumerate([
        _row("512890", "红利低波ETF华泰柏瑞", 300),
        _row("515450", "红利低波50ETF南方", 210),
        _row("510300", "沪深300ETF", 800),
        _row("159545", "港股通红利低波ETF", 3),   # 规模<5亿被过滤
        _row("510050", "上证50ETF", 150),
    ])})

    def test_dividend_filters_by_scale_and_keyword(self):
        out = eu.select_leaders(self.ROWS, "dividend", limit=10)
        self.assertEqual([r["code"] for r in out], ["512890", "515450"])
        self.assertEqual([r["rank"] for r in out], [1, 2])
        self.assertTrue(all("红利" in r["name"] for r in out))

    def test_broad_theme(self):
        out = eu.select_leaders(self.ROWS, "broad", limit=10)
        self.assertEqual([r["code"] for r in out], ["510300", "510050"])

    def test_limit_and_custom_min_scale(self):
        out = eu.select_leaders(self.ROWS, "dividend", limit=1)
        self.assertEqual(len(out), 1)
        out = eu.select_leaders(self.ROWS, "dividend", min_scale=2e8)
        self.assertEqual(len(out), 3)

    def test_unknown_theme(self):
        with self.assertRaises(ValueError):
            eu.select_leaders(self.ROWS, "nonexistent")


class TestFetchAndCache(unittest.TestCase):
    def test_fetch_stops_on_empty_page(self):
        page1 = {"data": {"diff": {"0": _row("512890", "红利低波ETF", 300)}}}
        page2 = {"data": {"diff": {}}}
        with mock.patch.object(eu, "em_get", side_effect=[FakeResponse(page1), FakeResponse(page2)]) as m:
            rows, partial = eu.fetch_etf_universe(max_pages=5)
        self.assertEqual(len(rows), 1)
        self.assertFalse(partial)
        self.assertEqual(m.call_count, 2)

    def test_fetch_marks_partial_on_error(self):
        page1 = {"data": {"diff": {"0": _row("512890", "红利低波ETF", 300)}}}
        with mock.patch.object(eu, "em_get", side_effect=[FakeResponse(page1), RuntimeError("RST")]):
            rows, partial = eu.fetch_etf_universe(max_pages=5)
        self.assertEqual(len(rows), 1)
        self.assertTrue(partial)

    def test_get_leaders_uses_cache(self):
        cache = FakeCache()
        with mock.patch.object(eu, "fetch_etf_universe") as fetch:
            fetch.return_value = (eu._parse_rows({"0": _row("512890", "红利低波ETF", 300)}), False)
            first = eu.get_leaders(cache, "dividend")
            second = eu.get_leaders(cache, "dividend")
        self.assertEqual(fetch.call_count, 1)  # 第二次命中缓存
        self.assertEqual(first["leaders"][0]["code"], "512890")
        self.assertEqual(second, first)
        self.assertFalse(first["partial"])

    def test_get_leaders_cache_key_includes_params(self):
        cache = FakeCache()
        with mock.patch.object(eu, "fetch_etf_universe", return_value=([], False)) as fetch:
            eu.get_leaders(cache, "dividend", limit=10)
            eu.get_leaders(cache, "dividend", limit=20)
        self.assertEqual(fetch.call_count, 2)


class TestAPIRouter(unittest.TestCase):
    """D79 端点契约：鉴权 + 参数校验 + 响应结构。数据源打桩，不打网络。"""

    def test_endpoint_contract(self):
        import os
        os.environ['API_AUTH_TOKEN'] = 'test-token-etf-d79'
        from fastapi.testclient import TestClient

        from quant_engine.main import app

        with mock.patch.object(eu, "fetch_etf_universe",
                               return_value=(eu._parse_rows({"0": _row("512890", "红利低波ETF", 300)}), False)):
            client = TestClient(app)
            r = client.get('/api/etf/leaders?theme=dividend',
                           headers={'X-API-Key': 'test-token-etf-d79'})
            self.assertEqual(r.status_code, 200)
            data = r.json()
            self.assertEqual(data['leaders'][0]['code'], '512890')
            self.assertFalse(data['partial'])
            self.assertEqual(client.get('/api/etf/leaders?theme=bad',
                                        headers={'X-API-Key': 'test-token-etf-d79'}).status_code, 400)
            self.assertEqual(client.get('/api/etf/leaders').status_code, 401)


class TestZeroSignalImpact(unittest.TestCase):
    def test_no_dataframe_mutation_helpers(self):
        # 数据源模块不导入 pandas/numpy，不触碰 DataFrame → 趋势信号零影响的设计保证
        import ast
        tree = ast.parse(Path(eu.__file__).read_text())
        # 只看 import 节点与属性访问，不看文档字符串（避免注释性文字触发误报）
        imports = [ast.unparse(n) for n in ast.walk(tree)
                   if isinstance(n, (ast.Import, ast.ImportFrom))]
        attrs = [n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)]
        self.assertFalse(any("pandas" in i or "numpy" in i for i in imports), imports)
        self.assertNotIn("attrs", attrs)


if __name__ == "__main__":
    unittest.main()
