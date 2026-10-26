"""股票池市场过滤测试（D44，修复 920285.BJ 池污染）

覆盖：get_stock_list 过滤 .BJ（拉取路径 + 缓存命中路径双覆盖）/
非 BJ 代码原样保留 / 空表与缺列不炸。
fixture 隔离：__new__ 替身 + FakeCache/FakeSource，不打网络
（MarketDataRepository.__init__ 会建 Redis 连接，参考
test_inflight_repo 的替身构造模式）。
"""
import sys
import threading
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.data.repositories import MarketDataRepository


class FakeCache:
    def __init__(self, initial=None):
        self._data = dict(initial or {})

    def get(self, key):
        return self._data.get(key)

    def set(self, key, value, ttl=0):
        self._data[key] = value
        return True


class ListSource:
    def __init__(self, df):
        self.df = df
        self.list_calls = 0

    def get_stock_list(self):
        self.list_calls += 1
        return self.df


def _pool_df():
    return pd.DataFrame([
        {'code': '000001.SZ', 'name': '平安银行', 'industry': ''},
        {'code': '600519.SH', 'name': '贵州茅台', 'industry': ''},
        {'code': '920285.BJ', 'name': '某某北交所', 'industry': ''},
        {'code': '920091.BJ', 'name': '另一只北交所', 'industry': ''},
    ])


def _make_repo(source, cache=None):
    dc = MarketDataRepository.__new__(MarketDataRepository)
    dc.cache = cache if cache is not None else FakeCache()
    dc.source = source
    dc._cache_lock = threading.Lock()
    dc._all_quotes_cache = None
    dc._all_quotes_cache_time = 0
    return dc


class TestStockListFilter(unittest.TestCase):
    def test_fetch_path_filters_bj(self):
        """拉取路径：.BJ 被过滤，沪深原样保留"""
        source = ListSource(_pool_df())
        dc = _make_repo(source)
        df = dc.get_stock_list()
        self.assertEqual(set(df['code']), {'000001.SZ', '600519.SH'})
        self.assertEqual(source.list_calls, 1)

    def test_cache_hit_path_filters_bj(self):
        """缓存命中路径：存量缓存（部署前含 BJ）也要过滤"""
        records = _pool_df().to_dict(orient='records')
        source = ListSource(_pool_df())
        dc = _make_repo(source, cache=FakeCache({'stock_list': records}))
        df = dc.get_stock_list()
        self.assertEqual(set(df['code']), {'000001.SZ', '600519.SH'})
        source.get_stock_list = None  # 缓存命中不应触发源（下方断言）
        self.assertEqual(source.list_calls, 0)

    def test_empty_pool_passthrough(self):
        """空池不炸（缓存穿透时源可能返回空）"""
        dc = _make_repo(ListSource(pd.DataFrame()))
        df = dc.get_stock_list()
        self.assertTrue(df.empty)

    def test_all_bj_pool_becomes_empty(self):
        """极端：池全为 BJ → 过滤后为空（不抛异常）"""
        dc = _make_repo(ListSource(pd.DataFrame([
            {'code': '920285.BJ', 'name': '北交所', 'industry': ''},
        ])))
        df = dc.get_stock_list()
        self.assertTrue(df.empty)

    def test_filter_markets_static(self):
        """静态方法直测：缺 code 列时原样返回"""
        df = pd.DataFrame([{'symbol': 'x'}])
        out = MarketDataRepository._filter_markets(df)
        self.assertEqual(len(out), 1)


if __name__ == '__main__':
    unittest.main()
