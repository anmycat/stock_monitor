"""MarketDataRepository in-flight 合并集成测试（docs/25 §5.2，2026-10-25）

覆盖：同代码并发日K取数只打一次源 / 首查后缓存命中不打源 /
minute/stock_list 同样合并 / __new__ 替身惰性创建合并器。
fixture 隔离：FakeCache + FakeSource，不打网络。
"""
import sys
import threading
import time
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.data.repositories import MarketDataRepository


class FakeCache:
    def __init__(self):
        self._data = {}

    def get(self, key):
        return self._data.get(key)

    def set(self, key, value, ttl=0):
        self._data[key] = value
        return True


class CountingSource:
    def __init__(self):
        self.daily_calls = 0
        self.minute_calls = 0
        self.list_calls = 0
        self._lock = threading.Lock()

    def get_daily_data(self, code, days):
        with self._lock:
            self.daily_calls += 1
        time.sleep(0.2)  # 放大并发窗口
        return pd.DataFrame({
            'trade_date': ['20261020', '20261021', '20261022'],
            'close': [10.5, 10.8, 11.0],
        })

    def get_minute_data(self, code, period):
        with self._lock:
            self.minute_calls += 1
        time.sleep(0.2)
        return pd.DataFrame({'trade_time': ['0935', '0940'], 'close': [10.5, 10.6]})

    def get_stock_list(self):
        with self._lock:
            self.list_calls += 1
        time.sleep(0.2)
        return pd.DataFrame({'code': ['000001.SZ', '600000.SH'],
                             'name': ['平安银行', '浦发银行']})


def _make_repo(source):
    dc = MarketDataRepository.__new__(MarketDataRepository)
    dc.cache = FakeCache()
    dc.source = source
    dc._cache_lock = threading.Lock()
    dc._all_quotes_cache = None
    dc._all_quotes_cache_time = 0
    return dc


class InflightRepoTests(unittest.TestCase):
    def test_concurrent_daily_data_single_fetch(self):
        """同代码并发日K → 源只打一次，两线程都拿到数据"""
        source = CountingSource()
        dc = _make_repo(source)
        results = []
        barrier = threading.Barrier(2)

        def worker():
            barrier.wait()
            results.append(dc.get_daily_data('000001.SZ', 120))

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        self.assertEqual(source.daily_calls, 1)
        self.assertEqual(len(results), 2)
        for df in results:
            self.assertFalse(df.empty)
            self.assertEqual(len(df), 3)
        # 结果已落缓存
        self.assertIsNotNone(dc.cache.get('daily:000001.SZ:120'))

    def test_second_call_hits_cache_without_source(self):
        source = CountingSource()
        dc = _make_repo(source)
        dc.get_daily_data('000001.SZ', 120)
        dc.get_daily_data('000001.SZ', 120)
        self.assertEqual(source.daily_calls, 1)

    def test_concurrent_minute_and_stock_list_merged(self):
        source = CountingSource()
        dc = _make_repo(source)
        results = []
        barrier = threading.Barrier(3)

        def worker(i):
            barrier.wait()
            if i < 2:
                results.append(dc.get_minute_data('000001.SZ', '5'))
            else:
                results.append(dc.get_stock_list())

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        self.assertEqual(source.minute_calls, 1)
        self.assertEqual(source.list_calls, 1)
        self.assertTrue(all(not r.empty for r in results))

    def test_new_constructed_double_gets_merger_lazily(self):
        """__new__ 替身未设 _inflight → 访问器惰性创建且可用"""
        source = CountingSource()
        dc = _make_repo(source)
        self.assertFalse(hasattr(dc, '_inflight'))
        merger = dc._merger()
        self.assertIsNotNone(merger)
        self.assertIs(dc._inflight, merger)  # 已缓存到实例


if __name__ == '__main__':
    unittest.main()
