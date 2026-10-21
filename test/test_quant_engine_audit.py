import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.backtest.engine import BacktestEngine
from quant_engine.data.repositories import MarketDataRepository
from quant_engine.data.source_manager import SourceManager


class FakeCache:
    def __init__(self):
        self._data = {}

    def get(self, key):
        return self._data.get(key)

    def set(self, key, value, ttl=0):
        self._data[key] = value
        return True


class FakeSource:
    def __init__(self, frame):
        self.frame = frame

    def get_realtime_quote(self, codes):
        return self.frame[self.frame["code"].isin(codes)].reset_index(drop=True)


class FakeDataCenter:
    def __init__(self, frame):
        self.frame = frame

    def get_daily_data(self, code, days):
        return self.frame.copy()


class DataCenterAuditTests(unittest.TestCase):
    def test_get_realtime_quote_caches_records(self):
        frame = pd.DataFrame(
            [
                {"code": "000001.SZ", "name": "Ping An", "price": 10.5, "change_pct": 1.2, "volume": 1000},
                {"code": "600000.SH", "name": "Pufa", "price": 8.1, "change_pct": -0.4, "volume": 2000},
            ]
        )
        dc = MarketDataRepository.__new__(MarketDataRepository)
        dc.cache = FakeCache()
        dc.source = FakeSource(frame)
        dc._cache_lock = None
        dc._all_quotes_cache = None
        dc._all_quotes_cache_time = 0
        dc.get_cached_quote = MarketDataRepository.get_cached_quote.__get__(dc, MarketDataRepository)
        dc.set_cached_quote = MarketDataRepository.set_cached_quote.__get__(dc, MarketDataRepository)

        result = MarketDataRepository.get_realtime_quote(dc, ["000001.SZ", "600000.SH"])

        self.assertEqual(len(result), 2)
        self.assertEqual(dc.cache.get("quote:000001.SZ")["code"], "000001.SZ")
        self.assertEqual(dc.cache.get("quote:600000.SH")["name"], "Pufa")

    def test_source_manager_rejects_empty_dataframe(self):
        manager = SourceManager()
        manager.register_source("test")

        def empty_func():
            return pd.DataFrame()

        result = manager.execute_with_retry("test", empty_func, max_retries=1, skip_delay=True)

        self.assertIsNone(result)
        self.assertEqual(manager.sources["test"].error_count, 1)


class BacktestEngineAuditTests(unittest.TestCase):
    def test_run_uses_initial_capital_argument(self):
        frame = pd.DataFrame(
            {
                "trade_date": ["20240101", "20240102", "20240103", "20240104", "20240105", "20240106"],
                "close": [10, 10.5, 10.8, 11.0, 11.2, 11.5],
            }
        )
        captured = []

        class DummyDispatcher:
            def __init__(self, datacenter):
                pass

        with patch("quant_engine.backtest.engine.StrategyDispatcher", DummyDispatcher):
            engine = BacktestEngine(FakeDataCenter(frame), initial_capital=100000)

        def fake_backtest(df, start_date, end_date, capital):
            captured.append(capital)
            return [], [capital, capital + 1000]

        engine._backtest_stock = fake_backtest

        result = engine.run(["000001.SZ"], "20240101", "20240131", initial_capital=500000)

        self.assertEqual(captured, [500000])
        self.assertEqual(result["equity_curve"][0], 500000)
        self.assertEqual(result["codes"], ["000001.SZ"])


if __name__ == "__main__":
    unittest.main()
