"""PostEvaluation SQLite 双轨测试（docs/25 §5.1，2026-10-25）

覆盖：save_run 双写归档 / runs:list 丢失时列表回退 / run 键过期时
evaluate_run 回退快照 / evaluate_run 归档评估结果。
fixture 隔离：FakeCache + 临时 SQLite，不打网络。
"""
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.backtest.post_evaluation import PostEvaluation
from quant_engine.data.store import HistoryStore


class FakeCache:
    def __init__(self):
        self._data = {}

    def get(self, key):
        return self._data.get(key)

    def set(self, key, value, ttl=0):
        self._data[key] = value
        return True


class FakeDatacenter:
    """evaluate_run 取后续日K用：T 日之后 3 天，收盘 10.5→11.0"""

    def get_daily_data(self, code, days):
        return pd.DataFrame({
            'trade_date': ['20261020', '20261021', '20261022', '20261023'],
            'close': [10.5, 10.8, 11.0, 11.2],
        })


class PostEvalSQLiteTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = HistoryStore(db_path=str(Path(self.tmp.name) / 'history.db'))
        # 将 post_evaluation 模块内的单例工厂指向临时库
        patcher = patch('quant_engine.backtest.post_evaluation.get_history_store',
                        return_value=self.store)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.pe = PostEvaluation(FakeCache(), FakeDatacenter())

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def _signals(self):
        return [{'code': '000001.SZ', 'name': '平安银行', 'action': 'BUY',
                 'price': 10.5, 'score': 72.0,
                 'timestamp': '2026-10-20T15:05:00'}]

    def test_save_run_archives_to_sqlite(self):
        ok = self.pe.save_run('run_20261020', self._signals(),
                              {'type': 'neutral', 'score': 55.0})
        self.assertTrue(ok)
        archived = self.store.get_eval_run('run_20261020')
        self.assertIsNotNone(archived)
        self.assertEqual(archived['signals'][0]['code'], '000001.SZ')
        self.assertEqual(archived['market_state']['type'], 'neutral')

    def test_run_list_falls_back_when_redis_list_lost(self):
        self.pe.save_run('run_20261020', self._signals(),
                         {'type': 'neutral', 'score': 55.0})
        # 模拟 Redis runs:list 丢失（TTL 过期/被清）
        self.pe.cache._data.pop('runs:list', None)
        self.pe.cache._data.pop('run:run_20261020', None)
        runs = self.pe.get_run_list(20)
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]['run_id'], 'run_20261020')
        self.assertEqual(runs[0]['total_signals'], 1)
        self.assertEqual(runs[0]['market_state'], 'neutral')

    def test_evaluate_run_falls_back_to_archive(self):
        self.pe.save_run('run_20261020', self._signals(),
                         {'type': 'neutral', 'score': 55.0})
        # Redis 双侧清空 → 评估应走 SQLite 快照
        self.pe.cache._data.pop('run:run_20261020', None)
        result = self.pe.evaluate_run('run_20261020')
        self.assertNotIn('error', result)
        self.assertEqual(result['total_signals'], 1)
        self.assertEqual(result['win_count'], 1)       # 11.2 > 10.5
        self.assertAlmostEqual(result['avg_pnl'], (11.2 - 10.5) / 10.5, places=6)
        # 评估结果已归档
        archived = self.store.get_eval_result('run_20261020')
        self.assertIsNotNone(archived)
        self.assertEqual(archived['win_count'], 1)

    def test_evaluate_run_missing_still_errors(self):
        result = self.pe.evaluate_run('run_missing')
        self.assertIn('error', result)


if __name__ == '__main__':
    unittest.main()
