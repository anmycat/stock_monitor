"""数据源管理器健壮性测试（D53，2026-11-01）

覆盖：
1. 空 DataFrame 不再被当成源故障（原实现 raise ValueError → error_count++
   → 连续 3 次让整个源进 5 分钟冷却，主源被误伤）
2. 真实异常仍按 3 次进冷却
3. 连续 3 轮冷却未恢复 → 熔断 30 分钟（期间完全跳过请求）
4. 熔断后 execute_with_retry 不再调用源函数（省掉网络超时）
"""
import sys
import time
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.data.source_manager import SourceManager


class TestEmptyNotError(unittest.TestCase):
    """空结果 ≠ 源故障"""

    def setUp(self):
        self.sm = SourceManager()
        self.sm.register_source('demo')

    def test_empty_dataframe_returns_none_without_error(self):
        sm = self.sm
        sm.execute_with_retry('demo', lambda: pd.DataFrame(), skip_delay=True)
        src = sm.sources['demo']
        self.assertEqual(src.error_count, 0, '空结果不应计入错误')
        self.assertEqual(src.empty_count, 1)
        self.assertTrue(src.is_available(), '单次空结果不应触发冷却')

    def test_empty_does_not_retry(self):
        """空结果不重试（省掉每次 1s sleep + 重复请求）"""
        calls = []

        def _empty():
            calls.append(1)
            return pd.DataFrame()

        self.sm.execute_with_retry('demo', _empty, max_retries=3, skip_delay=True)
        self.assertEqual(len(calls), 1, '空结果不应触发重试')

    def test_repeated_empty_escalates_to_cooldown(self):
        """连续空结果达阈值 → 升级为冷却"""
        sm = self.sm
        for _ in range(10):
            sm.execute_with_retry('demo', lambda: pd.DataFrame(), skip_delay=True)
        src = sm.sources['demo']
        self.assertFalse(src.is_available(), '连续空结果应触发冷却')
        self.assertGreater(src.error_count, 0)

    def test_success_resets_counters(self):
        sm = self.sm
        sm.execute_with_retry('demo', lambda: pd.DataFrame(), skip_delay=True)
        sm.execute_with_retry('demo', lambda: pd.DataFrame({'a': [1]}), skip_delay=True)
        src = sm.sources['demo']
        self.assertEqual(src.empty_count, 0)
        self.assertEqual(src.error_count, 0)


class TestErrorCooldown(unittest.TestCase):
    """真实异常 → 冷却 → 熔断"""

    def setUp(self):
        self.sm = SourceManager()
        self.sm.register_source('demo')

    def test_real_error_cooldown_after_three(self):
        sm = self.sm
        for _ in range(3):
            sm.execute_with_retry('demo', _raiser, max_retries=1, skip_delay=True)
        src = sm.sources['demo']
        self.assertGreaterEqual(src.error_count, 3)
        self.assertFalse(src.is_available(), '3 次真实异常应进入冷却')

    def test_quarantine_after_three_cooldown_rounds(self):
        """连续 3 轮冷却未恢复 → 熔断"""
        src = self.sm.sources['demo']
        for _ in range(9):  # 3 轮 × 3 次
            src.record_error(300)
        self.assertGreater(src.quarantined_until, time.time(), '应进入熔断')
        self.assertFalse(src.is_available())

    def test_quarantined_source_is_not_called(self):
        """熔断后 execute_with_retry 不再调用源函数"""
        sm = self.sm
        src = sm.sources['demo']
        src.quarantined_until = time.time() + 1800
        calls = []

        def _ok():
            calls.append(1)
            return pd.DataFrame({'a': [1]})

        result = sm.execute_with_retry('demo', _ok, skip_delay=True)
        self.assertIsNone(result)
        self.assertEqual(len(calls), 0, '熔断期间不应发起请求')

    def test_success_resets_cooldown_streak(self):
        src = self.sm.sources['demo']
        src.record_error(300)
        src.record_error(300)
        src.record_success()
        self.assertEqual(src._cooldown_streak, 0)
        self.assertEqual(src.error_count, 0)

    def test_status_report_includes_quarantine(self):
        report = self.sm.get_status_report()
        self.assertIn('quarantined', report['demo'])
        self.assertIn('empty_count', report['demo'])


def _raiser():
    raise RuntimeError('network down')


if __name__ == '__main__':
    unittest.main()
