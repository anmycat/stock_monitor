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


class TestHalfOpenProbe(unittest.TestCase):
    """S7/N-03：冷却/熔断结束 → HALF_OPEN 单次试探（2026-11-21 用户裁定启用）"""

    @staticmethod
    def _cooled_source():
        from quant_engine.data.source_manager import SourceStatus
        s = SourceStatus('demo')
        for _ in range(3):          # 3 次错误 → 冷却
            s.record_error(300)
        assert not s.is_available()
        assert s._needs_probe, '进入冷却后须标记「恢复需先试探」'
        s.cooldown_until = time.time() - 1  # 模拟冷却到期
        return s

    def test_probe_allowed_once_after_cooldown(self):
        s = self._cooled_source()
        allow, is_probe = s.try_probe()
        self.assertTrue(allow)
        self.assertTrue(is_probe)
        self.assertTrue(s.half_open, '冷却结束应进入半开态')
        # 探测在途 → 第二次调用拒绝并发
        allow2, is_probe2 = s.try_probe()
        self.assertFalse(allow2, '半开探测在途时不得放行第二次')
        self.assertTrue(is_probe2)

    def test_probe_success_closes_breaker(self):
        s = self._cooled_source()
        s.try_probe()
        s.record_success()
        self.assertFalse(s.half_open)
        self.assertFalse(s._needs_probe)
        self.assertEqual(s.cooldown_until, 0)
        self.assertEqual(s.quarantined_until, 0)
        self.assertTrue(s.is_available())

    def test_probe_failure_requarantines_immediately(self):
        s = self._cooled_source()
        s.try_probe()
        before = time.time()
        s.record_error(300, quarantine_seconds=1800)
        self.assertFalse(s.half_open)
        self.assertTrue(s._needs_probe)
        self.assertGreater(s.quarantined_until, before + 1700,
                           '探测失败应立即重新熔断（不等 3 次）')
        self.assertFalse(s.is_available())

    def test_probe_timeout_requarantines(self):
        s = self._cooled_source()
        s.try_probe(probe_timeout=0.0)   # 截止时间立即过期
        allow, is_probe = s.try_probe()  # 再次准入 → 判定超时重熔断
        self.assertFalse(allow)
        self.assertFalse(is_probe)
        self.assertGreater(s.quarantined_until, time.time())

    def test_quarantine_recovery_triggers_probe(self):
        from quant_engine.data.source_manager import SourceStatus
        s = SourceStatus('demo')
        for _ in range(9):
            s.record_error(300)
        self.assertGreater(s.quarantined_until, time.time())
        # 模拟熔断到期：熔断窗口(1800s)远长于冷却(300s)，到期时两窗口都已过期
        s.quarantined_until = time.time() - 1
        s.cooldown_until = time.time() - 1
        allow, is_probe = s.try_probe()
        self.assertTrue(allow)
        self.assertTrue(is_probe, '熔断结束应走半开探测，而非直接全量放行')

    def test_healthy_source_never_probes(self):
        from quant_engine.data.source_manager import SourceStatus
        s = SourceStatus('demo')
        allow, is_probe = s.try_probe()
        self.assertTrue(allow)
        self.assertFalse(is_probe, '健康源应正常放行、不进入半开')

    def test_execute_dispatch_forces_single_probe_attempt(self):
        """半开探测只打一次（不受默认 max_retries=2 放大）"""
        sm = SourceManager()
        sm.register_source('demo')
        src = sm.sources['demo']
        for _ in range(3):
            src.record_error(300)
        src.cooldown_until = time.time() - 1
        calls = []

        def _bad():
            calls.append(1)
            raise RuntimeError('still down')

        res = sm.execute_with_retry('demo', _bad, max_retries=2, skip_delay=True)
        self.assertIsNone(res)
        self.assertEqual(len(calls), 1, '探测命中即止，不重试')
        self.assertFalse(src.half_open)
        self.assertFalse(src.is_available())

    def test_status_report_exposes_half_open(self):
        sm = SourceManager()
        sm.register_source('demo')
        sm.sources['demo'].record_error(300)
        report = sm.get_status_report()
        self.assertIn('half_open', report['demo'])
        self.assertIn('needs_probe', report['demo'])


if __name__ == '__main__':
    unittest.main()
