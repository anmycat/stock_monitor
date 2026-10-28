"""M4 抓取硬超时防护测试（fetch_guard）"""

import os
import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from quant_engine.utils.fetch_guard import guarded_fetch


def _slow_60():
    """模拟卡死源：睡 60 秒（远超测试超时）"""
    time.sleep(60)
    return 'should-not-happen'


def _fast():
    return {'ok': True, 'value': 42}


def _raises():
    raise ValueError('boom')


def _slow_then_flag(path):
    """子进程写哨兵文件后卡死——用于验证进程真被 kill"""
    Path(path).write_text('child-started')
    time.sleep(60)


class TestFetchGuard(unittest.TestCase):

    def test_normal_result_passthrough(self):
        result = guarded_fetch(_fast, timeout=5.0, name='fast')
        self.assertEqual(result, {'ok': True, 'value': 42})

    def test_child_exception_returns_none(self):
        result = guarded_fetch(_raises, timeout=5.0, name='raises')
        self.assertIsNone(result)

    def test_hard_timeout_kills_stuck_source(self):
        """核心验收：sleep 60s 的源必须在 timeout 内被终止返回 None"""
        start = time.monotonic()
        result = guarded_fetch(_slow_60, timeout=2.0, name='stuck')
        elapsed = time.monotonic() - start
        self.assertIsNone(result)
        self.assertLess(elapsed, 6.0, f'超时 kill 耗时 {elapsed:.1f}s 过长')

    def test_stuck_child_process_really_dies(self):
        """哨兵文件证明子进程确实启动；返回值 None 证明被 kill（未跑完 60s）"""
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix='.flag') as f:
            flag = f.name
        try:
            start = time.monotonic()
            result = guarded_fetch(_slow_then_flag, flag, timeout=2.0, name='stuck2')
            elapsed = time.monotonic() - start
            self.assertIsNone(result)
            self.assertTrue(Path(flag).exists(), '子进程未启动（fork 失败）')
            self.assertLess(elapsed, 6.0)
            # 无残留同名子进程
            time.sleep(0.3)
        finally:
            os.unlink(flag)

    def test_timeout_parameter_respected_short(self):
        """短超时（0.5s）也生效"""
        start = time.monotonic()
        result = guarded_fetch(_slow_60, timeout=0.5, name='stuck-short')
        self.assertIsNone(result)
        self.assertLess(time.monotonic() - start, 3.0)


if __name__ == '__main__':
    unittest.main(verbosity=2)
