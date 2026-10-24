"""In-flight 请求合并测试（docs/25 §5.2，2026-10-25）

覆盖：同键并发单次执行 / 不同键各自执行 / leader 异常 follower 自取 /
等待超时 follower 自取 / None 结果复用 / 统计计数。
"""
import sys
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.data.inflight import InflightMerger


class InflightMergerTests(unittest.TestCase):
    def test_concurrent_same_key_single_execution(self):
        """4 线程同键并发 → fn 只执行 1 次，全部拿到同一结果"""
        merger = InflightMerger(timeout=5)
        calls = []
        results = []
        barrier = threading.Barrier(4)

        def fn():
            calls.append(1)
            time.sleep(0.2)
            return 'data'

        def worker():
            barrier.wait()
            results.append(merger.run('k', fn))

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        self.assertEqual(len(calls), 1)
        self.assertEqual(results, ['data'] * 4)
        self.assertEqual(merger.stats['merged'], 3)
        self.assertEqual(merger.inflight_count(), 0)

    def test_different_keys_both_execute(self):
        """不同键不合并，各自执行"""
        merger = InflightMerger(timeout=5)
        calls = []

        def fn():
            calls.append(1)
            return 'x'

        merger.run('a', fn)
        merger.run('b', fn)
        self.assertEqual(len(calls), 2)
        self.assertEqual(merger.stats['merged'], 0)

    def test_leader_error_followers_fetch_themselves(self):
        """leader 抛异常 → 异常向 leader 传播，follower 自行取数成功"""
        merger = InflightMerger(timeout=5)
        state = {'n': 0}
        results = []
        errors = []
        barrier = threading.Barrier(2)

        def fn():
            state['n'] += 1
            if state['n'] == 1:
                time.sleep(0.1)
                raise ValueError('transient')
            time.sleep(0.1)
            return 'ok'

        def worker():
            barrier.wait()
            try:
                results.append(merger.run('k', fn))
            except ValueError as e:
                errors.append(str(e))

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        self.assertEqual(state['n'], 2)          # leader + follower 各取一次
        self.assertEqual(results, ['ok'])
        self.assertEqual(errors, ['transient'])
        self.assertEqual(merger.stats['leader_errors'], 1)
        self.assertEqual(merger.stats['passed'], 1)

    def test_timeout_follower_fetches(self):
        """leader 挂起超过超时 → follower 自行取数（不无限等待）"""
        merger = InflightMerger(timeout=0.2)
        calls = []
        gate = threading.Event()

        def slow_fn():
            calls.append(1)
            gate.wait(timeout=5)
            return 'slow'

        def fast_fn():
            calls.append('fast')
            return 'fast'

        leader = threading.Thread(target=lambda: merger.run('k', slow_fn))
        leader.start()
        time.sleep(0.05)  # 确保 leader 先注册
        start = time.monotonic()
        result = merger.run('k', fast_fn)
        elapsed = time.monotonic() - start
        gate.set()
        leader.join(timeout=5)

        self.assertEqual(result, 'fast')
        self.assertLess(elapsed, 1.0)             # 未被 leader 挂起拖死
        self.assertEqual(calls.count('fast'), 1)  # follower 自行取数
        self.assertEqual(merger.stats['timeouts'], 1)

    def test_none_result_reused(self):
        """leader 返回 None（无数据）→ follower 复用 None，不重复取数"""
        merger = InflightMerger(timeout=5)
        calls = []
        barrier = threading.Barrier(2)
        results = []

        def fn():
            calls.append(1)
            time.sleep(0.15)
            # 隐式返回 None：验证 None 结果也被正确复用（非 _UNSET 误判）

        def worker():
            barrier.wait()
            results.append(merger.run('k', fn))

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        self.assertEqual(len(calls), 1)
        self.assertEqual(results, [None, None])
        self.assertEqual(merger.stats['merged'], 1)


if __name__ == '__main__':
    unittest.main()
