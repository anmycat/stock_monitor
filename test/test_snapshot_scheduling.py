"""D58 快照采集调度修复测试

D53 遗留缺陷（生产日志实证）：快照窗口仅 5~10 分钟，worker 交易时段是
「300s sleep + 扫描耗时」循环（实际周期 6~7 分钟），纯靠轮询碰运气命中
窗口 → 11-03 早盘与全日总结窗口全部错过 → 12:10/18:10 走"快照缺失"
降级路径，在非交易时段触发 get_sector_data（256 个实时报价请求）。

D58 修复三要素：
① 窗口放宽到 10 分钟且完全落在交易时段内
② market+signals 每轮都存（零额外成本），sectors 只在窗口内采一次
③ 睡眠夹到"下一个快照窗口起点"，保证必然醒来采集
"""
import os
import sys
import unittest
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from quant_engine.utils.session_gate import (
    T_DAILY_SNAPSHOT,
    T_MORNING_SNAPSHOT,
    in_snapshot_window,
    next_snapshot_at,
    seconds_until,
    snapshot_pending,
)


class TestSnapshotWindows(unittest.TestCase):
    """窗口定义"""

    def test_morning_window_inside_morning_session(self):
        """早盘窗口必须在 MORNING_SESSION（9:30-11:30）内"""
        start, end = T_MORNING_SNAPSHOT
        self.assertGreaterEqual(start, 9 * 60 + 30)
        self.assertLessEqual(end, 11 * 60 + 30)

    def test_daily_window_fully_inside_afternoon_session(self):
        """D58 修正：daily 上界收到 15:00

        15:00 后 phase 转 AFTER_HOURS，不再走交易时段分支 —— 窗口
        超出 15:00 等于设计了一个永远采不到的死区。
        """
        start, end = T_DAILY_SNAPSHOT
        self.assertGreaterEqual(start, 13 * 60)
        self.assertLessEqual(end, 15 * 60)

    def test_windows_are_wide_enough(self):
        """窗口至少 10 分钟（原设计仅 5 分钟）"""
        for name, win in (('morning', T_MORNING_SNAPSHOT),
                          ('daily', T_DAILY_SNAPSHOT)):
            self.assertGreaterEqual(win[1] - win[0], 10,
                                    f'{name} 窗口过窄（{win[1] - win[0]} 分钟）')


class TestNextSnapshotAt(unittest.TestCase):
    """next_snapshot_at：强制唤醒点计算"""

    def test_returns_window_start_before_window(self):
        now = datetime(2026, 11, 3, 10, 0)
        target = next_snapshot_at('morning', now)
        self.assertIsNotNone(target)
        self.assertEqual((target.hour, target.minute),
                         (T_MORNING_SNAPSHOT[0] // 60,
                          T_MORNING_SNAPSHOT[0] % 60))

    def test_returns_none_after_window_passed(self):
        """窗口已过 → None（等下一交易日，不重复采）"""
        now = datetime(2026, 11, 3, 15, 30)
        self.assertIsNone(next_snapshot_at('daily', now))
        self.assertIsNone(next_snapshot_at('morning', now))

    def test_inside_window_returns_window_start(self):
        """窗口内 → 仍返回窗口起点

        seconds_until 有 1 秒下限（防 0 秒忙等），所以窗口内调用返回 1 秒，
        worker 会在 1 秒后醒来重新检查 —— 此时已在窗口内，必然采到。
        """
        now = datetime(2026, 11, 3, 14, 55)
        target = next_snapshot_at('daily', now)
        self.assertIsNotNone(target)
        self.assertEqual(seconds_until(target), 1)


class TestSnapshotPending(unittest.TestCase):
    """snapshot_pending：是否需要采集"""

    def test_pending_inside_window_when_not_collected(self):
        now = datetime(2026, 11, 3, 14, 55)
        self.assertTrue(snapshot_pending('daily', False, now))

    def test_not_pending_when_already_collected(self):
        """已采过就不重复采（sectors 采集有 256 个请求的开销）"""
        now = datetime(2026, 11, 3, 14, 55)
        self.assertFalse(snapshot_pending('daily', True, now))

    def test_not_pending_outside_window(self):
        now = datetime(2026, 11, 3, 10, 0)
        self.assertFalse(snapshot_pending('morning', False, now))


class TestCollectionReliability(unittest.TestCase):
    """核心回归：穷举起始相位 × 扫描耗时，验证采集成功率 100%"""

    @staticmethod
    def _simulate(start, end, kind, scan_cost):
        """模拟 worker 循环：每轮检查窗口，睡眠夹到窗口起点"""
        now = start
        collected = False
        while now < end:
            if in_snapshot_window(kind, now):
                collected = True
                break
            sleep_sec = 300
            for k in ('morning', 'daily'):
                target = next_snapshot_at(k, now)
                if target is not None:
                    sleep_sec = min(sleep_sec, max(1, seconds_until(target)))
            now = now + timedelta(seconds=sleep_sec + scan_cost)
        return collected

    def test_always_collects_regardless_of_phase_and_cost(self):
        """任意起始相位 + 任意扫描耗时 → 必须采到

        D53 原设计在扫描耗时 300s 时成功率仅 5/20、420s 时 0/20
        （生产实测 11-03 两个窗口全部错过）。
        """
        for scan_cost in (60, 90, 150, 300, 420, 600):
            for offset in range(0, 400, 20):
                m_ok = self._simulate(
                    datetime(2026, 11, 3, 9, 30) + timedelta(seconds=offset),
                    datetime(2026, 11, 3, 11, 30), 'morning', scan_cost)
                d_ok = self._simulate(
                    datetime(2026, 11, 3, 13, 0) + timedelta(seconds=offset),
                    datetime(2026, 11, 3, 15, 0), 'daily', scan_cost)
                self.assertTrue(m_ok, f'morning 采集失败 scan={scan_cost}s offset={offset}s')
                self.assertTrue(d_ok, f'daily 采集失败 scan={scan_cost}s offset={offset}s')


if __name__ == '__main__':
    unittest.main()
