"""通知流量控制测试（D44，修复钉钉 660026 限流）

覆盖：最小发送间隔（3s 内第二票进缓冲）/ flush 合并为摘要发出 /
每分钟硬上限（窗口满进缓冲）/ 市场告警立即发 / 死锁防护
（send_market_alert 持锁路径不再调 send）/ flush 窗口满时保留到下轮。
fixture：mock notifier 替换真实渠道，不打网络。
"""
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.models.market import MarketState, MarketType
from quant_engine.models.signal import Signal, SignalAction
from quant_engine.services.notifications.manager import NotificationManager


def _signal(code='000001.SZ', name='平安银行', action=SignalAction.BUY,
            score=72.0):
    return Signal(
        code=code, name=name, action=action, level=None, score=score,
        position=0.15, reason=['测试'], strategy='趋势主升', price=10.5,
        target_price=11.0, stop_loss=9.8, timestamp=__import__('pandas').Timestamp.now(),
    )


def _market():
    return MarketState(
        type=MarketType.SIDEWAYS, score=50.0, trend=0.0, volume_ratio=1.0,
        breadth=0.5, timestamp=__import__('pandas').Timestamp.now(),
    )


class TestNotifierThrottle(unittest.TestCase):
    def setUp(self):
        self.manager = NotificationManager()
        self.mock_channel = MagicMock()
        self.mock_channel.name = 'MockChannel'
        self.mock_channel.send = MagicMock(return_value=True)
        self.manager.notifiers = [self.mock_channel]
        # 缩短最小间隔便于测试
        self.manager._min_interval = 3.0
        self.manager._max_per_minute = 18

    def test_first_signal_sent_immediately(self):
        ok = self.manager.send_signal(_signal('000001.SZ'))
        self.assertTrue(ok)
        self.assertEqual(self.mock_channel.send.call_count, 1)

    def test_second_signal_within_interval_buffered(self):
        self.manager.send_signal(_signal('000001.SZ'))
        ok = self.manager.send_signal(_signal('000002.SZ'))
        self.assertFalse(ok)  # 3s 内第二票进缓冲
        self.assertEqual(len(self.manager._pending_signals), 1)
        self.assertEqual(self.mock_channel.send.call_count, 1)

    def test_flush_merges_signals_into_digest(self):
        self.manager.send_signal(_signal('000001.SZ'))
        self.manager.send_signal(_signal('000002.SZ', name='万科A'))
        self.manager.send_signal(_signal('600519.SH', name='贵州茅台'))
        self.assertEqual(len(self.manager._pending_signals), 2)
        sent = self.manager.flush_signals()
        self.assertEqual(sent, 1)
        # 合并为一条摘要（而非 3 条独立消息）
        self.assertEqual(self.mock_channel.send.call_count, 2)
        title, content = self.mock_channel.send.call_args[0]
        self.assertIn('信号汇总', title)
        self.assertIn('000002.SZ', content)
        self.assertIn('600519.SH', content)
        self.assertEqual(len(self.manager._pending_signals), 0)

    def test_custom_message_throttled_and_flushed(self):
        self.manager.send_custom_message('异动1', '内容1')
        ok = self.manager.send_custom_message('异动2', '内容2')
        self.assertFalse(ok)
        self.assertEqual(len(self.manager._pending_texts), 1)
        self.manager.flush_signals()
        title, content = self.mock_channel.send.call_args[0]
        self.assertIn('消息汇总', title)
        self.assertIn('异动2', content)

    def test_max_per_minute_hard_cap(self):
        self.manager._max_per_minute = 2
        self.manager._min_interval = 0  # 隔离最小间隔，单独测窗口
        for i in range(3):
            self.manager.send_signal(_signal(f'00000{i}.SZ'))
        # 前 2 票立即发，第 3 票进缓冲
        self.assertEqual(self.mock_channel.send.call_count, 2)
        self.assertEqual(len(self.manager._pending_signals), 1)

    def test_flush_deferred_when_window_full(self):
        """flush 时窗口仍满 → 消息保留到下一轮，不丢失"""
        self.manager._max_per_minute = 1
        self.manager._min_interval = 0
        self.manager.send_signal(_signal('000001.SZ'))  # 立即发（占满窗口）
        self.manager.send_signal(_signal('000002.SZ'))  # 进缓冲
        sent = self.manager.flush_signals()
        self.assertEqual(sent, 0)
        self.assertEqual(len(self.manager._pending_signals), 1)  # 保留

    def test_market_alert_sent_immediately_no_deadlock(self):
        """市场告警立即发；且持锁路径不得调 send（非重入锁死锁防护）"""
        # 先发一票信号推进 _last_sent，市场告警应仍立即发出
        self.manager.send_signal(_signal('000001.SZ'))
        self.manager.send_market_alert(_market())
        self.assertEqual(self.mock_channel.send.call_count, 2)

    def test_market_alert_buffered_when_window_full(self):
        """窗口满时市场告警进缓冲（不再调 send）"""
        self.manager._max_per_minute = 1
        self.manager._min_interval = 0
        self.manager.send_signal(_signal('000001.SZ'))
        self.manager.send_market_alert(_market())
        self.assertEqual(len(self.manager._pending_texts), 1)
        # 窗口满后没有新的 send 调用（仍停在信号那 1 次）
        self.assertEqual(self.mock_channel.send.call_count, 1)

    def test_empty_flush_noop(self):
        sent = self.manager.flush_signals()
        self.assertEqual(sent, 0)
        self.assertEqual(self.mock_channel.send.call_count, 0)

    def test_window_resets_after_60s(self):
        import time as _time
        self.manager._min_interval = 0
        self.manager._max_per_minute = 1
        self.manager.send_signal(_signal('000001.SZ'))
        # 模拟 61 秒后：窗口应重置，可再发
        self.manager._window_start = _time.monotonic() - 61
        ok = self.manager.send_signal(_signal('000002.SZ'))
        self.assertTrue(ok)
        self.assertEqual(self.mock_channel.send.call_count, 2)


if __name__ == '__main__':
    unittest.main()
