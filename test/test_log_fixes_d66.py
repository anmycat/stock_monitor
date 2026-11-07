"""D66 日志问题修复的回归测试（2026-11-06）

ECS 近 7 天日志暴露的三类真实故障，逐一钉死：
1. U2 空正文：配置 2500 被 ensemble 默认 1500 的 min() 砍掉
2. 钉钉 410100 雪崩：节流时钟只在发送成功时推进，连续失败 → 逐条硬发
3. 日历越界：2026 年 XSHG 日历已失效，next_session 抛异常
4. 渠道失败丢消息：放行后发送失败应退回聚合缓冲
"""
import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.services.ai.ai_summary import AISummary, ModelConfig
from quant_engine.services.notifications.manager import (
    DingTalkNotifier,
    NotificationManager,
)
from quant_engine.utils import calendar as cal


class TestU2TokenBudget(unittest.TestCase):
    """① U2 max_tokens 必须真正用上配置值"""

    @staticmethod
    def _cfg(max_tokens=2500):
        return ModelConfig(
            name='云知声 U2', model_id='u2', api_key='sk-test',
            base_url='https://example.invalid/v1', max_tokens=max_tokens,
            supports_thinking=True)

    def test_default_call_uses_model_budget_not_hardcoded_1500(self):
        """ensemble/cascade 不传 max_tokens 时，payload 应为 2500 而非 1500"""
        cfg = self._cfg(2500)
        resp = mock.MagicMock(status_code=200)
        resp.json.return_value = {
            'choices': [{'message': {'content': 'OK'}}],
            'usage': {'total_tokens': 5}}
        s = AISummary.__new__(AISummary)
        s.models = {cfg.name: cfg}
        s.primary_model = cfg.name
        with mock.patch('httpx.Client') as client:
            client.return_value.__enter__.return_value.post.return_value = resp
            s._call_single_model(cfg, 'sys', 'user')
            payload = (client.return_value.__enter__.return_value
                       .post.call_args.kwargs['json'])
        self.assertEqual(payload['max_tokens'], 2500)

    def test_explicit_lower_budget_still_caps(self):
        """调用方显式给更小预算时仍以模型上限封顶"""
        cfg = self._cfg(800)
        resp = mock.MagicMock(status_code=200)
        resp.json.return_value = {
            'choices': [{'message': {'content': 'OK'}}],
            'usage': {'total_tokens': 5}}
        s = AISummary.__new__(AISummary)
        s.models = {cfg.name: cfg}
        s.primary_model = cfg.name
        with mock.patch('httpx.Client') as client:
            client.return_value.__enter__.return_value.post.return_value = resp
            s._call_single_model(cfg, 'sys', 'user', max_tokens=1500)
            payload = (client.return_value.__enter__.return_value
                       .post.call_args.kwargs['json'])
        self.assertEqual(payload['max_tokens'], 800)


class _FakeChannel:
    def __init__(self, ok=True):
        self.name = 'FakeChannel'
        self.ok = ok
        self.calls = 0
        self.rate_limited_until = 0.0

    def send(self, title, content):
        self.calls += 1
        return self.ok


class TestNotifyThrottleFailureStorm(unittest.TestCase):
    """② 节流时钟必须在「决定放行」时推进，而不是等成功"""

    def setUp(self):
        self.mgr = NotificationManager()
        self.mgr._min_interval = 3.0
        self.mgr._max_per_minute = 18

    def test_consecutive_failures_do_not_bypass_min_interval(self):
        failing = _FakeChannel(ok=False)
        self.mgr.notifiers = [failing]
        for i in range(12):
            self.mgr.send_custom_message(f'异动{i}', '内容')
        # 修复前：12 条全部硬发（ECS 10:00:56 一秒 13 条 410100）
        # 修复后：只放行第 1 条，其余进聚合缓冲
        self.assertEqual(failing.calls, 1,
                         '连续失败时最小间隔必须仍然生效')
        self.assertEqual(len(self.mgr._pending_texts), 12)

    def test_channel_backoff_buffers_everything(self):
        ch = _FakeChannel(ok=True)
        ch.rate_limited_until = __import__('time').monotonic() + 60
        self.mgr.notifiers = [ch]
        ok = self.mgr.send_custom_message('异动', '内容')
        self.assertFalse(ok)
        self.assertEqual(ch.calls, 0)
        self.assertEqual(len(self.mgr._pending_texts), 1)

    def test_failed_send_is_buffered_not_lost(self):
        failing = _FakeChannel(ok=False)
        self.mgr.notifiers = [failing]
        self.mgr.send_signal = mock.MagicMock()   # 只测 custom 路径
        ok = self.mgr.send_custom_message('重要告警', '内容')
        self.assertFalse(ok)
        self.assertEqual(len(self.mgr._pending_texts), 1)

    def test_dingtalk_sets_backoff_on_410100(self):
        notifier = DingTalkNotifier('https://example.invalid/hook')
        resp = mock.MagicMock(status_code=200)
        resp.json.return_value = {'errcode': 410100, 'errmsg': '发送速率过快'}
        with mock.patch('httpx.Client') as client:
            client.return_value.__enter__.return_value.post.return_value = resp
            ok = notifier.send('t', 'c')
        self.assertFalse(ok)
        self.assertGreater(notifier.rate_limited_until,
                           __import__('time').monotonic())


class TestCalendar2026(unittest.TestCase):
    """③ 2026 年不依赖已越界的 exchange_calendars"""

    def test_next_trading_day_from_weekend(self):
        # 2026-11-06 周五 → 下一交易日 11-09 周一
        d = cal.get_next_trading_day(datetime(2026, 11, 6))
        self.assertEqual(d.strftime('%Y-%m-%d'), '2026-11-09')

    def test_trading_days_skip_holiday(self):
        # 2026-10-01~10-07 国庆休市；10-08 周四恢复
        days = cal.get_trading_days_between(
            datetime(2026, 9, 30), datetime(2026, 10, 9))
        dates = [d.strftime('%Y-%m-%d') for d in days]
        self.assertIn('2026-09-30', dates)
        self.assertIn('2026-10-08', dates)
        self.assertNotIn('2026-10-01', dates)
        self.assertNotIn('2026-10-05', dates)

    def test_no_exception_warn_for_2026(self):
        """2026 年调用不应触发 exchange_calendars 异常降级告警"""
        with mock.patch.object(cal, '_warn_once') as warn:
            cal.get_next_trading_day(datetime(2026, 11, 6))
            cal.is_trading_day(datetime(2026, 11, 6))
        warn.assert_not_called()


if __name__ == '__main__':
    unittest.main()
