"""交易时段闸门与交易日历测试（D53，2026-11-01）

覆盖：
1. 2026 年沪深北休市日表（exchange_calendars 的 XSHG horizon 只到 2025-12-31，
   缺表会把法定节假日当交易日 → 休市日空跑扫描并访问行情服务器）
2. 行情访问窗口（is_quote_window）
3. 唤醒点计算（next_wakeup）：非交易时段整块静默、交易前唤起、阶段不跳过
"""
import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.utils.calendar import (
    CN_MARKET_HOLIDAYS,
    get_next_trading_day,
    is_trading_day,
)
from quant_engine.utils.session_gate import (
    in_snapshot_window,
    is_quote_window,
    next_wakeup,
    seconds_until,
)


class TestHolidayTable(unittest.TestCase):
    """2026 休市日表"""

    def test_known_holidays_not_trading_days(self):
        cases = [
            '2026-01-01', '2026-01-02',          # 元旦
            '2026-02-16', '2026-02-17', '2026-02-18', '2026-02-19',
            '2026-02-20', '2026-02-23',          # 春节
            '2026-04-06',                         # 清明
            '2026-05-01', '2026-05-04', '2026-05-05',   # 劳动节
            '2026-06-19',                         # 端午
            '2026-09-25',                         # 中秋
            '2026-10-01', '2026-10-02', '2026-10-05',
            '2026-10-06', '2026-10-07',          # 国庆
        ]
        for d in cases:
            with self.subTest(date=d):
                self.assertFalse(is_trading_day(datetime.strptime(d, '%Y-%m-%d')))
                self.assertIn(d, CN_MARKET_HOLIDAYS)

    def test_normal_trading_days(self):
        cases = ['2026-01-05', '2026-02-24', '2026-04-07', '2026-05-06',
                 '2026-06-22', '2026-09-28', '2026-10-08', '2026-11-02']
        for d in cases:
            with self.subTest(date=d):
                self.assertTrue(is_trading_day(datetime.strptime(d, '%Y-%m-%d')))

    def test_weekends_not_trading_days(self):
        self.assertFalse(is_trading_day(datetime(2026, 11, 1)))   # 周日
        self.assertFalse(is_trading_day(datetime(2026, 11, 7)))   # 周六

    def test_next_trading_day_skips_holiday(self):
        """国庆后第一个交易日 = 10/8（不是 10/8 之前的周末）"""
        nxt = get_next_trading_day(datetime(2026, 10, 7))
        self.assertEqual(nxt.strftime('%Y-%m-%d'), '2026-10-08')

    def test_next_trading_day_after_new_year_holiday(self):
        nxt = get_next_trading_day(datetime(2026, 1, 2))
        self.assertEqual(nxt.strftime('%Y-%m-%d'), '2026-01-05')


class TestQuoteWindow(unittest.TestCase):
    """行情访问窗口"""

    def test_open_during_sessions(self):
        for hh, mm in [(9, 2), (9, 30), (10, 0), (11, 30), (13, 0), (14, 59)]:
            with self.subTest(t=f'{hh:02d}:{mm:02d}'):
                self.assertTrue(is_quote_window(datetime(2026, 11, 2, hh, mm)))

    def test_closed_outside_sessions(self):
        for hh, mm in [(8, 59), (11, 31), (12, 15), (15, 6), (18, 15), (3, 0)]:
            with self.subTest(t=f'{hh:02d}:{mm:02d}'):
                self.assertFalse(is_quote_window(datetime(2026, 11, 2, hh, mm)))

    def test_closed_on_non_trading_day(self):
        self.assertFalse(is_quote_window(datetime(2026, 11, 1, 10, 0)))
        self.assertFalse(is_quote_window(datetime(2026, 10, 1, 10, 0)))


class TestWakeup(unittest.TestCase):
    """唤醒点计算"""

    def test_pre_market_wakes_at_self_check(self):
        t = next_wakeup('PRE_MARKET', datetime(2026, 11, 2, 9, 2))
        self.assertEqual((t.hour, t.minute), (9, 15))

    def test_self_check_wakes_at_call_auction(self):
        t = next_wakeup('SELF_CHECK', datetime(2026, 11, 2, 9, 16))
        self.assertEqual((t.hour, t.minute), (9, 25))

    def test_call_auction_wakes_at_open(self):
        t = next_wakeup('CALL_AUCTION', datetime(2026, 11, 2, 9, 26))
        self.assertEqual((t.hour, t.minute), (9, 30))

    def test_session_wakes_in_5_minutes(self):
        now = datetime(2026, 11, 2, 10, 30)
        t = next_wakeup('MORNING_SESSION', now)
        self.assertEqual(seconds_until(t, now), 300)

    def test_noon_break_wakes_for_morning_review(self):
        """11:30-12:10 午休 → 12:10 早盘总结（不能跳过该阶段）"""
        t = next_wakeup('NOON_BREAK', datetime(2026, 11, 2, 11, 35))
        self.assertEqual((t.hour, t.minute), (12, 10))

    def test_noon_break_second_window_wakes_at_afternoon(self):
        t = next_wakeup('NOON_BREAK', datetime(2026, 11, 2, 12, 30))
        self.assertEqual((t.hour, t.minute), (13, 0))

    def test_after_hours_wakes_for_daily_review(self):
        t = next_wakeup('AFTER_HOURS', datetime(2026, 11, 2, 15, 5))
        self.assertEqual((t.hour, t.minute), (18, 10))

    def test_after_daily_review_wakes_next_trading_day_premarket(self):
        t = next_wakeup('AFTER_HOURS', datetime(2026, 11, 2, 20, 0))
        self.assertEqual((t.hour, t.minute), (9, 0))
        self.assertGreater(t.day, 2)

    def test_daily_review_wakes_next_trading_day(self):
        t = next_wakeup('DAILY_REVIEW', datetime(2026, 11, 2, 18, 12))
        self.assertEqual((t.hour, t.minute), (9, 0))

    def test_weekend_wakes_next_trading_day(self):
        t = next_wakeup('WEEKEND', datetime(2026, 11, 1, 20, 0))
        self.assertEqual((t.hour, t.minute), (9, 0))
        self.assertEqual(t.strftime('%Y-%m-%d'), '2026-11-02')

    def test_non_trading_night_sleeps_long(self):
        """夜间（20:00）不应 60 秒轮询，而应睡到次交易日盘前"""
        now = datetime(2026, 11, 2, 20, 0)
        t = next_wakeup('AFTER_HOURS', now)
        self.assertGreater(seconds_until(t, now), 36000)


class TestSnapshotWindow(unittest.TestCase):
    """总结快照采集窗口"""

    def test_morning_snapshot_window(self):
        self.assertTrue(in_snapshot_window('morning', datetime(2026, 11, 2, 11, 27)))
        self.assertFalse(in_snapshot_window('morning', datetime(2026, 11, 2, 10, 0)))

    def test_daily_snapshot_window(self):
        self.assertTrue(in_snapshot_window('daily', datetime(2026, 11, 2, 14, 57)))
        self.assertFalse(in_snapshot_window('daily', datetime(2026, 11, 2, 13, 0)))


if __name__ == '__main__':
    unittest.main()
