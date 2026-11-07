"""交易日历工具 - 判断是否为交易日"""
from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
from loguru import logger

try:
    import exchange_calendars as xcals
    HAS_CALENDAR = True
except ImportError:
    HAS_CALENDAR = False
    logger.warning("exchange-calendars not installed, using simple weekend check")


# 沪深北休市日（工作日部分）——exchange_calendars 的 XSHG 日历仅覆盖到
# 2025-12-31，2026 年起必须用本表，否则法定节假日会被当成交易日，
# 系统会在休市日空跑扫描并访问行情服务器（D53 实测：日历越界每次调用
# 都抛异常并刷 WARNING）。
#
# 来源：上交所公告〔2025〕45 号《关于上海证券交易所2026年部分节假日休市
# 安排的通知》（2025-12-22 发布，沪深北三所同步）：
#   元旦 1/1-1/3 · 春节 2/15-2/23 · 清明 4/4-4/6 · 劳动节 5/1-5/5
#   端午 6/19-6/21 · 中秋 9/25-9/27 · 国庆 10/1-10/7
CN_MARKET_HOLIDAYS: set[str] = {
    # 元旦
    '2026-01-01', '2026-01-02',
    # 春节（2/14、2/28 为周末）
    '2026-02-16', '2026-02-17', '2026-02-18', '2026-02-19', '2026-02-20', '2026-02-23',
    # 清明
    '2026-04-06',
    # 劳动节（5/9 为周末）
    '2026-05-01', '2026-05-04', '2026-05-05',
    # 端午
    '2026-06-19',
    # 中秋
    '2026-09-25',
    # 国庆（9/20、10/10 为周末）
    '2026-10-01', '2026-10-02', '2026-10-05', '2026-10-06', '2026-10-07',
}

_WARNED: set[str] = set()  # 已告警过的标签（日历越界只告警一次，避免刷屏）


def _warn_once(tag: str, msg: str) -> None:
    """同一类告警只输出一次（日历越界每次调用都会失败，会刷屏）"""
    if tag in _WARNED:
        return
    _WARNED.add(tag)
    logger.warning(msg)


def _is_holiday(date: datetime) -> bool:
    """是否在沪深北休市日表内"""
    return date.strftime('%Y-%m-%d') in CN_MARKET_HOLIDAYS


def is_trading_day(date: datetime | None = None, market: str = 'XSHG') -> bool:
    """
    判断是否为交易日

    Args:
        date: 日期，默认为今天
        market: 市场代码，XSHG=上交所, XSHE=深交所

    Returns:
        bool: 是否为交易日
    """
    if date is None:
        date = datetime.now()

    # 周末肯定不是交易日
    if date.weekday() >= 5:
        return False

    # 静态休市日表优先（覆盖 exchange_calendars 的 horizon 之外）
    if _is_holiday(date):
        return False

    # D66：2026+ 静态表已是完整事实源（周末 + 法定休市日已排除），
    # 不再调用越界的 exchange_calendars（每次白抛一次异常）。
    if date.year > 2025 or not HAS_CALENDAR:
        return date.weekday() < 5
    try:
        cal = xcals.get_calendar(market)
        ts = pd.Timestamp(date).normalize()
        return cal.is_session(ts)
    except Exception as e:
        _warn_once('is_session',
                   f"Calendar check failed (fallback to weekend+holiday table): {e}")
        return date.weekday() < 5


def get_next_trading_day(date: datetime | None = None, market: str = 'XSHG') -> datetime:
    """
    获取下一个交易日
    
    Args:
        date: 起始日期，默认为今天
        market: 市场代码
    
    Returns:
        datetime: 下一个交易日
    """
    if date is None:
        date = datetime.now()
    
    # D66：2026 年起 XSHG 日历已越界（实测 next_session 对 2026-11-01
    # 抛 "is not a session of calendar"），静态休市日表才是 2026 的事实源。
    # 原来「先试 exchange_calendars、异常再降级」每轮都要白抛一次异常并刷
    # WARNING（ECS 近 7 天 3 次）。现在 2026+ 直接走静态表，日历库仅用于
    # 其 horizon 覆盖的 2025 及以前，且入参必须是有效 session。
    if HAS_CALENDAR and date.year <= 2025:
        try:
            cal = xcals.get_calendar(market)
            ts = pd.Timestamp(date).normalize()
            if cal.is_session(ts):
                return cal.next_session(ts).to_pydatetime()
        except Exception as e:
            _warn_once('next_session',
                       f"Calendar next day failed (fallback to weekend+holiday table): {e}")

    # 静态表路径：跳过周末 + 法定休市日
    next_day = date + timedelta(days=1)
    for _ in range(30):
        if next_day.weekday() < 5 and not _is_holiday(next_day):
            break
        next_day += timedelta(days=1)
    return next_day


def get_trading_days_between(start_date: datetime, end_date: datetime, market: str = 'XSHG') -> list:
    """
    获取两个日期之间的所有交易日
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
        market: 市场代码
    
    Returns:
        list: 交易日列表
    """
    # D66：2026+ 直接按「周末 + 静态休市日表」枚举，避免 sessions_in_range
    # 越界抛异常后降级成「只跳周末」——那会把法定节假日误算成交易日，
    # 直接破坏 D65 的「末根与今天相邻」护栏（节假日后首日会误判陈旧）。
    use_static = (start_date.year > 2025 or end_date.year > 2025
                  or not HAS_CALENDAR)
    if not use_static:
        try:
            cal = xcals.get_calendar(market)
            start = pd.Timestamp(start_date).normalize()
            end = pd.Timestamp(end_date).normalize()
            sessions = cal.sessions_in_range(start, end)
            return [s.to_pydatetime() for s in sessions]
        except Exception as e:
            logger.warning(f"Calendar range failed: {e}")

    days = []
    current = start_date
    while current <= end_date:
        if is_trading_day(current, market):
            days.append(current)
        current += timedelta(days=1)
    return days


def is_trading_time(date: datetime | None = None) -> bool:
    """
    判断是否为交易时间（包含交易日+交易时段检查）
    
    Returns:
        bool: 是否在交易时间内
    """
    if date is None:
        date = datetime.now()
    if not is_trading_day(date):
        return False
    h, m = date.hour, date.minute
    t = h * 60 + m
    return (9 * 60 + 30 <= t <= 11 * 60 + 30) or (13 * 60 <= t <= 15 * 60)


def get_market_phase() -> str:
    """
    获取当前市场阶段
    
    Returns:
        str: WEEKEND, PRE_MARKET, MORNING_SESSION, NOON_BREAK, AFTERNOON_SESSION, AFTER_HOURS
    """
    now = datetime.now()
    if not is_trading_day(now):
        return "WEEKEND"
    h, m = now.hour, now.minute
    t = h * 60 + m
    if t < 9 * 60 + 30:
        return "PRE_MARKET"
    elif t < 11 * 60 + 30:
        return "MORNING_SESSION"
    elif t < 13 * 60:
        return "NOON_BREAK"
    elif t < 15 * 60:
        return "AFTERNOON_SESSION"
    else:
        return "AFTER_HOURS"
