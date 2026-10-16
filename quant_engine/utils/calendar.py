"""交易日历工具 - 判断是否为交易日"""
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger

try:
    import exchange_calendars as xcals
    HAS_CALENDAR = True
except ImportError:
    HAS_CALENDAR = False
    logger.warning("exchange-calendars not installed, using simple weekend check")


def is_trading_day(date: datetime = None, market: str = 'XSHG') -> bool:
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
    
    if HAS_CALENDAR:
        try:
            cal = xcals.get_calendar(market)
            ts = pd.Timestamp(date).normalize()
            return cal.is_session(ts)
        except Exception as e:
            logger.warning(f"Calendar check failed: {e}")
            # 降级到简单判断
            return date.weekday() < 5
    else:
        # 没有exchange-calendars时，简单判断是否为周末
        return date.weekday() < 5


def get_next_trading_day(date: datetime = None, market: str = 'XSHG') -> datetime:
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
    
    if HAS_CALENDAR:
        try:
            cal = xcals.get_calendar(market)
            ts = pd.Timestamp(date).normalize()
            next_day = cal.next_session(ts)
            return next_day.to_pydatetime()
        except Exception as e:
            logger.warning(f"Calendar next day failed: {e}")
    
    # 降级逻辑
    next_day = date + timedelta(days=1)
    while next_day.weekday() >= 5:
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
    if HAS_CALENDAR:
        try:
            cal = xcals.get_calendar(market)
            start = pd.Timestamp(start_date).normalize()
            end = pd.Timestamp(end_date).normalize()
            sessions = cal.sessions_in_range(start, end)
            return [s.to_pydatetime() for s in sessions]
        except Exception as e:
            logger.warning(f"Calendar range failed: {e}")
    
    # 降级逻辑
    days = []
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def is_trading_time(date: datetime = None) -> bool:
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