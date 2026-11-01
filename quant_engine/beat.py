from __future__ import annotations

import time
from datetime import datetime, timedelta

from loguru import logger

from .data.repositories import MarketDataRepository
from .services.market.market_analyzer import MarketAnalyzer
from .services.notifications.manager import NotificationManager
from .utils.calendar import is_trading_day
from .utils.session_gate import _next_trading_day_at


def is_trading_session() -> bool:
    now = datetime.now()
    
    # 使用交易日历判断
    if not is_trading_day(now):
        return False
    
    h, m = now.hour, now.minute
    t = h * 60 + m
    return (9 * 60 + 30 <= t < 11 * 60 + 30) or (13 * 60 <= t < 15 * 60)


def next_wakeup(now: datetime | None = None) -> datetime:
    """下一次唤醒时刻（D53：非交易时段整块静默）

    交易时段（9:30-11:30 / 13:00-15:00）每 2 分钟一次市场状态巡检；
    其余时段整块睡到下一个事件点：
      交易日 09:00 → 盘前静态股票信息表刷新
      交易日 09:30 → 开盘
      午休 11:30 → 13:00 午后开盘
      收盘后 / 非交易日 → 次交易日 09:00
    """
    now = now or datetime.now()
    t = now.hour * 60 + now.minute
    if is_trading_day(now):
        if t < 9 * 60:
            return now.replace(hour=9, minute=0, second=0, microsecond=0)
        if t < 9 * 60 + 30:
            return now.replace(hour=9, minute=30, second=0, microsecond=0)
        if 11 * 60 + 30 <= t < 13 * 60:
            return now.replace(hour=13, minute=0, second=0, microsecond=0)
        if t >= 15 * 60:
            return _next_trading_day_at(9, 0, now)
    else:
        return _next_trading_day_at(9, 0, now)
    return now + timedelta(seconds=120)


def get_sleep_seconds() -> int:
    """兼容旧调用：距下次唤醒的秒数"""
    return max(1, int((next_wakeup() - datetime.now()).total_seconds()))


# D49：静态股票信息表刷新（ST/脱帽/退市变更推送）
# D53：刷新时机由"收盘后 15:35"改为"交易日盘前 9:00-9:30"——
# 遵守"非交易时间不访问行情服务器"（刷新需拉全市场股票列表），
# 且变更信息在当日信号生成前即可获知。
STOCK_INFO_REFRESH_FROM = 9 * 60
STOCK_INFO_REFRESH_TO = 9 * 60 + 30


def maybe_refresh_stock_info(notifier, state: dict) -> None:
    """交易日盘前刷新静态表，有变更则钉钉通知（每日一次）"""
    now = datetime.now()
    if not is_trading_day(now):
        return
    t = now.hour * 60 + now.minute
    if not (STOCK_INFO_REFRESH_FROM <= t < STOCK_INFO_REFRESH_TO):
        return
    if state.get('stock_info_date') == now.date().isoformat():
        return
    state['stock_info_date'] = now.date().isoformat()
    from .data.stock_info import format_stock_info_diff, get_stock_info_table
    try:
        diff = get_stock_info_table().refresh()
    except Exception as e:  # noqa: BLE001 盘后任务兜底
        logger.error(f"StockInfo refresh error: {e}")
        return
    summary = format_stock_info_diff(diff)
    if summary:
        notifier.send_custom_message('股票池静态信息变更', summary)
        logger.info(f"StockInfo 变更已推送:\n{summary}")


def main():
    logger.info("Beat scheduler started")
    datacenter = MarketDataRepository()
    analyzer = MarketAnalyzer(datacenter)
    notifier = NotificationManager()
    last_market_type = None
    stock_info_state: dict = {}

    while True:
        try:
            # 市场状态监控
            if is_trading_session():
                market = analyzer.analyze_market()
                if last_market_type and market.type != last_market_type:
                    notifier.send_market_alert(market)
                    logger.info(f"Market state changed: {last_market_type} -> {market.type.value}")
                last_market_type = market.type
                time.sleep(120)
            else:
                # 非交易时段：只做盘前静态表刷新（受 9:00-9:30 窗口限制），
                # 其余时间整块静默——原实现每 2 分钟空唤醒一次（整夜 720 次）
                maybe_refresh_stock_info(notifier, stock_info_state)
                target = next_wakeup()
                sleep_sec = max(1, int((target - datetime.now()).total_seconds()))
                logger.info(f"非交易时段：静默 {sleep_sec}s（下次唤醒 {target:%m-%d %H:%M}）")
                last_market_type = None
                time.sleep(sleep_sec)
        except Exception as e:  # noqa: BLE001 任务兜底
            logger.error(f"Beat error: {e}")
            time.sleep(60)


if __name__ == "__main__":
    main()
