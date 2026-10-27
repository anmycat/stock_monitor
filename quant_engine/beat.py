import time
from datetime import datetime, timedelta

from loguru import logger

from .data.repositories import MarketDataRepository
from .services.market.market_analyzer import MarketAnalyzer
from .services.notifications.manager import NotificationManager
from .utils.calendar import is_trading_day


def is_trading_session() -> bool:
    now = datetime.now()
    
    # 使用交易日历判断
    if not is_trading_day(now):
        return False
    
    h, m = now.hour, now.minute
    t = h * 60 + m
    return (9 * 60 + 30 <= t < 11 * 60 + 30) or (13 * 60 <= t < 15 * 60)


def get_sleep_seconds() -> int:
    now = datetime.now()
    h, m = now.hour, now.minute
    t = h * 60 + m
    if t < 9 * 60 + 30:
        return (9 * 60 + 30 - t) * 60
    elif 11 * 60 + 30 <= t < 13 * 60:
        return (13 * 60 - t) * 60
    elif t >= 15 * 60:
        tomorrow = now.replace(hour=9, minute=30, second=0) + timedelta(days=1)
        return int((tomorrow - now).total_seconds())
    return 120


# D49：盘后静态股票信息表刷新（15:35 后执行一次，ST/脱帽/退市变更推送）
STOCK_INFO_REFRESH_T = 15 * 60 + 35


def maybe_refresh_stock_info(notifier, state: dict) -> None:
    """每个非交易时段检查一次：收盘后刷新静态表，有变更则钉钉通知"""
    now = datetime.now()
    if not is_trading_day(now):
        return
    if now.hour * 60 + now.minute < STOCK_INFO_REFRESH_T:
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
                maybe_refresh_stock_info(notifier, stock_info_state)
                sleep_sec = get_sleep_seconds()
                if sleep_sec > 120:
                    logger.debug(f"Non-trading, sleeping {sleep_sec}s")
                last_market_type = None
                time.sleep(min(sleep_sec, 120))
        except Exception as e:  # noqa: BLE001 任务兜底
            logger.error(f"Beat error: {e}")
            time.sleep(60)


if __name__ == "__main__":
    main()
