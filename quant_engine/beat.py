import time
from datetime import datetime, timedelta
from loguru import logger
from .data.datacenter import DataCenter
from .services.market_analyzer import MarketAnalyzer
from .services.notifiers.manager import NotificationManager
from .models.market import MarketType
from .utils.calendar import is_trading_day
from .config import get_settings


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


def main():
    logger.info("Beat scheduler started")
    settings = get_settings()
    datacenter = DataCenter()
    analyzer = MarketAnalyzer(datacenter)
    notifier = NotificationManager()
    last_market_type = None

    while True:
        try:
            now = datetime.now()

            # 市场状态监控
            if is_trading_session():
                market = analyzer.analyze_market()
                if last_market_type and market.type != last_market_type:
                    notifier.send_market_alert(market)
                    logger.info(f"Market state changed: {last_market_type} -> {market.type.value}")
                last_market_type = market.type
                time.sleep(120)
            else:
                sleep_sec = get_sleep_seconds()
                if sleep_sec > 120:
                    logger.debug(f"Non-trading, sleeping {sleep_sec}s")
                last_market_type = None
                time.sleep(min(sleep_sec, 120))
        except Exception as e:
            logger.error(f"Beat error: {e}")
            time.sleep(60)


if __name__ == "__main__":
    main()