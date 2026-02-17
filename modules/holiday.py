import pandas_market_calendars as mcal
from datetime import datetime

def is_trading_day():
    today = datetime.now().date()
    cal = mcal.get_calendar('SSE')
    schedule = cal.schedule(start_date=today, end_date=today)
    return not schedule.empty
