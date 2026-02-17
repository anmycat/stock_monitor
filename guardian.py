import schedule, time
from logger import get_logger
from modules.holiday import is_trading_day

logger = get_logger("guardian")

def job():
    logger.info("System running")

schedule.every(10).seconds.do(job)

while True:
    try:
        if is_trading_day():
            schedule.run_pending()
        time.sleep(1)
    except Exception as e:
        logger.error(str(e))
