import os, sys

os.chdir("/stock_monitor")
sys.path.insert(0, "/stock_monitor")

from modules.auction_engine import get_auction_signals, format_auction_alert

signals = get_auction_signals(limit=15)
print("信号数:", len(signals))
print()
print(format_auction_alert(signals))
