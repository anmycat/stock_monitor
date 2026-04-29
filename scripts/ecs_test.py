#!/usr/bin/env python3
import sys
sys.path.insert(0, '/stock_monitor')

from modules.trader_brain import get_professional_trader
from modules.stock_scanner import scan_market_trade_candidates
from modules.sector_analysis import load_watchlist
from modules.fund_flow import get_famous_trader_stocks
from modules.market import get_quote
import json

print("=" * 60)
print("ECS A股监控系统测试")
print("=" * 60)

# 市场评估
print("\n[1] 市场评估")
trader = get_professional_trader()
result = trader.assess_market()
print(f"  趋势: {result.get('trend')}")
print(f"  信号: {result.get('signal')}")
print(f"  仓位: {result.get('position')}")
print(f"  置信度: {result.get('confidence')}")

# 自选股
print("\n[2] 自选股")
watchlist = load_watchlist()
stocks = watchlist.get("watch_stocks", [])
print(f"  数量: {len(stocks)}")
for s in stocks[:5]:
    code = s.get("code")
    try:
        q = get_quote(code)
        pct = q.get("pct_change", 0) or 0
        turn = q.get("turnover_rate", 0) or 0
        print(f"  - {s.get('name')} ({code}): 涨跌{pct:.2f}% 换手{turn:.2f}%")
    except Exception as e:
        print(f"  - {s.get('name')} ({code}): 获取失败")

# 游资关注
print("\n[3] 游资关注")
try:
    traders = get_famous_trader_stocks()
    print(f"  数量: {len(traders)}")
    for t in traders[:3]:
        print(f"  - {t.get('name')} ({t.get('code')}): 净买入{t.get('net_amount', 0)}万")
except Exception as e:
    print(f"  获取失败: {e}")

# 盘中候选扫描
print("\n[4] 盘中候选扫描")
try:
    candidates = scan_market_trade_candidates([], limit=5)
    print(f"  候选数量: {len(candidates)}")
    for c in candidates[:3]:
        print(f"  - {c.name} ({c.code}): 评分={c.score} 涨跌={c.pct_change}%")
except Exception as e:
    print(f"  扫描失败: {e}")

print("\n" + "=" * 60)
print("测试完成")
print("=" * 60)
