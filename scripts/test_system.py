#!/usr/bin/env python3
"""系统功能测试"""
import os, sys
os.chdir("/stock_monitor")
sys.path.insert(0, "/stock_monitor")

print("="*60)
print("系统功能测试")
print("="*60)

# 1. 交易日
print("\n【1. 交易日判断】")
from modules.market import is_trading_day, now_bj
print(f"是否交易日: {is_trading_day()}")
print(f"当前时间: {now_bj()}")

# 2. 集合竞价
print("\n【2. 集合竞价】")
from modules.market import get_call_auction_top10_with_status
from modules.auction_engine import get_auction_signals
rows, stale, err = get_call_auction_top10_with_status(limit=10)
print(f"竞价数据: {len(rows)}条")
signals = get_auction_signals(limit=10)
print(f"信号: {len(signals)}条")
for s in signals[:3]:
    print(f"  {s.code} {s.name} {s.score}分 {s.tags}")

# 3. ETF追踪
print("\n【3. ETF持仓】")
from modules.etf_tracker import get_etf_stock_alerts
try:
    result = get_etf_stock_alerts(etf_codes=["sh510300"], topn=3)
    print(f"结果: {type(result).__name__}")
    if isinstance(result, dict):
        print(f"  增持:{result.get('total_increased',0)} 减持:{result.get('total_decreased',0)}")
except Exception as e:
    print(f"  失败: {e}")

# 4. 资金流向
print("\n【4. 资金流向】")
from modules.fund_flow import get_dragon_tiger_list
try:
    dragon = get_dragon_tiger_list()
    print(f"龙虎榜: {len(dragon)}条")
except Exception as e:
    print(f"失败: {e}")

# 5. 板块
print("\n【5. 板块分析】")
from modules.sector_analysis import get_sector_list, get_market_breadth
try:
    sectors = get_sector_list()
    print(f"板块: {len(sectors)}")
    breadth = get_market_breadth()
    print(f"宽度: {breadth}")
except Exception as e:
    print(f"失败: {e}")

# 6. 股票扫描
print("\n【6. 股票扫描】")
from modules.stock_scanner import scan_market_trade_candidates
try:
    candidates = scan_market_trade_candidates([], limit=5)
    print(f"候选: {len(candidates)}条")
except Exception as e:
    print(f"失败: {e}")

# 7. 交易员
print("\n【7. 交易员决策】")
from modules.trader_brain import get_professional_trader
try:
    trader = get_professional_trader()
    a = trader.assess_market()
    print(f"趋势:{a.get('trend')} 置信度:{a.get('confidence')} 仓位:{a.get('position')}")
except Exception as e:
    print(f"失败: {e}")

# 8. 新闻
print("\n【8. 新闻】")
from modules.weekly_ops import fetch_finance_news
try:
    news = fetch_finance_news(limit=3)
    print(f"新闻: {len(news)}条")
except Exception as e:
    print(f"失败: {e}")

# 9. 情感
print("\n【9. 情感分析】")
from modules.sentiment import analyze_news_sentiment
try:
    if news:
        s = analyze_news_sentiment(news)
        print(f"情感:{s.get('overall_sentiment')} 分数:{s.get('score')}")
except Exception as e:
    print(f"失败: {e}")

# 10. 指数
print("\n【10. 指数开盘】")
from modules.etf_tracker import get_market_index_summary
try:
    idxs = get_market_index_summary()
    print(f"指数: {len(idxs)}条")
    for i in idxs[:3]:
        print(f"  {i.get('name')} {i.get('change_pct')}%")
except Exception as e:
    print(f"失败: {e}")

print("\n" + "="*60)
print("测试完成")
print("="*60)
