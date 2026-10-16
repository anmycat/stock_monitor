#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多空博弈社 —— 交易动作 vs 实盘 深度比对（在 compare_trades_vs_market.py 基础上扩展）
新增：
  六、极端择时案例（买在当日最高附近 / 卖在当日最低附近）
  七、相对当日收盘的基准检验（他的盘中决策是否跑赢"拿到收盘"）
  八、跨日持仓配对（FIFO，含隔夜）
  九、按个股汇总
  十、当日环境（振幅/涨跌）与他买入时点的关系
"""
import json
from collections import defaultdict, Counter

BASE = "/Users/hr.wh/WorkBuddy/2026-10-13-00-21-29"
d = json.load(open(f"{BASE}/data/trade_vs_market.json", encoding="utf-8"))
rows, pairs = d["rows"], d["pairs"]
market = json.load(open(f"{BASE}/raw/market_daily_unadjusted.json", encoding="utf-8"))

def avg(xs):
    return sum(xs) / len(xs) if xs else 0.0

print("=" * 80)
print("六、极端择时案例")
buys = [r for r in rows if r["dir"] == "买入"]
sells = [r for r in rows if r["dir"] == "卖出"]

worst_buys = sorted(buys, key=lambda r: r["timing"])[:6]
print("  【最差买入】买价最接近当日最高价（timing 分最低）：")
for r in worst_buys:
    print(f"    {r['date']} {r['stock']:6} 买 {r['price']:>7} @ {r['time'] or '无时间':8} | "
          f"当日[{r['l']}~{r['h']}] 收{r['c']} ({r['pct']:+.2f}%) | 位置{r['pos_pct']:.0f}% 分={r['timing']:.0f}")

best_buys = sorted(buys, key=lambda r: -r["timing"])[:4]
print("  【最好买入】买价最接近当日最低价：")
for r in best_buys:
    print(f"    {r['date']} {r['stock']:6} 买 {r['price']:>7} @ {r['time'] or '无时间':8} | "
          f"当日[{r['l']}~{r['h']}] 收{r['c']} ({r['pct']:+.2f}%) | 位置{r['pos_pct']:.0f}% 分={r['timing']:.0f}")

worst_sells = sorted(sells, key=lambda r: r["timing"])[:6]
print("  【最差卖出】卖价最接近当日最低价：")
for r in worst_sells:
    print(f"    {r['date']} {r['stock']:6} 卖 {r['price']:>7} @ {r['time'] or '无时间':8} | "
          f"当日[{r['l']}~{r['h']}] 收{r['c']} ({r['pct']:+.2f}%) | 位置{r['pos_pct']:.0f}% 分={r['timing']:.0f}")

print("\n七、基准检验：他的盘中决策 vs 「拿到当日收盘」")
# 买入: 若持有到收盘的收益 (c-p)/p ；卖出: 若不卖而持有到收盘的差异 (p-c)/p
buy_hold = [((r["c"] - r["price"]) / r["price"] * 100) for r in buys]
sell_hold = [((r["price"] - r["c"]) / r["price"] * 100) for r in sells]
print(f"  买入 {len(buys)} 笔：从买价持有到当日收盘，平均 {avg(buy_hold):+.2f}%")
print(f"     （正值=买完当天就涨；说明他买完当天是赚是亏）")
print(f"  卖出 {len(sells)} 笔：卖出价高于当日收盘的比例 "
      f"{sum(1 for x in sell_hold if x>0)}/{len(sells)}，平均高出 {avg(sell_hold):+.2f}%")
print(f"     （正值=卖在收盘价之上，盘中卖出决策有效）")
buy_good = sum(1 for x in buy_hold if x > 0)
sell_good = sum(1 for x in sell_hold if x > 0)
print(f"  合计：买对方向(涨) {buy_good}/{len(buys)}；卖对方向(高于收盘) {sell_good}/{len(sells)}")

print("\n八、跨日持仓配对（FIFO，含隔夜；无时间单按 12:00 占位，结果为近似值）")
by_stock = defaultdict(list)
for r in rows:
    if r["dir"] in ("买入", "卖出"):
        by_stock[r["code"]].append(r)
cross = []
for code, evs in by_stock.items():
    evs.sort(key=lambda x: (x["date"], x["time"] or "12:00"))
    inv = []
    for e in evs:
        if e["dir"] == "买入":
            inv.append(e)
        elif inv:
            b = inv.pop(0)
            spread = (e["price"] - b["price"]) / b["price"] * 100
            cross.append({
                "stock": b["stock"], "code": code,
                "buy_date": b["date"], "buy_price": b["price"],
                "sell_date": e["date"], "sell_price": e["price"],
                "spread_pct": round(spread, 2),
                "overnight": b["date"] != e["date"],
            })
pos_c = [c for c in cross if c["spread_pct"] > 0]
neg_c = [c for c in cross if c["spread_pct"] <= 0]
print(f"  跨日配对 {len(cross)} 笔 | 盈利 {len(pos_c)} 亏损 {len(neg_c)} | "
      f"平均价差 {avg([c['spread_pct'] for c in cross]):+.2f}%")
print(f"  其中隔夜配对 {sum(1 for c in cross if c['overnight'])} 笔，"
      f"平均 {avg([c['spread_pct'] for c in cross if c['overnight']]):+.2f}%")
print("  明细（按价差排序）：")
for c in sorted(cross, key=lambda x: x["spread_pct"]):
    flag = "✅" if c["spread_pct"] > 0 else "❌"
    n = " 隔夜" if c["overnight"] else ""
    print(f"    {flag} {c['stock']:6} 买{c['buy_price']:>7}({c['buy_date']}) -> 卖{c['sell_price']:>7}({c['sell_date']}){n}  {c['spread_pct']:+.2f}%")

print("\n九、按个股汇总")
print(f"  {'股票':<8}{'代码':<11}{'买':>3}{'卖':>3}{'买均位置%':>10}{'卖均位置%':>10}{'跨日均价差%':>12}")
per = {}
for code, evs in by_stock.items():
    b = [e for e in evs if e["dir"] == "买入"]
    s = [e for e in evs if e["dir"] == "卖出"]
    cs = [c["spread_pct"] for c in cross if c["code"] == code]
    name = market[code]["name"]
    per[name] = (len(b), len(s), avg([e["pos_pct"] for e in b]), avg([e["pos_pct"] for e in s]), avg(cs), code)
for name in sorted(per, key=lambda n: -per[n][0] - per[n][1]):
    nb, ns, bp, sp, cs, code = per[name]
    cs_s = f"{cs:+.2f}" if cs else "-"
    print(f"  {name:<8}{code:<11}{nb:>3}{ns:>3}{bp:>10.0f}{sp:>10.0f}{cs_s:>12}")

print("\n  盈亏集中度（跨日配对）：")
gross_pos = sum(c["spread_pct"] for c in cross if c["spread_pct"] > 0)
gross_neg = sum(c["spread_pct"] for c in cross if c["spread_pct"] <= 0)
top2 = sorted(cross, key=lambda x: -x["spread_pct"])[:2]
worst1 = sorted(cross, key=lambda x: x["spread_pct"])[:1]
print(f"    总盈利(价差求和) {gross_pos:+.2f}%   总亏损 {gross_neg:+.2f}%   净 {gross_pos+gross_neg:+.2f}%")
print(f"    最大2笔盈利 {top2[0]['stock']}{top2[0]['spread_pct']:+.2f}% + {top2[1]['stock']}{top2[1]['spread_pct']:+.2f}% "
      f"= {top2[0]['spread_pct']+top2[1]['spread_pct']:+.2f}%，占总盈利 "
      f"{(top2[0]['spread_pct']+top2[1]['spread_pct'])/gross_pos*100:.0f}%")
print(f"    最大1笔亏损 {worst1[0]['stock']}{worst1[0]['spread_pct']:+.2f}%，占总亏损 "
      f"{worst1[0]['spread_pct']/gross_neg*100:.0f}%")
print(f"    剔除最大2笔盈利后，其余 {len(cross)-2} 笔平均价差 "
      f"{avg([c['spread_pct'] for c in cross if c not in top2]):+.2f}%")

print("\n十、他选中的日子有多疯")
amp = [(r["h"] - r["l"]) / r["o"] * 100 for r in rows]
print(f"  当日平均振幅 {avg(amp):.1f}%（A股小盘题材常态约 5-8%）")
print(f"  振幅>10% 的交易日占比: {sum(1 for a in amp if a>10)}/{len(rows)}")
print(f"  当日涨跌停(±10%附近)出现次数: {sum(1 for r in rows if abs(r['pct'])>9.5)}/{len(rows)}")
# 买入时点 vs 当日振幅
big = [r for r in buys if (r["h"]-r["l"])/r["o"]*100 > 10]
small = [r for r in buys if (r["h"]-r["l"])/r["o"]*100 <= 10]
print(f"  大波动日(振幅>10%)买入 {len(big)} 笔，平均买在位置 {avg([r['pos_pct'] for r in big]):.0f}%")
print(f"  小波动日买入 {len(small)} 笔，平均买在位置 {avg([r['pos_pct'] for r in small]):.0f}%")
