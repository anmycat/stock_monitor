#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多空博弈社 —— 86条交易动作 vs 真实行情（不复权日K）逐条比对
产出：
  data/trade_vs_market.csv    逐条比对明细
  data/trade_vs_market.json   结构化结果
  终端打印汇总结论
"""
import json, csv, os
from collections import defaultdict, Counter

BASE = "/Users/hr.wh/WorkBuddy/2026-10-13-00-21-29"
trades = json.load(open(f"{BASE}/data/ks_video_trade_actions.json", encoding="utf-8"))
market = json.load(open(f"{BASE}/raw/market_daily_unadjusted.json", encoding="utf-8"))

NAME2CODE = {v["name"]: k for k, v in market.items() if k != "_meta"}
# 股票名在市场数据里的显示名可能带空格，做归一化
NAME2CODE_NORM = {v["name"].replace(" ", ""): k for k, v in market.items() if k != "_meta"}

def code_of(name):
    if not name:
        return None
    if name in NAME2CODE:
        return NAME2CODE[name]
    return NAME2CODE_NORM.get(name.replace(" ", ""))

# 截图类型 -> 是否"成交价"（成交价必须落在当日[最低,最高]内）
FILL_TYPES = {"成交明细", "历史成交", "已完成委托", "已完成委托列表"}
ORDER_TYPES = {"买入委托", "卖出委托", "历史委托", "卖出委托"}
SNAP_TYPES = {"持仓", "清仓"}

rows = []
skipped = []
for i, t in enumerate(trades, 1):
    name = (t.get("stock_name") or "").strip()
    code = code_of(name)
    price = t.get("price")
    date = t.get("date")
    stype = t.get("screenshot_type") or ""
    direction = t.get("type") or ""
    tm = t.get("time") or ""
    if not code or price is None:
        skipped.append((i, name, direction, price, "缺股票名或价格，无法比对"))
        continue
    day = market.get(code, {}).get("d", {}).get(date)
    if not day:
        skipped.append((i, name, direction, price, f"无 {date} 行情"))
        continue
    o, h, l, c, pc, pct = day
    rng = h - l
    pos = (price - l) / rng * 100 if rng > 0 else 50.0
    pos = max(-20.0, min(120.0, pos))
    is_fill = stype in FILL_TYPES
    is_order = stype in ORDER_TYPES
    # 真实性：成交价必须在当日区间内
    if is_fill:
        if price > h + 1e-6:
            verdict = "❌超出当日最高价"
        elif price < l - 1e-6:
            verdict = "❌低于当日最低价"
        else:
            verdict = "✅在区间内"
    elif is_order:
        verdict = "➖委托价（可偏离，未成交）"
    else:
        verdict = "✅在区间内" if (l - 1e-6 <= price <= h + 1e-6) else "❌超出当日区间"

    # 择时质量分：买在越低越好(100-pos)，卖在越高越好(pos)
    if direction == "买入":
        timing = 100 - pos
    elif direction == "卖出":
        timing = pos
    else:
        timing = None

    rows.append({
        "idx": i, "date": date, "video": t.get("video_code"), "stock": name, "code": code,
        "dir": direction, "time": tm, "price": price, "stype": stype,
        "o": o, "h": h, "l": l, "c": c, "pct": pct, "pos_pct": round(pos, 1),
        "timing": round(timing, 1) if timing is not None else None,
        "verdict": verdict, "is_fill": is_fill,
        "note": t.get("note") or "",
    })

# ---------- 汇总 ----------
fills = [r for r in rows if r["is_fill"]]
out_of_range = [r for r in fills if r["verdict"].startswith("❌")]
in_range = [r for r in fills if r["verdict"].startswith("✅")]

buys = [r for r in rows if r["dir"] == "买入" and r["timing"] is not None]
sells = [r for r in rows if r["dir"] == "卖出" and r["timing"] is not None]

def avg(xs):
    return sum(xs) / len(xs) if xs else 0

# 分档：0-33 差 / 33-66 中 / 66-100 好
def bucket(xs):
    b = Counter()
    for r in xs:
        s = r["timing"]
        b["差(0-33)" if s < 33.33 else ("中(33-66)" if s < 66.67 else "好(67-100)")] += 1
    return b

# ---------- 同日同标的 T+0 配对（仅带时间的成交） ----------
by_key = defaultdict(list)
for r in rows:
    if r["is_fill"] and r["time"] and r["dir"] in ("买入", "卖出"):
        by_key[(r["code"], r["date"])].append(r)

pairs = []
for (code, date), evs in by_key.items():
    evs.sort(key=lambda x: x["time"])
    inv = []  # (price, time)
    for e in evs:
        if e["dir"] == "买入":
            inv.append((e["price"], e["time"]))
        else:
            if inv:
                bp, bt = inv.pop(0)
                spread = (e["price"] - bp) / bp * 100
                pairs.append({
                    "code": code, "stock": market[code]["name"], "date": date,
                    "buy_time": bt, "buy_price": bp, "sell_time": e["time"], "sell_price": e["price"],
                    "spread_pct": round(spread, 2),
                })
pos_pairs = [p for p in pairs if p["spread_pct"] > 0]
neg_pairs = [p for p in pairs if p["spread_pct"] <= 0]

# ---------- 输出 CSV ----------
out_csv = f"{BASE}/data/trade_vs_market.csv"
with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["序号", "日期", "视频", "股票", "代码", "方向", "时间", "截图价",
                "当日开", "当日高", "当日低", "当日收", "当日涨跌%",
                "价格在振幅中位置%", "择时质量分", "比对结论", "截图类型", "备注"])
    for r in rows:
        w.writerow([r["idx"], r["date"], r["video"], r["stock"], r["code"], r["dir"], r["time"],
                    r["price"], r["o"], r["h"], r["l"], r["c"], r["pct"],
                    r["pos_pct"], r["timing"] if r["timing"] is not None else "",
                    r["verdict"], r["stype"], r["note"]])

out_json = f"{BASE}/data/trade_vs_market.json"
json.dump({"rows": rows, "pairs": pairs, "skipped": skipped}, open(out_json, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

# ---------- 打印 ----------
print("=" * 78)
print("一、数据覆盖")
print(f"  交易动作总数        : {len(trades)}")
print(f"  成功比对            : {len(rows)}")
print(f"  跳过(无价/无行情)   : {len(skipped)}  -> {skipped}")
print(f"  其中成交类(需验真)  : {len(fills)}  委托类: {sum(1 for r in rows if r['stype'] in ORDER_TYPES)}  快照类: {sum(1 for r in rows if r['stype'] in SNAP_TYPES)}")

print("\n二、截图真实性校验（成交价 vs 当日真实[最低,最高]）")
print(f"  ✅ 落在当日区间内   : {len(in_range)} / {len(fills)}  ({len(in_range)/max(1,len(fills))*100:.0f}%)")
print(f"  ❌ 超出当日区间     : {len(out_of_range)}")
for r in out_of_range:
    print(f"     - {r['date']} {r['stock']}({r['code']}) {r['dir']} {r['price']} @ {r['time'] or '无时间'} | 当日 [{r['l']}~{r['h']}]")

print("\n三、买卖时点质量（价格在当日振幅中的位置）")
print(f"  买入 {len(buys)} 笔  平均择时分 {avg([r['timing'] for r in buys]):.1f}  分布 {dict(bucket(buys))}")
print(f"  卖出 {len(sells)} 笔  平均择时分 {avg([r['timing'] for r in sells]):.1f}  分布 {dict(bucket(sells))}")
print(f"  （0=买在当日最高/卖在当日最低；100=买在当日最低/卖在当日最高）")

print("\n四、同日同标的 T+0 配对（先买后卖，FIFO）")
print(f"  可配对笔数          : {len(pairs)}")
print(f"  正价差(T成功)       : {len(pos_pairs)}   平均 {avg([p['spread_pct'] for p in pos_pairs]):.2f}%")
print(f"  负价差(T失败)       : {len(neg_pairs)}   平均 {avg([p['spread_pct'] for p in neg_pairs]):.2f}%")
if pairs:
    print(f"  全部配对平均价差    : {avg([p['spread_pct'] for p in pairs]):.2f}%")
print("  明细：")
for p in sorted(pairs, key=lambda x: (x["date"], x["stock"])):
    flag = "✅" if p["spread_pct"] > 0 else "❌"
    print(f"    {flag} {p['date']} {p['stock']:6} 买{p['buy_price']:>7}({p['buy_time']}) -> 卖{p['sell_price']:>7}({p['sell_time']})  {p['spread_pct']:+.2f}%")

print("\n五、当日涨跌环境（这些交易发生在什么日子里）")
allpct = [r["pct"] for r in rows]
up_days = sum(1 for p in allpct if p > 0)
print(f"  交易日涨跌分布      : 上涨 {up_days} / 下跌 {len(allpct)-up_days}")
print(f"  当日涨跌幅 平均 {avg(allpct):.2f}%  最大 {max(allpct):.2f}%  最小 {min(allpct):.2f}%")
print(f"  当日振幅>7% 的占比  : {sum(1 for r in rows if (r['h']-r['l'])/r['o']*100>7)}/{len(rows)}")

print(f"\n输出: {out_csv}\n      {out_json}")
