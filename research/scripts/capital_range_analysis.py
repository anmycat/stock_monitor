#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
资金性质识别 × 规则射程交叉验证
1. 对 27 只票跑 SC-01(慢资金)/HC-01(起爆)/YZ-01(游资)/DR-01(派发) 识别
2. 每只票输出资金属性画像（各类型天数占比）
3. 交叉验证：信号日命中 HC/SC/YZ 属性时，各交易规则的实际表现差异 → 实证"射程"
"""
import json, csv, os, sys
from collections import defaultdict, Counter

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE, "scripts"))
from backtest_engine import build_series, MARKET, MARKET_EXT, match_block, eval_cond

RULES = json.load(open(os.path.join(BASE, "rules/methodology_rules.json"), encoding="utf-8"))
TRADES = list(csv.DictReader(open(os.path.join(BASE, "data/backtest_trades.csv"), encoding="utf-8-sig")))

# ---------- 1. 资金属性识别 ----------
def classify(rows):
    """返回 {date: [属性...]}"""
    cap = RULES["capital_rules"]
    out = {}
    for r in rows:
        tags = []
        if match_block(cap["slow_capital"]["conditions"], r):
            tags.append("慢资金")
        if match_block(cap["hot_capital"]["conditions"], r):
            tags.append("起爆")
        if match_block(cap["youzi_capital"]["conditions"], r):
            tags.append("游资")
        if match_block(cap["distribution_risk"]["conditions"], r):
            tags.append("派发风险")
        if tags:
            out[r["date"]] = tags
    return out


def main():
    pools = {"main": {c: build_series(c, MARKET) for c in MARKET["daily"]},
             "ext": {c: build_series(c, MARKET_EXT) for c in MARKET_EXT["daily"]}}

    # 2. 每只票的资金属性画像
    print("=== 个股资金属性画像（各属性天数 / 有效天数）===")
    stock_profile = {}
    for pool_name, all_rows in pools.items():
        for code, rows in all_rows.items():
            tags = classify(rows)
            cnt = Counter()
            for t in tags.values():
                for x in t:
                    cnt[x] += 1
            n = len(rows)
            stock_profile[code] = {"name": rows[0]["name"], "pool": pool_name,
                                   "n_days": n, "tags": dict(cnt)}
            if cnt:
                pct = {k: f"{v}/{n}({v/n*100:.0f}%)" for k, v in cnt.most_common()}
                print(f"  [{pool_name}] {rows[0]['name']:8} {code} {' '.join(f'{k}:{v}' for k, v in pct.items())}")

    # 3. 射程交叉：信号日的资金属性 vs 规则表现
    print("\n=== 射程交叉验证：信号日资金属性 × 规则表现 ===")
    cross = defaultdict(list)
    for t in TRADES:
        code = t["code"]
        sig_date = t["signal_date"]
        prof = stock_profile.get(code)
        if not prof:
            continue
        # 找信号日前最近一次识别记录（属性有持续性，用信号日本身识别）
        tags = classify(pools[prof["pool"]][code])
        day_tags = tags.get(sig_date, [])
        key = (t["rule"], "+".join(day_tags) if day_tags else "无属性")
        cross[key].append(float(t["pnl_pct"]))

    print(f"{'规则':6}{'信号日属性':14}{'n':>4}{'均值':>8}{'胜率':>7}")
    for (rule, tag), pnls in sorted(cross.items()):
        if len(pnls) < 2:
            continue
        avg = sum(pnls) / len(pnls)
        wr = sum(1 for x in pnls if x > 0) / len(pnls) * 100
        print(f"{rule:6}{tag:14}{len(pnls):>4}{avg:>7.2f}%{wr:>6.0f}%")

    # 4. 汇总：属性匹配度总结
    print("\n=== 关键射程结论 ===")
    # 起爆属性日 vs 无属性日，各规则表现
    for rule in ("R01", "R02", "R04", "R05", "R06"):
        hot = [x for (r, tag), v in cross.items() if r == rule and "起爆" in tag for x in v]
        cold = [x for (r, tag), v in cross.items() if r == rule and tag == "无属性" for x in v]
        if hot and cold:
            print(f"  {rule}: 起爆属性日均值 {sum(hot)/len(hot):+.2f}%(n={len(hot)}) vs "
                  f"无属性日均值 {sum(cold)/len(cold):+.2f}%(n={len(cold)})")
        elif hot:
            print(f"  {rule}: 起爆属性日均值 {sum(hot)/len(hot):+.2f}%(n={len(hot)})，无属性日无信号")

    out = {"stock_profile": stock_profile,
           "cross": {f"{r}|{t}": {"n": len(v), "avg": round(sum(v)/len(v), 2),
                                   "win_rate": round(sum(1 for x in v if x > 0)/len(v)*100, 1)}
                     for (r, t), v in cross.items()}}
    with open(os.path.join(BASE, "data/capital_range_analysis.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("\n已落盘 data/capital_range_analysis.json")


if __name__ == "__main__":
    main()
