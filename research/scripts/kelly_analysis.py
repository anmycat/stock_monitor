#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kelly 仓位深化 + 权益曲线模拟
1. 按规则×池分组计算 Kelly
2. 相关性修正：同标的同时只允许1仓 → 有效独立信号数≈标的数，Kelly按 1/sqrt(相关簇) 折扣
3. 连亏保护模拟：按修正后仓位跑权益曲线，输出最大回撤/终值
"""
import json, csv, os, math
from collections import defaultdict

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
S = json.load(open(os.path.join(BASE, "data/backtest_summary.json"), encoding="utf-8"))
TRADES = list(csv.DictReader(open(os.path.join(BASE, "data/backtest_trades.csv"), encoding="utf-8-sig")))
KCFG = json.load(open(os.path.join(BASE, "rules/methodology_rules.json"), encoding="utf-8"))["kelly"]["params"]

FRACTION = KCFG["fraction"]           # 半凯利
MAX_POS = KCFG["max_single_position_pct"]  # 单规则上限25%


def kelly_table():
    """按 规则×池 输出 Kelly 表"""
    rows = []
    for pool in ("main", "ext"):
        for rid, blk in S[pool]["rules"].items():
            st = blk.get("stats")
            if not st or st["n"] < 5:
                continue
            p = st["win_rate"] / 100
            b = st["payoff_ratio"]
            q = 1 - p
            f_full = (b * p - q) / b if b > 0 else 0
            f_half = max(0.0, f_full) * FRACTION
            rows.append({
                "pool": "主池" if pool == "main" else "扩展池",
                "rule": rid.split("_")[0], "rule_key": rid, "name": blk["name"],
                "n": st["n"], "win_rate": st["win_rate"],
                "payoff": b, "expectancy": st["expectancy"],
                "kelly_full": round(f_full * 100, 1),
                "kelly_half": round(f_half * 100, 1),
                "kelly_capped": round(min(f_half, MAX_POS) * 100, 1),
            })
    return rows


def correlation_discount(trades, rule_id, codes=None):
    """相关性折扣：同标的信号占比例越高，折扣越大。
    独立信号假设下 Kelly 有效；若 N 笔信号集中在 K 个标的，有效信息量≈K。
    折扣系数 = sqrt(K/N)"""
    by_stock = defaultdict(int)
    for t in trades:
        if t["rule"] == rule_id and (codes is None or t["code"] in codes):
            by_stock[t["code"]] += 1
    n = sum(by_stock.values())
    k = len(by_stock)
    if n == 0:
        return 1.0, 0, 0
    return math.sqrt(k / n), n, k


def equity_curve(trades, pos_size, codes=None, start=100.0):
    """按固定仓位跑权益曲线（按 buy_date 顺序）"""
    trades = [t for t in trades if codes is None or t["code"] in codes]
    trades = sorted(trades, key=lambda x: x["buy_date"])
    eq = start
    peak = eq
    mdd = 0.0
    for t in trades:
        pnl = float(t["pnl_pct"]) / 100
        # 倒T的机会成本单不计入本金曲线（少赚非亏）
        if t.get("reason") == "到期追回(机会成本)":
            continue
        eq *= (1 + pnl * pos_size)
        peak = max(peak, eq)
        mdd = min(mdd, (eq - peak) / peak * 100)
    return eq, mdd


def main():
    rows = kelly_table()
    print("=== Kelly 表（规则×池）===")
    print(f"{'池':6}{'规则':28}{'n':>4}{'胜率':>7}{'盈亏比':>7}{'期望':>8}{'Kelly全':>8}{'半凯利':>8}{'上限':>6}")
    for r in rows:
        print(f"{r['pool']:6}{r['rule'][:26]:28}{r['n']:>4}{r['win_rate']:>6.1f}%{r['payoff']:>7.2f}"
              f"{r['expectancy']:>7.2f}%{r['kelly_full']:>7.1f}%{r['kelly_half']:>7.1f}%{r['kelly_capped']:>5.1f}%")

    print("\n=== 相关性修正（同标的信号聚集度，分池）===")
    MAIN_CODES = set(json.load(open(os.path.join(BASE, "raw/market_full.json"), encoding="utf-8"))["daily"].keys())
    EXT_CODES = set(json.load(open(os.path.join(BASE, "raw/market_full_ext.json"), encoding="utf-8"))["daily"].keys())
    for r in rows:
        codes = MAIN_CODES if r["pool"] == "主池" else EXT_CODES
        disc, n, k = correlation_discount(TRADES, r["rule"], codes)
        r["corr_discount"] = round(disc, 2)
        r["adj_position"] = round(r["kelly_capped"] / 100 * disc * 100, 1)
        print(f"  {r['pool']} {r['rule_key'][:26]:28} n={n:3} 标的数={k:2} 折扣={disc:.2f} "
              f"→ 修正仓位 {r['adj_position']:.1f}%")

    print("\n=== 权益曲线模拟（修正仓位，起点100）===")
    results = []
    for r in rows:
        pool_codes = MAIN_CODES if r["pool"] == "主池" else EXT_CODES
        trades = [t for t in TRADES if t["rule"] == r["rule"] and t["code"] in pool_codes]
        if not trades:
            continue
        eq, mdd = equity_curve(trades, r["adj_position"] / 100)
        results.append({**r, "final_equity": round(eq, 1), "max_dd": round(mdd, 1)})
        print(f"  {r['pool']} {r['rule_key'][:26]:28} 仓位{r['adj_position']:>5.1f}% → 终值{eq:>7.1f} 最大回撤{mdd:>6.1f}%")

    # 全部有效规则合并权益曲线（各规则修正仓位独立运行后合并）
    print("\n=== 组合模拟（各规则独立跑，仓位互不叠加）===")
    total_eq = 0.0
    for r in results:
        pool_codes = MAIN_CODES if r["pool"] == "主池" else EXT_CODES
        trades = [t for t in TRADES if t["rule"] == r["rule"] and t["code"] in pool_codes]
        eq, _ = equity_curve(trades, r["adj_position"] / 100)
        total_eq += eq - 100
    print(f"  各规则独立权益增益合计: {total_eq:+.1f}%（起点100，非复利叠加）")

    out = {"kelly_params": KCFG,
           "table": results,
           "note": "相关性折扣=sqrt(标的数/信号数)；倒T机会成本单不计入本金曲线"}
    with open(os.path.join(BASE, "data/kelly_analysis.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("\n已落盘 data/kelly_analysis.json")


if __name__ == "__main__":
    main()
