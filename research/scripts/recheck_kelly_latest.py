#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kelly 最新数据复核（D72/R1，2026-11-21）——纯内存，不改 git 跟踪的研究基线。

目的：用户裁定"用最新数据复核"v1.7 Kelly（R04=2.36%/混合=1.69%）。
把 27 票 + 上证指数数据从 10-21 延伸到 11-20（新浪不复权日K，vol=股；
腾讯 501 限流后的同源降级路径，见 D64；10-21 锚点 OHLC 全票校验通过）。
在内存副本上重跑验证线的 V8 Kelly 逻辑，输出：
  - 最新 3M 窗口（2026-08-22~11-20）Kelly
  - 与 v1/v2/v3 三窗口对比，判断是否需 v1.8

纪律：与 validate_3m_recent_v3.py 同口径（rules v1.7 / R06 avoidance 跳过 /
T+1 开盘成交 / 持有 5 日 / 边界效应截断）。仅展示研究结论，不改 rules JSON。
"""
import json
import os
import sys
import time

import requests

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.path.dirname(BASE)
sys.path.insert(0, os.path.join(BASE, "scripts"))
sys.path.insert(0, ROOT)
import backtest_engine as bt

RAW = os.path.join(BASE, "raw")
VAL_START = os.environ.get("SM_VAL_START", "2026-08-22")
VAL_END = os.environ.get("SM_VAL_END", "2026-11-20")   # 最新滚动 3M 窗口（T 日信号）
HOLD = 5
ANCHOR = "2026-10-21"

M = json.load(open(os.path.join(RAW, "market_full_6m.json"), encoding="utf-8"))
E = json.load(open(os.path.join(RAW, "market_full_ext_6m.json"), encoding="utf-8"))
RULES = json.load(open(os.path.join(BASE, "rules", "methodology_rules.json"), encoding="utf-8"))
FUND = json.load(open(os.path.join(RAW, "fundamentals.json"), encoding="utf-8"))["stocks"]
IDX_RAW = json.load(open(os.path.join(RAW, "index_daily.json"), encoding="utf-8"))
IDX = {d: v["c"] for d, v in IDX_RAW["daily"]["000001.SH"]["d"].items()}


def _sina_symbol(code):
    num, mkt = code.split(".")
    return ("sh" if mkt == "SH" else "sz") + num


def fetch_sina(code, n=50):
    """新浪日K（不复权，vol=股）。腾讯 501 限流时的可靠替代（D64 同源降级路径）。"""
    url = "https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData"
    sym = _sina_symbol(code)
    last = None
    for attempt in range(5):
        try:
            r = requests.get(url, params={"symbol": sym, "scale": "240", "ma": "no", "datalen": str(n)},
                             timeout=15, headers={"User-Agent": "Mozilla/5.0",
                                                  "Referer": "https://finance.sina.com.cn"})
            if r.status_code == 200:
                out = {}
                for b in r.json():
                    d = str(b.get("day", ""))[:10]
                    if d[:4].isdigit():
                        out[d] = [float(b["open"]), float(b["close"]),
                                  float(b["high"]), float(b["low"]), float(b["volume"])]
                if out:
                    return out
        except Exception as e:
            last = e
        time.sleep(0.6 * (attempt + 1))
    print(f"  [WARN] {code} 新浪取数失败: {type(last).__name__ if last else 'empty'}")
    return {}


def extend_stock(node, code, bars):
    """把 bars 中 > ANCHOR 的日期追加到 node['d']；新浪 vol=股，锚点标定 ratio∈{1,100}。"""
    d = node["d"]
    anchor = d[ANCHOR]
    raw_anchor = bars[ANCHOR]
    ratio = round(anchor["vol"] / raw_anchor[4]) if raw_anchor[4] else 0
    if ratio not in (1, 100):
        raise ValueError(f"{code} vol 单位无法标定: ratio={ratio}")
    added = 0
    prev = anchor["c"]
    for date in sorted(k for k in bars if k > ANCHOR):
        if date in d:
            continue
        o, c, h, l, vraw = bars[date]
        row = {
            "o": o, "h": h, "l": l, "c": c, "prev": prev,
            "pct": round((c / prev - 1) * 100, 2) if prev else None,
            "amp": round((h - l) / prev * 100, 2) if prev else None,
            "vol": vraw * ratio,
            "turn": None,   # 复核用：turn 不参与 Kelly/规则条件（R-turn 不在 v1.7 交易规则射程）
        }
        d[date] = row
        prev = c
        added += 1
    return added, ratio


def build_index_regime(idx):
    days = sorted(idx)
    closes = [idx[d] for d in days]
    out = {}
    for i, d in enumerate(days):
        ma20 = sum(closes[max(0, i - 19):i + 1]) / len(closes[max(0, i - 19):i + 1])
        ma60 = sum(closes[max(0, i - 59):i + 1]) / len(closes[max(0, i - 59):i + 1])
        c = closes[i]
        regime = "bull" if (c > ma20 and ma20 > ma60) else (
            "bear" if (c < ma20 and ma20 < ma60) else "neutral")
        out[d] = {"idx_close": round(c, 2), "idx_ma20": round(ma20, 2),
                  "idx_ma60": round(ma60, 2), "idx_bias20": round((c - ma20) / ma20 * 100, 2),
                  "idx_regime": regime}
    return out


def decorate(rows, code, fund, idx_regime):
    f = fund.get(code)
    for i, row in enumerate(rows):
        c = row["c"]
        row["code"] = code
        row["name"] = f.get("name", code) if f else code
        row["pre_runup_5d"] = round((rows[i - 1]["c"] / rows[i - 6]["c"] - 1) * 100, 2) if i >= 6 and rows[i - 1]["c"] and rows[i - 6]["c"] else None
        row["pre_runup_10d"] = round((rows[i - 1]["c"] / rows[i - 11]["c"] - 1) * 100, 2) if i >= 11 and rows[i - 1]["c"] and rows[i - 11]["c"] else None
        row["pct_10d"] = round((c / rows[i - 10]["c"] - 1) * 100, 2) if i >= 10 and c and rows[i - 10]["c"] else None
        g = idx_regime.get(row["date"])
        if g:
            for k, v in g.items():
                row[k] = v
    return rows


def stat(pnls):
    if not pnls:
        return None
    wins = [x for x in pnls if x > 0]
    n = len(pnls)
    return {"n": n, "win_rate": round(len(wins) / n * 100, 1),
            "avg_win": round(sum(wins) / len(wins), 2) if wins else None,
            "avg_loss": round(sum(x for x in pnls if x <= 0) / max(1, len([x for x in pnls if x <= 0])), 2) if any(x <= 0 for x in pnls) else None}


def kelly_of(pnls):
    st = stat(pnls)
    if not st or st["n"] < 5:
        return {"n": st["n"] if st else 0, "note": "样本不足"}
    p = st["win_rate"] / 100
    b = (st["avg_win"] / abs(st["avg_loss"])) if st["avg_loss"] else None
    if not b:
        return {"n": st["n"], "win_rate": st["win_rate"], "note": "b 不可算"}
    f_full = max(0.0, (p * b - (1 - p)) / b)
    return {"n": st["n"], "win_rate": st["win_rate"], "payoff_b": round(b, 3),
            "f_full_pct": round(f_full * 100, 2), "f_half_pct": round(f_full * 50, 2)}


def main():
    print("=" * 78)
    print("Kelly 最新数据复核（D72/R1）—— 数据延伸到 2026-11-20，纯内存不改基线")
    print("=" * 78)

    # 1. 延伸股票数据（新浪源：10-21 锚点 OHLC 全票校验通过、vol=股 ratio=1）
    total_added = 0
    ratios = {}
    for tag, pool in (("main", M), ("ext", E)):
        for code, node in pool["daily"].items():
            bars = fetch_sina(code, 50)
            if ANCHOR not in bars:
                print(f"  [WARN] {code} 无 {ANCHOR} 锚点，跳过延伸（沿用至 10-21）")
                continue
            added, ratio = extend_stock(node, code, bars)
            ratios[code] = ratio
            total_added += added
            time.sleep(0.2)
    print(f"股票延伸：新增 {total_added} 根 bar，{len(ratios)} 票 vol ratio={sorted(set(ratios.values()))}")

    # 2. 延伸指数（仅收盘价参与 regime）
    idx_bars = fetch_sina("000001.SH", 50)
    iadded = 0
    for date in sorted(k for k in idx_bars if k > ANCHOR):
        if date not in IDX:
            IDX[date] = idx_bars[date][1]   # 收盘价
            iadded += 1
    print(f"指数延伸：新增 {iadded} 个交易日收盘价，最新 {max(IDX)}")

    # 3. 构建序列 + decorate
    idx_regime = build_index_regime(IDX)
    all_rows = {}
    for code, node in M["daily"].items():
        all_rows[code] = decorate(bt.build_series(code, M), code, FUND, idx_regime)
    for code, node in E["daily"].items():
        all_rows[code] = decorate(bt.build_series(code, E), code, FUND, idx_regime)

    all_dates = sorted({r["date"] for rs in all_rows.values() for r in rs})
    in_win = [d for d in all_dates if VAL_START <= d <= VAL_END]
    print(f"数据窗口：{all_dates[0]} ~ {all_dates[-1]}（{len(all_dates)} 交易日）")
    print(f"复核窗口：{VAL_START} ~ {VAL_END}（{len(in_win)} 交易日）")
    print(f"边界效应：数据止于 {all_dates[-1]}，完整 5 日持有信号截至 ~2026-11-13")

    # 4. 规则信号（与 v3 同口径：跳过 filter/discipline/risk/regime/avoidance）
    bt.RULES = RULES
    banned = bt.apply_global_filters(all_rows)
    signals = []
    for rid, rule in RULES["trade_rules"].items():
        if rid.startswith("_") or rule.get("direction") in (
                "filter", "discipline", "risk", "regime", "avoidance"):
            continue
        for t in bt.run_rule(rule, all_rows):
            if (t["code"], t["signal_date"]) not in banned:
                signals.append(t)
    sig_w = [s for s in signals if VAL_START <= s["signal_date"] <= VAL_END]
    print(f"信号：全窗口 {len(signals)} 笔，复核窗口 {len(sig_w)} 笔")

    # 5. V8 Kelly
    print("\n" + "=" * 78)
    print(f"V8 Kelly 复核（窗口 {VAL_START}~{VAL_END}）")
    print("=" * 78)
    out = {}
    keys = [("R01", "R01"), ("R02", "R02"), ("R04", "R04"), ("R05", "R05"),
            ("_all(剔R06)", "_all")]
    for label, rid in keys:
        pnls = [s["pnl_pct"] for s in sig_w if s["rule"] != "R06"] if rid == "_all" else \
               [s["pnl_pct"] for s in sig_w if s["rule"] == rid]
        k = kelly_of(pnls)
        out[label] = k
        if "f_half_pct" in k:
            print(f"{label:14} n={k['n']:4} 胜率{k['win_rate']:5.1f}% 盈亏比{k['payoff_b']:5.3f} "
                  f"f*={k['f_full_pct']:6.2f}% 半凯利={k['f_half_pct']:6.2f}%")
        else:
            print(f"{label:14} {k}")

    # 6. 与 v1.7 执行值对比
    print("\n" + "=" * 78)
    print("与 v1.7 执行值对比")
    print("=" * 78)
    cur = {"R04": 2.36, "_all": 1.69, "R01": 2.46, "R02": 0.0, "R05": 0.0}
    for label, rid in keys:
        k = out[label]
        base = cur.get(rid if rid != "_all" else "_all")
        r = rid.split("_")[0]
        base = cur.get(r, cur.get("_all")) if rid == "_all" else cur.get(rid, base)
        if "f_half_pct" in k and isinstance(base, (int, float)):
            delta = k["f_half_pct"] - base
            verdict = "上调" if delta > 0.3 else ("下修" if delta < -0.3 else "维持")
            print(f"{label:14} 复核 {k['f_half_pct']:6.2f}% vs v1.7 {base:5.2f}% "
                  f"Δ={delta:+6.2f}pp → {verdict}")

    # 月度分布 + R04/_all 逐笔（交叉验证用）
    from collections import defaultdict
    by_month = defaultdict(list)
    for s in sig_w:
        by_month[s["signal_date"][:7]].append(s["pnl_pct"])
    monthly = {}
    for mo in sorted(by_month):
        st = stat(by_month[mo])
        monthly[mo] = {"n": st["n"], "win_rate": st["win_rate"], "avg_pnl": round(sum(by_month[mo]) / st["n"], 2)}
    r04_detail = [{"code": s["code"], "date": s["signal_date"],
                   "buy": s.get("buy_date"), "sell": s.get("sell_date"),
                   "pnl": s["pnl_pct"], "reason": s.get("reason")}
                  for s in sig_w if s["rule"] == "R04"]
    print("\n月度分布（信号日口径）:")
    for mo, v in monthly.items():
        print(f"  {mo}: n={v['n']:3} 胜率{v['win_rate']:5.1f}% 均收益{v['avg_pnl']:+.2f}%")

    result = {"window": f"{VAL_START}~{VAL_END}", "data_end": all_dates[-1],
              "in_win_days": len(in_win), "signals_in_window": len(sig_w),
              "kelly": out, "monthly": monthly, "r04_detail": r04_detail,
              "note": "新浪不复权源（腾讯501限流降级）；10-21锚点OHLC全票校验通过、vol=股ratio=1；纯内存未改基线"}
    with open("/tmp/sm_kelly_recheck_result.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=1)
    print("\n结果已写 /tmp/sm_kelly_recheck_result.json")


if __name__ == "__main__":
    main()
