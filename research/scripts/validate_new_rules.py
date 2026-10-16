#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
三条新规则（R11市赚率 / R12预期差 / R13牛熊指标）的最近3个月实盘验证。
数据窗口：2026-07-06 ~ 2026-10-13（主池66个交易日 / 扩展池71个交易日）

样本：
  A. 规则信号（backtest_engine 重新生成，已叠加R07/R08全局过滤）——双池27只
  B. 多空博弈社82条实盘交易动作（data/trade_vs_market_reasoning.json）

纪律：全部用信号日(T)及之前的数据；PR为point-in-time（中报发布前FY25口径，发布后滚动TTM）；
      牛熊regime取信号日上证指数状态。无未来函数。

输出：data/new_rules_validation.json + 控制台报告
"""
import json, csv, os, sys

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE, "scripts"))
import backtest_engine as bt

FUND = json.load(open(os.path.join(BASE, "raw/fundamentals.json"), encoding="utf-8"))["stocks"]
IDX = json.load(open(os.path.join(BASE, "raw/index_daily.json"), encoding="utf-8"))["daily"]
TRADES_B = json.load(open(os.path.join(BASE, "data/trade_vs_market_reasoning.json"), encoding="utf-8"))


# ---------- 指标装饰 ----------
def build_index_regime():
    """上证指数 MA20/MA60 + regime 序列（当日可知）"""
    days = sorted(IDX.keys())
    closes = [IDX[d] for d in days]
    out = {}
    for i, d in enumerate(days):
        ma20 = sum(closes[max(0, i - 19):i + 1]) / len(closes[max(0, i - 19):i + 1])
        ma60 = sum(closes[max(0, i - 59):i + 1]) / len(closes[max(0, i - 59):i + 1])
        c = closes[i]
        if c > ma20 and ma20 > ma60:
            regime = "bull"
        elif c < ma20 and ma20 < ma60:
            regime = "bear"
        else:
            regime = "neutral"
        out[d] = {"idx_close": round(c, 2), "idx_ma20": round(ma20, 2),
                  "idx_ma60": round(ma60, 2), "idx_bias20": round((c - ma20) / ma20 * 100, 2),
                  "idx_regime": regime}
    return out


IDX_REGIME = build_index_regime()


def decorate(rows, code):
    """给每行附加：pct_10d, pre_runup_5d/10d, pe_ttm, roe_ttm, pr, idx_* """
    f = FUND.get(code)
    for i, row in enumerate(rows):
        c = row["c"]
        # 前N日涨幅（不含当日）
        row["pre_runup_5d"] = round((rows[i - 1]["c"] / rows[i - 6]["c"] - 1) * 100, 2) if i >= 6 and rows[i - 1]["c"] and rows[i - 6]["c"] else None
        row["pre_runup_10d"] = round((rows[i - 1]["c"] / rows[i - 11]["c"] - 1) * 100, 2) if i >= 11 and rows[i - 1]["c"] and rows[i - 11]["c"] else None
        # 前10日涨跌幅（含当日，备用）
        row["pct_10d"] = round((c / rows[i - 10]["c"] - 1) * 100, 2) if i >= 10 and c and rows[i - 10]["c"] else None
        # 市赚率（point-in-time）
        pr = pe = roe = None
        if f and c:
            if row["date"] >= f["h1_26_pub"]:
                eps = f["eps_fy25"] + f["eps_h1_26"] - f["eps_h1_25"]
                roe = f["roe_fy25"] + f["roe_h1_26"] - f["roe_h1_25"]
            else:
                eps, roe = f["eps_fy25"], f["roe_fy25"]
            row["eps_ttm"], row["roe_ttm"] = round(eps, 4), round(roe, 2)
            if eps > 0:
                pe = c / eps
                row["pe_ttm"] = round(pe, 2)
                if roe > 0:
                    # 市赚率 PR = PE ÷ ROE（ROE以百分数数值代入，如14.03）
                    # 校准：可口可乐1988建仓 PE≈12.8 / ROE≈27% → PR≈0.474（丁宁原始口径）
                    pr = pe / roe
                    row["pr"] = round(pr, 3)
        # 指数regime
        g = IDX_REGIME.get(row["date"])
        if g:
            for k, v in g.items():
                row[k] = v
    return rows


def pr_band(pr, pe, roe):
    if pr is None:
        return "na_loss" if (pe is None or pe <= 0 or roe is None or roe <= 0) else "na"
    if pr <= 0.4:
        return "le_04"
    if pr <= 0.6:
        return "b04_06"
    if pr <= 0.8:
        return "b06_08"
    return "gt_08"


# ---------- 统计 ----------
def stat(pnls):
    if not pnls:
        return None
    wins = [x for x in pnls if x > 0]
    n = len(pnls)
    return {
        "n": n,
        "win_rate": round(len(wins) / n * 100, 1),
        "avg_pnl": round(sum(pnls) / n, 2),
        "total_pnl": round(sum(pnls), 2),
        "avg_win": round(sum(wins) / len(wins), 2) if wins else None,
        "avg_loss": round(sum(x for x in pnls if x <= 0) / max(1, len([x for x in pnls if x <= 0])), 2) if any(x <= 0 for x in pnls) else None,
    }


def fmt(s):
    if not s:
        return "  n=  0"
    return (f"  n={s['n']:3} 胜率{s['win_rate']:5.1f}% 均盈亏{s['avg_pnl']:+.2f}% "
            f"累计{s['total_pnl']:+8.2f}%")


# ---------- 主流程 ----------
def main():
    # 1. 构建序列 + 装饰
    all_rows, all_ext = {}, {}
    for code in bt.MARKET["daily"]:
        all_rows[code] = decorate(bt.build_series(code), code)
    for code in bt.MARKET_EXT["daily"]:
        all_ext[code] = decorate(bt.build_series(code, bt.MARKET_EXT), code)
    rows_all = {**all_rows, **all_ext}
    n_days = len({r["date"] for rs in rows_all.values() for r in rs})
    print(f"数据窗口：{min(r['date'] for rs in rows_all.values() for r in rs)} ~ "
          f"{max(r['date'] for rs in rows_all.values() for r in rs)}（{n_days}个交易日，27只票）")

    # 2. 生成规则信号（与 docs/09 同口径：R07/R08全局过滤已叠加）
    banned = bt.apply_global_filters(all_rows)
    banned_ext = bt.apply_global_filters(all_ext)
    signals = []
    for rid, rule in bt.RULES["trade_rules"].items():
        if rid.startswith("_") or rule.get("direction") in ("filter", "discipline", "risk", "regime"):
            continue
        for pool, ban in ((all_rows, banned), (all_ext, banned_ext)):
            for t in bt.run_rule(rule, pool):
                if (t["code"], t["signal_date"]) not in ban:
                    row = next(r for r in rows_all[t["code"]] if r["date"] == t["signal_date"])
                    t["pool"] = "main" if pool is all_rows else "ext"
                    t["pr"] = row.get("pr"); t["pe_ttm"] = row.get("pe_ttm"); t["roe_ttm"] = row.get("roe_ttm")
                    t["pr_band"] = pr_band(row.get("pr"), row.get("pe_ttm"), row.get("roe_ttm"))
                    t["pre_runup_5d"] = row.get("pre_runup_5d"); t["pre_runup_10d"] = row.get("pre_runup_10d")
                    t["idx_regime"] = row.get("idx_regime"); t["idx_bias20"] = row.get("idx_bias20")
                    signals.append(t)
    print(f"规则信号样本：{len(signals)} 笔（R01-R06，已叠加R07/R08过滤）")

    # 3. 博主实盘样本
    buys, sells = [], []
    miss = 0
    for rec in TRADES_B:
        row = next((r for r in rows_all.get(rec["code"], []) if r["date"] == rec["date"]), None)
        ttype = rec.get("trade", {}).get("type", "")
        outcome = rec.get("next_pct")  # 信号日收盘→次日收盘
        item = {
            "code": rec["code"], "date": rec["date"], "type": ttype,
            "reason": rec.get("reason"), "outcome": outcome,
            "pr": row.get("pr") if row else None,
            "pe_ttm": row.get("pe_ttm") if row else None,
            "roe_ttm": row.get("roe_ttm") if row else None,
            "pr_band": pr_band(row.get("pr"), row.get("pe_ttm"), row.get("roe_ttm")) if row else "no_data",
            "pre_runup_5d": row.get("pre_runup_5d") if row else None,
            "pre_runup_10d": row.get("pre_runup_10d") if row else None,
            "idx_regime": row.get("idx_regime") if row else None,
            "stock": rec.get("trade", {}).get("stock_name"),
        }
        if row is None:
            miss += 1
        (buys if "买入" in ttype else sells).append(item)
    print(f"博主实盘样本：买入 {len(buys)} 条 / 卖出 {len(sells)} 条（无行情匹配 {miss} 条）")

    result = {"meta": {
        "window": "2026-07-06 ~ 2026-10-13（3个月，27只票）",
        "samples": {"rule_signals": len(signals), "blogger_buys": len(buys), "blogger_sells": len(sells)},
        "discipline": "信号日T收盘可知数据；PR point-in-time；regime取信号日指数状态",
    }}

    # ============ 验证一：R11 市赚率 ============
    print("\n" + "=" * 70)
    print("验证一 R11 市赚率（PR=PE_TTM/ROE_TTM/100，丁宁：≤0.4四折最优，>0.8回避）")
    print("=" * 70)
    band_order = ["le_04", "b04_06", "b06_08", "gt_08", "na_loss", "na", "no_data"]
    band_label = {"le_04": "PR≤0.4 四折带", "b04_06": "0.4<PR≤0.6 可买带", "b06_08": "0.6<PR≤0.8 观察带",
                  "gt_08": "PR>0.8 回避带", "na_loss": "亏损/ROE≤0 不适用", "na": "无PR数据", "no_data": "无行情匹配"}
    r11 = {}
    for b in band_order:
        sigs = [s for s in signals if s["pr_band"] == b]
        bb = [x for x in buys if x["pr_band"] == b]
        r11[b] = {"label": band_label[b],
                  "signals": stat([s["pnl_pct"] for s in sigs]),
                  "signals_n_by_rule": {r: sum(1 for s in sigs if s["rule"] == r) for r in sorted({s["rule"] for s in sigs})},
                  "blogger_buys": stat([x["outcome"] for x in bb if x["outcome"] is not None]),
                  "blogger_buy_stocks": sorted({x["stock"] for x in bb})}
        if r11[b]["signals"] or r11[b]["blogger_buys"]:
            print(f"\n[{band_label[b]}]")
            print("  规则信号:" + fmt(r11[b]["signals"]))
            print("  博主买入:" + fmt(r11[b]["blogger_buys"]))
    # R11过滤效果：剔除 gt_08 + na_loss
    kept = [s for s in signals if s["pr_band"] not in ("gt_08", "na_loss")]
    dropped = [s for s in signals if s["pr_band"] in ("gt_08", "na_loss")]
    kept_b = [x for x in buys if x["pr_band"] not in ("gt_08", "na_loss", "no_data") and x["outcome"] is not None]
    dropped_b = [x for x in buys if x["pr_band"] in ("gt_08", "na_loss") and x["outcome"] is not None]
    r11["filter_effect"] = {
        "rule_signals_before": stat([s["pnl_pct"] for s in signals]),
        "rule_signals_after": stat([s["pnl_pct"] for s in kept]),
        "rule_signals_dropped": stat([s["pnl_pct"] for s in dropped]),
        "blogger_buys_after": stat([x["outcome"] for x in kept_b]),
        "blogger_buys_dropped": stat([x["outcome"] for x in dropped_b]),
    }
    print("\n[R11过滤前后：剔除PR>0.8与亏损股]")
    print("  规则信号 前:" + fmt(r11["filter_effect"]["rule_signals_before"]))
    print("  规则信号 后:" + fmt(r11["filter_effect"]["rule_signals_after"]))
    print("  被剔除者:" + fmt(r11["filter_effect"]["rule_signals_dropped"]))
    print("  博主买入 后:" + fmt(r11["filter_effect"]["blogger_buys_after"]))
    print("  博主买入 剔除:" + fmt(r11["filter_effect"]["blogger_buys_dropped"]))
    # 个股最新PR表
    latest = {}
    for code, f in FUND.items():
        row = rows_all[code][-1]
        latest[code] = {"name": f["name"], "close": row["c"], "pe_ttm": row.get("pe_ttm"),
                        "roe_ttm": row.get("roe_ttm"), "pr": row.get("pr"),
                        "band": pr_band(row.get("pr"), row.get("pe_ttm"), row.get("roe_ttm"))}
    r11["stock_pr_snapshot"] = latest
    print("\n[个股PR快照（2026-10-13收盘，滚动TTM口径）]")
    for code, v in sorted(latest.items(), key=lambda kv: (kv[1]["pr"] is None, kv[1]["pr"] or 0)):
        print(f"  {code} {v['name']:6} 收{v['close']:>8} PE_TTM={str(v['pe_ttm']):>8} ROE_TTM={str(v['roe_ttm']):>7}% PR={str(v['pr']):>7} [{band_label[v['band']]}]")
    result["R11_market_earning_ratio"] = r11

    # ============ 验证二：R12 预期差 ============
    print("\n" + "=" * 70)
    print("验证二 R12 预期差（信号日前涨幅已透支→不追；闷得而蜜：只交易预期差）")
    print("=" * 70)
    r12 = {"scan_5d": {}, "scan_10d": {}, "per_rule_at_default": {}}
    for t5 in [10, 15, 20, 25, 30]:
        kept = [s for s in signals if s["pre_runup_5d"] is not None and s["pre_runup_5d"] <= t5]
        drop = [s for s in signals if s["pre_runup_5d"] is not None and s["pre_runup_5d"] > t5]
        r12["scan_5d"][str(t5)] = {"kept": stat([s["pnl_pct"] for s in kept]),
                                   "dropped": stat([s["pnl_pct"] for s in drop])}
        print(f"  pre5≤{t5:>2}%: 保留" + fmt(r12["scan_5d"][str(t5)]["kept"]) +
              " | 剔除" + fmt(r12["scan_5d"][str(t5)]["dropped"]))
    for t10 in [15, 20, 25, 30, 40]:
        kept = [s for s in signals if s["pre_runup_10d"] is not None and s["pre_runup_10d"] <= t10]
        drop = [s for s in signals if s["pre_runup_10d"] is not None and s["pre_runup_10d"] > t10]
        r12["scan_10d"][str(t10)] = {"kept": stat([s["pnl_pct"] for s in kept]),
                                     "dropped": stat([s["pnl_pct"] for s in drop])}
        print(f"  pre10≤{t10:>2}%: 保留" + fmt(r12["scan_10d"][str(t10)]["kept"]) +
              " | 剔除" + fmt(r12["scan_10d"][str(t10)]["dropped"]))
    # 默认规则 pre5≤15 且 pre10≤25
    kept = [s for s in signals if (s["pre_runup_5d"] is None or s["pre_runup_5d"] <= 15) and (s["pre_runup_10d"] is None or s["pre_runup_10d"] <= 25)]
    drop = [s for s in signals if not ((s["pre_runup_5d"] is None or s["pre_runup_5d"] <= 15) and (s["pre_runup_10d"] is None or s["pre_runup_10d"] <= 25))]
    r12["filter_effect"] = {"before": stat([s["pnl_pct"] for s in signals]),
                            "after": stat([s["pnl_pct"] for s in kept]),
                            "dropped": stat([s["pnl_pct"] for s in drop])}
    print("\n[R12默认规则 pre5≤15%且pre10≤25%]")
    print("  规则信号 前:" + fmt(r12["filter_effect"]["before"]))
    print("  规则信号 后:" + fmt(r12["filter_effect"]["after"]))
    print("  被剔除者:" + fmt(r12["filter_effect"]["dropped"]))
    # 分规则（默认阈值）——看R12是否误杀动量规则
    for rid in sorted({s["rule"] for s in signals}):
        ss = [s for s in signals if s["rule"] == rid]
        k = [s for s in ss if (s["pre_runup_5d"] is None or s["pre_runup_5d"] <= 15) and (s["pre_runup_10d"] is None or s["pre_runup_10d"] <= 25)]
        r12["per_rule_at_default"][rid] = {"name": ss[0]["rule_name"], "before": stat([s["pnl_pct"] for s in ss]),
                                           "after": stat([s["pnl_pct"] for s in k])}
        print(f"    {rid} {ss[0]['rule_name'][:12]:14} 前:" + fmt(r12["per_rule_at_default"][rid]["before"]) +
              " 后:" + fmt(r12["per_rule_at_default"][rid]["after"]))
    # 博主买入
    kb = [x for x in buys if x["outcome"] is not None and x["pre_runup_5d"] is not None]
    kept_b = [x for x in kb if x["pre_runup_5d"] <= 15 and (x["pre_runup_10d"] is None or x["pre_runup_10d"] <= 25)]
    drop_b = [x for x in kb if not (x["pre_runup_5d"] <= 15 and (x["pre_runup_10d"] is None or x["pre_runup_10d"] <= 25))]
    r12["blogger"] = {"before": stat([x["outcome"] for x in kb]),
                      "after": stat([x["outcome"] for x in kept_b]),
                      "dropped": stat([x["outcome"] for x in drop_b])}
    print("  博主买入 前:" + fmt(r12["blogger"]["before"]) + " 后:" + fmt(r12["blogger"]["after"]) +
          " 剔除:" + fmt(r12["blogger"]["dropped"]))
    result["R12_expectation_gap"] = r12

    # ============ 验证三：R13 牛熊指标 ============
    print("\n" + "=" * 70)
    print("验证三 R13 牛熊指标（上证指数>MA20且MA20>MA60=bull；反之为bear）")
    print("=" * 70)
    # regime 时间线
    win_days = [d for d in sorted(IDX_REGIME) if "2026-07-06" <= d <= "2026-10-13"]
    reg_days = {}
    for d in win_days:
        reg_days.setdefault(IDX_REGIME[d]["idx_regime"], []).append(d)
    r13 = {"regime_days": {k: len(v) for k, v in reg_days.items()},
           "regime_timeline": [], "by_regime": {}, "filter_effect": {}, "index_perf": {}}
    # 连续regime区间
    cur, start = None, None
    for d in win_days:
        rg = IDX_REGIME[d]["idx_regime"]
        if rg != cur:
            if cur:
                r13["regime_timeline"].append({"regime": cur, "from": start, "to": prev})
            cur, start = rg, d
        prev = d
    r13["regime_timeline"].append({"regime": cur, "from": start, "to": prev})
    print("\n[regime区间]")
    for seg in r13["regime_timeline"]:
        i0 = IDX_REGIME[seg["from"]]["idx_close"]; i1 = IDX_REGIME[seg["to"]]["idx_close"]
        seg["idx_change_pct"] = round((i1 / i0 - 1) * 100, 2)
        print(f"  {seg['from']} ~ {seg['to']}  {seg['regime']:8} 指数 {i0}→{i1} ({seg['idx_change_pct']:+.2f}%)")
    for rg in ("bull", "bear", "neutral"):
        sigs = [s for s in signals if s["idx_regime"] == rg]
        bb = [x for x in buys if x["idx_regime"] == rg and x["outcome"] is not None]
        r13["by_regime"][rg] = {"signals": stat([s["pnl_pct"] for s in sigs]),
                                "blogger_buys": stat([x["outcome"] for x in bb]),
                                "signal_n_by_rule": {r: sum(1 for s in sigs if s["rule"] == r) for r in sorted({s["rule"] for s in sigs})}}
        if r13["by_regime"][rg]["signals"] or r13["by_regime"][rg]["blogger_buys"]:
            print(f"\n[{rg}] 天数{len(reg_days.get(rg, []))}")
            print("  规则信号:" + fmt(r13["by_regime"][rg]["signals"]))
            print("  博主买入:" + fmt(r13["by_regime"][rg]["blogger_buys"]))
    # bear禁开仓效果
    kept = [s for s in signals if s["idx_regime"] != "bear"]
    drop = [s for s in signals if s["idx_regime"] == "bear"]
    r13["filter_effect"] = {"before": stat([s["pnl_pct"] for s in signals]),
                            "after": stat([s["pnl_pct"] for s in kept]),
                            "dropped": stat([s["pnl_pct"] for s in drop])}
    print("\n[R13过滤：bear regime禁止开新仓]")
    print("  规则信号 前:" + fmt(r13["filter_effect"]["before"]))
    print("  规则信号 后:" + fmt(r13["filter_effect"]["after"]))
    print("  被禁者:" + fmt(r13["filter_effect"]["dropped"]))
    # 指数自身表现（bull日 vs bear日 平均日涨跌）
    pcts = json.load(open(os.path.join(BASE, "raw/index_daily.json"), encoding="utf-8"))
    del pcts
    result["R13_bull_bear_regime"] = r13

    # ============ 组合A（证伪）：三条当全局硬闸门齐上 ============
    print("\n" + "=" * 70)
    print("组合A（证伪）：三条当全局硬闸门齐上 R11+R12+R13_bear_ban")
    print("=" * 70)
    def pass_all(s):
        if s["pr_band"] in ("gt_08", "na_loss"):
            return False
        if s["pre_runup_5d"] is not None and s["pre_runup_5d"] > 15:
            return False
        if s["pre_runup_10d"] is not None and s["pre_runup_10d"] > 25:
            return False
        if s["idx_regime"] == "bear":
            return False
        return True
    kept = [s for s in signals if pass_all(s)]
    combo = {"before": stat([s["pnl_pct"] for s in signals]), "after": stat([s["pnl_pct"] for s in kept]),
             "kept_by_rule": {r: sum(1 for s in kept if s["rule"] == r) for r in sorted({s["rule"] for s in kept})}}
    print("  前:" + fmt(combo["before"]))
    print("  后:" + fmt(combo["after"]))
    print("  保留分布:", combo["kept_by_rule"])
    result["combo_naive_all_three"] = combo

    # ============ 组合B（修复后采纳配置） ============
    print("\n" + "=" * 70)
    print("组合B（修复后采纳配置）：R12默认过滤 + R13急跌禁低吸变体（R11回归选股层）")
    print("=" * 70)
    def pass_adopted(s):
        if s["pre_runup_5d"] is not None and s["pre_runup_5d"] > 15:
            return False
        if s["pre_runup_10d"] is not None and s["pre_runup_10d"] > 25:
            return False
        if s["rule"] == "R05" and s.get("idx_bias20") is not None and s["idx_bias20"] <= -2.0:
            return False
        return True
    kept = [s for s in signals if pass_adopted(s)]
    combo_b = {"before": stat([s["pnl_pct"] for s in signals]), "after": stat([s["pnl_pct"] for s in kept]),
               "kept_by_rule": {r: sum(1 for s in kept if s["rule"] == r) for r in sorted({s["rule"] for s in kept})}}
    print("  前:" + fmt(combo_b["before"]))
    print("  后:" + fmt(combo_b["after"]))
    print("  保留分布:", combo_b["kept_by_rule"])
    result["combo_adopted"] = combo_b

    # ============ 补充剖切：相关性 / R13变体 / 敏感性 ============
    print("\n" + "=" * 70)
    print("补充剖切")
    print("=" * 70)
    extra = {}

    # (1) PR 与收益的秩相关（Spearman，信号样本 + 博主买入）
    def spearman(pairs):
        n = len(pairs)
        if n < 10:
            return None
        xs = sorted(range(n), key=lambda i: pairs[i][0])
        ys = sorted(range(n), key=lambda i: pairs[i][1])
        rx = {i: r for r, i in enumerate(xs)}
        ry = {i: r for r, i in enumerate(ys)}
        d2 = sum((rx[i] - ry[i]) ** 2 for i in range(n))
        return round(1 - 6 * d2 / (n * (n * n - 1)), 3)

    sig_pr = [(s["pr"], s["pnl_pct"]) for s in signals if s["pr"] is not None]
    blog_pr = [(x["pr"], x["outcome"]) for x in buys if x["pr"] is not None and x["outcome"] is not None]
    extra["pr_spearman"] = {"rule_signals": spearman(sig_pr), "blogger_buys": spearman(blog_pr)}
    print(f"\n[PR与收益的Spearman秩相关] 规则信号: {extra['pr_spearman']['rule_signals']} (n={len(sig_pr)})"
          f" | 博主买入: {extra['pr_spearman']['blogger_buys']} (n={len(blog_pr)})")

    # (2) R13 变体：指数急跌禁低吸(R05) / 指数急涨禁追高(R01/R02)——结合docs/07"急跌低吸必亏"教训
    variants = {}
    for name, bias_lo, bias_hi, rules in (
        ("idx_bias20<=-2 禁R05低吸（急跌不接飞刀）", -2.0, None, ["R05"]),
        ("idx_bias20<=-3 禁R05低吸", -3.0, None, ["R05"]),
        ("idx_bias20>=+2 禁R01/R02追高（急涨不追）", None, 2.0, ["R01", "R02"]),
        ("idx_bias20>=+3 禁R01/R02追高", None, 3.0, ["R01", "R02"]),
    ):
        def hit(s):
            b = s.get("idx_bias20")
            if b is None:
                return False
            if bias_lo is not None and b <= bias_lo and s["rule"] in rules:
                return True
            if bias_hi is not None and b >= bias_hi and s["rule"] in rules:
                return True
            return False
        drop = [s for s in signals if hit(s)]
        keep = [s for s in signals if not hit(s)]
        variants[name] = {"before": stat([s["pnl_pct"] for s in signals]),
                          "after": stat([s["pnl_pct"] for s in keep]),
                          "dropped": stat([s["pnl_pct"] for s in drop])}
        print(f"\n[{name}]")
        print("  前:" + fmt(variants[name]["before"]) + " 后:" + fmt(variants[name]["after"]) +
              " 被禁:" + fmt(variants[name]["dropped"]))
    extra["R13_variants"] = variants

    # (3) 敏感性：剔除R06倒T（其P&L为T价差口径，与其它规则不可比）
    sig_no_t = [s for s in signals if s["rule"] != "R06"]
    sens = {"all_rules": stat([s["pnl_pct"] for s in signals]), "excl_R06": stat([s["pnl_pct"] for s in sig_no_t])}
    for label, flt in (
        ("R11", lambda s: s["pr_band"] not in ("gt_08", "na_loss")),
        ("R12", lambda s: (s["pre_runup_5d"] is None or s["pre_runup_5d"] <= 15) and (s["pre_runup_10d"] is None or s["pre_runup_10d"] <= 25)),
        ("R13_bear_ban", lambda s: s["idx_regime"] != "bear"),
    ):
        keep = [s for s in sig_no_t if flt(s)]
        sens[label] = {"before": stat([s["pnl_pct"] for s in sig_no_t]), "after": stat([s["pnl_pct"] for s in keep])}
        print(f"\n[敏感性·剔除R06后 {label}] 前:" + fmt(sens[label]["before"]) + " 后:" + fmt(sens[label]["after"]))
    extra["sensitivity_excl_R06"] = sens
    result["extra_analysis"] = extra

    # 4. 落盘
    with open(os.path.join(BASE, "data/new_rules_validation.json"), "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    with open(os.path.join(BASE, "data/new_rules_signals.csv"), "w", newline="", encoding="utf-8-sig") as f:
        cols = ["rule", "rule_name", "pool", "code", "stock", "signal_date", "buy_date", "buy_price",
                "sell_date", "sell_price", "pnl_pct", "reason", "pr_band", "pr", "pe_ttm", "roe_ttm",
                "pre_runup_5d", "pre_runup_10d", "idx_regime", "idx_bias20"]
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(sorted(signals, key=lambda x: (x["signal_date"], x["code"])))
    print(f"\n已落盘：data/new_rules_validation.json + data/new_rules_signals.csv（{len(signals)}笔信号带三维标签）")

if __name__ == "__main__":
    main()
