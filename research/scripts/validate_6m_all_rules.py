#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全部规则的最近半年实盘验证（2026-04-13 ~ 2026-10-13，27只票）。
对照基线：docs/09-13 的 3 个月结论（2026-07-06 ~ 10-13）。

验证对象（rules/methodology_rules.json v1.2 全部）：
  V1 交易规则 R01-R06 各自前瞻收益（T+1开盘买入，持有5日，含R07/R08全局过滤）
  V2 资金四分类 SC-01/HC-01/YZ-01/DR-01 × 规则射程（属性日收益 + 规则在属性日表现）
  V3 R11 市赚率分带（point-in-time：中报发布前FY25口径，8月底后滚动TTM）
  V4 R12 预期差（pre5>15% / pre10>25% 透支过滤）
  V5 R13 牛熊：原版bear禁开仓 + 变体（idx_bias20<=-2 禁R05低吸）
  V6 筹码 C01-C05 + 峰位×获利盘二维交叉
  V7 采纳配置（R12默认过滤 + R13急跌禁低吸变体）
  V8 Kelly 参数（半年窗口胜率/盈亏比 → f*）

纪律：信号日T收盘可知数据；T+1开盘成交；无未来函数。
输出：data/validation_6m.json + data/signals_6m.csv + 控制台报告
"""
import json, csv, os, sys

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE, "scripts"))
import backtest_engine as bt
import chip_distribution as cd

# ---------- 数据源切换为 6 个月 ----------
bt.MARKET = json.load(open(os.path.join(BASE, "raw/market_full_6m.json"), encoding="utf-8"))
bt.MARKET_EXT = json.load(open(os.path.join(BASE, "raw/market_full_ext_6m.json"), encoding="utf-8"))
RULES = json.load(open(os.path.join(BASE, "rules/methodology_rules.json"), encoding="utf-8"))

FUND = json.load(open(os.path.join(BASE, "raw/fundamentals.json"), encoding="utf-8"))["stocks"]
_idx_raw = json.load(open(os.path.join(BASE, "raw/index_daily.json"), encoding="utf-8"))
if "daily" in _idx_raw:   # 嵌套结构 {"daily": {"000001.SH": {"d": {date: {...}}}}}
    _idx_inner = _idx_raw["daily"]["000001.SH"]["d"]
    IDX = {d: v["c"] for d, v in _idx_inner.items()}
else:                      # 旧平铺结构 {date: close}
    IDX = _idx_raw

VAL_START, VAL_END = "2026-04-13", "2026-10-13"   # 半年窗口（T日信号）
HOLD = 5


# ---------- 指数 regime ----------
def build_index_regime():
    days = sorted(IDX.keys())
    closes = [IDX[d] if isinstance(IDX[d], (int, float)) else IDX[d]["c"] for d in days]
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
    """附加 pre_runup / pct_10d / PR(point-in-time) / 指数regime / 资金属性"""
    f = FUND.get(code)
    for i, row in enumerate(rows):
        c = row["c"]
        row["pre_runup_5d"] = round((rows[i - 1]["c"] / rows[i - 6]["c"] - 1) * 100, 2) if i >= 6 and rows[i - 1]["c"] and rows[i - 6]["c"] else None
        row["pre_runup_10d"] = round((rows[i - 1]["c"] / rows[i - 11]["c"] - 1) * 100, 2) if i >= 11 and rows[i - 1]["c"] and rows[i - 11]["c"] else None
        row["pct_10d"] = round((c / rows[i - 10]["c"] - 1) * 100, 2) if i >= 10 and c and rows[i - 10]["c"] else None
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
                    pr = pe / roe
                    row["pr"] = round(pr, 3)
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
    all_dates = sorted({r["date"] for rs in rows_all.values() for r in rs})
    print(f"数据窗口：{all_dates[0]} ~ {all_dates[-1]}（{len(all_dates)}个交易日，27只票）")
    print(f"验证窗口：{VAL_START} ~ {VAL_END}（半年）")

    # 2. 规则信号（R01-R06，叠加R07/R08全局过滤，docs/09同口径）
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
                    t["in_window"] = VAL_START <= t["signal_date"] <= VAL_END
                    signals.append(t)
    sig_w = [s for s in signals if s["in_window"]]
    print(f"规则信号：全窗口 {len(signals)} 笔，其中半年验证窗口 {len(sig_w)} 笔")

    # 3. 基准：验证窗口内每个样本日 T+1 开盘买入持有 5 日
    base_pnls = []
    for code, rows in rows_all.items():
        for i, row in enumerate(rows):
            if not (VAL_START <= row["date"] <= VAL_END) or i < 1 or not row["c"]:
                continue
            if i + 1 >= len(rows):
                continue
            buy = rows[i + 1]["o"]
            si = i + 1 + HOLD
            if si >= len(rows) or not buy or not rows[si]["c"]:
                continue
            base_pnls.append((rows[si]["c"] / buy - 1) * 100)
    baseline = stat(base_pnls)
    print("基准（窗口内每个样本日都买，持有5日）:" + fmt(baseline))

    result = {
        "meta": {
            "window": f"{VAL_START} ~ {VAL_END}（半年，27只票，{len([d for d in all_dates if VAL_START <= d <= VAL_END])}个交易日）",
            "samples": {"rule_signals": len(sig_w), "baseline_days": len(base_pnls)},
            "discipline": "信号日T收盘可知数据；T+1开盘成交；持有5日；R07/R08全局过滤已叠加",
            "baseline": baseline,
        }
    }

    # ============ V1 交易规则 ============
    print("\n" + "=" * 78)
    print("V1 交易规则 R01-R06（半年窗口，T+1开盘买入持有5日；R06为倒T价差口径单列）")
    print("=" * 78)
    v1 = {}
    for rid in ("R01_momentum_breakout", "R02_limitup_open_low", "R03_first_board_retreat",
                "R04_capitulation_rebound", "R05_deep_pullback_buy", "R06_inverse_T"):
        rid_key = rid.split("_")[0]
        sigs = [s for s in sig_w if s["rule"] == rid_key]
        st = stat([s["pnl_pct"] for s in sigs])
        v1[rid] = st
        print(f"{rid:28}" + fmt(st))
    r06 = [s["pnl_pct"] for s in sig_w if s["rule"] == "R06"]
    oth = [s["pnl_pct"] for s in sig_w if s["rule"] != "R06"]
    print(f"\nR06倒T: n={len(r06)} 累计{sum(r06):+.2f}% | 其它规则: n={len(oth)} 累计{sum(oth):+.2f}%")
    v1["_r06_vs_others"] = {"r06_n": len(r06), "r06_total": round(sum(r06), 2),
                            "others_n": len(oth), "others_total": round(sum(oth), 2)}
    result["V1_trade_rules"] = v1

    # ============ V2 资金四分类 × 规则射程 ============
    print("\n" + "=" * 78)
    print("V2 资金四分类（属性日基准收益 + R01-R05在各属性日的表现）")
    print("=" * 78)
    cap_rules = {k: v for k, v in RULES["capital_rules"].items() if isinstance(v, dict) and "id" in v}
    # 给每个样本日打资金属性标签
    day_attr = {}   # (code,date) -> [ids]
    for code, rows in rows_all.items():
        for i, row in enumerate(rows):
            if not (VAL_START <= row["date"] <= VAL_END):
                continue
            tags = []
            for k, rule in cap_rules.items():
                conds = rule.get("conditions", {})
                if bt.match_block(conds, row):
                    tags.append(rule["id"])
            day_attr[(code, row["date"])] = tags
    v2 = {}
    for cid in ("SC-01", "HC-01", "YZ-01", "DR-01"):
        days = [(c, d) for (c, d), tags in day_attr.items() if cid in tags]
        # 属性日基准
        pnls = []
        for (c, d) in days:
            rows = rows_all[c]
            i = next(j for j, r in enumerate(rows) if r["date"] == d)
            if i + 1 >= len(rows):
                continue
            buy = rows[i + 1]["o"]; si = i + 1 + HOLD
            if si >= len(rows) or not buy or not rows[si]["c"]:
                continue
            pnls.append((rows[si]["c"] / buy - 1) * 100)
        st = stat(pnls)
        # R01-R05 在该属性日的信号
        by_rule = {}
        for rid in ("R01_momentum_breakout", "R02_limitup_open_low", "R03_first_board_retreat",
                    "R04_capitulation_rebound", "R05_deep_pullback_buy"):
            rid_key = rid.split("_")[0]
            sigs = [s for s in sig_w if s["rule"] == rid_key and (s["code"], s["signal_date"]) in days]
            by_rule[rid] = stat([s["pnl_pct"] for s in sigs])
        v2[cid] = {"attr_days": st, "rules_in_attr": by_rule}
        nm = next(v["name"] for v in cap_rules.values() if v["id"] == cid)
        print(f"\n{cid} {nm}（属性日 {st['n'] if st else 0} 天）")
        print("  属性日基准:" + fmt(st))
        for rid, s in by_rule.items():
            if s:
                print(f"  {rid:28}" + fmt(s))
    result["V2_capital_attr"] = v2

    # ============ V3 R11 市赚率 ============
    print("\n" + "=" * 78)
    print("V3 R11 市赚率（PR=PE_TTM/ROE_TTM，point-in-time；≤0.4四折/0.4-0.6可买/>0.8回避）")
    print("=" * 78)
    band_order = ["le_04", "b04_06", "b06_08", "gt_08", "na_loss", "na"]
    band_label = {"le_04": "PR≤0.4 四折带", "b04_06": "0.4<PR≤0.6 可买带", "b06_08": "0.6<PR≤0.8 观察带",
                  "gt_08": "PR>0.8 回避带", "na_loss": "亏损/ROE≤0 不适用", "na": "无PR数据"}
    v3 = {}
    for b in band_order:
        sigs = [s for s in sig_w if s["pr_band"] == b]
        st = stat([s["pnl_pct"] for s in sigs])
        v3[b] = {"label": band_label[b], "signals": st,
                 "n_by_rule": {r: sum(1 for s in sigs if s["rule"] == r) for r in sorted({s["rule"] for s in sigs})}}
        if st:
            print(f"{band_label[b]:16}" + fmt(st) + f"  {v3[b]['n_by_rule']}")
    # 池内各票最新PR
    latest_pr = {}
    for code, rows in rows_all.items():
        row = next((r for r in reversed(rows) if r.get("pr") is not None), None)
        if row:
            latest_pr[code] = {"stock": row["name"], "pr": row["pr"], "pe": row.get("pe_ttm"), "roe": row.get("roe_ttm")}
    v3["_pool_latest_pr"] = latest_pr
    result["V3_pr_bands"] = v3

    # ============ V4 R12 预期差 ============
    print("\n" + "=" * 78)
    print("V4 R12 预期差（信号日前5日>15% 或 前10日>25% = 利好已反映，放弃追入）")
    print("=" * 78)
    v4 = {}
    for label, cond in (
        ("pre5>15%", lambda s: s["pre_runup_5d"] is not None and s["pre_runup_5d"] > 15),
        ("pre5<=15%", lambda s: s["pre_runup_5d"] is not None and s["pre_runup_5d"] <= 15),
        ("pre10>25%", lambda s: s["pre_runup_10d"] is not None and s["pre_runup_10d"] > 25),
        ("pre10<=25%", lambda s: s["pre_runup_10d"] is not None and s["pre_runup_10d"] <= 25),
    ):
        sigs = [s for s in sig_w if cond(s)]
        v4[label] = stat([s["pnl_pct"] for s in sigs])
        print(f"{label:12}" + fmt(v4[label]))
    # 过滤前后
    kept = [s for s in sig_w if not ((s["pre_runup_5d"] is not None and s["pre_runup_5d"] > 15) or
                                     (s["pre_runup_10d"] is not None and s["pre_runup_10d"] > 25))]
    v4["_filter_effect"] = {"before": stat([s["pnl_pct"] for s in sig_w]),
                            "after": stat([s["pnl_pct"] for s in kept])}
    print("过滤前:" + fmt(v4["_filter_effect"]["before"]))
    print("过滤后:" + fmt(v4["_filter_effect"]["after"]))
    result["V4_expectation_gap"] = v4

    # ============ V5 R13 牛熊（原版+变体） ============
    print("\n" + "=" * 78)
    print("V5 R13 牛熊指标（上证 MA20/MA60 regime）")
    print("=" * 78)
    v5 = {}
    for rg in ("bull", "neutral", "bear"):
        sigs = [s for s in sig_w if s["idx_regime"] == rg]
        v5[rg] = stat([s["pnl_pct"] for s in sigs])
        print(f"regime={rg:8}" + fmt(v5[rg]))
    # 原版：bear禁开仓
    kept_bear_ban = [s for s in sig_w if s["idx_regime"] != "bear"]
    v5["_original_bear_ban"] = {"before": stat([s["pnl_pct"] for s in sig_w]),
                                "after": stat([s["pnl_pct"] for s in kept_bear_ban])}
    print("原版bear禁开仓 前:" + fmt(v5["_original_bear_ban"]["before"]))
    print("原版bear禁开仓 后:" + fmt(v5["_original_bear_ban"]["after"]))
    # 变体：指数急跌禁R05低吸
    for th in (-2.0, -3.0):
        banned_r05 = [s for s in sig_w if s["rule"] == "R05" and s["idx_bias20"] is not None and s["idx_bias20"] <= th]
        kept_v = [s for s in sig_w if not (s["rule"] == "R05" and s["idx_bias20"] is not None and s["idx_bias20"] <= th)]
        v5[f"_variant_ban_r05_bias{th:+.0f}"] = {
            "banned": stat([s["pnl_pct"] for s in banned_r05]),
            "before": stat([s["pnl_pct"] for s in sig_w]),
            "after": stat([s["pnl_pct"] for s in kept_v]),
        }
        print(f"变体 指数bias20<={th:+.0f}% 禁R05低吸：被禁{len(banned_r05)}笔" +
              (f" 均盈亏{v5[f'_variant_ban_r05_bias{th:+.0f}']['banned']['avg_pnl']:+.2f}%" if banned_r05 else ""))
        print("    前:" + fmt(v5[f"_variant_ban_r05_bias{th:+.0f}"]["before"]) +
              "  后:" + fmt(v5[f"_variant_ban_r05_bias{th:+.0f}"]["after"]))
    # 变体：急涨禁追高
    for th in (2.0, 3.0):
        banned_chase = [s for s in sig_w if s["rule"] in ("R01_momentum_breakout", "R02_limitup_open_low")
                        and s["idx_bias20"] is not None and s["idx_bias20"] >= th]
        kept_v = [s for s in sig_w if not (s["rule"] in ("R01_momentum_breakout", "R02_limitup_open_low")
                                           and s["idx_bias20"] is not None and s["idx_bias20"] >= th)]
        v5[f"_variant_ban_chase_bias{th:+.0f}"] = {
            "banned": stat([s["pnl_pct"] for s in banned_chase]),
            "before": stat([s["pnl_pct"] for s in sig_w]),
            "after": stat([s["pnl_pct"] for s in kept_v]),
        }
        if banned_chase:
            print(f"变体 指数bias20>= {th:+.0f}% 禁R01/R02追高：被禁{len(banned_chase)}笔 "
                  f"均盈亏{stat([s['pnl_pct'] for s in banned_chase])['avg_pnl']:+.2f}%")
            print("    前:" + fmt(v5[f"_variant_ban_chase_bias{th:+.0f}"]["before"]) +
                  "  后:" + fmt(v5[f"_variant_ban_chase_bias{th:+.0f}"]["after"]))
    # regime天数分布
    v5["_regime_days"] = {rg: sum(1 for d in IDX_REGIME if VAL_START <= d <= VAL_END and IDX_REGIME[d]["idx_regime"] == rg)
                          for rg in ("bull", "neutral", "bear")}
    print("窗口内regime天数:", v5["_regime_days"])
    result["V5_regime"] = v5

    # ============ V6 筹码 C01-C05 + 二维交叉 ============
    print("\n" + "=" * 78)
    print("V6 筹码峰规则（通达信口径移动成本分布，与东财接口交叉验证过）")
    print("=" * 78)
    pool = cd.load_pool(filenames=("market_full_6m.json", "market_full_ext_6m.json"))
    chip_met = {}
    for code, blk in pool.items():
        chip_met[code] = {"name": blk["name"], "metrics": cd.compute_chip_series(blk["klines"], decay=1.0)}

    # 筹码样本（每个样本日 T+1开盘买持有5日）
    chip_samples = []
    for code, blk in pool.items():
        klines = blk["klines"]
        for i, k in enumerate(klines):
            d = k["date"]
            if not (VAL_START <= d <= VAL_END) or i < 1 or not k["c"]:
                continue
            m = chip_met[code]["metrics"].get(d)
            if not m:
                continue
            if i + 1 >= len(klines):
                continue
            buy = klines[i + 1]["o"]; si = i + 1 + HOLD
            if si >= len(klines) or not buy or not klines[si]["c"]:
                continue
            pnl = (klines[si]["c"] / buy - 1) * 100
            chip_samples.append({
                "code": code, "stock": blk["name"], "date": d, "pnl": pnl,
                "peak_pos": m["peak_pos"], "profit": m["profit_ratio"],
                "conc": m["concentration"], "npk": m["n_peaks"],
            })
    print(f"筹码样本：{len(chip_samples)} 个样本日")

    def band_stat(sel):
        return stat([s["pnl"] for s in sel])

    v6 = {"baseline": band_stat(chip_samples), "rules": {}, "cross": {}}
    # C01 低位突破确认（pos<=0.6 & 60%<=prof<85%）
    c01 = [s for s in chip_samples if s["peak_pos"] <= 0.6 and 0.6 <= s["profit"] < 0.85]
    v6["rules"]["C01"] = band_stat(c01)
    print("C01 低位突破确认(pos<=0.6 & 获利盘60-85%):" + fmt(v6["rules"]["C01"]))
    # C02 低位多峰收敛（pos<=0.3 & npk>=4）
    c02 = [s for s in chip_samples if s["peak_pos"] <= 0.3 and s["npk"] >= 4]
    v6["rules"]["C02"] = band_stat(c02)
    print("C02 低位多峰收敛(pos<=0.3 & 峰数>=4):" + fmt(v6["rules"]["C02"]))
    # C03 高位派发（pos>0.6 & prof>=85%）
    c03 = [s for s in chip_samples if s["peak_pos"] > 0.6 and s["profit"] >= 0.85]
    v6["rules"]["C03"] = band_stat(c03)
    print("C03 高位派发(pos>0.6 & 获利盘>=85%):" + fmt(v6["rules"]["C03"]))
    # C03预警版：prof>=85% 且 pre5<=3%（需要pre_runup）
    c03w = []
    for s in chip_samples:
        row = next((r for r in rows_all[s["code"]] if r["date"] == s["date"]), None)
        if row and s["profit"] >= 0.85 and row.get("pre_runup_5d") is not None and row["pre_runup_5d"] <= 3:
            c03w.append(s)
    v6["rules"]["C03_warning"] = band_stat(c03w)
    print("C03预警版(获利>=85% & pre5<=3%):" + fmt(v6["rules"]["C03_warning"]))
    # C04 高位半山腰（pos>0.6 & 30%<=prof<60%）
    c04 = [s for s in chip_samples if s["peak_pos"] > 0.6 and 0.3 <= s["profit"] < 0.6]
    v6["rules"]["C04"] = band_stat(c04)
    print("C04 高位半山腰(pos>0.6 & 获利盘30-60%):" + fmt(v6["rules"]["C04"]))
    # C05 高位派发完成（pos>0.9 & npk<=2）
    c05 = [s for s in chip_samples if s["peak_pos"] > 0.9 and s["npk"] <= 2]
    v6["rules"]["C05"] = band_stat(c05)
    print("C05 高位派发完成(pos>0.9 & 峰数<=2):" + fmt(v6["rules"]["C05"]))
    # 二维交叉：峰位×获利盘
    print("\n二维交叉：主峰位置 × 获利盘（持有5日均值%）")
    print("%-12s %10s %10s %10s %10s" % ("峰位\\获利盘", "≤30%", "30-60%", "60-85%", "≥85%"))
    for plo, phi, plabel in [(0, 0.3, "≤0.3"), (0.3, 0.6, "0.3-0.6"), (0.6, 0.9, "0.6-0.9"), (0.9, 1.01, ">0.9")]:
        row_line = "%-12s" % plabel
        for alo, ahi in [(0, 0.3), (0.3, 0.6), (0.6, 0.85), (0.85, 1.01)]:
            sel = [s["pnl"] for s in chip_samples if plo <= s["peak_pos"] < phi and alo <= s["profit"] < ahi]
            row_line += " %10s" % (("%+.2f(%d)" % (sum(sel) / len(sel), len(sel))) if len(sel) >= 3 else "—")
            v6["cross"][f"pos{plo}-{phi}_prof{alo}-{ahi}"] = band_stat([s for s in chip_samples if plo <= s["peak_pos"] < phi and alo <= s["profit"] < ahi])
        print(row_line)
    result["V6_chip_rules"] = v6
    result["V6_chip_samples_n"] = len(chip_samples)

    # ============ V7 采纳配置组合 ============
    print("\n" + "=" * 78)
    print("V7 采纳配置拆解（R12单独 / R13急跌禁低吸单独 / 组合；R11为选股层不参与）")
    print("=" * 78)
    def pass_r12(s):
        if s["pre_runup_5d"] is not None and s["pre_runup_5d"] > 15:
            return False
        if s["pre_runup_10d"] is not None and s["pre_runup_10d"] > 25:
            return False
        return True
    def pass_r13v(s):
        if s["rule"] == "R05" and s["idx_bias20"] is not None and s["idx_bias20"] <= -2:
            return False
        return True
    def pass_adopted(s):
        return pass_r12(s) and pass_r13v(s)
    kept_r12 = [s for s in sig_w if pass_r12(s)]
    kept_r13v = [s for s in sig_w if pass_r13v(s)]
    kept = [s for s in sig_w if pass_adopted(s)]
    v7 = {
        "before": stat([s["pnl_pct"] for s in sig_w]),
        "r12_only": stat([s["pnl_pct"] for s in kept_r12]),
        "r13_variant_only": stat([s["pnl_pct"] for s in kept_r13v]),
        "combo": stat([s["pnl_pct"] for s in kept]),
        "kept_by_rule": {r: sum(1 for s in kept if s["rule"] == r) for r in sorted({s["rule"] for s in kept})},
    }
    print("原始(无过滤):   " + fmt(v7["before"]))
    print("仅R12过滤:      " + fmt(v7["r12_only"]))
    print("仅R13急跌禁低吸:" + fmt(v7["r13_variant_only"]))
    print("R12+R13组合:    " + fmt(v7["combo"]))
    print("保留分布:", v7["kept_by_rule"])
    result["V7_adopted_combo"] = v7

    # ============ V8 Kelly 参数 ============
    print("\n" + "=" * 78)
    print("V8 Kelly 仓位参数（半年窗口重估）")
    print("=" * 78)
    v8 = {}
    for rid in ("R01_momentum_breakout", "R02_limitup_open_low", "R03_first_board_retreat",
                "R04_capitulation_rebound", "R05_deep_pullback_buy", "_all"):
        if rid == "_all":
            pnls = [s["pnl_pct"] for s in sig_w if s["rule"] != "R06"]
        else:
            pnls = [s["pnl_pct"] for s in sig_w if s["rule"] == rid.split("_")[0]]
        st = stat(pnls)
        if not st or st["n"] < 5:
            v8[rid] = {"n": st["n"] if st else 0, "note": "样本不足"}
            print(f"{rid:28} 样本不足(n={st['n'] if st else 0})")
            continue
        p = st["win_rate"] / 100
        b = (st["avg_win"] / abs(st["avg_loss"])) if st["avg_loss"] else None
        if b:
            f_full = max(0.0, (p * b - (1 - p)) / b)
            f_half = f_full / 2
            v8[rid] = {"win_rate": st["win_rate"], "payoff_b": round(b, 3),
                       "f_full_pct": round(f_full * 100, 2), "f_half_pct": round(f_half * 100, 2),
                       "n": st["n"]}
            print(f"{rid:28} 胜率{st['win_rate']:5.1f}% 盈亏比{b:5.2f} f*={f_full*100:6.2f}% 半凯利={f_half*100:6.2f}%")
    result["V8_kelly"] = v8

    # ---------- 落盘 ----------
    with open(os.path.join(BASE, "data/validation_6m.json"), "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    with open(os.path.join(BASE, "data/signals_6m.csv"), "w", newline="", encoding="utf-8-sig") as f:
        cols = ["rule", "rule_name", "pool", "code", "stock", "signal_date", "buy_date", "buy_price",
                "sell_date", "sell_price", "pnl_pct", "reason", "pr_band", "pr", "pe_ttm", "roe_ttm",
                "pre_runup_5d", "pre_runup_10d", "idx_regime", "idx_bias20"]
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(sorted(sig_w, key=lambda x: (x["signal_date"], x["code"])))
    with open(os.path.join(BASE, "data/chip_samples_6m.csv"), "w", newline="", encoding="utf-8-sig") as f:
        cols = ["code", "stock", "date", "pnl", "peak_pos", "profit", "conc", "npk"]
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(chip_samples)
    print(f"\n已落盘：data/validation_6m.json + data/signals_6m.csv（{len(sig_w)}笔）+ data/chip_samples_6m.csv（{len(chip_samples)}样本）")


if __name__ == "__main__":
    main()
