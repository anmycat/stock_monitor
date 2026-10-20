#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
P3 后量超前量 · 牛熊分段复验（D28 中间态收口）

背景：docs/20 的 P3 结论（n=491，5日超额 -2.51%，聚类 t=-2.26，4/4个月为负）
出自 2026-07~10 成长股熊市窗口。按 docs/09 §8.3"单市况结论不外推"纪律，
本脚本用 ≥1 年全市场数据（2024-07-15~2026-10-20，548 交易日）按指数趋势标注
牛/熊/震荡 regime，分段回测 P3，回答唯一问题：

    "缩量回调后放量长阳 = 派发风险"是熊市特征，还是全 regime 成立？

纪律与口径同 docs/20 / docs/09 §11（无未来函数）：
  - T 日收盘信号 → T+1 开盘买 → 持有 5 日收盘卖；剔一字涨停入场
  - 基准 = 全样本同纪律 5 日前瞻收益（按买入日聚合）；Welch + 按日聚类双显著性
  - regime 标注：上证指数收盘 vs MA60（±2% 缓冲），BULL/BEAR/FLAT，按买入日归类
  - 通过线（项目约定）：两 regime 超额均 ≤0（或至少不显著为正）才可提交 rules 升版；
    任一 regime 显著为正 → 否决或加 regime 条件缩窄

输出：research/data/p3_regime_validation.json + research/data/p3_regime_signals.csv
"""
import json, os, csv, math, sys
from collections import defaultdict

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE, "scripts"))
import validate_candlestick_patterns as V   # 复用检测/纪律/统计（main 有 __main__ 保护）

RAW = os.path.join(BASE, "raw", "pattern_pool_klines_2y.json")
OUT_JSON = os.path.join(BASE, "data", "p3_regime_validation.json")
OUT_CSV = os.path.join(BASE, "data", "p3_regime_signals.csv")

WIN_START, WIN_END = "2025-01-06", "2026-10-13"   # 500根数据起点≈2024-09-23，+70个预热交易日≥65根K；末次信号可完成5日持有
HOLD = 5
REGIME_MA, REGIME_BUF = 60, 0.02
VOLMULTS = [1.5, 2.0, 2.2]


def build_regimes(index_blk, ma=REGIME_MA, buf=REGIME_BUF):
    """上证指数收盘 vs MA60 → 每日 regime（BULL/BEAR/FLAT）"""
    bars = index_blk["bars"]
    closes = [b[2] for b in bars]
    reg = {}
    for i in range(ma, len(bars)):
        m = sum(closes[i - ma:i]) / ma
        c = closes[i]
        if c > m * (1 + buf):
            reg[bars[i][0]] = "BULL"
        elif c < m * (1 - buf):
            reg[bars[i][0]] = "BEAR"
        else:
            reg[bars[i][0]] = "FLAT"
    return reg


def regime_spans(reg):
    """各 regime 的首末日期与天数（报告用）"""
    out = {}
    for r in ("BULL", "BEAR", "FLAT"):
        ds = sorted(d for d, v in reg.items() if v == r)
        if ds:
            out[r] = {"days": len(ds), "first": ds[0], "last": ds[-1]}
    return out


def main():
    t0 = __import__("time").time()
    data = json.load(open(RAW, encoding="utf-8"))
    print(f"原始池：{data['meta']['stocks_ok']} 只（{data['meta']['fetched_at']}）")
    dates_all = sorted({b[0] for s in data["stocks"].values() for b in s["bars"]})
    print(f"数据区间：{dates_all[0]} ~ {dates_all[-1]}（{len(dates_all)} 天）")

    # 复用验证器的检测/纪律函数，窗口改挂本脚本的 2 年窗口
    V.WIN_START, V.WIN_END = WIN_START, WIN_END
    uni = V.build_universe(data)
    print(f"过滤后票池：{len(uni)} 只（窗口前≥{V.MIN_PRE_BARS}根K + 日均额≥{V.MIN_AMT/1e4:.0f}万 + 有成交≥{V.MIN_ACTIVE_DAYS}天）")

    # regime 标注（上证为主，创业板交叉参考）
    reg = build_regimes(data["index"]["000001"])
    reg_spans = regime_spans(reg)
    reg_gem = build_regimes(data["index"]["399006"])
    agree = sum(1 for d in reg if d in reg_gem and reg[d] == reg_gem[d])
    both = sum(1 for d in reg if d in reg_gem)
    print(f"regime（上证 vs MA60±2%）：{json.dumps(reg_spans, ensure_ascii=False)}")
    print(f"  与创业板指标注一致率：{agree}/{both} = {agree/both*100:.1f}%")
    sh = data["index"]["000001"]["bars"]
    w = [b for b in sh if WIN_START <= b[0] <= WIN_END]
    print(f"窗口内上证指数：{w[0][2]} → {w[-1][2]}（{(w[-1][2]/w[0][2]-1)*100:+.1f}%）")

    # ---------- 全市场基准（同纪律，按买入日聚合） ----------
    base_by_day = defaultdict(list)
    for code, u in uni.items():
        s = u["s"]
        for i in range(u["i0"], s["n"] - HOLD):
            if not V.in_win(s, i):
                continue
            fr = V.fwd_ret(s, i, HOLD)
            if fr:
                base_by_day[fr[0]].append(fr[3])
    base_day_mean = {d: sum(v) / len(v) for d, v in base_by_day.items()}
    base_all = [x for v in base_by_day.values() for x in v]
    bstat = V.stat(base_all)
    print(f"全市场基准：{bstat['n']} 个(票,日)，5日均值 {bstat['mean']:+.3f}%，胜率 {bstat['win_rate']}%")

    # ---------- P3 信号采集（多组 volmult） ----------
    def collect(volmult):
        sigs = []
        for code, u in uni.items():
            s = u["s"]
            for i in V.find_p3(s, u["i0"], stage=3, volmult=volmult):
                fr = V.fwd_ret(s, i, HOLD)
                if not fr:
                    continue
                buy_date, _, sell_date, pnl = fr
                if V.is_yizi_limitup(s, i + 1):
                    continue
                r = reg.get(buy_date)
                if r is None:
                    continue
                sigs.append({"code": code, "name": u["name"], "signal_date": s["dates"][i],
                             "buy_date": buy_date, "sell_date": sell_date,
                             "pnl": round(pnl, 3), "regime": r, "volmult": volmult})
        return sigs

    def group_stats(sigs):
        """总体 + 分 regime：原始均值 / 超额(Welch) / 聚类显著性"""
        out = {}
        pnls = [x["pnl"] for x in sigs]
        st = V.stat(pnls)
        w = V.welch(pnls, base_all)
        cl = V.clustered_stat(sigs, base_day_mean)
        out["overall"] = {"raw": st, "welch_vs_base": w, "clustered": cl,
                          "n_days": len({x["buy_date"] for x in sigs})}
        for r in ("BULL", "FLAT", "BEAR"):
            sub = [x for x in sigs if x["regime"] == r]
            if len(sub) < 5:
                out[r] = {"n": len(sub), "note": "样本不足(<5)"}
                continue
            rp = [x["pnl"] for x in sub]
            # 该 regime 的全市场基准（只取同 regime 买入日）
            rbase = [x for d, v in base_by_day.items() if reg.get(d) == r for x in v]
            rw = V.welch(rp, rbase) if len(rbase) >= 5 else None
            rcl = V.clustered_stat(sub, base_day_mean)
            # 分月一致性（按信号月）
            mm = defaultdict(list)
            for x in sub:
                mm[x["signal_date"][:7]].append(x["pnl"])
            months = {m: {"n": len(v), "mean": round(sum(v) / len(v), 3)} for m, v in sorted(mm.items())}
            neg_m = sum(1 for m in months if months[m]["mean"] < 0)
            out[r] = {"n": len(sub), "raw": V.stat(rp), "welch_vs_regime_base": rw,
                      "clustered": rcl, "months": months,
                      "neg_months": f"{neg_m}/{len(months)}"}
        return out

    results = {"meta": {
        "raw_file": os.path.basename(RAW), "fetched_at": data["meta"]["fetched_at"],
        "window": [WIN_START, WIN_END], "hold": HOLD, "universe": len(uni),
        "data_range": [dates_all[0], dates_all[-1]], "trading_days": len(dates_all),
        "regime_rule": f"上证指数收盘 vs MA{REGIME_MA}（±{int(REGIME_BUF*100)}% 缓冲）",
        "regime_spans": reg_spans, "gem_agreement": round(agree / both * 100, 1),
        "baseline": bstat, "sh_window_chg_pct": round((w[-1][2] / w[0][2] - 1) * 100, 2),
        "doc20_reference": {"window": "2026-07-01~10-12", "P3_n": 491, "P3_excess_5d": -2.507,
                            "P3_welch_t": -5.96, "P3_clustered_t": -2.26, "months_neg": "4/4"},
    }}

    all_sigs = []
    for vm in VOLMULTS:
        sigs = collect(vm)
        all_sigs.extend(sigs)
        gs = group_stats(sigs)
        results[f"volmult_{vm}"] = gs
        o = gs["overall"]
        wtxt = (f"超额 {o['welch_vs_base']['mean_diff']:+.3f}% (t={o['welch_vs_base']['t']})"
                if o["welch_vs_base"] else "超额 n/a")
        ctxt = f"聚类t={o['clustered']['t_clustered']}" if o["clustered"] else "聚类t=n/a"
        print(f"\n== volmult={vm} ==  n={o['raw']['n']}  原始均值 {o['raw']['mean']:+.3f}%  "
              f"胜率 {o['raw']['win_rate']}%  {wtxt}  {ctxt}")
        for r in ("BULL", "FLAT", "BEAR"):
            g = gs[r]
            if "note" in g:
                print(f"   {r}: {g['note']}")
                continue
            rw = g["welch_vs_regime_base"]
            rwtxt = (f"超额 {rw['mean_diff']:+.3f}% (t={rw['t']})"
                     if rw else "超额 n/a")
            rcl = g["clustered"]
            rctxt = f"聚类t={rcl['t_clustered']}" if rcl else "聚类t=n/a"
            print(f"   {r}: n={g['n']} 原始 {g['raw']['mean']:+.3f}% 胜率 {g['raw']['win_rate']}% "
                  f"{rwtxt} {rctxt} 负月{g['neg_months']}")

    # docs/20 同窗口子集复现校验（2026-07-01~10-12，volmult=2.0）
    sub = [x for x in all_sigs if x["volmult"] == 2.0 and "2026-07-01" <= x["signal_date"] <= "2026-10-12"]
    if sub:
        sp = [x["pnl"] for x in sub]
        sw = V.welch(sp, base_all)
        scl = V.clustered_stat(sub, base_day_mean)
        results["doc20_subwindow_check"] = {
            "n": len(sub), "raw_mean": round(sum(sp) / len(sp), 3),
            "excess": sw["mean_diff"] if sw else None, "welch_t": sw["t"] if sw else None,
            "clustered_t": scl["t_clustered"] if scl else None,
            "note": "与 docs/20 口径差异：2年数据允许更多股票通过预热过滤，n 应 ≥491，方向/量级应一致"}
        print(f"\ndocs/20 子窗口复现：n={len(sub)}（docs/20 为 491）超额 {sw['mean_diff']:+.3f}%"
              f"（docs/20 为 -2.507%）t={sw['t']}")

    # ---------- 半年 × regime 分解（回答：负Edge是"熊市"还是"这段熊市"） ----------
    sigs20 = [x for x in all_sigs if x["volmult"] == 2.0]
    halves = [("2025H1", "2025-01-06", "2025-06-30"), ("2025H2", "2025-07-01", "2025-12-31"),
              ("2026H1", "2026-01-01", "2026-06-30"), ("2026H2", "2026-07-01", "2026-10-13")]
    decomp = {}
    print("\n== 半年 × regime 分解（volmult=2.0，超额 vs 同regime全市场基准）==")
    for hname, h0, h1 in halves:
        decomp[hname] = {}
        for r in ("BULL", "FLAT", "BEAR"):
            cell = [x for x in sigs20 if x["regime"] == r and h0 <= x["buy_date"] <= h1]
            if len(cell) < 5:
                decomp[hname][r] = {"n": len(cell), "note": "样本不足(<5)"}
                print(f"  {hname} {r}: 样本不足({len(cell)})")
                continue
            cp = [x["pnl"] for x in cell]
            cbase = [x for d, v in base_by_day.items()
                     if reg.get(d) == r and h0 <= d <= h1 for x in v]
            cw = V.welch(cp, cbase) if len(cbase) >= 5 else None
            ccl = V.clustered_stat(cell, base_day_mean)
            decomp[hname][r] = {"n": len(cell), "raw_mean": round(sum(cp) / len(cp), 3),
                                "excess": cw["mean_diff"] if cw else None,
                                "t": cw["t"] if cw else None,
                                "clustered_t": ccl["t_clustered"] if ccl else None}
            et = f"{cw['mean_diff']:+.3f}% (t={cw['t']})" if cw else "n/a"
            ct = f"聚类t={ccl['t_clustered']}" if ccl else "聚类t=n/a"
            print(f"  {hname} {r}: n={len(cell)} 原始 {sum(cp)/len(cp):+.3f}% 超额 {et} {ct}")
    results["half_regime_decomp"] = decomp

    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=1)
    with open(OUT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        wcsv = csv.DictWriter(f, fieldnames=["code", "name", "signal_date", "buy_date",
                                             "sell_date", "pnl", "regime", "volmult"])
        wcsv.writeheader()
        wcsv.writerows(all_sigs)
    print(f"\n输出：{OUT_JSON} + {OUT_CSV}（{len(all_sigs)} 条信号），耗时 {__import__('time').time()-t0:.0f}s")


if __name__ == "__main__":
    main()
