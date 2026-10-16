#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
最近3个月实盘验证（2026-07-16 ~ 2026-10-13，27只票，主池17+扩展池10）。
对照基线：data/validation_6m.json（2026-04-13 ~ 10-13 半年窗口）。

验证对象：
  V1-V8  与半年验证同口径（R01-R06 / 资金四分类 / R11市赚率 / R12预期差 /
         R13牛熊变体 / 筹码C01-C05 / 采纳配置 / Kelly）
  V9     迭代计划拟议配置 v1.5 = 采纳配置（R12默认过滤 + R13急跌禁低吸变体）
         + 新增 C01 筹码确认叠加层（R01/R02 买入需 峰位<=0.6 且 获利盘60-85%）
  V10    月度稳定性（7/8/9/10 月分别统计）+ 6M vs 3M 对照

纪律：信号日T收盘可知数据；T+1开盘成交；持有5日；无未来函数。
输出：data/validation_3m.json + data/signals_3m.csv + data/chip_samples_3m.csv
"""
import json, csv, os, sys

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE, "scripts"))
import validate_6m_all_rules as v6   # 复用其数据加载/装饰/统计（模块级副作用仅加载数据）

bt = v6.bt
cd = v6.cd
RULES = v6.RULES
decorate = v6.decorate
pr_band = v6.pr_band
stat = v6.stat
fmt = v6.fmt
IDX_REGIME = v6.IDX_REGIME

VAL_START, VAL_END = "2026-07-16", "2026-10-13"   # 最近3个月窗口（T日信号）
HOLD = 5


def main():
    # 1. 构建序列 + 装饰
    all_rows, all_ext = {}, {}
    for code in bt.MARKET["daily"]:
        all_rows[code] = decorate(bt.build_series(code), code)
    for code in bt.MARKET_EXT["daily"]:
        all_ext[code] = decorate(bt.build_series(code, bt.MARKET_EXT), code)
    rows_all = {**all_rows, **all_ext}
    all_dates = sorted({r["date"] for rs in rows_all.values() for r in rs})
    in_win_days = [d for d in all_dates if VAL_START <= d <= VAL_END]
    print(f"数据窗口：{all_dates[0]} ~ {all_dates[-1]}（{len(all_dates)}个交易日，27只票）")
    print(f"验证窗口：{VAL_START} ~ {VAL_END}（{len(in_win_days)}个交易日）")

    # 2. 规则信号（R01-R06，叠加R07/R08全局过滤）
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
    sig_w = [s for s in signals if VAL_START <= s["signal_date"] <= VAL_END]
    print(f"规则信号：全窗口 {len(signals)} 笔，其中3个月窗口 {len(sig_w)} 笔")

    # 3. 基准
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
            "window": f"{VAL_START} ~ {VAL_END}（最近3个月，27只票，{len(in_win_days)}个交易日）",
            "samples": {"rule_signals": len(sig_w), "baseline_days": len(base_pnls)},
            "discipline": "信号日T收盘可知数据；T+1开盘成交；持有5日；R07/R08全局过滤已叠加",
            "baseline": baseline,
        }
    }

    # ============ V1 交易规则 ============
    print("\n" + "=" * 78)
    print("V1 交易规则 R01-R06（3个月窗口，T+1开盘买入持有5日）")
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

    # ============ V2 资金四分类 ============
    print("\n" + "=" * 78)
    print("V2 资金四分类（属性日基准收益 + R01-R05在各属性日的表现）")
    print("=" * 78)
    cap_rules = {k: v for k, v in RULES["capital_rules"].items() if isinstance(v, dict) and "id" in v}
    day_attr = {}
    for code, rows in rows_all.items():
        for i, row in enumerate(rows):
            if not (VAL_START <= row["date"] <= VAL_END):
                continue
            tags = []
            for k, rule in cap_rules.items():
                if bt.match_block(rule.get("conditions", {}), row):
                    tags.append(rule["id"])
            day_attr[(code, row["date"])] = tags
    v2 = {}
    for cid in ("SC-01", "HC-01", "YZ-01", "DR-01"):
        days = [(c, d) for (c, d), tags in day_attr.items() if cid in tags]
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
    print("V3 R11 市赚率（PR=PE_TTM/ROE_TTM，point-in-time）")
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
    kept = [s for s in sig_w if not ((s["pre_runup_5d"] is not None and s["pre_runup_5d"] > 15) or
                                     (s["pre_runup_10d"] is not None and s["pre_runup_10d"] > 25))]
    v4["_filter_effect"] = {"before": stat([s["pnl_pct"] for s in sig_w]),
                            "after": stat([s["pnl_pct"] for s in kept])}
    print("过滤前:" + fmt(v4["_filter_effect"]["before"]))
    print("过滤后:" + fmt(v4["_filter_effect"]["after"]))
    result["V4_expectation_gap"] = v4

    # ============ V5 R13 牛熊 ============
    print("\n" + "=" * 78)
    print("V5 R13 牛熊指标（上证 MA20/MA60 regime）")
    print("=" * 78)
    v5 = {}
    for rg in ("bull", "neutral", "bear"):
        sigs = [s for s in sig_w if s["idx_regime"] == rg]
        v5[rg] = stat([s["pnl_pct"] for s in sigs])
        print(f"regime={rg:8}" + fmt(v5[rg]))
    kept_bear_ban = [s for s in sig_w if s["idx_regime"] != "bear"]
    v5["_original_bear_ban"] = {"before": stat([s["pnl_pct"] for s in sig_w]),
                                "after": stat([s["pnl_pct"] for s in kept_bear_ban])}
    print("原版bear禁开仓 前:" + fmt(v5["_original_bear_ban"]["before"]))
    print("原版bear禁开仓 后:" + fmt(v5["_original_bear_ban"]["after"]))
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
    v5["_regime_days"] = {rg: sum(1 for d in IDX_REGIME if VAL_START <= d <= VAL_END and IDX_REGIME[d]["idx_regime"] == rg)
                          for rg in ("bull", "neutral", "bear")}
    print("窗口内regime天数:", v5["_regime_days"])
    result["V5_regime"] = v5

    # ============ V6 筹码 C01-C05 ============
    print("\n" + "=" * 78)
    print("V6 筹码峰规则（通达信口径移动成本分布）")
    print("=" * 78)
    pool = cd.load_pool(filenames=("market_full_6m.json", "market_full_ext_6m.json"))
    chip_met = {}
    for code, blk in pool.items():
        chip_met[code] = {"name": blk["name"], "metrics": cd.compute_chip_series(blk["klines"], decay=1.0)}
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
    v6c = {"baseline": stat([s["pnl"] for s in chip_samples]), "rules": {}, "cross": {}}
    c01 = [s for s in chip_samples if s["peak_pos"] <= 0.6 and 0.6 <= s["profit"] < 0.85]
    v6c["rules"]["C01"] = stat([s["pnl"] for s in c01])
    print("C01 低位突破确认(pos<=0.6 & 获利盘60-85%):" + fmt(v6c["rules"]["C01"]))
    c02 = [s for s in chip_samples if s["peak_pos"] <= 0.3 and s["npk"] >= 4]
    v6c["rules"]["C02"] = stat([s["pnl"] for s in c02])
    print("C02 低位多峰收敛(pos<=0.3 & 峰数>=4):" + fmt(v6c["rules"]["C02"]))
    c03 = [s for s in chip_samples if s["peak_pos"] > 0.6 and s["profit"] >= 0.85]
    v6c["rules"]["C03"] = stat([s["pnl"] for s in c03])
    print("C03 高位派发(pos>0.6 & 获利盘>=85%):" + fmt(v6c["rules"]["C03"]))
    c03w = []
    for s in chip_samples:
        row = next((r for r in rows_all[s["code"]] if r["date"] == s["date"]), None)
        if row and s["profit"] >= 0.85 and row.get("pre_runup_5d") is not None and row["pre_runup_5d"] <= 3:
            c03w.append(s)
    v6c["rules"]["C03_warning"] = stat([s["pnl"] for s in c03w])
    print("C03预警版(获利>=85% & pre5<=3%):" + fmt(v6c["rules"]["C03_warning"]))
    c04 = [s for s in chip_samples if s["peak_pos"] > 0.6 and 0.3 <= s["profit"] < 0.6]
    v6c["rules"]["C04"] = stat([s["pnl"] for s in c04])
    print("C04 高位半山腰(pos>0.6 & 获利盘30-60%):" + fmt(v6c["rules"]["C04"]))
    c05 = [s for s in chip_samples if s["peak_pos"] > 0.9 and s["npk"] <= 2]
    v6c["rules"]["C05"] = stat([s["pnl"] for s in c05])
    print("C05 高位派发完成(pos>0.9 & 峰数<=2):" + fmt(v6c["rules"]["C05"]))
    print("\n二维交叉：主峰位置 × 获利盘（持有5日均值%）")
    print("%-12s %10s %10s %10s %10s" % ("峰位\\获利盘", "≤30%", "30-60%", "60-85%", "≥85%"))
    for plo, phi, plabel in [(0, 0.3, "≤0.3"), (0.3, 0.6, "0.3-0.6"), (0.6, 0.9, "0.6-0.9"), (0.9, 1.01, ">0.9")]:
        row_line = "%-12s" % plabel
        for alo, ahi in [(0, 0.3), (0.3, 0.6), (0.6, 0.85), (0.85, 1.01)]:
            sel = [s["pnl"] for s in chip_samples if plo <= s["peak_pos"] < phi and alo <= s["profit"] < ahi]
            row_line += " %10s" % (("%+.2f(%d)" % (sum(sel) / len(sel), len(sel))) if len(sel) >= 3 else "—")
            v6c["cross"][f"pos{plo}-{phi}_prof{alo}-{ahi}"] = stat([s["pnl"] for s in chip_samples if plo <= s["peak_pos"] < phi and alo <= s["profit"] < ahi])
        print(row_line)
    result["V6_chip_rules"] = v6c
    result["V6_chip_samples_n"] = len(chip_samples)

    # ============ V7 采纳配置（v1.4 现行） ============
    print("\n" + "=" * 78)
    print("V7 采纳配置拆解（v1.4：R12默认过滤 + R13急跌禁低吸变体）")
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

    # ============ V8 Kelly ============
    print("\n" + "=" * 78)
    print("V8 Kelly 仓位参数（3个月窗口重估）")
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
            v8[rid] = {"win_rate": st["win_rate"], "payoff_b": round(b, 3),
                       "f_full_pct": round(f_full * 100, 2), "f_half_pct": round(f_full * 50, 2), "n": st["n"]}
            print(f"{rid:28} 胜率{st['win_rate']:5.1f}% 盈亏比{b:5.2f} f*={f_full*100:6.2f}% 半凯利={f_full*50:6.2f}%")
    result["V8_kelly"] = v8

    # ============ V9 迭代计划拟议配置 v1.5 ============
    print("\n" + "=" * 78)
    print("V9 迭代计划拟议配置 v1.5 = V7采纳配置 + C01筹码确认叠加层（R01/R02）")
    print("=" * 78)
    chip_lookup = {(s["code"], s["date"]): s for s in chip_samples}

    def pass_c01_overlay(s):
        """R01/R02 买入信号需满足 C01：峰位<=0.6 且 获利盘 60-85%"""
        if s["rule"] not in ("R01", "R02"):
            return True
        m = chip_lookup.get((s["code"], s["signal_date"]))
        if m is None:
            return False
        return m["peak_pos"] <= 0.6 and 0.6 <= m["profit"] < 0.85

    def pass_plan(s):
        return pass_adopted(s) and pass_c01_overlay(s)

    plan_kept = [s for s in sig_w if pass_plan(s)]
    c01_banned = [s for s in sig_w if pass_adopted(s) and not pass_c01_overlay(s)]
    v9 = {
        "before_all": stat([s["pnl_pct"] for s in sig_w]),
        "adopted_v14": stat([s["pnl_pct"] for s in kept]),
        "plan_v15": stat([s["pnl_pct"] for s in plan_kept]),
        "c01_overlay_banned": stat([s["pnl_pct"] for s in c01_banned]),
        "plan_by_rule": {r: stat([s["pnl_pct"] for s in plan_kept if s["rule"] == r])
                         for r in sorted({s["rule"] for s in plan_kept})},
        "plan_kept_n": len(plan_kept),
    }
    print("原始(无过滤):      " + fmt(v9["before_all"]))
    print("v1.4 采纳配置:     " + fmt(v9["adopted_v14"]))
    print("C01叠加层淘汰:     " + fmt(v9["c01_overlay_banned"]))
    print("v1.5 迭代计划配置: " + fmt(v9["plan_v15"]))
    for r, st in v9["plan_by_rule"].items():
        if st:
            print(f"  {r:28}" + fmt(st))
    # 迭代配置 Kelly
    if v9["plan_v15"] and v9["plan_v15"]["n"] >= 5:
        p = v9["plan_v15"]["win_rate"] / 100
        b = v9["plan_v15"]["avg_win"] / abs(v9["plan_v15"]["avg_loss"]) if v9["plan_v15"]["avg_loss"] else None
        if b:
            f_full = max(0.0, (p * b - (1 - p)) / b)
            v9["plan_kelly"] = {"win_rate": v9["plan_v15"]["win_rate"], "payoff_b": round(b, 3),
                                "f_full_pct": round(f_full * 100, 2), "f_half_pct": round(f_full * 50, 2)}
            print(f"  迭代配置 Kelly: 胜率{v9['plan_v15']['win_rate']}% 盈亏比{b:.2f} "
                  f"f*={f_full*100:.2f}% 半凯利={f_full*50:.2f}%")
    result["V9_plan_v15"] = v9

    # ============ V10 月度稳定性 + 6M对照 ============
    print("\n" + "=" * 78)
    print("V10 月度稳定性（v1.5 迭代配置逐月） + 6M vs 3M 对照")
    print("=" * 78)
    months = {}
    for s in plan_kept:
        months.setdefault(s["signal_date"][:7], []).append(s["pnl_pct"])
    v10 = {"monthly_plan": {m: stat(v) for m, v in sorted(months.items())}}
    for m, st in sorted(v10["monthly_plan"].items()):
        print(f"  {m}: " + fmt(st))
    # 6M 对照
    try:
        v6m = json.load(open(os.path.join(BASE, "data/validation_6m.json"), encoding="utf-8"))
        cmp_rows = []
        for rid in ("R01_momentum_breakout", "R02_limitup_open_low", "R04_capitulation_rebound",
                    "R05_deep_pullback_buy", "R06_inverse_T"):
            s3 = v1.get(rid); s6 = v6m["V1_trade_rules"].get(rid)
            cmp_rows.append({"rule": rid,
                             "m3": {"n": s3["n"], "win_rate": s3["win_rate"], "avg_pnl": s3["avg_pnl"]} if s3 else None,
                             "m6": {"n": s6["n"], "win_rate": s6["win_rate"], "avg_pnl": s6["avg_pnl"]} if s6 else None})
        combo6 = v6m["V7_adopted_combo"]["combo"]
        v10["compare_6m_3m"] = {
            "V1": cmp_rows,
            "baseline": {"m3": baseline, "m6": v6m["meta"]["baseline"]},
            "adopted_combo": {"m3": v7["combo"], "m6": combo6},
            "signals_n": {"m3": len(sig_w), "m6": v6m["meta"]["samples"]["rule_signals"]},
        }
        print("\n6M vs 3M（采纳配置组合）:")
        print("  6M: " + fmt(combo6))
        print("  3M: " + fmt(v7["combo"]))
        print("  基准 6M: " + fmt(v6m["meta"]["baseline"]) + " | 3M: " + fmt(baseline))
    except Exception as e:
        v10["compare_6m_3m"] = {"error": str(e)}
    result["V10_monthly_compare"] = v10

    # ---------- 落盘 ----------
    with open(os.path.join(BASE, "data/validation_3m.json"), "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    with open(os.path.join(BASE, "data/signals_3m.csv"), "w", newline="", encoding="utf-8-sig") as f:
        cols = ["rule", "rule_name", "pool", "code", "stock", "signal_date", "buy_date", "buy_price",
                "sell_date", "sell_price", "pnl_pct", "reason", "pr_band", "pr", "pe_ttm", "roe_ttm",
                "pre_runup_5d", "pre_runup_10d", "idx_regime", "idx_bias20"]
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(sorted(sig_w, key=lambda x: (x["signal_date"], x["code"])))
    with open(os.path.join(BASE, "data/chip_samples_3m.csv"), "w", newline="", encoding="utf-8-sig") as f:
        cols = ["code", "stock", "date", "pnl", "peak_pos", "profit", "conc", "npk"]
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(chip_samples)
    # 迭代配置信号明细（v1.5）
    with open(os.path.join(BASE, "data/plan_v15_signals_3m.csv"), "w", newline="", encoding="utf-8-sig") as f:
        cols = ["rule", "rule_name", "pool", "code", "stock", "signal_date", "buy_date", "buy_price",
                "sell_date", "sell_price", "pnl_pct", "reason", "idx_regime", "idx_bias20",
                "pre_runup_5d", "pre_runup_10d"]
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(sorted(plan_kept, key=lambda x: (x["signal_date"], x["code"])))
    print(f"\n已落盘：data/validation_3m.json + data/signals_3m.csv（{len(sig_w)}笔）"
          f" + data/chip_samples_3m.csv（{len(chip_samples)}样本）"
          f" + data/plan_v15_signals_3m.csv（{len(plan_kept)}笔）")


if __name__ == "__main__":
    main()
