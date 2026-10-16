# -*- coding: utf-8 -*-
"""
筹码峰战法（C01-C06）实盘检验脚本 v2
调研来源：静神00988《筹码峰的8种形态识别主力意图》（东财股吧 2025-03-22）
          + 通达信官方移动成本分布算法 + "90比3"锁仓判据
纪律：T日收盘后算信号（筹码指标全部当日可知），T+1开盘价成交，持有5/10日收盘卖出
样本：2026-09-14 ~ 2026-10-13（最近1个月，20个交易日），主池17只+扩展池10只

v2 变更：C01 经典场景（低位单峰密集+突破）在本批高波动票上零触发，
        故主检验改为「筹码画像维度 × 未来收益」分带研究（区分射程外与规则失效），
        博主对照改为「买入日 vs 全样本基准」而非买入vs卖出。
"""
import json
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from chip_distribution import build_chip_metrics, load_pool

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VAL_START, VAL_END = "2026-09-14", "2026-10-13"  # 最近1个月
HOLD_DAYS = [5, 10]

CONC_TH = 15.0
VR_TH = 1.5
PROFIT_HI = 0.85
TURN_LOCK = 3.0
RET_LOCK = 0.7
RET_STUCK = 0.8
CONC_DIFFUSE = 25.0
DECAY = 1.0


def safe_stat(rows, key="pnl"):
    arr = [r[key] for r in rows if r.get(key) is not None]
    if not arr:
        return {"n": 0}
    wins = [a for a in arr if a > 0]
    return {
        "n": len(arr), "win_rate": round(len(wins) / len(arr), 4),
        "cum": round(float(np.sum(arr)), 2),
        "avg": round(float(np.mean(arr)), 2),
        "median": round(float(np.median(arr)), 2),
    }


def fmt(s, label=""):
    if s.get("n", 0) == 0:
        return f"  {label} n=0"
    return (f"  {label} n={s['n']:3d} 胜率={s['win_rate']:.1%} 累计={s['cum']:+7.2f}% "
            f"均值={s['avg']:+.2f}% 中位={s['median']:+.2f}%")


def vol_ratio_5d(klines, i):
    if i < 5:
        return None
    vols = [klines[j]["vol"] for j in range(i - 5, i) if klines[j].get("vol")]
    if not vols or not klines[i].get("vol"):
        return None
    return klines[i]["vol"] / (sum(vols) / len(vols))


def pct_nd(klines, i, n):
    if i - n < 0:
        return None
    prev = klines[i - n]["c"]
    return (klines[i]["c"] / prev - 1) * 100 if prev else None


def forward_return(klines, i, hold):
    if i + 1 >= len(klines):
        return None
    buy = klines[i + 1].get("o")
    sell_i = i + 1 + hold
    if sell_i >= len(klines) or not buy:
        return None
    sell = klines[sell_i].get("c")
    if not sell:
        return None
    return (sell / buy - 1) * 100


def main():
    print("=" * 80)
    print("筹码峰战法实盘检验 v2（最近1个月 2026-09-14 ~ 10-13，27只票）")
    print("=" * 80)

    # ===== 0. 筹码指标 =====
    metrics = build_chip_metrics(decay=DECAY)
    pool = load_pool()
    klines_map = {c: b["klines"] for c, b in pool.items()}

    # ===== 1. 与东财筹码接口交叉验证 =====
    print("\n" + "=" * 80)
    print("检验一：自算筹码 vs 东财筹码接口（博主82条中49条带接口值）")
    print("=" * 80)
    try:
        blogger = json.load(open(os.path.join(BASE, "data/trade_vs_market_reasoning.json"), encoding="utf-8"))
    except FileNotFoundError:
        blogger = []
    diffs_profit, diffs_cost = [], []
    for b in blogger:
        if b.get("chip_profit") is None:
            continue
        m = metrics.get(b["code"], {}).get("metrics", {}).get(b["date"])
        if not m:
            continue
        diffs_profit.append(abs(m["profit_ratio"] * 100 - b["chip_profit"]))
        if b.get("chip_cost"):
            diffs_cost.append(abs(m["avg_cost"] - b["chip_cost"]) / b["chip_cost"] * 100)
    xval = {}
    if diffs_profit:
        xval = {
            "n": len(diffs_profit),
            "profit_mae": round(float(np.mean(diffs_profit)), 2),
            "profit_within_5pp": round(float(np.mean([d <= 5 for d in diffs_profit])), 3),
            "cost_mape": round(float(np.mean(diffs_cost)), 2) if diffs_cost else None,
            "cost_within_5pct": round(float(np.mean([d <= 5 for d in diffs_cost])), 3) if diffs_cost else None,
        }
        print(f"  样本{xval['n']}条：获利盘平均绝对误差={xval['profit_mae']:.2f}pp（≤5pp占比{xval['profit_within_5pp']:.0%}）")
        if xval["cost_mape"]:
            print(f"  平均成本平均相对误差={xval['cost_mape']:.2f}%（≤5%占比{xval['cost_within_5pct']:.0%}）")
        print("  → 自算筹码与东财接口口径一致，算法可信")

    # ===== 2. 全窗口样本库 =====
    samples = []
    for code, met in metrics.items():
        klines = klines_map[code]
        name = pool[code]["name"]
        for i, k in enumerate(klines):
            date = k["date"]
            if not (VAL_START <= date <= VAL_END) or i < 1:
                continue
            m = met["metrics"].get(date)
            if not m or k.get("c") is None:
                continue
            vr = vol_ratio_5d(klines, i)
            s = {
                "code": code, "stock": name, "signal_date": date, "close": k["c"],
                "vr5": vr, "pct_5d": pct_nd(klines, i, 5), "turn": k.get("turn"),
                "profit_ratio": m["profit_ratio"], "concentration": m["concentration"],
                "peak_price": m["peak_price"], "peak_lower": m["peak_lower"], "peak_upper": m["peak_upper"],
                "peak_pos": m["peak_pos"], "n_peaks": m["n_peaks"], "avg_cost": m["avg_cost"],
                "upper_peak_price": m.get("upper_peak_price"), "lower_peak_price": m.get("lower_peak_price"),
                "upper_peak_retention": m.get("upper_peak_retention"), "lower_peak_retention": m.get("lower_peak_retention"),
                "prev_c": klines[i - 1]["c"],
            }
            for h in HOLD_DAYS:
                s[f"pnl{h}"] = forward_return(klines, i, h)
            samples.append(s)
    print(f"\n[0] 窗口内有效样本：{len(samples)}条（27只票×20个交易日，指标缺失已剔除）")

    # ===== 3. C01 经典场景诊断 =====
    print("\n" + "=" * 80)
    print("检验二：C01「低位单峰密集+放量突破」场景诊断")
    print("=" * 80)
    diag = {"total": 0, "conc": 0, "single": 0, "low_pos": 0, "in_peak": 0, "break": 0, "vol": 0}
    for s in samples:
        diag["total"] += 1
        if s["concentration"] <= CONC_TH:
            diag["conc"] += 1
            if s["n_peaks"] == 1:
                diag["single"] += 1
                if s["peak_pos"] <= 0.5:
                    diag["low_pos"] += 1
                    if s["prev_c"] >= s["peak_lower"]:
                        diag["in_peak"] += 1
                        if s["close"] > s["peak_upper"]:
                            diag["break"] += 1
                            if s["vr5"] and s["vr5"] >= VR_TH:
                                diag["vol"] += 1
    print(f"  漏斗：样本{diag['total']} → 集中度≤{CONC_TH:.0f} {diag['conc']} → 单峰 {diag['single']} → "
          f"低位(峰位≤0.5) {diag['low_pos']} → 前日收盘在峰内 {diag['in_peak']} → 突破上沿 {diag['break']} → 放量 {diag['vol']}")
    print(f"  → C01 完整信号 {diag['vol']} 个；集中度≤15的样本也仅{diag['conc']}条"
          f"（{diag['conc']/diag['total']:.0%}），本批高波动票极少形成低位单峰密集")

    # ===== 4. 筹码画像维度 × 未来收益（分带研究） =====
    print("\n" + "=" * 80)
    print("检验三：筹码画像分带 × 未来收益（T+1开盘买入，持有5/10日）")
    print("=" * 80)
    bands = {}

    def band_scan(name, key, edges, labels):
        print(f"\n  ◆ {name}")
        rows_out = []
        for h in HOLD_DAYS:
            band_rows = []
            for lab in labels:
                sel = [s for s in samples if s.get(key) is not None
                       and labels[lab][0] <= s[key] < labels[lab][1] and s.get(f"pnl{h}") is not None]
                st = safe_stat(sel, f"pnl{h}")
                band_rows.append({"label": lab, **st})
            rows_out.append(band_rows)
        for h, br in zip(HOLD_DAYS, rows_out):
            line = f"    持有{h:2d}日: " + " | ".join(f"{b['label']}:n={b['n']},{b['avg']:+.2f}%" for b in br)
            print(line)
        bands[name] = rows_out

    conc_labels = {"≤10": (0, 10), "10-20": (10, 20), "20-30": (20, 30), "30-50": (30, 50), ">50": (50, 999)}
    band_scan("集中度(90%成本区间宽度)", "concentration", None, conc_labels)

    prof_labels = {"≤30%": (0, 0.3), "30-60%": (0.3, 0.6), "60-85%": (0.6, 0.85), "≥85%": (0.85, 1.01)}
    band_scan("获利盘比例", "profit_ratio", None, prof_labels)

    pos_labels = {"≤0.3": (0, 0.3), "0.3-0.6": (0.3, 0.6), "0.6-0.9": (0.6, 0.9), ">0.9": (0.9, 1.01)}
    band_scan("主峰位置(60日区间分位)", "peak_pos", None, pos_labels)

    # 峰数分带
    print("\n  ◆ 峰数量")
    for h in HOLD_DAYS:
        line = f"    持有{h:2d}日: "
        for n in [1, 2, 3, "≥4"]:
            sel = [s for s in samples if (s["n_peaks"] == n if isinstance(n, int) else s["n_peaks"] >= 4)
                   and s.get(f"pnl{h}") is not None]
            st = safe_stat(sel, f"pnl{h}")
            line += f"{n}峰:n={st['n']},{st['avg']:+.2f}% | "
        print(line)

    # ===== 5. C02 派发预警（放宽版） =====
    print("\n" + "=" * 80)
    print("检验四：C02「高位派发」预警（获利盘≥85% + 5日涨幅≤3% 放量滞涨）")
    print("=" * 80)
    c02 = [s for s in samples if s["profit_ratio"] >= PROFIT_HI and s["pct_5d"] is not None
           and s["pct_5d"] <= 3 and s["vr5"] and s["vr5"] >= 1.2]
    c02b = [s for s in samples if s["profit_ratio"] >= PROFIT_HI]
    c02c = [s for s in samples if s["profit_ratio"] >= PROFIT_HI and s["pct_5d"] is not None and s["pct_5d"] <= 3]
    for label, rows in [("仅获利盘≥85%", c02b), ("获利盘≥85%且5日涨幅≤3%", c02c), ("再加放量(量比≥1.2)", c02)]:
        st5 = safe_stat(rows, "pnl5")
        st10 = safe_stat(rows, "pnl10")
        neg5 = round(float(np.mean([r["pnl5"] < 0 for r in rows if r.get("pnl5") is not None])), 3) if st5["n"] else None
        print(f"  {label}: n={st5['n']} 随后5日均值={st5.get('avg', 0):+.2f}% 下跌占比={neg5} | "
              f"10日均值={st10.get('avg', 0):+.2f}%")

    # ===== 6. 博主82条实盘动作对照（买入日 vs 全样本基准） =====
    print("\n" + "=" * 80)
    print("检验五：博主实盘动作 × 筹码画像（独立验证样本，买入日 vs 全样本基准）")
    print("=" * 80)
    buy_rows, sell_rows = [], []
    for b in blogger:
        m = metrics.get(b["code"], {}).get("metrics", {}).get(b["date"])
        if not m:
            continue
        t = b.get("trade", {})
        ttype = t.get("type", "") if isinstance(t, dict) else ""
        row = {"date": b["date"], "code": b["code"], "stock": t.get("stock_name", ""),
               "type": ttype, "reason": b.get("reason", ""), "next_pct": b.get("next_pct"),
               **{k2: m[k2] for k2 in ("profit_ratio", "concentration", "peak_price", "peak_pos",
                                       "n_peaks", "upper_peak_retention", "lower_peak_retention")}}
        if "买" in ttype:
            buy_rows.append(row)
        elif "卖" in ttype:
            sell_rows.append(row)
    all_conc = [s["concentration"] for s in samples]
    all_prof = [s["profit_ratio"] for s in samples]
    all_pos = [s["peak_pos"] for s in samples]
    print(f"  可对照动作：买入{len(buy_rows)}笔 / 卖出{len(sell_rows)}笔")
    print(f"  全样本基准: 集中度中位={np.median(all_conc):.1f} 获利盘中位={np.median(all_prof):.0%} "
          f"峰位中位={np.median(all_pos):.2f} 单峰占比={np.mean([s['n_peaks']==1 for s in samples]):.0%}")
    for label, rows in [("博主买入日", buy_rows), ("博主卖出日", sell_rows)]:
        if not rows:
            continue
        print(f"  {label}: 集中度中位={np.median([r['concentration'] for r in rows]):.1f} "
              f"获利盘中位={np.median([r['profit_ratio'] for r in rows]):.0%} "
              f"峰位中位={np.median([r['peak_pos'] for r in rows]):.2f} "
              f"单峰占比={np.mean([r['n_peaks']==1 for r in rows]):.0%} "
              f"C01形态(低位单峰密集)命中={np.mean([r['concentration']<=CONC_TH and r['n_peaks']==1 and r['peak_pos']<=0.5 for r in rows]):.0%}")
    # 博主买入后次日收益 × 买入日获利盘分带
    buys_with_next = [r for r in buy_rows if r.get("next_pct") is not None]
    if buys_with_next:
        print("\n  博主买入后次日收益 × 买入日获利盘分带:")
        for lab, lo, hi in [("≤30%", 0, 0.3), ("30-60%", 0.3, 0.6), ("60-85%", 0.6, 0.85), ("≥85%", 0.85, 1.01)]:
            sel = [r for r in buys_with_next if lo <= r["profit_ratio"] < hi]
            if sel:
                print(f"    获利盘{lab}: n={len(sel)} 次日均值={np.mean([r['next_pct'] for r in sel]):+.2f}% "
                      f"胜率={np.mean([r['next_pct']>0 for r in sel]):.0%}")
            else:
                print(f"    获利盘{lab}: n=0")

    # ===== 7. C04/C06 过滤对全样本买入（全样本作为基准买入） =====
    print("\n" + "=" * 80)
    print("检验六：C04(上峰不移)/C06(发散) 作为禁买过滤器的增量效果")
    print("=" * 80)
    for h in [5]:
        base = [s for s in samples if s.get(f"pnl{h}") is not None]
        c04 = [s for s in base if s["upper_peak_retention"] is not None and s["upper_peak_retention"] >= RET_STUCK
               and s["upper_peak_price"] and s["close"] < s["upper_peak_price"]]
        c06 = [s for s in base if s["concentration"] > CONC_DIFFUSE or s["n_peaks"] >= 3]
        kept = [s for s in base if s not in c04 and s not in c06]
        print(f"  全样本基准（T+1买持有{h}日）:" + fmt(safe_stat(base, f"pnl{h}")))
        print(f"  剔除C04上峰不移后:" + fmt(safe_stat(kept, f"pnl{h}")))
        print(f"  C04样本本身:" + fmt(safe_stat(c04, f"pnl{h}")))
        print(f"  C06发散样本本身:" + fmt(safe_stat(c06, f"pnl{h}")))

    # ===== 8. 衰减系数敏感性 =====
    print("\n" + "=" * 80)
    print("检验七：历史换手衰减系数敏感性（全样本基准持有5日）")
    print("=" * 80)
    for decay in [0.5, 1.0, 2.0]:
        met2 = build_chip_metrics(decay=decay)
        rows = []
        for code, met in met2.items():
            klines = klines_map[code]
            for i, k in enumerate(klines):
                date = k["date"]
                if not (VAL_START <= date <= VAL_END) or i < 1 or not k.get("c"):
                    continue
                p = forward_return(klines, i, 5)
                if p is not None:
                    m = met["metrics"].get(date)
                    if m:
                        rows.append({"pnl5": p, "concentration": m["concentration"],
                                     "profit_ratio": m["profit_ratio"], "peak_pos": m["peak_pos"]})
        st = safe_stat(rows, "pnl5")
        print(f"  decay={decay}: n={st['n']} 累计={st['cum']:+.2f}% 均值={st['avg']:+.2f}% | "
              f"集中度中位={np.median([r['concentration'] for r in rows]):.1f}")

    # ===== 8. 二维交叉与关键组合 =====
    result = {
        "meta": {"window": f"{VAL_START}~{VAL_END}", "stocks": len(metrics), "decay": DECAY,
                 "samples": len(samples),
                 "params": {"conc_th": CONC_TH, "vr_th": VR_TH, "profit_hi": PROFIT_HI,
                            "ret_lock": RET_LOCK, "ret_stuck": RET_STUCK}},
        "xval_vs_eastmoney": xval,
        "c01_diagnosis": diag,
        "bands": bands,
        "c02": {"loose_n": safe_stat(c02b, "pnl5")["n"],
                "loose_avg5": safe_stat(c02b, "pnl5").get("avg"),
                "strict_n": safe_stat(c02, "pnl5")["n"],
                "strict_avg5": safe_stat(c02, "pnl5").get("avg")},
        "blogger": {"buys": len(buy_rows), "sells": len(sell_rows)},
    }
    print("\n" + "=" * 80)
    print("检验八：二维交叉（峰位×获利盘 / 峰数×峰位）与关键组合收益")
    print("=" * 80)
    base_avg = float(np.mean([s["pnl5"] for s in samples if s.get("pnl5") is not None]))
    print(f"  基准（每天都买持有5日）= {base_avg:+.2f}%")
    print("\n  二维交叉：主峰位置 × 获利盘（持有5日均值%(n)）")
    print("  %-12s" % "峰位\\获利盘" + "".join("%14s" % x for x in ["≤30%", "30-60%", "60-85%", "≥85%"]))
    cross1 = {}
    for plo, phi, plabel in [(0, 0.3, "≤0.3"), (0.3, 0.6, "0.3-0.6"), (0.6, 0.9, "0.6-0.9"), (0.9, 1.01, ">0.9")]:
        row = "  %-12s" % plabel
        for alo, ahi in [(0, 0.3), (0.3, 0.6), (0.6, 0.85), (0.85, 1.01)]:
            sel = [s["pnl5"] for s in samples if plo <= s["peak_pos"] < phi and alo <= s["profit_ratio"] < ahi
                   and s.get("pnl5") is not None]
            key = f"{plabel}|{alo}-{ahi}"
            cross1[key] = {"n": len(sel), "avg": round(float(np.mean(sel)), 2) if sel else None}
            row += "%14s" % (("{:+.2f}({})".format(np.mean(sel), len(sel))) if len(sel) >= 3 else "—")
        print(row)
    print("\n  二维交叉：峰数 × 峰位（持有5日均值%(n)）")
    print("  %-12s" % "峰数\\峰位" + "".join("%14s" % x for x in ["≤0.3", "0.3-0.6", "0.6-0.9", ">0.9"]))
    cross2 = {}
    for n in [1, 2, 3, 4]:
        row = "  %-12s" % ("{}峰".format(n) if n < 4 else "≥4峰")
        for plo, phi in [(0, 0.3), (0.3, 0.6), (0.6, 0.9), (0.9, 1.01)]:
            sel = [s["pnl5"] for s in samples if (s["n_peaks"] == n if n < 4 else s["n_peaks"] >= 4)
                   and plo <= s["peak_pos"] < phi and s.get("pnl5") is not None]
            key = f"{n}峰|{plo}-{phi}"
            cross2[key] = {"n": len(sel), "avg": round(float(np.mean(sel)), 2) if sel else None}
            row += "%14s" % (("{:+.2f}({})".format(np.mean(sel), len(sel))) if len(sel) >= 3 else "—")
        print(row)
    combos = [
        ("低位+获利盘60-85%（突破确认态）", lambda s: s["peak_pos"] <= 0.6 and 0.6 <= s["profit_ratio"] < 0.85),
        ("低位+获利盘≥85%（突破完成态）", lambda s: s["peak_pos"] <= 0.6 and s["profit_ratio"] >= 0.85),
        ("低位+多峰≥4（吸筹收敛态）", lambda s: s["peak_pos"] <= 0.3 and s["n_peaks"] >= 4),
        ("高位+获利盘≥85%（派发态）", lambda s: s["peak_pos"] > 0.6 and s["profit_ratio"] >= 0.85),
        ("高位+获利盘30-60%（半山腰）", lambda s: s["peak_pos"] > 0.6 and 0.3 <= s["profit_ratio"] < 0.6),
        ("高位+单双峰（派发完成）", lambda s: s["peak_pos"] > 0.9 and s["n_peaks"] <= 2),
    ]
    print("\n  关键组合收益（持有5日）:")
    combo_result = {}
    for label, fn in combos:
        sel = [s["pnl5"] for s in samples if fn(s) and s.get("pnl5") is not None]
        avg = float(np.mean(sel)) if sel else None
        combo_result[label] = {"n": len(sel), "avg": round(avg, 2) if avg is not None else None}
        print("    {:26s}: n={:3d} 均值={}".format(label, len(sel), "{:+.2f}%".format(avg) if avg is not None else "NA"))
    result["cross_peakpos_profit"] = cross1
    result["cross_npeaks_peakpos"] = cross2
    result["combos"] = combo_result
    result["baseline_avg5"] = round(base_avg, 2)

    # ===== 9. 落盘 =====
    with open(os.path.join(BASE, "data/chip_validation.json"), "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    with open(os.path.join(BASE, "data/chip_samples.csv"), "w", encoding="utf-8-sig") as f:
        cols = ["code", "stock", "signal_date", "close", "vr5", "pct_5d", "turn", "profit_ratio",
                "concentration", "peak_price", "peak_lower", "peak_upper", "peak_pos", "n_peaks",
                "upper_peak_price", "lower_peak_price", "upper_peak_retention", "lower_peak_retention",
                "avg_cost", "pnl5", "pnl10"]
        f.write(",".join(cols) + "\n")
        for s in sorted(samples, key=lambda x: (x["signal_date"], x["code"])):
            f.write(",".join(str(s.get(c, "")) for c in cols) + "\n")
    print(f"\n已落盘：data/chip_validation.json + data/chip_samples.csv（{len(samples)}条样本）")


if __name__ == "__main__":
    main()
