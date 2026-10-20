#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
四大K线形态实盘验证：长阴短柱次日反包 / 底部双倍阳 / 后量超前量 / 探底形态

数据：research/raw/pattern_pool_klines.json（通达信日K，不复权，量=手，2026-10-20 拉取）
纪律（与 research/docs/09 实盘模拟验证方法论一致，无未来函数）：
  - 信号日 T 收盘后确认（只用 T 日及之前数据）
  - T+1 开盘价成交（跳空按实际开盘价）
  - 持有 HOLD 日后收盘卖出（默认5日，附10日）
  - 剔除一字涨停入场日（h==l 且涨幅≥9.5%，现实中无法买在开盘）
基准：窗口内所有(票,日)同样纪律的5日前瞻收益（全市场基准）+ 按交易日聚类显著性
输出：research/data/pattern_validation.json + research/data/pattern_signals.csv
"""
import json, os, csv, math
from collections import defaultdict

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = os.path.join(BASE, "raw", "pattern_pool_klines.json")
OUT_JSON = os.path.join(BASE, "data", "pattern_validation.json")
OUT_CSV = os.path.join(BASE, "data", "pattern_signals.csv")

WIN_START, WIN_END = "2026-07-01", "2026-10-12"   # T日信号窗口（3个月+，末次信号可完成5日持有）
HOLD = 5
MIN_PRE_BARS = 65          # 窗口前至少65根K（60日回看+缓冲，近似排除次新/长期停牌）
MIN_AMT = 5e7              # 窗口内日均成交额≥5000万
MIN_ACTIVE_DAYS = 55       # 窗口内有成交天数≥55（剔除长期停牌）


# ---------- 统计工具 ----------
def stat(pnls):
    n = len(pnls)
    if n == 0:
        return None
    m = sum(pnls) / n
    sd = (sum((x - m) ** 2 for x in pnls) / (n - 1)) ** 0.5 if n > 1 else 0.0
    t = m / (sd / math.sqrt(n)) if sd > 0 else 0.0
    srt = sorted(pnls)
    med = srt[n // 2] if n % 2 else (srt[n // 2 - 1] + srt[n // 2]) / 2
    wins = sum(1 for x in pnls if x > 0)
    return {"n": n, "mean": round(m, 3), "median": round(med, 3), "std": round(sd, 3),
            "t": round(t, 2), "win_rate": round(wins / n * 100, 1),
            "avg_win": round(sum(x for x in pnls if x > 0) / max(1, wins), 3),
            "avg_loss": round(sum(x for x in pnls if x <= 0) / max(1, n - wins), 3)}


def welch(a, b):
    """两样本Welch t检验，返回 t 与近似p值（正态近似）"""
    na, nb = len(a), len(b)
    if na < 2 or nb < 2:
        return None
    ma, mb = sum(a) / na, sum(b) / nb
    va = sum((x - ma) ** 2 for x in a) / (na - 1)
    vb = sum((x - mb) ** 2 for x in b) / (nb - 1)
    se = math.sqrt(va / na + vb / nb)
    if se == 0:
        return None
    t = (ma - mb) / se
    p = 2 * (1 - 0.5 * (1 + math.erf(abs(t) / math.sqrt(2))))
    return {"t": round(t, 2), "p": round(p, 4), "mean_diff": round(ma - mb, 3)}


def clustered_stat(trades, base_by_day):
    """按买入日聚类的显著性：每日(形态均值-基准均值)为一个观测"""
    by_day = defaultdict(list)
    for tr in trades:
        by_day[tr["buy_date"]].append(tr["pnl"])
    daily = []
    for d, pnls in by_day.items():
        if d in base_by_day:
            daily.append(sum(pnls) / len(pnls) - base_by_day[d])
    if len(daily) < 5:
        return None
    m = sum(daily) / len(daily)
    sd = (sum((x - m) ** 2 for x in daily) / (len(daily) - 1)) ** 0.5
    t = m / (sd / math.sqrt(len(daily))) if sd > 0 else 0.0
    pos = sum(1 for x in daily if x > 0)
    return {"n_days": len(daily), "mean_daily_excess": round(m, 3),
            "t_clustered": round(t, 2), "pos_days_ratio": round(pos / len(daily) * 100, 1)}


# ---------- 数据加载与派生 ----------
def prep_stock(blk):
    bars = blk["bars"]
    s = {"dates": [b[0] for b in bars], "o": [b[1] for b in bars], "c": [b[2] for b in bars],
         "h": [b[3] for b in bars], "l": [b[4] for b in bars], "v": [b[5] for b in bars],
         "amt": [b[6] for b in bars]}
    n = len(bars)
    s["n"] = n
    s["vol_ma10"] = [None] * n
    for i in range(n):
        if i >= 10 and all(s["v"][k] > 0 for k in range(i - 10, i)):
            s["vol_ma10"][i] = sum(s["v"][k] for k in range(i - 10, i)) / 10
    s["pos60"] = [None] * n
    s["rng60"] = [None] * n
    for i in range(n):
        if i >= 60:
            lo = min(s["l"][i - 60:i]); hi = max(s["h"][i - 60:i])
            if hi > lo:
                s["pos60"][i] = (s["c"][i] - lo) / (hi - lo)
                s["rng60"][i] = (hi - lo) / lo
    return s


def build_universe(data):
    uni = {}
    for code, blk in data["stocks"].items():
        s = prep_stock(blk)
        idxs = [i for i, d in enumerate(s["dates"]) if WIN_START <= d <= WIN_END]
        if not idxs:
            continue
        i0 = idxs[0]
        if i0 < MIN_PRE_BARS:
            continue
        amts = [s["amt"][i] for i in idxs if s["v"][i] > 0]
        active = sum(1 for i in idxs if s["v"][i] > 0)
        if len(amts) < MIN_ACTIVE_DAYS or sum(amts) / len(amts) < MIN_AMT:
            continue
        uni[code] = {"s": s, "name": blk["name"], "i0": i0,
                     "win_days": len(idxs), "avg_amt": sum(amts) / len(amts)}
    return uni


# ---------- 前瞻收益 ----------
def fwd_ret(s, i_signal, hold=HOLD):
    """信号日i（收盘已知）→ 次日开盘买 → hold日后收盘卖"""
    j = i_signal + 1
    k = j + hold
    if k >= s["n"]:
        return None
    buy = s["o"][j]; sell = s["c"][k]
    if not buy or not sell or s["v"][j] <= 0 or s["v"][k] <= 0:
        return None
    return s["dates"][j], buy, s["dates"][k], (sell / buy - 1) * 100


def is_yizi_limitup(s, j):
    """入场日一字涨停（无法买在开盘）"""
    pct = (s["c"][j] / s["c"][j - 1] - 1) * 100 if j >= 1 and s["c"][j - 1] else 0
    return s["h"][j] == s["l"][j] and pct >= 9.5


# ---------- 形态上下文 ----------
def ctx_ok(s, i, pos60_max, rng60_max=None, rng20_max=None):
    if s["pos60"][i] is None or s["pos60"][i] > pos60_max:
        return False
    if rng60_max is not None and (s["rng60"][i] is None or s["rng60"][i] > rng60_max):
        return False
    if rng20_max is not None:
        if i < 20:
            return False
        lo = min(s["l"][i - 20:i]); hi = max(s["h"][i - 20:i])
        if lo <= 0 or (hi - lo) / lo > rng20_max:
            return False
    return True


def yang_body(s, i):
    return (s["c"][i] - s["o"][i]) / s["o"][i] * 100 if s["o"][i] else 0


def bear_body(s, i):
    return (s["o"][i] - s["c"][i]) / s["o"][i] * 100 if s["o"][i] else 0


def in_win(s, i):
    return WIN_START <= s["dates"][i] <= WIN_END


# ---------- 形态检测（stage=条件累计阶段，返回信号日索引列表） ----------
P1_STAGES = ["S0 横盘震荡(20日振幅≤30% & 60日位置≤0.6)",
             "S1 +长阴(实体≥4%)", "S2 +短柱(量≤0.7×10日均量)",
             "S3 +次日放量阳线(量≥1.5×长阴量)", "S4 +反包(收盘破前日最高)=完整"]


def find_p1(s, i0, stage=4, body_th=4.0, vol_th=0.7, rng20_max=0.30, pos60_max=0.6):
    """长阴短柱次日反包。stage 0-4 条件累计；信号日=最后满足条件日（S3/S4为T+1，其余为T）"""
    out = []
    for i in range(max(i0, 61), s["n"] - 1):
        if not in_win(s, i) or s["v"][i] <= 0 or s["v"][i + 1] <= 0 or s["vol_ma10"][i] is None:
            continue
        if not ctx_ok(s, i, pos60_max, rng20_max=rng20_max):
            continue
        if stage >= 1:
            if bear_body(s, i) < body_th or bear_body(s, i) >= 9.5:
                continue
        if stage >= 2 and s["v"][i] > vol_th * s["vol_ma10"][i]:
            continue
        t2 = i + 1
        if stage >= 3:
            if s["c"][t2] <= s["o"][t2] or s["v"][t2] < 1.5 * s["v"][i]:
                continue
        if stage >= 4 and s["c"][t2] <= s["h"][i]:
            continue
        out.append(t2 if stage >= 3 else i)
    return out


P2_STAGES = ["S0 底部区域(60日位置≤0.3 & 60日振幅≤0.85)",
             "S1 +第一根倍量阳线", "S2 +连续第二根倍量阳=完整"]


def find_p2(s, i0, stage=2, volmult=1.8, pos60_max=0.3):
    """底部双倍阳。S1信号日=第一根阳线日，S2=第二根"""
    out = []
    for i in range(max(i0, 61), s["n"] - 1):
        if not in_win(s, i) or s["v"][i] <= 0 or s["v"][i + 1] <= 0:
            continue
        if s["vol_ma10"][i] is None:
            continue
        if not ctx_ok(s, i, pos60_max, rng60_max=0.85):
            continue
        if stage >= 1:
            if s["c"][i] <= s["o"][i] or s["v"][i] < volmult * s["vol_ma10"][i]:
                continue
        if stage >= 2:
            if s["c"][i + 1] <= s["o"][i + 1] or s["vol_ma10"][i + 1] is None \
                    or s["v"][i + 1] < volmult * s["vol_ma10"][i + 1]:
                continue
        out.append(i + 1 if stage >= 2 else i)
    return out


P3_STAGES = ["S0 缩量回调段(前轮高点后2-8日缩量回落)",
             "S1 +大阳线(实体≥3%)", "S2 +放量(量≥2×10日均量)",
             "S3 +后量超前量(量>前轮高点量)=完整", "S4 +变体:收盘价也超前轮高点"]


def find_p3(s, i0, stage=3, volmult=2.0):
    """后量超前量。S0信号日=回调末日(即大阳前一日)，S1+信号日=大阳日"""
    out = []
    for i in range(max(i0, 61), s["n"]):
        if not in_win(s, i) or s["v"][i] <= 0 or s["vol_ma10"][i] is None:
            continue
        # 前轮上涨高点量 j ∈ [i-15, i-4]
        j = None
        for k in range(max(1, i - 15), i - 3):
            if s["v"][k] <= 0:
                continue
            if j is None or s["v"][k] > s["v"][j]:
                j = k
        if j is None or not (2 <= i - 1 - j <= 8):
            continue
        seg = list(range(j + 1, i))
        if len(seg) < 2:
            continue
        shrink_days = sum(1 for k in seg if s["v"][k] < s["v"][k - 1])
        if shrink_days / len(seg) < 0.6 or s["v"][i - 1] > 0.7 * s["v"][j] \
                or s["c"][i - 1] >= s["c"][j]:
            continue
        if stage == 0:
            out.append(i - 1)
            continue
        if s["c"][i] <= s["o"][i] or yang_body(s, i) < 3.0:
            continue
        if stage >= 2 and s["v"][i] < volmult * s["vol_ma10"][i]:
            continue
        if stage >= 3 and s["v"][i] <= s["v"][j]:
            continue
        if stage >= 4 and s["c"][i] <= s["c"][j]:
            continue
        out.append(i)
    return out


P4_STAGES = ["S0 低位运行(60日位置≤0.35 & 60日振幅≤0.90)",
             "S1 +中大阳线(实体≥3%)放量(≥1.5×)", "S2 +连续两日回调",
             "S3 +探底长下影阳(破前低收回)=完整"]


def find_p4(s, i0, stage=3, shadow_th=0.02, pos60_max=0.35):
    """探底形态。S1信号日=中大阳日，S2=回调结束日，S3=探底阳日"""
    out = []
    for i in range(max(i0, 61), s["n"] - 3):
        if not in_win(s, i):
            continue
        d0, d1, d2, d3 = i, i + 1, i + 2, i + 3
        if any(s["v"][x] <= 0 for x in (d0, d1, d2, d3)):
            continue
        if not ctx_ok(s, d0, pos60_max, rng60_max=0.90):
            continue
        if stage >= 1:
            if s["c"][d0] <= s["o"][d0] or yang_body(s, d0) < 3.0 or s["vol_ma10"][d0] is None \
                    or s["v"][d0] < 1.5 * s["vol_ma10"][d0]:
                continue
        if stage >= 2:
            if not (s["c"][d1] < s["c"][d0] and s["c"][d2] < s["c"][d1]):
                continue
        if stage >= 3:
            if s["c"][d3] <= s["o"][d3]:
                continue
            shadow = (min(s["o"][d3], s["c"][d3]) - s["l"][d3]) / s["c"][d3]
            if shadow < shadow_th or s["l"][d3] >= s["l"][d0] or s["c"][d3] <= s["l"][d0]:
                continue
        if stage == 0:
            out.append(d0)
        elif stage == 1:
            out.append(d0)
        elif stage == 2:
            out.append(d2)
        else:
            out.append(d3)
    return out


PATTERNS = {"P1": (find_p1, P1_STAGES, "长阴短柱·次日反包"),
            "P2": (find_p2, P2_STAGES, "底部双倍阳"),
            "P3": (find_p3, P3_STAGES, "后量超前量"),
            "P4": (find_p4, P4_STAGES, "探底形态")}


# ---------- 主流程 ----------
def main():
    data = json.load(open(RAW, encoding="utf-8"))
    print(f"原始池：{data['meta']['stocks_ok']} 只（{data['meta']['fetched_at']}，{data['meta']['source']}）")
    uni = build_universe(data)
    print(f"过滤后票池：{len(uni)} 只（窗口前≥{MIN_PRE_BARS}根K + 日均额≥{MIN_AMT/1e4:.0f}万 + 有成交≥{MIN_ACTIVE_DAYS}天）")
    idx = data["index"]
    sh = idx.get("000001")
    if sh:
        w = [b for b in sh["bars"] if WIN_START <= b[0] <= WIN_END]
        if w:
            print(f"窗口内上证指数：{w[0][0]} {w[0][2]} → {w[-1][0]} {w[-1][2]}"
                  f"（{(w[-1][2]/w[0][2]-1)*100:+.1f}%）")

    # ---------- 基准 ----------
    base5, base_by_day5 = [], defaultdict(list)
    stock_days = 0
    for code, u in uni.items():
        s = u["s"]
        for i in range(u["i0"], s["n"]):
            if not in_win(s, i):
                continue
            stock_days += 1
            r5 = fwd_ret(s, i, 5)
            if r5:
                base5.append(r5[3]); base_by_day5[r5[0]].append(r5[3])
    base5_by_day = {d: sum(v) / len(v) for d, v in base_by_day5.items()}
    b5 = stat(base5)
    print(f"基准（窗口内所有票×日，T+1开盘买持有5日）：n={b5['n']} 均值{b5['mean']:+.3f}% 胜率{b5['win_rate']}%")
    print(f"股票×交易日：{stock_days}")

    # ---------- 完整形态信号（5日+10日） ----------
    signals = []
    for code, u in uni.items():
        s = u["s"]
        for pid, (fn, _, name) in PATTERNS.items():
            for i_sig in fn(s, u["i0"]):
                for hold in (5, 10):
                    r = fwd_ret(s, i_sig, hold)
                    if not r:
                        continue
                    bd, bp, sd, pnl = r
                    signals.append({"pattern": pid, "pattern_name": name, "code": code,
                                    "name": u["name"], "signal_date": s["dates"][i_sig],
                                    "buy_date": bd, "buy_price": round(bp, 2), "sell_date": sd,
                                    "hold": hold, "pnl": round(pnl, 3),
                                    "yizi_limitup": is_yizi_limitup(s, i_sig + 1)})
    print(f"完整形态信号（5日+10日，未剔一字板）：{len(signals)}")
    sig5 = [x for x in signals if x["hold"] == 5 and not x["yizi_limitup"]]
    sig10 = [x for x in signals if x["hold"] == 10 and not x["yizi_limitup"]]

    result = {"meta": {
        "window": f"{WIN_START} ~ {WIN_END}", "hold": HOLD, "universe_n": len(uni),
        "stock_days": stock_days, "raw_fetch": data["meta"]["fetched_at"],
        "source": data["meta"]["source"], "adjust": "不复权(通达信日K)",
        "discipline": "T日收盘信号→T+1开盘买→持有N日收盘卖；剔除一字涨停入场日；基准=窗口内所有(票,日)同纪律",
        "baseline_5d": b5,
    }}

    # ---------- 主结果 ----------
    result["patterns"] = {}
    for pid, (_, _, name) in PATTERNS.items():
        p5 = [x["pnl"] for x in sig5 if x["pattern"] == pid]
        p10 = [x["pnl"] for x in sig10 if x["pattern"] == pid]
        st5, st10 = stat(p5), stat(p10)
        if not st5:
            result["patterns"][pid] = {"name": name, "hold5": None}
            continue
        w5 = welch(p5, base5)
        cl5 = clustered_stat([x for x in sig5 if x["pattern"] == pid], base5_by_day)
        monthly = {}
        for x in sig5:
            if x["pattern"] == pid:
                monthly.setdefault(x["signal_date"][:7], []).append(x["pnl"])
        monthly = {m: stat(v) for m, v in sorted(monthly.items())}
        yz = [x["pnl"] for x in signals if x["pattern"] == pid and x["hold"] == 5 and x["yizi_limitup"]]
        result["patterns"][pid] = {
            "name": name, "hold5": st5, "hold10": st10, "excess_5d_vs_baseline": w5,
            "clustered_5d": cl5, "monthly_5d": monthly,
            "yizi_excluded": {"n": len(yz), "mean": round(sum(yz) / len(yz), 3) if yz else None},
        }
        print(f"\n===== {pid} {name} =====")
        print(f"  持有5日: n={st5['n']} 均值{st5['mean']:+.3f}% 中位数{st5['median']:+.3f}% "
              f"胜率{st5['win_rate']}% t={st5['t']}")
        print(f"  持有10日: n={st10['n']} 均值{st10['mean']:+.3f}% 胜率{st10['win_rate']}%" if st10 else "  持有10日: 无")
        if w5:
            print(f"  超额vs全市场基准: {w5['mean_diff']:+.3f}% (Welch t={w5['t']}, p={w5['p']})")
        if cl5:
            print(f"  按买入日聚类: 日均超额{cl5['mean_daily_excess']:+.3f}% t={cl5['t_clustered']} "
                  f"超额为正的交易日占比{cl5['pos_days_ratio']}% ({cl5['n_days']}天)")
        for m, st in monthly.items():
            print(f"    {m}: n={st['n']} 均值{st['mean']:+.3f}% 胜率{st['win_rate']}%")
        if yz:
            print(f"  [剔除一字涨停入场 {len(yz)} 笔，其均值{sum(yz)/len(yz):+.3f}%]")

    # ---------- 条件分解 ----------
    result["stages"] = {}
    for pid, (fn, stage_names, name) in PATTERNS.items():
        rows = {}
        for st_idx, label in enumerate(stage_names):
            pnls = []
            for code, u in uni.items():
                s = u["s"]
                for i_sig in fn(s, u["i0"], stage=st_idx):
                    r = fwd_ret(s, i_sig, 5)
                    if r and not is_yizi_limitup(s, i_sig + 1):
                        pnls.append(r[3])
            st = stat(pnls)
            rows[label] = st
            if st:
                print(f"{pid} {label}: n={st['n']} 均值{st['mean']:+.3f}% 胜率{st['win_rate']}%")
        result["stages"][pid] = rows

    # ---------- 参数敏感性 ----------
    sens = {}
    grids = {
        "P1": [("实体幅度%", [3.0, 4.0, 5.0], "body_th"), ("短柱量比(≤x×10日均量)", [0.6, 0.7, 0.8], "vol_th")],
        "P2": [("倍量倍数", [1.5, 1.8, 2.2], "volmult"), ("底部位置(60日位置≤x)", [0.2, 0.3, 0.4], "pos60_max")],
        "P3": [("放量倍数", [1.5, 2.0, 2.5], "volmult")],
        "P4": [("下影线幅度", [0.015, 0.02, 0.03], "shadow_th"), ("低位位置(60日位置≤x)", [0.25, 0.35, 0.45], "pos60_max")],
    }
    for pid, grid in grids.items():
        fn = PATTERNS[pid][0]
        rows = []
        for label, vals, key in grid:
            for v in vals:
                pnls = []
                for code, u in uni.items():
                    s = u["s"]
                    for i_sig in fn(s, u["i0"], **{key: v}):
                        r = fwd_ret(s, i_sig, 5)
                        if r and not is_yizi_limitup(s, i_sig + 1):
                            pnls.append(r[3])
                st = stat(pnls)
                rows.append({"param": label, "value": v, "n": st["n"] if st else 0,
                             "mean": st["mean"] if st else None,
                             "win_rate": st["win_rate"] if st else None,
                             "excess": round(st["mean"] - b5["mean"], 3) if st else None})
        sens[pid] = rows
        print(f"\n{pid} 参数敏感性（5日超额 vs 基准 {b5['mean']:+.3f}%）：")
        for r in rows:
            print(f"   {r['param']}={r['value']}: n={r['n']} 均值{r['mean']:+.3f}% 超额{r['excess']:+.3f}% 胜率{r['win_rate']}%")
    result["sensitivity"] = sens

    # ---------- 指数 ----------
    result["index"] = {}
    for code, blk in idx.items():
        bars = [b for b in blk["bars"] if WIN_START <= b[0] <= WIN_END]
        if len(bars) >= 2:
            result["index"][code] = {"name": blk["name"], "start": bars[0][0], "end": bars[-1][0],
                                     "ret_pct": round((bars[-1][2] / bars[0][2] - 1) * 100, 2)}

    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    cols = ["pattern", "pattern_name", "code", "name", "signal_date", "buy_date", "buy_price",
            "sell_date", "hold", "pnl", "yizi_limitup"]
    with open(OUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(sorted(signals, key=lambda x: (x["pattern"], x["signal_date"], x["code"])))
    print(f"\n已落盘：{OUT_JSON} + {OUT_CSV}（{len(signals)}条信号记录）")


if __name__ == "__main__":
    main()
