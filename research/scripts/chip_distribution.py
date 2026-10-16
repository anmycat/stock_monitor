# -*- coding: utf-8 -*-
"""
筹码分布（移动成本分布）算法模块
口径：通达信官方《移动筹码分布》——三角形分布（峰在当日中价）+ 换手率衰减递推
    D(t) = D(t-1) * (1 - turnover(t)) + TriCost(t) * turnover(t)
    TriCost: 当日换手筹码在 [low, high] 之间以中价为峰做三角形分布

派生指标（全部由 T 日收盘后数据计算，当日可知，不违反回测纪律）：
    profit_ratio   获利盘比例（收盘价以下的筹码占比）
    avg_cost       平均成本（筹码加权平均价）
    cost5/cost95   5%/95% 分位价（90% 成本区间 COST(5)~COST(95)）
    concentration  90%成本集中度 = (cost95-cost5)/((cost95+cost5)/2)*100
    peak_price     主峰价格（筹码量最大的价位）
    peak_upper/peak_lower  主峰上沿/下沿（峰高 50% 处宽度）
    n_peaks        独立峰数量（峰高≥主峰15%，峰间谷深≤较低峰60%则合并）
    upper_peak/lower_peak  现价上方最近的峰 / 下方最近的峰
    peak_pos       主峰在近60日价格区间中的位置分位（0=最低，1=最高）
    peak_share     主峰筹码占比
    upper/lower_peak_retention  20日前后上/下峰筹码保留率（峰是否"移动/锁定"）

数据约定（与 raw/market_full.json 一致）：
    日K字段 o/h/l/c/prev/pct/amp/vol(手)/turn(换手率%)
"""
import json
import os

import numpy as np

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

GRID_N = 100  # 每只票的价格网格数


def tri_dist(low, high, peak, total, n=GRID_N):
    """三角形分布：峰在 peak，low/high 处为 0，总面积 = total。返回 (prices, weights)"""
    prices = np.linspace(low, high, n)
    weights = np.zeros(n)
    span_lo = peak - low
    span_hi = high - peak
    for i, p in enumerate(prices):
        if p <= peak:
            weights[i] = (p - low) / span_lo if span_lo > 1e-12 else 1.0
        else:
            weights[i] = (high - p) / span_hi if span_hi > 1e-12 else 1.0
    weights = np.clip(weights, 0.0, None)
    s = weights.sum()
    if s <= 0:
        weights[:] = 1.0 / n
        weights *= total
    else:
        weights = weights / s * total
    return prices, weights


def _percentile_price(prices, cum, total, q):
    """累积分布达到 q(0~1) 处的价格"""
    target = total * q
    idx = int(np.searchsorted(cum, target))
    idx = min(max(idx, 0), len(prices) - 1)
    return float(prices[idx])


def detect_peaks(prices, dist, min_rel_height=0.15, merge_gap=5, valley_ratio=0.6):
    """检测独立筹码峰。返回 [{price, height, idx}] 按价格降序"""
    n = len(dist)
    if n == 0:
        return []
    mx = float(dist.max())
    if mx <= 0:
        return []
    raw = []
    for i in range(1, n - 1):
        if dist[i] >= dist[i - 1] and dist[i] > dist[i + 1] and dist[i] >= min_rel_height * mx:
            raw.append(i)
    if not raw:
        raw = [int(dist.argmax())]
    # 合并间隔 < merge_gap 的相邻峰（保留高的）
    merged = []
    for i in raw:
        if merged and i - merged[-1] < merge_gap:
            if dist[i] > dist[merged[-1]]:
                merged[-1] = i
        else:
            merged.append(i)
    # 谷深过滤：两峰之间最低点 > valley_ratio*min(峰高) → 是同一片密集区，合并
    filtered = []
    for i in merged:
        if filtered:
            lo, hi = min(filtered[-1], i), max(filtered[-1], i)
            valley = float(dist[lo:hi + 1].min())
            if valley > valley_ratio * min(dist[filtered[-1]], dist[i]):
                if dist[i] > dist[filtered[-1]]:
                    filtered[-1] = i
                continue
        filtered.append(i)
    return [{"price": float(prices[i]), "height": float(dist[i]), "idx": i} for i in filtered]


def inject_to_grid(w_local, local_prices, prices, dx_new):
    """把局部网格 [local_prices] 上的分布 w_local 按价格映射到全局网格 prices。
    密度守恒：density=值/局部格宽，全局值=density×全局格宽；区间外严格置零。
    退化情况（low==high，一字板）：全部筹码放入最接近的单一格子。"""
    out = np.zeros(len(prices))
    if len(local_prices) <= 1 or local_prices[-1] <= local_prices[0]:
        idx = int(np.argmin(np.abs(prices - local_prices[0])))
        out[idx] = float(w_local.sum())
        return out
    dx_local = (local_prices[-1] - local_prices[0]) / (len(local_prices) - 1)
    density = w_local / dx_local
    out = np.interp(prices, local_prices, density) * dx_new
    out[prices < local_prices[0]] = 0.0
    out[prices > local_prices[-1]] = 0.0
    return out


def compute_chip_series(klines, decay=1.0, grid_n=GRID_N):
    """
    输入：按日期升序的日K列表 [{'date','o','h','l','c','vol','turn',...}]
    输出：{date: 指标dict}，与输入等长
    初始假设：首个交易日全部流通盘成本 = 当日三角形分布（63日衰减后影响可忽略）
    """
    prices = None
    dist = None
    out = {}
    hist = []  # (date, prices, dist) 用于20日峰保留率
    for k in klines:
        date = k["date"]
        low, high, close = k.get("l"), k.get("h"), k.get("c")
        vol, turn = k.get("vol"), k.get("turn")
        if low is None or high is None or close is None or vol is None or turn is None:
            continue  # 数据缺失日跳过（分布不更新）
        typ = (high + low + close) / 3.0  # 中价近似（成交额/成交量）
        if vol is None or turn is None or turn <= 0 or high <= low:
            if dist is None:
                continue
            # 停牌日：分布不变
        else:
            local_p, w_local = tri_dist(low, high, typ, vol, grid_n)
            if dist is None:
                # 初始化：全流通盘 = vol/turn，成本为当日三角形分布
                float_shares = vol / (turn / 100.0)
                init_p, init_w = tri_dist(low, high, typ, float_shares, grid_n)
                prices = init_p
                dx = (prices[-1] - prices[0]) / (len(prices) - 1) if len(prices) > 1 else 1.0
                dist = inject_to_grid(init_w, init_p, prices, dx)
            else:
                if prices is None or prices[-1] < high or prices[0] > low:
                    # 价格超出既有网格：扩展重建。插值必须按密度守恒（值/格宽），
                    # 否则网格变宽后旧筹码面积缩水，新旧峰比例失真
                    new_lo, new_hi = min(prices[0], low), max(prices[-1], high)
                    old_p, old_d = prices, dist
                    old_dx = (old_p[-1] - old_p[0]) / (len(old_p) - 1) if len(old_p) > 1 else 1.0
                    prices, _ = tri_dist(new_lo, new_hi, (new_lo + new_hi) / 2, old_d.sum(), grid_n)
                    new_dx = (prices[-1] - prices[0]) / (len(prices) - 1) if len(prices) > 1 else 1.0
                    dist = np.interp(prices, old_p, old_d / old_dx) * new_dx
                dx = (prices[-1] - prices[0]) / (len(prices) - 1) if len(prices) > 1 else 1.0
                # 当日成本按价格位置映射到全局网格（不能按索引直加，否则铺满全区间）
                w = inject_to_grid(w_local, local_p, prices, dx)
                t = min(turn / 100.0, 1.0)
                # 递推：旧筹码按换手率(×衰减系数)遗忘，当日成交量作为新筹码注入
                # 守恒性：vol = 流通盘×turn 时，衰减移除与注入相等，总量恒定
                eff_t = min(t * decay, 1.0)
                dist = dist * (1.0 - eff_t) + w * decay
        if dist is None or dist.sum() <= 0:
            continue

        total = dist.sum()
        cum = np.cumsum(dist)
        # 获利盘 = 持仓成本 ≤ 收盘价 的筹码占比（与通达信口径一致）
        idx = int(np.searchsorted(prices, close, side="right"))
        profit_ratio = float(cum[idx - 1]) / total if idx > 0 else 0.0
        profit_ratio = min(max(profit_ratio, 0.0), 1.0)
        avg_cost = float((prices * dist).sum() / total)
        cost5 = _percentile_price(prices, cum, total, 0.05)
        cost95 = _percentile_price(prices, cum, total, 0.95)
        mid = (cost95 + cost5) / 2.0
        concentration = (cost95 - cost5) / mid * 100.0 if mid > 0 else 999.0

        peaks = detect_peaks(prices, dist)
        main_peak = peaks[0] if peaks else {"price": avg_cost, "height": 0.0, "idx": 0}
        for p in peaks:
            if p["height"] > main_peak["height"]:
                main_peak = p
        peak_share = float(main_peak["height"]) / total * GRID_N if total > 0 else 0.0
        # 主峰宽：峰高 50% 处宽度
        half = main_peak["height"] * 0.5
        li = main_peak["idx"]
        while li > 0 and dist[li - 1] >= half:
            li -= 1
        ri = main_peak["idx"]
        while ri < len(dist) - 1 and dist[ri + 1] >= half:
            ri += 1
        peak_lower, peak_upper = float(prices[li]), float(prices[ri])

        upper = [p for p in peaks if p["price"] > close]
        lower = [p for p in peaks if p["price"] < close]
        upper_peak = min(upper, key=lambda p: p["price"]) if upper else None
        lower_peak = max(lower, key=lambda p: p["price"]) if lower else None

        # 峰位在近60日价格区间的位置
        hist.append((date, prices, dist))
        recent = [k for k in klines if k["date"] <= date and k.get("l") is not None][-60:]
        lo60 = min(k["l"] for k in recent)
        hi60 = max(k["h"] for k in recent)
        peak_pos = (main_peak["price"] - lo60) / (hi60 - lo60) if hi60 > lo60 else 0.5

        # 20日峰保留率（上峰/下峰筹码是否"移动/锁定"）
        upper_ret = lower_ret = None
        if len(hist) > 20:
            _, p20, d20 = hist[-21]
            t20 = d20.sum()
            if t20 > 0 and upper_peak:
                mask = (p20 >= upper_peak["price"] * 0.97) & (p20 <= upper_peak["price"] * 1.03)
                upper_ret = float(d20[mask].sum()) / t20
            if t20 > 0 and lower_peak:
                mask = (p20 >= lower_peak["price"] * 0.97) & (p20 <= lower_peak["price"] * 1.03)
                lower_ret = float(d20[mask].sum()) / t20

        out[date] = {
            "date": date,
            "close": close,
            "profit_ratio": round(profit_ratio, 4),
            "avg_cost": round(avg_cost, 3),
            "cost5": round(cost5, 3),
            "cost95": round(cost95, 3),
            "concentration": round(concentration, 2),
            "peak_price": round(main_peak["price"], 3),
            "peak_lower": round(peak_lower, 3),
            "peak_upper": round(peak_upper, 3),
            "peak_share": round(peak_share, 3),
            "peak_pos": round(peak_pos, 3),
            "n_peaks": len(peaks),
            "upper_peak_price": round(upper_peak["price"], 3) if upper_peak else None,
            "lower_peak_price": round(lower_peak["price"], 3) if lower_peak else None,
            "upper_peak_retention": round(upper_ret, 3) if upper_ret is not None else None,
            "lower_peak_retention": round(lower_ret, 3) if lower_ret is not None else None,
        }
    return out


def load_pool(filenames=("market_full.json", "market_full_ext.json")):
    """加载主池+扩展池日K，返回 {code: {'name':..., 'klines':[...]}}"""
    pool = {}
    for fn in filenames:
        path = os.path.join(BASE, "raw", fn)
        if not os.path.exists(path):
            continue
        d = json.load(open(path, encoding="utf-8"))
        for code, blk in d["daily"].items():
            if code in pool:
                continue
            klines = []
            for dt in sorted(blk["d"].keys()):
                r = blk["d"][dt]
                def _f(x):
                    try:
                        return float(x)
                    except (TypeError, ValueError):
                        return None
                klines.append({
                    "date": dt, "o": _f(r.get("o")), "h": _f(r.get("h")), "l": _f(r.get("l")),
                    "c": _f(r.get("c")), "prev": _f(r.get("prev")), "pct": _f(r.get("pct")),
                    "vol": _f(r.get("vol")), "turn": _f(r.get("turn")), "amp": _f(r.get("amp")),
                })
            pool[code] = {"name": blk.get("name", code), "klines": klines}
    return pool


def build_chip_metrics(decay=1.0):
    """全池计算筹码指标序列。返回 {code: {'name':..., 'metrics': {date: {...}}}}"""
    pool = load_pool()
    result = {}
    for code, blk in pool.items():
        metrics = compute_chip_series(blk["klines"], decay=decay)
        result[code] = {"name": blk["name"], "metrics": metrics}
    return result


if __name__ == "__main__":
    import sys
    decay = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    res = build_chip_metrics(decay=decay)
    dates = sorted({d for v in res.values() for d in v["metrics"]})
    print(f"衰减系数={decay} | 票数={len(res)} | 指标覆盖 {dates[0]} ~ {dates[-1]}（{len(dates)}天）")
    # 抽查一只票最近5天
    code = sorted(res)[0]
    for dt in dates[-3:]:
        m = res[code]["metrics"][dt]
        print(f"{code} {res[code]['name']} {dt}: 获利盘={m['profit_ratio']:.0%} 集中度={m['concentration']} "
              f"主峰={m['peak_price']} 峰数={m['n_peaks']} 峰位={m['peak_pos']} 主峰占比={m['peak_share']}")
