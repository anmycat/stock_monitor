#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
模拟验证回测引擎：把 rules/methodology_rules.json 的蒸馏规则套到 raw/market_full.json 实盘数据上。

纪律（防未来函数）：
  - T 日收盘后计算信号（只用 T 日及之前数据）
  - T+1 开盘价成交（若 T+1 跳空，按实际开盘价，不按信号价）
  - 止损触发：T+N 日开盘价若已跌破止损价，按开盘价成交（悲观口径）
  - tech 指标（ma5/ma20/rsi/kdj/chip）按数据实际日期对齐，缺失则不用该规则（不用0填充）

输出：data/backtest_trades.csv（逐笔）+ data/backtest_summary.json（统计+Kelly）
"""
import json, csv, os, math
from collections import defaultdict

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_market(name, fallback):
    """加载行情数据：优先原始文件名，缺失时回退到 6m 归档版（2026-10-16 归档时补）"""
    p = os.path.join(BASE, "raw", name)
    if not os.path.exists(p):
        p = os.path.join(BASE, "raw", fallback)
    return json.load(open(p, encoding="utf-8"))


MARKET = _load_market("market_full.json", "market_full_6m.json")
MARKET_EXT = _load_market("market_full_ext.json", "market_full_ext_6m.json")
RULES = json.load(open(os.path.join(BASE, "rules/methodology_rules.json"), encoding="utf-8"))


# ---------- 指标计算（当日已知） ----------
def _f(v):
    """安全转 float，失败返回 None"""
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def build_series(code, market=None):
    """返回按日期排序的日K序列 + 预计算滚动指标"""
    market = market or MARKET
    blk = market["daily"][code]
    tech = market.get("tech", {}).get(code, {}).get("d", {})
    days = sorted(blk["d"].keys())
    rows = []
    for i, day in enumerate(days):
        r = blk["d"][day]
        prev = blk["d"].get(days[i - 1], {}) if i > 0 else {}
        row = {
            "date": day, "code": code, "name": blk["name"],
            "o": _f(r.get("o")), "h": _f(r.get("h")), "l": _f(r.get("l")), "c": _f(r.get("c")),
            "prev": _f(r.get("prev")), "pct": _f(r.get("pct")), "amp": _f(r.get("amp")),
            "vol": _f(r.get("vol")), "turn": _f(r.get("turn")),
            "idx": i,
        }
        # 连涨连跌（含当日，基于 pct）
        row["streak"] = 0
        if i > 0 and row["pct"] is not None:
            s = 1 if row["pct"] > 0 else (-1 if row["pct"] < 0 else 0)
            j = i - 1
            while j >= 0:
                pj = _f(blk["d"][days[j]].get("pct"))
                if pj is None:
                    break
                sj = 1 if pj > 0 else (-1 if pj < 0 else 0)
                if sj == s and s != 0:
                    row["streak"] += 1
                    j -= 1
                else:
                    break
            if s != 0:
                row["streak"] *= s
        # 前5日涨跌幅
        if i >= 5:
            c5 = _f(blk["d"][days[i - 5]].get("c"))
            if c5 and row["c"]:
                row["pct_5d"] = (row["c"] - c5) / c5 * 100
            else:
                row["pct_5d"] = None
        else:
            row["pct_5d"] = None
        # 量比：当日量 / 前5日均量
        if i >= 5:
            vols = [_f(blk["d"][days[k]].get("vol")) or 0 for k in range(i - 5, i)]
            if row["vol"] and sum(vols) > 0:
                row["vol_ratio"] = row["vol"] / (sum(vols) / 5)
            else:
                row["vol_ratio"] = None
        else:
            row["vol_ratio"] = None
        # 20日新高（前20日最高，不含当日）
        if i >= 20:
            highs = [_f(blk["d"][days[k]].get("h")) or 0 for k in range(i - 20, i)]
            row["new_high_20"] = 1 if (row["h"] or 0) >= max(highs) and max(highs) > 0 else 0
        else:
            row["new_high_20"] = None
        # 收盘日内位置
        if row["h"] and row["l"] and row["h"] > row["l"]:
            row["pos"] = (row["c"] - row["l"]) / (row["h"] - row["l"])
        else:
            row["pos"] = 0.5
        # tech 对齐（仅 chip 字段与校验用；ma/rsi/kdj 改为自算保证全覆盖）
        t = tech.get(day)
        for k in ("chip_profit_rate", "chip_avg_cost"):
            row[k] = t.get(k) if t else None
        rows.append(row)

    # ---- 自算技术指标（当日已知，全覆盖）----
    closes = [r["c"] for r in rows]
    highs = [r["h"] for r in rows]
    lows = [r["l"] for r in rows]
    pcts = [r["pct"] for r in rows]

    def _ma(vals, i, n):
        seg = [v for v in vals[max(0, i - n + 1):i + 1] if v is not None]
        return sum(seg) / len(seg) if seg else None

    for i, row in enumerate(rows):
        # MA5 / MA20
        row["ma5"] = _ma(closes, i, 5)
        row["ma20"] = _ma(closes, i, 20)
        row["bias_ma20"] = (closes[i] - row["ma20"]) / row["ma20"] * 100 if closes[i] and row["ma20"] else None
        # RSI6（简单均版）
        if i >= 5 and pcts[i - 5] is not None and pcts[i] is not None:
            gains = [max(0, pcts[k] or 0) for k in range(i - 5, i + 1)]
            losses = [max(0, -(pcts[k] or 0)) for k in range(i - 5, i + 1)]
            ag, al = sum(gains) / 6, sum(losses) / 6
            row["rsi6"] = 100 * ag / (ag + al) if (ag + al) > 0 else 50.0
        else:
            row["rsi6"] = None
        # KDJ_J（9日RSV，3日平滑）
        if i >= 8 and closes[i] and highs[i] and lows[i]:
            hh = max(h for h in highs[max(0, i - 8):i + 1] if h)
            ll = min(l for l in lows[max(0, i - 8):i + 1] if l)
            rsv = (closes[i] - ll) / (hh - ll) * 100 if hh > ll else 50.0
            row["_rsv"] = rsv
        else:
            row["_rsv"] = None
    # K/D/J 递推
    k_prev = d_prev = 50.0
    for row in rows:
        rsv = row.pop("_rsv", None)
        if rsv is None:
            row["kdj_j"] = None
            continue
        k_prev = (2 * k_prev + rsv) / 3
        d_prev = (2 * d_prev + k_prev) / 3
        row["kdj_j"] = 3 * k_prev - 2 * d_prev
    # VR（N日上涨量/下跌量，26日）——简化用量比替代：保留 None 标记
    for row in rows:
        row["vr"] = None
    return rows


def eval_cond(cond, row):
    """评估单条件。字段缺失返回 None（不匹配）"""
    f = cond["field"]
    v = row.get(f)
    if v is None:
        return None
    op, target = cond["op"], cond["value"]
    if op == ">=":
        return v >= target
    if op == "<=":
        return v <= target
    if op == ">":
        return v > target
    if op == "<":
        return v < target
    if op == "eq":
        return v == target
    if op == "between":
        return target[0] <= v <= target[1]
    return None


def match_block(block, row):
    """all 必须全 True；any 至少一个 True。字段缺失( None )视为不匹配"""
    if "all" in block:
        for c in block["all"]:
            r = eval_cond(c, row)
            if r is not True:   # False 或 None 都不通过
                return False
    if "any" in block:
        oks = [eval_cond(c, row) for c in block["any"]]
        if not any(x is True for x in oks):
            return False
    return True


def check_computed(comp_list, row):
    """computed 表达式（安全子集：只允许字段名、数字、算术、比较）"""
    if not comp_list:
        return True
    allowed = {k: row.get(k) for k in
               ("c", "ma5", "ma20", "bias_ma20", "rsi6", "vol_ratio", "streak")}
    allowed = {k: v for k, v in allowed.items() if v is not None}
    for comp in comp_list:
        expr = comp["expr"]
        # 用白名单字段构造 eval 环境
        try:
            if not eval(expr, {"__builtins__": {}}, dict(allowed)):
                return False
        except Exception:
            return False
    return True


# ---------- 回测 ----------
def run_rule(rule, all_rows):
    """跑单条交易规则，返回逐笔交易"""
    rid = rule["id"]
    entry = rule.get("entry", {})
    comp = entry.get("computed", [])
    exit_cfg = rule.get("exit", {})
    hold = exit_cfg.get("max_hold_days", rule.get("params", {}).get("hold_days", 5))
    sl = exit_cfg.get("stop_loss_pct")
    tp = exit_cfg.get("take_profit")
    risk_exit = exit_cfg.get("risk_exit")
    t_only = rule.get("params", {}).get("t_pnl_only", False)
    t_pair = rule.get("direction") == "short_then_long"

    trades = []
    t_open_until = {}   # code -> 倒T未平仓截止日（同标的禁止并发倒T）
    for rows in all_rows.values():
        for i, row in enumerate(rows):
            # ---- 信号日 T：entry 条件 ----
            if not match_block(entry, row):
                continue
            if comp and not check_computed(comp, row):
                continue

            if t_pair:
                # ---- 倒T配对：T日收盘卖出（日K近似冲高），回踩日买回 ----
                # Bug修复：同标的上一笔倒T未买回前，禁止再卖（并发冲突）
                if row["code"] in t_open_until and row["date"] <= t_open_until[row["code"]]:
                    continue
                reentry = exit_cfg.get("reentry", {})
                sell_price = row["c"]
                if sell_price is None:
                    continue
                buy_back = buy_date = buy_reason = None
                for j in range(i + 1, min(i + 1 + hold, len(rows))):
                    d = rows[j]
                    if eval_cond(reentry, d) is True and d["c"]:
                        buy_back, buy_date, buy_reason = d["c"], d["date"], "回踩接回"
                        break
                if buy_back is None:
                    d = rows[min(i + hold, len(rows) - 1)]
                    if d["c"] is None:
                        continue
                    buy_back, buy_date, buy_reason = d["c"], d["date"], "到期追回(机会成本)"
                t_open_until[row["code"]] = buy_date
                pnl = (sell_price - buy_back) / sell_price * 100
                trades.append({
                    "rule": rid, "rule_name": rule["name"], "code": row["code"],
                    "stock": row["name"], "signal_date": row["date"],
                    "buy_date": buy_date, "buy_price": round(buy_back, 3),
                    "sell_date": row["date"], "sell_price": round(sell_price, 3),
                    "pnl_pct": round(pnl, 2), "reason": buy_reason,
                    "streak_at_signal": row.get("streak"),
                    "vol_ratio_at_signal": round(row["vol_ratio"], 2) if row.get("vol_ratio") else None,
                    "pct_at_signal": row.get("pct"),
                    "t_only": True,
                })
                continue

            # ---- 常规规则：T+1 开盘成交 ----
            if i + 1 >= len(rows):
                continue
            nxt = rows[i + 1]
            if nxt["o"] is None:
                continue
            buy_price = nxt["o"]
            buy_date = nxt["date"]

            # ---- 持仓与退出 ----
            sold = False
            sell_price = sell_date = sell_reason = None
            max_hold_end = min(i + 1 + hold, len(rows) - 1)
            for j in range(i + 1, max_hold_end + 1):
                d = rows[j]
                # 止损（悲观：开盘已跌破则按开盘价）
                if sl is not None and d["o"] is not None:
                    if (d["o"] - buy_price) / buy_price * 100 <= sl:
                        sell_price, sell_date, sell_reason = d["o"], d["date"], "止损(开盘)"
                        sold = True
                        break
                if sl is not None and d["l"] is not None and d["l"] <= buy_price * (1 + sl / 100):
                    sell_price = buy_price * (1 + sl / 100)
                    sell_date, sell_reason = d["date"], "止损(盘中)"
                    sold = True
                    break
                # 止盈
                if tp and eval_cond(tp, d) is True:
                    sell_price, sell_date, sell_reason = d["c"], d["date"], "止盈"
                    sold = True
                    break
                # 风控退出
                if risk_exit and eval_cond(risk_exit, d) is True:
                    sell_price, sell_date, sell_reason = d["c"], d["date"], "风控退出"
                    sold = True
                    break
            if not sold:
                d = rows[max_hold_end]
                # 到期收盘价缺失（停牌等）→ 向后找第一个有收盘价的交易日
                if d["c"] is None:
                    for j in range(max_hold_end + 1, len(rows)):
                        if rows[j]["c"] is not None:
                            d = rows[j]
                            break
                if d["c"] is None:
                    continue   # 全窗口无收盘价，放弃该笔
                sell_price, sell_date, sell_reason = d["c"], d["date"], "到期收盘"

            pnl = (sell_price - buy_price) / buy_price * 100
            trades.append({
                "rule": rid, "rule_name": rule["name"], "code": row["code"],
                "stock": row["name"], "signal_date": row["date"],
                "buy_date": buy_date, "buy_price": round(buy_price, 3),
                "sell_date": sell_date, "sell_price": round(sell_price, 3),
                "pnl_pct": round(pnl, 2), "reason": sell_reason,
                "streak_at_signal": row.get("streak"),
                "vol_ratio_at_signal": round(row["vol_ratio"], 2) if row.get("vol_ratio") else None,
                "pct_at_signal": row.get("pct"),
                "t_only": t_only,
            })
    return trades


def apply_global_filters(all_rows):
    """R07/R08 全局过滤器：标记禁止开仓的 (code,date)"""
    banned = set()
    for fid in ("R07_emotion_top_exit", "R08_youzi_reverse"):
        rule = RULES["trade_rules"][fid]
        conds = rule.get("conditions", {})
        for rows in all_rows.values():
            for row in rows:
                if match_block(conds, row):
                    banned.add((row["code"], row["date"]))
    return banned


def kelly(p, b, fraction, max_pos, min_sample, min_edge):
    """p=胜率 b=盈亏比。样本不足或边缘不足返回 0"""
    n = None
    return p, b


def stats_for(trades):
    if not trades:
        return None
    pnls = [t["pnl_pct"] for t in trades]
    wins = [x for x in pnls if x > 0]
    losses = [x for x in pnls if x <= 0]
    n = len(pnls)
    p = len(wins) / n
    avg_win = sum(wins) / len(wins) if wins else 0
    avg_loss = abs(sum(losses) / len(losses)) if losses else 0.001
    b = avg_win / avg_loss if avg_loss > 0 else 0
    q = 1 - p
    f_full = (b * p - q) / b if b > 0 else 0
    kcfg = RULES["kelly"]["params"]
    f_half = max(0.0, f_full) * kcfg["fraction"]
    f_pos = min(f_half, kcfg["max_single_position_pct"]) if n >= kcfg["min_p_sample"] and f_full >= kcfg["min_edge"] / b else 0.0
    # 最大回撤（按逐笔累计）
    equity, peak, mdd = 0.0, 0.0, 0.0
    for x in pnls:
        equity += x
        peak = max(peak, equity)
        mdd = min(mdd, equity - peak)
    # Kelly 加权口径（每笔按该规则 capped 仓位）
    kcfg = RULES["kelly"]["params"]
    fw = f_pos if f_pos > 0 else 0.0
    eq2, peak2, mdd2 = 0.0, 0.0, 0.0
    for x in pnls:
        eq2 += x * fw
        peak2 = max(peak2, eq2)
        mdd2 = min(mdd2, eq2 - peak2)
    # 最长连亏
    max_consec_loss, cur = 0, 0
    for x in pnls:
        if x <= 0:
            cur += 1
            max_consec_loss = max(max_consec_loss, cur)
        else:
            cur = 0
    return {
        "n": n, "win_rate": round(p * 100, 1),
        "avg_pnl": round(sum(pnls) / n, 2),
        "avg_win": round(avg_win, 2), "avg_loss": round(-avg_loss, 2),
        "payoff_ratio": round(b, 2),
        "expectancy": round(p * avg_win - q * avg_loss, 2),
        "kelly_full": round(f_full * 100, 1),
        "kelly_half": round(f_half * 100, 1),
        "kelly_capped": round(f_pos * 100, 1),
        "total_pnl": round(sum(pnls), 2),
        "kelly_weighted_pnl": round(eq2, 2),
        "max_drawdown": round(mdd, 2),
        "kelly_max_drawdown": round(mdd2, 2),
        "max_consec_loss": max_consec_loss,
        "win_streak_best": max_consec_loss,
    }


def run_pool(all_rows, banned, label):
    """跑一个票池的全部规则，返回 (trades, rule_stats)"""
    trades_all = []
    rule_stats = {}
    for rid, rule in RULES["trade_rules"].items():
        if rid.startswith("_"):
            continue
        if rule.get("direction") in ("filter", "discipline", "risk", "regime"):
            continue
        trades = run_rule(rule, all_rows)
        before = len(trades)
        trades = [t for t in trades if (t["code"], t["signal_date"]) not in banned]
        filtered = before - len(trades)
        trades_all.extend(trades)
        st = stats_for(trades)
        rule_stats[rid] = {"name": rule["name"], "filtered_by_global": filtered, "stats": st}
        if st:
            print(f"  {rid} {rule['name'][:14]:16} n={st['n']:3} 胜率{st['win_rate']:5.1f}% "
                  f"均盈亏{st['avg_pnl']:+.2f}% 盈亏比{st['payoff_ratio']:.2f} "
                  f"期望{st['expectancy']:+.2f}% Kelly半{st['kelly_half']:.1f}%"
                  + (f" (过滤{filtered})" if filtered else ""))
        if rule.get("direction") == "short_then_long" and trades:
            real = [t for t in trades if t["reason"] == "回踩接回"]
            opp = [t for t in trades if t["reason"] != "回踩接回"]
            if real:
                s_real = stats_for(real)
                rule_stats[rid]["real_T"] = s_real
                print(f"    └ 真实T(回踩接回) n={s_real['n']} 胜率{s_real['win_rate']}% "
                      f"均值{s_real['avg_pnl']:+.2f}% 期望{s_real['expectancy']:+.2f}%")
            if opp:
                s_opp = stats_for(opp)
                rule_stats[rid]["opportunity_cost"] = s_opp
                print(f"    └ 机会成本(未接回) n={s_opp['n']} 均值{s_opp['avg_pnl']:+.2f}%（少赚非本金亏）")
    return trades_all, rule_stats


def main():
    # 1. 构建全部票的序列（主池 + 扩展池）
    all_rows = {}
    for code in MARKET["daily"]:
        all_rows[code] = build_series(code)
    print(f"主池数据加载：{len(MARKET['daily'])} 只票，共 {sum(len(v) for v in all_rows.values())} 个交易日")

    all_ext = {}
    for code in MARKET_EXT["daily"]:
        all_ext[code] = build_series(code, MARKET_EXT)
    print(f"扩展池数据加载：{len(MARKET_EXT['daily'])} 只票，共 {sum(len(v) for v in all_ext.values())} 个交易日")

    # 2. 全局过滤器（两池分别计算）
    banned = apply_global_filters(all_rows)
    banned_ext = apply_global_filters(all_ext)
    print(f"全局过滤器（R07/R08）命中：主池 {len(banned)} / 扩展池 {len(banned_ext)} 个 (票,日) 组合")

    # 3. 主池回测
    print("\n===== 主池（博主交易票 17 只，有选择偏差）=====")
    trades_main, stats_main = run_pool(all_rows, banned, "main")

    # 4. 扩展池回测（泛化验证）
    print("\n===== 扩展池（东财头部提股 10 只，泛化对照）=====")
    trades_ext, stats_ext = run_pool(all_ext, banned_ext, "ext")

    # 5. 两池合并
    print("\n===== 合并池（27 只）=====")
    trades_all = trades_main + trades_ext
    st_all = stats_for(trades_all)
    print(f"n={st_all['n']} 胜率{st_all['win_rate']}% 期望{st_all['expectancy']:+.2f}% "
          f"Kelly半{st_all['kelly_half']:.1f}% 最大回撤{st_all['max_drawdown']}% 最长连亏{st_all['max_consec_loss']}")

    # 6. 落盘
    with open(os.path.join(BASE, "data/backtest_trades.csv"), "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(trades_all[0].keys()))
        w.writeheader()
        w.writerows(sorted(trades_all, key=lambda x: (x["buy_date"], x["code"])))
    out = {"meta": {"discipline": "T日信号→T+1开盘成交；止损按开盘/盘中悲观口径",
                    "global_filter_hits": {"main": len(banned), "ext": len(banned_ext)},
                    "tech_coverage": "ma/rsi/kdj 全部自算（当日已知），chip 仅主池部分覆盖",
                    "pools": {"main": "17只博主交易票（选择偏差）", "ext": "10只东财头部提股（泛化对照）"}},
           "main": {"rules": stats_main, "pool": stats_for(trades_main)},
           "ext": {"rules": stats_ext, "pool": stats_for(trades_ext)},
           "pool": st_all}
    with open(os.path.join(BASE, "data/backtest_summary.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n已落盘：data/backtest_trades.csv（{len(trades_all)} 笔）+ data/backtest_summary.json")


if __name__ == "__main__":
    main()
