#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""多空博弈社 86 条交易动作 × 实盘行情 逐条对照 + 动作理由推导（v2 细化版）

输入:
  data/ks_video_trade_actions.json   86 条交易动作
  raw/market_full.json               17 只票不复权日K(628个交易日) + 技术指标

输出:
  data/trade_vs_market_reasoning.csv / .json
  data/trade_reasoning_summary.txt

v2 细化:
  - K线形态分类（一字板/T字板/涨停开板/大阳大阴/十字星/长上下影）
  - 交易时段分类（早盘抢筹/上午/午后/尾盘/收盘前）
  - 买入理由 16 类：打板、涨停开板低吸、深跌低吸、深度回调低吸、超卖反弹、
    突破追涨、高开追击、连涨追涨、放量上攻跟随、缩量地量低吸、套牢区博反弹、
    均线承接、尾盘买入、早盘抢筹、强势中继、常规
  - 卖出理由 12 类：跌停割肉、涨停出货、冲高止盈、连涨兑现、放量滞涨出货、
    低开杀跌、杀跌止损、深度回撤止损、上涨中兑现、尾盘卖出、常规
  - 同日做T配对识别与盈亏
  - 理由 × 决策质量交叉表（哪个理由赚钱/亏钱）
"""
import csv
import json
import os
from collections import Counter, defaultdict

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

NAME2CODE = {
    "香江控股": "600162.SH", "爱丽家居": "603221.SH", "金螳螂": "002081.SZ",
    "立新能源": "001258.SZ", "百合花": "603823.SH", "黄河旋风": "600172.SH",
    "贤丰控股": "002141.SZ", "深物业A": "000011.SZ", "津药药业": "600488.SH",
    "华电辽能": "600396.SH", "红宝丽": "002165.SZ", "康欣新材": "600076.SH",
    "国芳集团": "601086.SH", "远东股份": "600869.SH", "新金路": "000510.SZ",
    "哈药股份": "600664.SH", "博纳影业": "001330.SZ",
}


def load():
    trades = json.load(open(os.path.join(BASE, "data/ks_video_trade_actions.json"), encoding="utf-8"))
    market = json.load(open(os.path.join(BASE, "raw/market_full.json"), encoding="utf-8"))
    return trades, market


def ctx_for(market, code, day):
    d = market["daily"].get(code, {}).get("d", {})
    days = sorted(d)
    if day not in d:
        return None, [], None
    i = days.index(day)
    hist = [{"date": x, **d[x]} for x in days[:i]]
    tech = market["tech"].get(code, {}).get("d", {}).get(day)
    return d[day], hist, tech


def pct(a, b):
    if not b:
        return None
    return (a - b) / b * 100.0


def build_context(market, code, day):
    cur, hist, tech = ctx_for(market, code, day)
    if cur is None:
        return None
    c = {
        "code": code, "date": day,
        "o": cur["o"], "h": cur["h"], "l": cur["l"], "c": cur["c"],
        "pct": cur["pct"], "amp": cur["amp"], "vol": cur["vol"], "turn": cur["turn"],
        "prev_c": cur["prev"],
    }
    if len(hist) >= 1:
        c["pct_1d"] = pct(cur["c"], hist[-1]["c"])
    if len(hist) >= 3:
        c["pct_3d"] = pct(cur["c"], hist[-3]["c"])
    if len(hist) >= 5:
        c["pct_5d"] = pct(cur["c"], hist[-5]["c"])
    if len(hist) >= 10:
        c["pct_10d"] = pct(cur["c"], hist[-10]["c"])
    run = 0
    if cur["pct"] > 0:
        run = 1
        for k in reversed(hist):
            if k["pct"] > 0:
                run += 1
            else:
                break
    elif cur["pct"] < 0:
        run = -1
        for k in reversed(hist):
            if k["pct"] < 0:
                run -= 1
            else:
                break
    c["streak"] = run
    window = hist[-19:] + [{"date": day, **cur}]
    hi = max(x["h"] for x in window)
    lo = min(x["l"] for x in window)
    c["is_20d_high"] = cur["h"] >= hi - 1e-9
    c["is_20d_low"] = cur["l"] <= lo + 1e-9
    c["dist_20d_high_pct"] = pct(cur["c"], hi)
    c["dist_20d_low_pct"] = pct(cur["c"], lo)
    if len(hist) >= 5:
        avg5 = sum(x["vol"] for x in hist[-5:]) / 5.0
        c["vol_ratio_5d"] = cur["vol"] / avg5 if avg5 else None
    # 10日均量比（更稳）
    if len(hist) >= 10:
        avg10 = sum(x["vol"] for x in hist[-10:]) / 10.0
        c["vol_ratio_10d"] = cur["vol"] / avg10 if avg10 else None
    c["gap_pct"] = pct(cur["o"], cur["prev"])
    # 均线（自算，避免 tech 缺日）
    if len(hist) >= 4:
        c["ma5"] = (sum(x["c"] for x in hist[-4:]) + cur["c"]) / 5.0
    if len(hist) >= 19:
        c["ma20"] = (sum(x["c"] for x in hist[-19:]) + cur["c"]) / 20.0
    days = sorted(market["daily"].get(code, {}).get("d", {}))
    i = days.index(day)
    c["next_close"] = market["daily"][code]["d"][days[i + 1]]["c"] if i + 1 < len(days) else None
    c["next_pct"] = market["daily"][code]["d"][days[i + 1]]["pct"] if i + 1 < len(days) else None
    # 20日内最高收盘（用于"创新高收盘"）
    if len(hist) >= 19:
        c["is_20d_close_high"] = cur["c"] >= max(x["c"] for x in hist[-19:])
    if tech:
        c["tech_bias20"] = tech.get("bias_ma20"); c["tech_rsi6"] = tech.get("rsi6")
        c["tech_kdj_j"] = tech.get("kdj_j"); c["tech_vr"] = tech.get("vr")
        c["chip_profit"] = tech.get("chip_profit_rate")
        c["chip_cost"] = tech.get("chip_avg_cost")
    return c


def pos_pct(price, c):
    if price is None or c is None:
        return None
    if c["h"] == c["l"]:
        return 50.0
    return (price - c["l"]) / (c["h"] - c["l"]) * 100.0


def candle_shape(c):
    """K线形态分类"""
    prev = c.get("prev_c") or c["o"]
    body = abs(c["c"] - c["o"]) / prev * 100 if prev else 0
    upper = (c["h"] - max(c["o"], c["c"])) / prev * 100 if prev else 0
    lower = (min(c["o"], c["c"]) - c["l"]) / prev * 100 if prev else 0
    up = c["c"] >= c["o"]
    if c["amp"] is not None and c["amp"] < 0.5:
        return "一字板(涨)" if up else "一字板(跌)"
    if c["pct"] >= 9.8:
        if upper >= 2:
            return "涨停但大幅开板(长上影)"
        if lower >= 2:
            return "T字板(涨停)"
        return "涨停光头板"
    if c["pct"] <= -9.8:
        if lower >= 2:
            return "跌停但盘中撬板(长下影)"
        return "跌停光脚板"
    if body >= 5:
        return "大阳线" if up else "大阴线"
    if body < 1.0:
        if upper >= 2 and lower >= 2:
            return "十字星(长上下影)"
        if upper >= 2:
            return "长上影小实体(冲高回落)"
        if lower >= 2:
            return "长下影小实体(探底回升)"
        return "十字星"
    if upper >= 3:
        return "长上影(冲高回落)"
    if lower >= 3:
        return "长下影(探底回升)"
    return "小阳线" if up else "小阴线"


def day_phase(tm):
    """交易时段分类"""
    if not tm:
        return "时间未知"
    try:
        parts = tm.split(":")
        h, m = int(parts[0]), int(parts[1])
    except Exception:
        return "时间未知"
    mins = h * 60 + m
    if mins < 9 * 60 + 31:
        return "集合竞价"
    if mins < 9 * 60 + 46:
        return "早盘抢筹(9:31-9:45)"
    if mins < 10 * 60 + 31:
        return "早盘(9:46-10:30)"
    if mins < 11 * 60 + 31:
        return "上午盘中(10:31-11:30)"
    if mins < 13 * 60 + 31:
        return "午盘休市前后"
    if mins < 14 * 60 + 1:
        return "午后(13:31-14:00)"
    if mins < 14 * 60 + 46:
        return "尾盘(14:01-14:45)"
    return "收盘前(14:46-15:00)"


def infer_reason(direction, price, c, tm=None):
    """规则化推断动作理由。返回 (理由短语, 证据列表)"""
    ev = []
    if c is None or price is None:
        return "无行情数据，无法判定", ev

    pos = pos_pct(price, c)
    shape = candle_shape(c)
    phase = day_phase(tm)
    limit_up = c["pct"] >= 9.8
    limit_dn = c["pct"] <= -9.8
    near_high = pos >= 85
    near_low = pos <= 15
    p5 = c.get("pct_5d")
    p3 = c.get("pct_3d")
    vr = c.get("vol_ratio_5d")
    rsi = c.get("tech_rsi6")
    kdj = c.get("tech_kdj_j")
    chip = c.get("chip_profit")
    bias = c.get("tech_bias20")
    ma20 = c.get("ma20")
    ma5 = c.get("ma5")
    near_ma20 = ma20 and abs(pct(price, ma20) or 99) <= 2.5
    near_ma5 = ma5 and abs(pct(price, ma5) or 99) <= 1.5
    opening_band = phase in ("早盘抢筹(9:31-9:45)", "早盘(9:46-10:30)")
    closing_band = phase in ("尾盘(14:01-14:45)", "收盘前(14:46-15:00)")

    def E(s):
        ev.append(s)

    if direction == "买入":
        # 1) 涨停价打板
        if limit_up and near_high:
            E(f"当日涨停(+{c['pct']:.1f}%)，买在最高价附近(位置{pos:.0f}%)，时段{phase}")
            if vr and vr >= 2:
                E(f"量为5日均量{vr:.1f}倍，放量涨停")
            return "打板/涨停价排队买入", ev
        # 2) 涨停开板低吸
        if limit_up and not near_high:
            E(f"当日涨停(+{c['pct']:.1f}%)但盘中开板，低吸于位置{pos:.0f}%")
            if "长上影" in shape or "开板" in shape:
                E(f"K线形态：{shape}（封板不牢，博弈回封）")
            return "涨停开板低吸（博回封）", ev
        # 3) 跌停/深跌低吸
        if limit_dn or c["pct"] <= -6:
            E(f"当日大跌({c['pct']:.1f}%)，买在位置{pos:.0f}%")
            if p5 is not None and p5 <= -10:
                E(f"前5日累计跌{p5:.1f}%，跌势深重")
            if rsi is not None and rsi < 30:
                E(f"RSI6={rsi:.0f}，技术超卖")
            return "深跌低吸/跌停板博弈反弹", ev
        # 4) 深度回调低吸
        if p5 is not None and p5 <= -10 and pos <= 55:
            E(f"前5日累计跌{p5:.1f}%，当日买在位置{pos:.0f}%")
            if near_ma20:
                E(f"价格回到20日线({ma20:.2f})附近")
            if chip is not None and chip < 40:
                E(f"获利盘仅{chip:.0f}%，浮筹清洗较充分")
            return "深度回调后低吸", ev
        # 5) 超卖反弹
        if (rsi is not None and rsi < 30) or (kdj is not None and kdj < 0):
            if pos <= 60:
                E(f"RSI6={rsi if rsi is not None else '?'}，KDJ_J={kdj if kdj is not None else '?'}，技术超卖")
                E(f"买在位置{pos:.0f}%（当日{c['pct']:+.1f}%）")
                return "超卖反弹买入", ev
        # 6) 套牢区买入
        if chip is not None and chip < 25 and pos <= 60:
            E(f"获利盘仅{chip:.0f}%（深度套牢区），买在位置{pos:.0f}%")
            return "套牢区博反弹买入", ev
        # 7) 突破20日新高
        if c.get("is_20d_high") and pos >= 60:
            E(f"盘中创20日新高，买在位置{pos:.0f}%")
            if vr:
                E(f"量为5日均量{vr:.1f}倍")
            if bias is not None and bias > 12:
                E(f"乖离20日线+{bias:.0f}%，已严重超买仍追")
            return "突破追涨（创20日新高）", ev
        # 8) 高开缺口追击
        if c.get("gap_pct") and c["gap_pct"] >= 3 and pos >= 55:
            E(f"高开{c['gap_pct']:.1f}%，买在位置{pos:.0f}%，时段{phase}")
            return "高开缺口追击", ev
        # 9) 连涨追涨
        if c["streak"] >= 2 and c["pct"] > 0:
            E(f"已连涨{c['streak']}日，当日再买在位置{pos:.0f}%（{c['pct']:+.1f}%）")
            if bias is not None and bias > 12:
                E(f"乖离20日线+{bias:.0f}%")
            return f"连涨{c['streak']}日追涨", ev
        # 10) 放量上攻跟随
        if vr and vr >= 2 and c["pct"] > 0:
            E(f"当日+{c['pct']:.1f}%，放量至5日均量{vr:.1f}倍，买在位置{pos:.0f}%")
            return "放量上攻跟随买入", ev
        # 11) 缩量地量低吸
        if vr and vr <= 0.7 and pos <= 60:
            E(f"量能萎缩至5日均量的{vr:.0%}（地量），买在位置{pos:.0f}%")
            return "缩量地量低吸", ev
        # 12) 均线承接
        if near_ma20 or near_ma5:
            ma_ref = ma20 if near_ma20 else ma5
            E(f"买在{'20日' if near_ma20 else '5日'}线({ma_ref:.2f})附近，当日{c['pct']:+.1f}%")
            return "均线承接买入", ev
        # 13) 尾盘买入
        if closing_band:
            E(f"尾盘时段({phase})买入，位置{pos:.0f}%，当日{c['pct']:+.1f}%")
            return "尾盘买入（博次日溢价）", ev
        # 14) 早盘抢筹
        if opening_band and c["pct"] > 1:
            E(f"早盘({phase})追击，当日+{c['pct']:.1f}%，位置{pos:.0f}%")
            return "早盘抢筹买入", ev
        # 15) 强势中继
        if c["pct"] > 3 and pos >= 40:
            E(f"当日+{c['pct']:.1f}%（{shape}），买在位置{pos:.0f}%")
            if bias is not None and bias > 15:
                E(f"乖离20日线+{bias:.0f}%，严重超买区买入")
            return "强势中追涨", ev
        # 16) 常规
        E(f"当日{c['pct']:+.1f}%（{shape}），买在位置{pos:.0f}%，时段{phase}")
        if vr:
            E(f"量/5日均量={vr:.1f}")
        return "常规买入（盘面无显著异动）", ev

    # ---- 卖出 ----
    # 1) 跌停割肉
    if limit_dn and near_low:
        E(f"当日跌停({c['pct']:.1f}%)且卖在最低价附近(位置{pos:.0f}%)")
        if p5 is not None and p5 <= -10:
            E(f"前5日已跌{p5:.1f}%，跌势中割肉")
        return "跌停割肉/排队出货", ev
    # 2) 涨停出货
    if limit_up and near_high:
        E(f"当日涨停(+{c['pct']:.1f}%)且卖在最高价附近(位置{pos:.0f}%)")
        if "开板" in shape:
            E(f"K线形态：{shape}，趁开板出货")
        return "涨停价出货/止盈", ev
    # 3) 连涨兑现
    if c["streak"] >= 3 and pos >= 50:
        E(f"已连涨{c['streak']}日，卖在位置{pos:.0f}%")
        if bias is not None and bias > 15:
            E(f"乖离20日线+{bias:.0f}%，乖离过大主动兑现")
        return "连涨后兑现", ev
    # 4) 放量滞涨出货
    if vr and vr >= 2 and pos >= 70 and c["pct"] < 2:
        E(f"放量至5日均量{vr:.1f}倍但涨幅仅{c['pct']:+.1f}%（{shape}），高位换手")
        return "放量滞涨/高位换手出货", ev
    # 5) 冲高止盈
    if near_high:
        E(f"卖在当日最高附近(位置{pos:.0f}%)，当日{c['pct']:+.1f}%")
        if bias is not None and bias > 15:
            E(f"乖离20日线+{bias:.0f}%")
        return "冲高止盈", ev
    # 6) 低开杀跌
    if c.get("gap_pct") and c["gap_pct"] <= -3 and near_low:
        E(f"低开{c['gap_pct']:.1f}%，卖在最低附近(位置{pos:.0f}%)")
        return "低开杀跌卖出", ev
    # 7) 深度回撤止损
    if p5 is not None and p5 <= -10 and pos <= 45:
        E(f"前5日累计跌{p5:.1f}%，卖在位置{pos:.0f}%")
        if chip is not None and chip < 30:
            E(f"获利盘仅{chip:.0f}%，多数持仓亏损，止损离场")
        return "深度回撤中止损", ev
    # 8) 杀跌止损
    if near_low and c["pct"] < 0:
        E(f"卖在当日最低附近(位置{pos:.0f}%)，当日{c['pct']:+.1f}%")
        return "杀跌止损", ev
    # 9) 尾盘卖出
    if closing_band:
        E(f"尾盘时段({phase})卖出，位置{pos:.0f}%，当日{c['pct']:+.1f}%")
        return "尾盘卖出（不赌隔夜）", ev
    # 10) 上涨中兑现
    if c["pct"] > 0:
        E(f"当日+{c['pct']:.1f}%，卖在位置{pos:.0f}%")
        return "上涨中兑现", ev
    # 11) 常规
    E(f"当日{c['pct']:+.1f}%（{shape}），卖在位置{pos:.0f}%，时段{phase}")
    return "常规卖出", ev


def quality(direction, price, c):
    if c is None or price is None:
        return {}
    out = {}
    out["to_close_pct"] = pct(c["c"], price)
    if c.get("next_close"):
        out["to_next_pct"] = pct(c["next_close"], price)
    return out


def pair_same_day_t(rows):
    """同日同票一买一卖 → 做T配对（按时间排序 FIFO 匹配）。
    正T=先买后卖；倒T=先卖后买（卖后回补）。"""
    by_key = defaultdict(list)
    for r in rows:
        if r.get("price") is None or r["dir"] not in ("买入", "卖出"):
            continue
        by_key[(r["date"], r["stock"])].append(r)
    pairs = []
    for (day, stock), items in by_key.items():
        if not any(x["dir"] == "买入" for x in items) or not any(x["dir"] == "卖出" for x in items):
            continue
        # 按时间排序（无时间的排最后）
        items = sorted(items, key=lambda x: (x["time"] or "99", x["idx"]))
        buy_stack = []      # 未匹配买入
        pending_sells = []  # 未匹配卖出（先于买入发生）
        for x in items:
            if x["dir"] == "买入":
                if pending_sells:
                    s = pending_sells.pop(0)
                    t_pnl = (s["price"] - x["price"]) / x["price"] * 100
                    pairs.append({"date": day, "stock": stock, "kind": "倒T(先卖后买)",
                                  "buy_time": x["time"], "buy_price": x["price"],
                                  "sell_time": s["time"], "sell_price": s["price"],
                                  "t_pnl_pct": t_pnl})
                else:
                    buy_stack.append(x)
            else:  # 卖出
                if buy_stack:
                    b = buy_stack.pop(0)
                    t_pnl = (x["price"] - b["price"]) / b["price"] * 100
                    pairs.append({"date": day, "stock": stock, "kind": "正T(先买后卖)",
                                  "buy_time": b["time"], "buy_price": b["price"],
                                  "sell_time": x["time"], "sell_price": x["price"],
                                  "t_pnl_pct": t_pnl})
                else:
                    pending_sells.append(x)
    return pairs


def main():
    trades, market = load()
    rows = []
    contexts = {}
    holdings = []   # 持仓快照（非交易动作）
    for idx, t in enumerate(trades):
        name = (t.get("stock_name") or "").strip()
        code = NAME2CODE.get(name)
        day = t.get("date")
        raw_type = t.get("type") or ""
        direction = "卖出" if raw_type == "清仓" else raw_type
        price = t.get("price")
        tm = t.get("time") or ""

        if not code or price is None:
            rows.append({
                "idx": idx, "video": t.get("video_code"), "date": day,
                "stock": name or "(截断)", "dir": raw_type, "time": tm,
                "price": price, "reason": "缺股票名或价格，跳过", "evidence": "",
                "pct": "", "amp": "", "pos": "", "vol_ratio": "", "streak": "",
                "pct_5d": "", "bias20": "", "chip_profit": "", "to_close": "",
                "to_next": "", "shape": "", "phase": "", "note": t.get("note", ""),
            })
            continue

        c = build_context(market, code, day)

        # 持仓快照：不算交易动作，单独记录持仓盈亏与后市关系
        if raw_type == "持仓":
            q = quality("卖出", price, c)
            holdings.append({"date": day, "stock": name, "price": price, "time": tm,
                             "note": t.get("note", ""), "to_next": q.get("to_next_pct"),
                             "pct": c.get("pct") if c else None})
            rows.append({
                "idx": idx, "video": t.get("video_code"), "date": day,
                "stock": name, "dir": "持仓", "time": tm, "price": price,
                "reason": "持仓快照（非交易动作）", "evidence": t.get("note", ""),
                "pct": round(c["pct"], 1) if c else "", "amp": "", "pos": "",
                "vol_ratio": "", "streak": "", "pct_5d": "", "bias20": "",
                "chip_profit": "", "to_close": "", "to_next": round(q["to_next_pct"], 2) if q.get("to_next_pct") is not None else "",
                "shape": "", "phase": "", "note": t.get("note", ""),
            })
            continue

        reason, ev = infer_reason(direction, price, c, tm)
        q = quality(direction, price, c)

        def f(x, n=2):
            return round(x, n) if isinstance(x, (int, float)) else ""

        rows.append({
            "idx": idx, "video": t.get("video_code"), "date": day,
            "stock": name, "dir": direction, "time": tm, "price": price,
            "reason": reason, "evidence": "；".join(ev),
            "pct": f(c.get("pct"), 1) if c else "",
            "amp": f(c.get("amp"), 1) if c else "",
            "pos": f(pos_pct(price, c), 0) if c else "",
            "vol_ratio": f(c.get("vol_ratio_5d"), 2) if c else "",
            "streak": c.get("streak") if c else "",
            "pct_5d": f(c.get("pct_5d"), 1) if c else "",
            "bias20": f(c.get("tech_bias20"), 1) if c else "",
            "chip_profit": f(c.get("chip_profit"), 0) if c else "",
            "to_close": f(q.get("to_close_pct")) if q else "",
            "to_next": f(q.get("to_next_pct")) if q else "",
            "shape": candle_shape(c) if c else "",
            "phase": day_phase(tm),
            "note": t.get("note", ""),
        })
        contexts[idx] = (c, reason, ev, q)

    # 输出 CSV
    cols = ["idx", "video", "date", "stock", "dir", "time", "price", "reason", "evidence",
            "pct", "amp", "pos", "vol_ratio", "streak", "pct_5d", "bias20",
            "chip_profit", "to_close", "to_next", "shape", "phase", "note"]
    out_csv = os.path.join(BASE, "data/trade_vs_market_reasoning.csv")
    with open(out_csv, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)

    # 输出 JSON
    full = []
    for idx, t in enumerate(trades):
        name = (t.get("stock_name") or "").strip()
        code = NAME2CODE.get(name)
        if idx in contexts:
            c, reason, ev, q = contexts[idx]
            c2 = dict(c)
            c2.update({"reason": reason, "evidence": ev, "quality": q,
                       "trade": t, "pos_pct": pos_pct(t.get("price"), c),
                       "shape": candle_shape(c), "phase": day_phase(t.get("time"))})
            full.append(c2)
    with open(os.path.join(BASE, "data/trade_vs_market_reasoning.json"), "w", encoding="utf-8") as f:
        json.dump(full, f, ensure_ascii=False, indent=2, default=str)

    # ================= 汇总 =================
    lines = []
    P = lines.append
    P("=" * 80)
    P("多空博弈社 交易动作 × 实盘行情 对照与决策逻辑推导（v2）")
    P("=" * 80)
    analyzed = [r for r in rows if r["reason"] not in ("缺股票名或价格，跳过", "持仓快照（非交易动作）")]
    P(f"\n可分析交易动作: {len(analyzed)} / {len(rows)}（其余缺股票名或价格，截图被截断）")
    if holdings:
        P(f"另有 {len(holdings)} 条持仓快照（非交易动作）：")
        for h in holdings:
            P(f"  {h['date']} {h['stock']} 价{h['price']} → 次日{h['to_next']:+.2f}% | {h['note'][:60]}")

    # 一、理由分布
    P("\n【一】动作理由分布（规则化推断）")
    rc = Counter(r["reason"] for r in analyzed)
    for k, v in rc.most_common():
        P(f"  {v:3} 条  {k}")

    # 二、买卖分开
    P("\n【二】买入 vs 卖出 的理由差异")
    for d in ("买入", "卖出"):
        sub = [r for r in analyzed if r["dir"] == d]
        P(f"  --- {d} ({len(sub)}条) ---")
        cc = Counter(r["reason"] for r in sub)
        for k, v in cc.most_common(8):
            P(f"    {v:3}  {k}")

    # 三、择时特征
    P("\n【三】择时特征（日内位置分位）")
    buys = [r for r in analyzed if r["dir"] == "买入" and r["pos"] != ""]
    sells = [r for r in analyzed if r["dir"] == "卖出" and r["pos"] != ""]
    if buys:
        bp = [float(r["pos"]) for r in buys]
        P(f"  买入位置均值 {sum(bp)/len(bp):.0f}%  追高(≥85%) {sum(1 for x in bp if x>=85)}/{len(bp)}  低吸(≤15%) {sum(1 for x in bp if x<=15)}/{len(bp)}")
    if sells:
        sp = [float(r["pos"]) for r in sells]
        P(f"  卖出位置均值 {sum(sp)/len(sp):.0f}%  止盈(≥85%) {sum(1 for x in sp if x>=85)}/{len(sp)}  杀跌(≤15%) {sum(1 for x in sp if x<=15)}/{len(sp)}")

    # 四、交易时段
    P("\n【四】交易时段分布")
    for d in ("买入", "卖出"):
        sub = [r for r in analyzed if r["dir"] == d]
        cc = Counter(r["phase"] for r in sub)
        P(f"  --- {d} ---")
        for k, v in cc.most_common():
            P(f"    {v:3}  {k}")

    # 五、涨跌停日
    P("\n【五】涨跌停日的动作")
    lim_buy = [r for r in analyzed if r["dir"] == "买入" and r["pct"] != "" and float(r["pct"]) >= 9.8]
    lim_sell = [r for r in analyzed if r["dir"] == "卖出" and r["pct"] != "" and float(r["pct"]) >= 9.8]
    lim_buy_dn = [r for r in analyzed if r["dir"] == "买入" and r["pct"] != "" and float(r["pct"]) <= -9.8]
    lim_sell_dn = [r for r in analyzed if r["dir"] == "卖出" and r["pct"] != "" and float(r["pct"]) <= -9.8]
    P(f"  涨停日买入 {len(lim_buy)} 条；涨停日卖出 {len(lim_sell)} 条")
    P(f"  跌停日买入 {len(lim_buy_dn)} 条；跌停日卖出 {len(lim_sell_dn)} 条")

    # 六、决策质量
    P("\n【六】事后决策质量（价格→当日收盘 / 次日收盘）")
    for label in ("买入", "卖出"):
        vals = [float(r["to_close"]) for r in analyzed if r["dir"] == label and r["to_close"] != ""]
        if vals:
            P(f"  {label}→当日收盘: 均值 {sum(vals)/len(vals):+.2f}%  为正 {sum(1 for v in vals if v>0)}/{len(vals)}")
        vals = [float(r["to_next"]) for r in analyzed if r["dir"] == label and r["to_next"] != ""]
        if vals:
            P(f"  {label}→次日收盘: 均值 {sum(vals)/len(vals):+.2f}%  为正 {sum(1 for v in vals if v>0)}/{len(vals)}")

    # 七、理由 × 质量交叉表（哪个理由赚钱）
    P("\n【七】理由 × 决策质量（按次日盈亏排序，识别'有效理由'与'亏损理由'）")
    grp = defaultdict(list)
    for r in analyzed:
        if r["to_next"] != "":
            grp[r["reason"]].append(float(r["to_next"]))
    stat = []
    for k, v in grp.items():
        stat.append((k, len(v), sum(v) / len(v), sum(1 for x in v if x > 0)))
    stat.sort(key=lambda x: -x[2])
    P(f"  {'理由':<28}{'条数':>4}{'次日均值':>9}{'胜率':>7}")
    for k, n, m, w in stat:
        P(f"  {k:<28}{n:>4}{m:>+8.2f}%{w/n*100:>6.0f}%")

    # 八、做T配对
    P("\n【八】同日做T配对（买+卖，按时间FIFO匹配）")
    pairs = pair_same_day_t(rows)
    if pairs:
        P(f"  识别到 {len(pairs)} 组配对：")
        for p in sorted(pairs, key=lambda x: x["date"]):
            P(f"    {p['date']} {p['stock']:6} [{p['kind']}] 买{p['buy_price']:>7}({p['buy_time']}) → "
              f"卖{p['sell_price']:>7}({p['sell_time']})  盈亏 {p['t_pnl_pct']:+.2f}%")
        pnls = [p["t_pnl_pct"] for p in pairs]
        pos_t = [p for p in pairs if p["kind"].startswith("正T")]
        rev_t = [p for p in pairs if p["kind"].startswith("倒T")]
        P(f"  总体盈亏均值 {sum(pnls)/len(pnls):+.2f}%；为正 {sum(1 for x in pnls if x>0)}/{len(pnls)}")
        if pos_t:
            pp = [p["t_pnl_pct"] for p in pos_t]
            P(f"  正T（先买后卖）{len(pos_t)}组：均值 {sum(pp)/len(pp):+.2f}%，为正 {sum(1 for x in pp if x>0)}/{len(pp)}")
        if rev_t:
            rp = [p["t_pnl_pct"] for p in rev_t]
            P(f"  倒T（先卖后买）{len(rev_t)}组：均值 {sum(rp)/len(rp):+.2f}%，为正 {sum(1 for x in rp if x>0)}/{len(rp)}")
    else:
        P("  未识别到同日买卖配对")

    # 九、代表性样本
    P("\n【九】代表性样本（按 |次日盈亏| 最大）")
    scored = [r for r in analyzed if r["to_next"] != ""]
    scored.sort(key=lambda r: -abs(float(r["to_next"])))
    for r in scored[:15]:
        P(f"  {r['date']} {r['stock']:6} {r['dir']} 价{r['price']:>7} 位置{r['pos']:>3}% "
          f"{r['shape']:<14} 次日{r['to_next']:>6}% | {r['reason']}")

    txt = "\n".join(lines)
    with open(os.path.join(BASE, "data/trade_reasoning_summary.txt"), "w", encoding="utf-8") as f:
        f.write(txt)
    print(txt)
    print(f"\n已输出: data/trade_vs_market_reasoning.csv / .json / trade_reasoning_summary.txt")


if __name__ == "__main__":
    main()
