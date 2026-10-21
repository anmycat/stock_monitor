#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
延伸调研池行情数据至 2026-10-21（27 只票 + 上证指数，不复权日K）。

双源（互相独立）：
  A. 腾讯 fqkline 不复权参数（param=xxx,day,,,N, 尾逗号 → 'day' 键）；
     _convert_code/_get_headers 复用生产 quant_engine TencentSource
     （M2'/D32 后版本）——同时构成生产数据链路校验
  B. 通达信 TCP get_security_bars/get_index_bars（独立源，
     fetch_pattern_pool.py 同款双市场尝试 + 逐根校验防乱码）

vol 单位（2026-10-22 实测标定，勿想当然）：
  - 腾讯股票 vol：多数为手，但 688498（科创板高价股）为股——逐票以
    10-20 锚点标定（research_vol/腾讯raw ∈ {≈1, ≈100}，否则拒绝）
  - 腾讯指数 vol：股（10-20 与旧文件逐值相同）；TDX 指数 vol：手
  - TDX 股票 vol：手（×100=股）
  - 旧 index_daily.json 的 vol 单位实为【股】（extend_market_data.py
    旧注记"手"有误，本次勘误；不影响任何计算——指数仅收盘价参与 regime）

三重校验（任一失败拒绝合并）：
  1. 锚点：10-20 bar 两源 OHLC 与 research raw 既有 10-20 bar 一致
     （OHLC ±0.011；股票 vol ±150 股）
  2. 双源一致：10-21 bar 腾讯 vs TDX（OHLC ±0.011，vol ±150 股）
  3. turn 估算：浮动股本由 10-16 bar（vol/turn）推导，10-19/10-20
     回代验证误差<1% 后用于 10-21；指数无 turn（amount 由 TDX 提供）

合并：仅追加 2026-10-21 一根 bar，不动已有 bar；prev/pct/amp 由 OHLC 计算。
主源选择：股票 vol 用 TDX×100（研究线既有交叉验证源，腾讯为独立校验）；
          指数 vol 用腾讯（与旧文件单位逐值一致），amount 用 TDX。
输出：原地更新 raw/market_full_6m.json、raw/market_full_ext_6m.json、
      raw/index_daily.json（git 跟踪）
"""
import json, os, re, sys, time

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = os.path.join(BASE, "raw")
sys.path.insert(0, os.path.join(BASE, ".."))  # stock_monitor 根
import requests
from quant_engine.data.sources.tencent import TencentSource
from quant_engine.data.sources.tdx import TdxSource

TAIL_DATE = "2026-10-21"
N_BARS = 8          # 每源取数根数（覆盖 10-14~10-21）
ANCHOR_DATE = "2026-10-20"
TURN_CALIB = ("2026-10-16", "2026-10-19", ANCHOR_DATE)   # turn 推导/回代日
DT_RE = re.compile(r"^20\d\d-\d\d-\d\d")

errors = []


def err(msg):
    errors.append(msg)
    print("  [FAIL] " + msg, flush=True)


def calibrate_scale(research_vol, raw_vol, tag):
    """以 10-20 锚点标定某源 vol 单位：返回 1 或 100；异常返回 None"""
    if not raw_vol:
        err(f"{tag} 锚点 vol 为 0")
        return None
    ratio = research_vol / raw_vol
    for s in (1.0, 100.0):
        if abs(ratio / s - 1) < 0.005:
            return int(s)
    err(f"{tag} vol 单位无法标定: research={research_vol} raw={raw_vol} ratio={ratio:.4f}")
    return None


# ---------- 源 A：腾讯不复权日K（生产代码路径） ----------
def fetch_tencent(code):
    """返回 {date: [o, c, h, l, vol_raw]}；失败返 None"""
    qt = TencentSource._convert_code(code)
    for attempt in range(3):
        try:
            r = requests.get(
                TencentSource.DAILY_URL,
                params={"param": f"{qt},day,,,{N_BARS},"},   # 尾逗号=不复权
                headers=TencentSource._get_headers(), timeout=15)
            node = (r.json().get("data") or {}).get(qt) or {}
            bars = node.get("day") or node.get("qfqday") or []
            out = {}
            for b in bars:
                if len(b) < 6 or not DT_RE.match(str(b[0] or "")):
                    continue
                out[b[0]] = [float(b[1]), float(b[2]), float(b[3]), float(b[4]), float(b[5])]
            return out if out else None
        except Exception as e:
            if attempt == 2:
                err(f"腾讯 {code} 3 次失败: {e}")
            time.sleep(1.0)
    return None


# ---------- 源 B：通达信 TCP（独立源） ----------
def fetch_tdx(api, code, is_index=False):
    """返回 {date: [o, c, h, l, vol_raw, amount]}；失败返 None"""
    mkt = 1 if code.endswith(".SH") else 0
    bare = code.split(".")[0]
    for m in (mkt, 1 - mkt):
        try:
            if is_index:
                raw = api.get_index_bars(9, m, bare, 0, N_BARS)
            else:
                raw = api.get_security_bars(9, m, bare, 0, N_BARS)
        except Exception:
            raw = None
        if not raw or not DT_RE.match(str(raw[0].get("datetime", ""))):
            continue
        out, prev_date = {}, ""
        for b in raw:
            d = str(b.get("datetime", ""))[:10]
            if d <= prev_date or not (b["close"] > 0 and b["open"] > 0):
                return None
            prev_date = d
            out[d] = [b["open"], b["close"], b["high"], b["low"], b["vol"], b["amount"]]
        return out if out else None
    return None


def check_ohlc(tag, rec, ref):
    """rec/ref 均为 {o,c,h,l,vol(股)}"""
    for k in ("o", "c", "h", "l"):
        if abs(rec[k] - ref[k]) > 0.011:
            err(f"{tag} {k}: {rec[k]} vs {ref[k]}")
    if abs(rec["vol"] - ref["vol"]) > 150.0:
        err(f"{tag} vol: {rec['vol']} vs {ref['vol']}")


def main():
    t0 = time.time()
    main_data = json.load(open(os.path.join(RAW, "market_full_6m.json"), encoding="utf-8"))
    ext_data = json.load(open(os.path.join(RAW, "market_full_ext_6m.json"), encoding="utf-8"))
    idx_data = json.load(open(os.path.join(RAW, "index_daily.json"), encoding="utf-8"))
    main_codes = set(main_data["daily"].keys())
    all_codes = sorted(set(main_data["daily"].keys()) | set(ext_data["daily"].keys()))
    print(f"股票池：{len(all_codes)} 只（主池 {len(main_codes)} + 扩展 {len(all_codes)-len(main_codes)}）")

    src = TdxSource()
    api = src._get_api()
    print(f"TDX 服务器：{src._server}")

    tail = {}        # code -> bar dict（STOCK_KEYS）
    idx_tail = None
    n_ok = 0
    unit_notes = []

    for code in all_codes:
        fn = "market_full_6m.json" if code in main_codes else "market_full_ext_6m.json"
        blk = json.load(open(os.path.join(RAW, fn), encoding="utf-8"))["daily"][code]
        old = blk["d"]
        if ANCHOR_DATE not in old:
            err(f"{code} 旧数据缺 {ANCHOR_DATE} 锚点 bar")
            continue
        a_bar = old[ANCHOR_DATE]

        tc = fetch_tencent(code)
        if not tc or TAIL_DATE not in tc or ANCHOR_DATE not in tc:
            err(f"{code} 腾讯取数失败或无 {TAIL_DATE}")
            continue
        tx = fetch_tdx(api, code)
        if not tx or TAIL_DATE not in tx or ANCHOR_DATE not in tx:
            err(f"{code} TDX 取数失败或无 {TAIL_DATE}")
            continue

        # 标定两源 vol 单位（锚点 10-20）
        s_tc = calibrate_scale(a_bar["vol"], tc[ANCHOR_DATE][4], f"腾讯 {code}")
        s_tx = calibrate_scale(a_bar["vol"], tx[ANCHOR_DATE][4], f"TDX {code}")
        if s_tc is None or s_tx is None:
            continue
        if s_tc != 100:
            unit_notes.append(f"{code} 腾讯vol单位=×{s_tc}")

        # 校验 1：锚点 10-20（两源换算为股后 vs 旧文件）
        for tag, bars, s in (("腾讯", tc, s_tc), ("TDX", tx, s_tx)):
            b = bars[ANCHOR_DATE]
            check_ohlc(f"{tag}锚点 {code}@{ANCHOR_DATE}",
                       {"o": b[0], "c": b[1], "h": b[2], "l": b[3], "vol": b[4] * s},
                       {"o": a_bar["o"], "c": a_bar["c"], "h": a_bar["h"], "l": a_bar["l"], "vol": a_bar["vol"]})

        # 校验 2：10-21 双源一致（均换算为股）
        b1, b2 = tc[TAIL_DATE], tx[TAIL_DATE]
        check_ohlc(f"双源 {code}@{TAIL_DATE}",
                   {"o": b1[0], "c": b1[1], "h": b1[2], "l": b1[3], "vol": b1[4] * s_tc},
                   {"o": b2[0], "c": b2[1], "h": b2[2], "l": b2[3], "vol": b2[4] * s_tx})

        # 校验 3：turn 估算（浮动股本由 10-16 推导，10-19/10-20 回代，用 TDX 主源 vol）
        calib = old.get(TURN_CALIB[0])
        if not calib or not calib.get("turn"):
            err(f"{code} 缺 10-16 turn 标定 bar")
            continue
        float_shares = calib["vol"] / calib["turn"] * 100
        for d in TURN_CALIB[1:]:
            ob = old.get(d)
            b_tdx = tx.get(d)
            if not ob or not b_tdx or not ob.get("turn"):
                err(f"{code} 缺 {d} 回代 bar")
                continue
            est = b_tdx[4] * s_tx / float_shares * 100
            if abs(est - ob["turn"]) / ob["turn"] > 0.01:
                err(f"{code} {d} turn 回代误差过大: 估{est:.2f} vs 实{ob['turn']}")

        # 组装 10-21 bar（TDX×scale 为主源）
        o, c, h, l = b2[0], b2[1], b2[2], b2[3]
        vol = b2[4] * s_tx
        prev = a_bar["c"]
        tail[code] = {
            "o": o, "h": h, "l": l, "c": c, "prev": prev,
            "pct": round((c - prev) / prev * 100, 2),
            "amp": round((h - l) / prev * 100, 2),
            "vol": vol,
            "turn": round(vol / float_shares * 100, 2),
        }
        n_ok += 1
        if n_ok % 10 == 0:
            print(f"  进度 {n_ok}/{len(all_codes)}  耗时{time.time()-t0:.0f}s", flush=True)

    # ---- 指数（上证 000001.SH；腾讯 vol=股，TDX vol=手，旧文件 vol=股） ----
    old_idx = idx_data["daily"]["000001.SH"]["d"]
    if ANCHOR_DATE in old_idx:
        a_idx = old_idx[ANCHOR_DATE]
        tc_i = fetch_tencent("000001.SH")
        tx_i = fetch_tdx(api, "000001.SH", is_index=True)
        if tc_i and tx_i and TAIL_DATE in tc_i and TAIL_DATE in tx_i:
            s_tc_i = calibrate_scale(a_idx["vol"], tc_i[ANCHOR_DATE][4], "腾讯 指数")
            s_tx_i = calibrate_scale(a_idx["vol"], tx_i[ANCHOR_DATE][4], "TDX 指数")
            if s_tc_i is not None and s_tx_i is not None:
                if s_tc_i != 1:
                    unit_notes.append(f"指数 腾讯vol单位=×{s_tc_i}")
                if s_tx_i != 100:
                    unit_notes.append(f"指数 TDXvol单位=×{s_tx_i}")
                # 锚点
                for tag, bars, s in (("腾讯", tc_i, s_tc_i), ("TDX", tx_i, s_tx_i)):
                    b = bars[ANCHOR_DATE]
                    check_ohlc(f"{tag}锚点 指数@{ANCHOR_DATE}",
                               {"o": b[0], "c": b[1], "h": b[2], "l": b[3], "vol": b[4] * s},
                               {"o": a_idx["o"], "c": a_idx["c"], "h": a_idx["h"], "l": a_idx["l"], "vol": a_idx["vol"]})
                # 双源 10-21
                b1, b2 = tc_i[TAIL_DATE], tx_i[TAIL_DATE]
                check_ohlc(f"双源 指数@{TAIL_DATE}",
                           {"o": b1[0], "c": b1[1], "h": b1[2], "l": b1[3], "vol": b1[4] * s_tc_i},
                           {"o": b2[0], "c": b2[1], "h": b2[2], "l": b2[3], "vol": b2[4] * s_tx_i})
                prev = a_idx["c"]
                idx_tail = {
                    "o": b2[0], "h": b2[2], "l": b2[3], "c": b2[1], "prev": prev,
                    "pct": round((b2[1] - prev) / prev * 100, 2),
                    "amp": round((b2[2] - b2[3]) / prev * 100, 2),
                    "vol": b1[4] * s_tc_i,     # 腾讯 vol=股，与旧文件单位逐值一致
                    "amount": b2[5],           # amount 取 TDX
                }
        else:
            err("指数取数失败")
    else:
        err("指数旧数据缺 10-20 锚点 bar")

    if errors:
        print(f"\n校验失败（{len(errors)} 项），拒绝合并：")
        sys.exit(1)
    if len(tail) != len(all_codes) or idx_tail is None:
        print(f"\n数据不完整（股票 {len(tail)}/{len(all_codes)}，指数 {idx_tail is not None}），拒绝合并")
        sys.exit(1)
    print(f"校验通过：{len(all_codes)} 票 + 指数，锚点双源一致、{TAIL_DATE} 双源一致、turn 回代<1%")
    if unit_notes:
        print("vol 单位标定备注：" + "；".join(unit_notes))

    # ---- 合并（仅追加 10-21） ----
    for code in all_codes:
        fn = "market_full_6m.json" if code in main_codes else "market_full_ext_6m.json"
        path = os.path.join(RAW, fn)
        data = json.load(open(path, encoding="utf-8"))
        blk = data["daily"][code]
        if TAIL_DATE in blk["d"]:
            print(f"  {code} {TAIL_DATE} 已存在，跳过")
            continue
        blk["d"][TAIL_DATE] = tail[code]
        blk["d"] = dict(sorted(blk["d"].items()))
        data["_meta"]["extended_20261022"] = (
            f"2026-10-22 追加 {TAIL_DATE} 日K（TDX 主源×100=股，腾讯不复权为独立校验源，"
            f"vol 单位逐票锚点标定；10-20 锚点双源与旧文件一致，turn 浮动股本标定回代<1%）")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

    path = os.path.join(RAW, "index_daily.json")
    data = json.load(open(path, encoding="utf-8"))
    blk = data["daily"]["000001.SH"]
    if TAIL_DATE not in blk["d"]:
        blk["d"][TAIL_DATE] = idx_tail
        blk["d"] = dict(sorted(blk["d"].items()))
        data["extended_20261022"] = (
            f"2026-10-22 追加 {TAIL_DATE} 上证指数日K（腾讯 vol=股与旧文件单位一致 + TDX 独立校验，"
            f"仅收盘价参与 regime 计算；勘误：本文件 vol 单位实为股，旧注记'手'有误）")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

    for fn in ("market_full_6m.json", "market_full_ext_6m.json", "index_daily.json"):
        d = json.load(open(os.path.join(RAW, fn), encoding="utf-8"))
        if fn == "index_daily.json":
            inner = d["daily"]["000001.SH"]["d"]
            ds = sorted(inner.keys())
            print(f"{fn}: {ds[0]} ~ {ds[-1]}（{len(ds)}天）")
        else:
            ds = sorted(next(iter(d["daily"].values()))["d"].keys())
            print(f"{fn}: {ds[0]} ~ {ds[-1]}（{len(ds)}天，{len(d['daily'])}只）")
    print(f"完成，总耗时 {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
