#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
K线形态验证数据准备：全市场A股日K（通达信TCP）+ 股票池（新浪列表）+ 指数基准。

坑（2026-10-20 实测，117.34.114.x 网关；订正记录）：TDX 协议标准就是 0=深/1=沪
（与生产代码 TdxSource.MARKET_TO_TDX 一致，探活码 600000 用 1）——本脚本初版曾把
映射写反（沪市误传 0），该网关对错误 market 不报错，返 None 或乱码（datetime 非20xx）。
故保留双市场尝试 + 严格校验（datetime格式/价格>0/日期单调），乱码一律拒收。

口径：
  - 股票池：新浪 hs_a 全市场列表，剔除北交所(920/4/8开头)与 ST/*ST
  - 日K：通达信 get_security_bars(category=9 日线)，不复权，成交量单位=手
  - 指数：上证000001 / 沪深300 / 深证成指399001 / 创业板指399006
  - 每只票取最近 N 根日K（--bars，默认200≈2025-12~2026-10-20；500≈2024-09~2026-10-20 覆盖牛熊两regime）
输出：research/raw/pattern_pool_klines[_2y].json + 同名 .log
"""
import json, os, re, sys, time, argparse

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE, ".."))  # stock_monitor 根
import requests
from quant_engine.data.sources.tdx import TdxSource

SINA_URL = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
H = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn"}
DT_RE = re.compile(r"^20\d\d-\d\d-\d\d")


def parse_args():
    ap = argparse.ArgumentParser(description="全市场A股日K取数（通达信TCP）")
    ap.add_argument("--bars", type=int, default=200, help="每只票取数根数（默认200）")
    ap.add_argument("--out", default="pattern_pool_klines.json", help="输出文件名（raw/ 下）")
    return ap.parse_args()


ARGS = parse_args()
BARS = ARGS.bars
OUT = os.path.join(BASE, "raw", ARGS.out)
LOG = OUT.replace(".json", ".log")


def log(msg):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def fetch_universe():
    """新浪 hs_a 全市场列表 → [{code, name, mkt_hint}]，剔除北交所与ST"""
    items, page = [], 1
    while page <= 80:
        rows = []
        for attempt in range(3):
            try:
                r = requests.get(SINA_URL, params={"page": page, "num": 100, "sort": "symbol",
                                                   "asc": 1, "node": "hs_a"}, headers=H, timeout=20)
                rows = json.loads(r.content.decode("gbk"))
                break
            except Exception as e:
                log(f"  sina page{page} 第{attempt+1}次失败: {e}")
                time.sleep(1.5)
        if not rows:
            break
        items.extend(rows)
        if len(rows) < 100:
            break
        page += 1
        time.sleep(0.15)
    pool = []
    for it in items:
        sym, name = it.get("symbol", ""), it.get("name", "")
        if sym[:2] not in ("sh", "sz"):
            continue
        if "ST" in name:
            continue
        # TDX 协议标准映射：沪市票→1，深市票→0（双市场校验兜底防乱码）
        pool.append({"code": sym[2:], "name": name, "mkt_hint": 1 if sym[:2] == "sh" else 0})
    return pool


def valid_bars(raw):
    """严格校验：拒绝乱码/空/价格异常"""
    if not raw or len(raw) < 50:
        return None
    out, prev_date = [], ""
    for b in raw:
        dt = b.get("datetime", "")
        if not DT_RE.match(dt):
            return None
        d = dt[:10]
        if d <= prev_date:
            return None
        prev_date = d
        if not (b["close"] > 0 and b["open"] > 0 and b["high"] > 0 and b["low"] > 0
                and b["vol"] >= 0 and b["amount"] >= 0):
            return None
        if b["high"] < b["low"] or b["high"] < b["close"] or b["low"] > b["close"]:
            return None
        out.append([d, b["open"], b["close"], b["high"], b["low"], b["vol"], b["amount"]])
    return out


def main():
    t0 = time.time()
    open(LOG, "w").close()
    pool = fetch_universe()
    log(f"股票池：{len(pool)} 只（新浪hs_a，剔除北交所/ST）")

    src = TdxSource()
    api = src._get_api()
    log(f"TDX 服务器：{src._server}")

    stocks, failed = {}, []
    consec_fail = 0
    for i, it in enumerate(pool):
        code, hint = it["code"], it["mkt_hint"]
        bars = None
        for mkt in (hint, 1 - hint):
            try:
                raw = api.get_security_bars(9, mkt, code, 0, BARS)
            except Exception:
                raw = None
            bars = valid_bars(raw)
            if bars:
                break
        if bars and len(bars) >= 100:
            stocks[code] = {"name": it["name"], "bars": bars}
            consec_fail = 0
        else:
            failed.append(code)
            consec_fail += 1
            if consec_fail >= 25:      # 连续失败过多→重连
                try:
                    api = src._get_api()
                    log(f"  连续失败{consec_fail}只，重连 {src._server}")
                except Exception as e:
                    log(f"  重连失败: {e}")
                consec_fail = 0
        if (i + 1) % 500 == 0:
            log(f"  进度 {i+1}/{len(pool)}  成功{len(stocks)} 失败{len(failed)} 耗时{time.time()-t0:.0f}s")

    # 指数基准（TDX 标准映射：沪市指数→1，深市指数→0）
    index = {}
    for name, mkt, code in (("上证指数", 1, "000001"), ("沪深300", 1, "000300"),
                            ("深证成指", 0, "399001"), ("创业板指", 0, "399006")):
        try:
            raw = api.get_index_bars(9, mkt, code, 0, BARS)
            if raw and DT_RE.match(raw[0]["datetime"]):
                index[code] = {"name": name, "bars": [[b["datetime"][:10], b["open"], b["close"],
                                                       b["high"], b["low"], b["vol"], b["amount"],
                                                       b.get("up_count"), b.get("down_count")] for b in raw]}
        except Exception as e:
            log(f"指数 {name} 失败: {e}")

    meta = {"fetched_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "source": "通达信TCP 117.34.114.x:7709 get_security_bars(category=9)；股票池=新浪hs_a；TDX协议标准market映射(沪1深0)，双市场尝试+逐根校验防乱码",
            "universe_raw": len(pool), "stocks_ok": len(stocks), "failed": len(failed),
            "bars_per_stock": BARS, "adjust": "不复权", "vol_unit": "手",
            "failed_codes_sample": failed[:80]}
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump({"meta": meta, "stocks": stocks, "index": index}, f, ensure_ascii=False)
    sz = os.path.getsize(OUT) / 1048576
    log(f"完成：{len(stocks)} 只票 + {len(index)} 个指数，失败 {len(failed)} 只，"
        f"总耗时 {time.time()-t0:.0f}s，文件 {sz:.1f}MB → {OUT}")
    dates = sorted({b[0] for s in stocks.values() for b in s["bars"]})
    log(f"交易日范围：{dates[0]} ~ {dates[-1]}（{len(dates)}天）")
    if stocks:
        k = next(iter(stocks))
        log(f"样例 {k} {stocks[k]['name']}: {stocks[k]['bars'][-1]}")
    # 与东财已知值交叉校验（浦发银行 2026-10-20 收9.76/量605133手）
    if "600000" in stocks:
        last = stocks["600000"]["bars"][-1]
        log(f"交叉校验 600000 末日: {last} （东财同值 9.76 / 605133手）")


if __name__ == "__main__":
    main()
