#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生产数据链路交叉校验（D31/D32 工程改动 vs 研究线可信数据）。

目的：证明 M2'（腾讯动态代码池 + 分批报价）与 B5（sanitize 数据卫生）
两轮工程改动未破坏取数正确性——用研究线已双源校验的 raw/market_full_*.json
（TDX 主源 + 腾讯独立校验，止于 2026-10-21）作基准。

校验项：
  1. 池完整性：TencentSource.get_all_stocks()（M2' 三层池主路=新浪 hs_a）
     返回的全市场池须覆盖研究池 27 只票（主池 17 + 扩展 10）
  2. 价格一致：27 只票的生产实时报价（休市态=末收盘）vs 研究 raw 10-21
     收盘价，|差| <= 0.011 元（源端四舍五入容差）
  3. B5 卫生：sanitize_quotes 对 27 只 Legit 行 dropped=0；
     对全市场池的剔除率须 <20% 且剔除原因仅为 N/C 前缀新股豁免外的事项
     （新股上市首五日不设涨跌幅，属预期内 exempt 不计 dropped）
输出：stdout 报告（研究/scripts/cross_check_production_1022.log）
"""
import json, os, sys, time

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = os.path.join(BASE, "raw")
sys.path.insert(0, os.path.join(BASE, ".."))  # stock_monitor 根
import pandas as pd
from quant_engine.data.sources.tencent import TencentSource
from quant_engine.utils.market_sanity import sanitize_quotes

TAIL_DATE = "2026-10-21"
fails = []


def main():
    t0 = time.time()
    # ---- 研究池基准 ----
    research = {}
    for fn in ("market_full_6m.json", "market_full_ext_6m.json"):
        d = json.load(open(os.path.join(RAW, fn), encoding="utf-8"))
        for code, blk in d["daily"].items():
            bar = blk["d"].get(TAIL_DATE)
            if bar:
                research[code] = {"name": blk["name"], "close": bar["c"]}
    print(f"研究池基准：{len(research)} 只票有 {TAIL_DATE} 收盘价")

    # ---- 1+2. M2' 全市场池 + 价格比对 ----
    df = TencentSource.get_all_stocks()
    print(f"M2' get_all_stocks: {len(df)} 行，列={list(df.columns)}")
    if df.empty:
        fails.append("get_all_stocks 返回空")
        return
    pool_codes = set(df["code"].astype(str))
    missing = [c for c in research if c not in pool_codes]
    if missing:
        fails.append(f"研究池 {len(missing)} 只不在生产池: {missing[:10]}")
    else:
        print(f"池完整性：27/27 研究票全部在 M2' 生产池内（{len(df)} 只全市场）")

    sub = df[df["code"].astype(str).isin(research)].copy()
    sub["code"] = sub["code"].astype(str)
    n_cmp = n_bad = 0
    for _, row in sub.iterrows():
        code = row["code"]
        ref = research[code]["close"]
        price = row.get("price") or row.get("close") or row.get("current")
        if price is None:
            fails.append(f"{code} 生产行无价格字段")
            continue
        n_cmp += 1
        if abs(float(price) - ref) > 0.011:
            n_bad += 1
            fails.append(f"{code} {research[code]['name']} 价格不一致: 生产{price} vs 研究{ref}")
    print(f"价格一致：{n_cmp} 只可比，{n_bad} 只超差（容差 0.011 元）")

    # ---- 3a. B5 对 27 只 legit 行 ----
    clean, stats = sanitize_quotes(sub)
    print(f"B5 sanitize（27 只研究票）: {stats}")
    if stats["dropped"] != 0:
        fails.append(f"B5 误剔 legit 行 {stats['dropped']} 条: {stats['reasons']}")

    # ---- 3b. B5 对全市场池 ----
    clean_all, stats_all = sanitize_quotes(df)
    print(f"B5 sanitize（全市场 {len(df)} 行）: dropped={stats_all['dropped']} "
          f"ratio={stats_all['drop_ratio']:.2%} reasons={stats_all['reasons']}")
    if stats_all["drop_ratio"] > 0.20:
        fails.append(f"全市场剔除率 {stats_all['drop_ratio']:.1%} > 20%（suspect 阈值）")
    non_exempt = {k: v for k, v in stats_all["reasons"].items()
                  if k != "new_listing_exempt" and v > 0}
    if non_exempt:
        fails.append(f"全市场存在非新股豁免剔除: {non_exempt}")

    print(f"\n耗时 {time.time()-t0:.0f}s")
    if fails:
        print(f"校验失败（{len(fails)} 项）:")
        for f_ in fails:
            print("  [FAIL] " + f_)
        sys.exit(1)
    print("生产链路交叉校验全部通过：M2' 池完整 + 价格与研究双源数据一致 + B5 零误剔")


if __name__ == "__main__":
    main()
