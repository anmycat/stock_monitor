#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""解析落盘的扩展池日K结果，合并进 raw/market_full_ext.json（与主池分开，用于泛化验证）"""
import json, os

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TR = "/Users/hr.wh/.workbuddy/projects/Users-hr.wh-WorkBuddy-2026-10-13-00-21-29/9ac5c217-5538-4516-87a8-1ccbb7571601/tool-results"
FILES = [
    "mcp-neodata-quote_and_kline-1791922937718-4e6834.txt",
    "mcp-neodata-quote_and_kline-1791922937757-066c79.txt",
]

ext = {}
for fn in FILES:
    raw = open(os.path.join(TR, fn), encoding="utf-8").read()
    d = json.loads(raw)
    for blk in d.get("result", []):
        code = blk["code"]
        name = blk["name"]
        ext.setdefault(code, {"name": name, "d": {}})
        for k in blk.get("kline_list", []):
            day = k["trade_date"]
            rec = {
                "o": k.get("open"), "h": k.get("high"), "l": k.get("low"),
                "c": k.get("close"), "prev": k.get("pre_close"),
                "pct": k.get("change_pct"), "amp": k.get("amplitude"),
                "vol": k.get("volume"), "turn": k.get("turnover_rate"),
            }
            old = ext[code]["d"].get(day)
            if old:
                for kk, vv in rec.items():
                    if isinstance(vv, (int, float)) and vv == 0 and old.get(kk) not in (None, 0):
                        continue
                    old[kk] = vv
            else:
                ext[code]["d"][day] = rec

out = {"_meta": {"desc": "扩展验证池：东财头部用户高频提股，用于规则泛化验证（非博主交易票，规避选择偏差）",
                 "source": "neodata quote_and_kline 不复权 2026-07-06~2026-10-13"},
       "daily": ext}
with open(os.path.join(BASE, "raw/market_full_ext.json"), "w", encoding="utf-8") as f:
    json.dump(out, f, ensure_ascii=False)
for code, blk in sorted(ext.items()):
    days = sorted(blk["d"].keys())
    print(f"{blk['name']:8} {code} {days[0]} -> {days[-1]} ({len(days)}天)")
