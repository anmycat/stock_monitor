#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""收集 tool-results 目录里所有 neodata 行情调用结果，合并为完整日K/技术指标数据集。

只收 adjust_type=不复权 的 quote_and_kline 与 technical 的 tech_data。
过滤"未开盘"/"省略中间历史行情"占位行。
"""
import json
import os
import re
import glob
from collections import defaultdict

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULT_DIR = "/Users/hr.wh/.workbuddy/projects/Users-hr.wh-WorkBuddy-2026-10-13-00-21-29/9ac5c217-5538-4516-87a8-1ccbb7571601/tool-results"
OUT = os.path.join(BASE, "raw/market_full.json")

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def is_placeholder(v):
    """占位行：未开盘 / 省略区间"""
    s = str(v)
    return ("未开盘" in s) or ("省略" in s) or s in ("-", "")


def load_json_files():
    """yield (filename, dict) 所有能解析成 neodata 响应的文件"""
    for path in sorted(glob.glob(os.path.join(RESULT_DIR, "chatcmpl-tool-*.txt"))):
        try:
            with open(path, encoding="utf-8") as f:
                raw = f.read()
        except Exception:
            continue
        # render 文件是 {"content": "<json string>"}
        obj = None
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        if isinstance(obj, dict) and isinstance(obj.get("content"), str):
            try:
                obj = json.loads(obj["content"])
            except Exception:
                continue
        if not isinstance(obj, dict):
            continue
        if not isinstance(obj.get("result"), list):
            continue
        # 只认含 kline_list 或 tech_data 的
        ok = any(isinstance(r, dict) and ("kline_list" in r or "tech_data" in r) for r in obj["result"])
        if ok:
            yield os.path.basename(path), obj


def merge_daily(dst, code, name, day, rec):
    """合并单条日K：0 值（缺字段的调用）不得覆盖已有非 0 值"""
    slot = dst.setdefault(code, {"name": name, "d": {}})
    if name:
        slot["name"] = name
    old = slot["d"].get(day)
    if old is None:
        slot["d"][day] = rec
        return
    for k, v in rec.items():
        if isinstance(v, (int, float)) and v == 0:
            continue          # 0 视为缺失，不覆盖
        if k in ("o", "h", "l", "c", "prev", "pct") and old.get(k) not in (None, 0):
            continue          # 价格字段已有值则不覆盖
        old[k] = v


def main():
    daily = defaultdict(lambda: {"name": None, "d": {}})   # code -> {name, d: {date: {...}}}
    tech = defaultdict(lambda: {"name": None, "d": {}})    # code -> {name, d: {date: {...}}}
    files_used = []

    for fname, obj in load_json_files():
        files_used.append(fname)
        for r in obj["result"]:
            if not isinstance(r, dict):
                continue

            # ---- quote_and_kline ----
            if "kline_list" in r:
                if str(r.get("adjust_type", "")) != "不复权":
                    continue
                code = r.get("code")
                if not code:
                    continue
                if r.get("name"):
                    daily[code]["name"] = r["name"]
                for k in r["kline_list"]:
                    d = k.get("trade_date", "")
                    if not DATE_RE.match(d) or is_placeholder(k.get("open")):
                        continue
                    merge_daily(daily, code, r.get("name"), d, {
                        "o": float(k["open"]), "h": float(k["high"]),
                        "l": float(k["low"]), "c": float(k["close"]),
                        "prev": float(k.get("prev_close") or 0),
                        "pct": float(k.get("pct_change") or 0),
                        "amp": float(k.get("amplitude") or 0),
                        "vol": float(k.get("volume") or 0),
                        "turn": float(k.get("turnover_rate") or 0),
                    })

            # ---- technical ----
            if "tech_data" in r:
                code = r.get("stock_code")
                if not code:
                    continue
                if r.get("stock_name"):
                    tech[code]["name"] = r["stock_name"]
                for k in r["tech_data"]:
                    td = k.get("trading_date")
                    if not td:
                        continue
                    d = f"{str(td)[:4]}-{str(td)[4:6]}-{str(td)[6:8]}"
                    if not DATE_RE.match(d) or is_placeholder(k.get("close")):
                        continue
                    tech[code]["d"][d] = {kk: vv for kk, vv in k.items() if kk != "trading_date"}

    out = {
        "_meta": {
            "source": "neodata quote_and_kline(不复权) + technical",
            "files": files_used,
            "note": "daily 为不复权日K；tech 为技术指标（含筹码/均线/乖离，注意 technical 均线自前复权计算）",
        },
        "daily": {c: {"name": v["name"], "d": dict(sorted(v["d"].items()))} for c, v in sorted(daily.items())},
        "tech": {c: {"name": v["name"], "d": dict(sorted(v["d"].items()))} for c, v in sorted(tech.items())},
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)

    print(f"文件数: {len(files_used)}")
    print(f"daily 股票数: {len(out['daily'])}  tech 股票数: {len(out['tech'])}")
    for c, v in out["daily"].items():
        ds = list(v["d"])
        print(f"  [日K] {c} {v['name']:8} {len(ds):3}天  {ds[0]} ~ {ds[-1]}")
    for c, v in out["tech"].items():
        ds = list(v["d"])
        print(f"  [技术] {c} {v['name']:8} {len(ds):3}天  {ds[0]} ~ {ds[-1]}")


if __name__ == "__main__":
    main()
