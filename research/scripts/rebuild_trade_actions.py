#!/usr/bin/env python3
"""干净重建 ks_video_trade_actions.json / _table.md：
batch1(5 视频, 30 条, 已验证) + batch2(10 视频, 56 条, 子代理 OCR 复核后校正)。
两批 video_code 互不重叠，无需跨批去重；仅批内按 (code,stock,type,time,price) 去重。
"""
import json, os
from collections import Counter

base = "/Users/hr.wh/WorkBuddy/2026-10-13-00-21-29"
b1_codes = {"JKn0ajlI", "Jdg4jr6z", "nZTKyjXA", "JgZ1IFSc", "nmP9R9Lt"}

cur = json.load(open(os.path.join(base, "data/ks_video_trade_actions.json"), encoding="utf-8"))
raw2 = json.load(open(os.path.join(base, "data/ks_video_trade_actions_batch2_raw.json"), encoding="utf-8"))

b1 = [r for r in cur if r["video_code"] in b1_codes]
b2 = [r for r in raw2 if r["video_code"] not in b1_codes]

def dedup(recs):
    out, seen = [], set()
    for r in recs:
        key = (r["video_code"], r["stock_name"], r["type"], str(r.get("time") or ""), str(r.get("price") or ""))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

b1d, b2d = dedup(b1), dedup(b2)
allrecs = b1d + b2d
allrecs.sort(key=lambda x: (x["date"], x.get("time") or ""))

out_json = os.path.join(base, "data/ks_video_trade_actions.json")
json.dump(allrecs, open(out_json, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

# Markdown 表
md = ["# 多空博弈社 快手视频交易动作明细（截图 OCR）\n",
      "| 日期 | 视频 | App | 截图类型 | 标的 | 动作 | 时间 | 价格 | 盈亏/仓位 | 证据 | 备注 |",
      "|------|------|-----|----------|------|------|------|------|-----------|------|------|"]
for r in allrecs:
    price = f"{r['price']:.3f}" if isinstance(r['price'], (int, float)) else ""
    pnl = ""
    if isinstance(r.get('pnl_amount'), (int, float)):
        pnl += f"{r['pnl_amount']:+.2f}"
    if isinstance(r.get('pnl_pct'), (int, float)):
        pnl += f"/{r['pnl_pct']*100:.2f}%"
    if isinstance(r.get('position_pct'), (int, float)):
        pnl += f" 仓位{r['position_pct']*100:.1f}%"
    md.append(
        f"| {r['date']} | {r['video_code']} | {r['app']} | {r['screenshot_type']} | {r['stock_name']} | {r['type']} | {r.get('time','')} | {price} | {pnl} | {r.get('evidence_level','')} | {str(r.get('note',''))[:55]} |"
    )

stocks = Counter(r["stock_name"] for r in allrecs if r["stock_name"])
types = Counter(r["type"] for r in allrecs)
videos = Counter(r["video_code"] for r in allrecs)
md.append(f"\n\n## 聚合统计\n")
md.append(f"- 总交易动作记录: **{len(allrecs)}** 条（去重后；第一批 {len(b1d)} + 第二批 {len(b2d)}）\n")
md.append(f"- 覆盖视频: {len(videos)} 条（第一批 5 + 第二批 10 交易视频；另有 3 条非交易已剔除）\n")
md.append(f"- 动作类型: {dict(types.most_common())}\n")
md.append(f"- 涉及标的 Top: {dict(stocks.most_common(15))}\n")

md_path = os.path.join(base, "data/ks_video_trade_actions_table.md")
open(md_path, "w", encoding="utf-8").write("\n".join(md))
print(f"batch1={len(b1d)} batch2={len(b2d)} total={len(allrecs)} videos={len(videos)}")
print("top stocks:", stocks.most_common(15))
print("action types:", dict(types))
print("JkxoMpoR:", videos.get('JkxoMpoR'))
