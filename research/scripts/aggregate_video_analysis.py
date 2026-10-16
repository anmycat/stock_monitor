#!/usr/bin/env python3
"""合并子代理的快手视频截图分析结果，生成扁平交易动作表与蒸馏摘要。"""
import json
import os
import re
from collections import Counter
from datetime import datetime

base = "/Users/hr.wh/WorkBuddy/2026-10-13-00-21-29"

meta = json.load(open(os.path.join(base, "raw/ks_videos_meta.json"), encoding="utf-8"))
video_date = {}
for r in meta:
    ts = r.get("video", {}).get("timestamp")
    if ts:
        try:
            dt = datetime.fromtimestamp(int(ts) / 1000)
            video_date[r["code"]] = dt.strftime("%Y-%m-%d")
        except Exception:
            video_date[r["code"]] = ""
    else:
        video_date[r["code"]] = ""

records = []
for batch in "ABC":
    f = os.path.join(base, f"data/ks_video_analysis_{batch}.json")
    d = json.load(open(f, encoding="utf-8"))
    for v in d:
        code = v["video_code"]
        seen_img = set()
        for img in v.get("images", []):
            fn = img.get("filename", "")
            # 跳过非交易截图
            if img.get("screenshot_type") == "其他":
                continue
            # 跳过重复图（按 text_in_image 去重）
            txt = img.get("text_in_image", "")
            key = (code, txt)
            if key in seen_img and txt:
                continue
            seen_img.add(key)
            # 跳过明确无 action 的封面
            if not img.get("actions"):
                continue
            # 跳过非 atlas 的 cover（除非 cover 含交易内容）
            if img.get("kind") == "cover" and img.get("screenshot_type") == "其他":
                continue
            for a in img["actions"]:
                records.append({
                    "video_code": code,
                    "hint": v.get("hint", ""),
                    "date": video_date.get(code, ""),
                    "filename": fn,
                    "kind": img.get("kind", ""),
                    "app": img.get("app", ""),
                    "screenshot_type": img.get("screenshot_type", ""),
                    "stock_name": img.get("stock_name") or a.get("stock_name") or "",
                    "type": a.get("type", ""),
                    "time": a.get("time", ""),
                    "quantity": a.get("quantity"),
                    "price": a.get("price"),
                    "cost": a.get("cost"),
                    "pnl_amount": a.get("pnl_amount"),
                    "pnl_pct": a.get("pnl_pct"),
                    "position_pct": a.get("position_pct"),
                    "account_snapshot": img.get("account_snapshot", {}),
                    "note": a.get("note", ""),
                    "evidence_level": img.get("evidence_level", ""),
                })

# 按日期+时间排序
records.sort(key=lambda x: (x["date"], x["time"] or ""))
out = os.path.join(base, "data/ks_video_trade_actions.json")
json.dump(records, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

# Markdown 表格
md_lines = ["# 多空博弈社 快手视频交易动作明细（截图 OCR）\n",
            "| 日期 | 视频 | App | 截图类型 | 标的 | 动作 | 时间 | 价格 | 成本 | 盈亏/仓位 | 证据 | 备注 |",
            "|------|------|-----|----------|------|------|------|------|------|-----------|------|------|"]
for r in records:
    price = f"{r['price']:.3f}" if isinstance(r['price'], (int, float)) else ""
    cost = f"{r['cost']:.3f}" if isinstance(r['cost'], (int, float)) else ""
    pnl = ""
    if isinstance(r['pnl_amount'], (int, float)):
        pnl += f"{r['pnl_amount']:+.2f}"
    if isinstance(r['pnl_pct'], (int, float)):
        pnl += f"/{r['pnl_pct']*100:.2f}%"
    if isinstance(r['position_pct'], (int, float)):
        pnl += f" 仓位{r['position_pct']*100:.1f}%"
    md_lines.append(
        f"| {r['date']} | {r['video_code']} | {r['app']} | {r['screenshot_type']} | {r['stock_name']} | {r['type']} | {r['time']} | {price} | {cost} | {pnl} | {r['evidence_level']} | {r['note'][:60]} |"
    )

# 聚合摘要
stocks = Counter(r["stock_name"] for r in records if r["stock_name"])
types = Counter(r["type"] for r in records)
md_lines.append("\n\n## 聚合统计\n")
md_lines.append(f"- 总交易动作记录: **{len(records)}** 条\n")
md_lines.append(f"- 涉及标的: {dict(stocks.most_common(15))}\n")
md_lines.append(f"- 动作类型: {dict(types.most_common())}\n")

md_path = os.path.join(base, "data/ks_video_trade_actions_table.md")
open(md_path, "w", encoding="utf-8").write("\n".join(md_lines))

print(f"records: {len(records)}")
print(f"saved {out}")
print(f"saved {md_path}")
print("\ntop stocks:", stocks.most_common(10))
print("action types:", dict(types))
