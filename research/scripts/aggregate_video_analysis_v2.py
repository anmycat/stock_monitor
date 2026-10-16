#!/usr/bin/env python3
"""合并子代理分析结果，提取正确股票名称、去重、生成表格。"""
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
    try:
        video_date[r["code"]] = datetime.fromtimestamp(int(ts) / 1000).strftime("%Y-%m-%d")
    except Exception:
        video_date[r["code"]] = ""

KNOWN_STOCKS = ["爱丽家居", "黄河旋风", "远东股份", "贤丰控股", "国芳集团",
                "香江控股", "深物业A", "哈药股份", "博纳影业", "中毅达", "*ST东易", "*ST宇顺",
                "康欣新材", "金螳螂", "百合花", "新金路", "红宝丽", "立新能源", "津药药业", "华电辽能"]


def extract_stock(note):
    if not note:
        return ""
    # 1) 命中已知名称，取最长
    found = [s for s in KNOWN_STOCKS if s in note]
    if found:
        return max(found, key=len)
    # 2) 括号前抓：如 "黄河旋风（600172.SH）"
    m = re.search(r"([\u4e00-\u9fa5]{2,8}(?:控股|集团|股份|家居|旋风|影业|物业A))\s*[（(]", note)
    if m:
        return m.group(1)
    return ""


records = []
seen = set()
for batch in "ABC":
    f = os.path.join(base, f"data/ks_video_analysis_{batch}.json")
    d = json.load(open(f, encoding="utf-8"))
    for v in d:
        code = v["video_code"]
        for img in v.get("images", []):
            if img.get("screenshot_type") == "其他" or not img.get("actions"):
                continue
            # 跳过 cover 与 atlas 内容重复：保留 atlas
            txt = img.get("text_in_image", "")
            if img.get("kind") == "cover" and txt and any(
                txt in other.get("text_in_image", "") or other.get("text_in_image", "") in txt
                for other in v.get("images", [])
                if other.get("kind") == "atlas"
            ):
                continue
            for a in img["actions"]:
                # 优先用 action.note 里的股票名（更精确），否则 fallback 到 img.stock_name
                stock = extract_stock(a.get("note", ""))
                if not stock:
                    stock = img.get("stock_name") or ""
                if not stock or "/" in stock or stock in ("持仓",):
                    stock = extract_stock(img.get("note", ""))
                # 重复检测 key
                t = a.get("time") or ""
                p = a.get("price")
                tp = a.get("type", "")
                key = (code, stock, tp, str(t), str(p))
                if key in seen:
                    continue
                seen.add(key)
                records.append({
                    "video_code": code,
                    "hint": v.get("hint", ""),
                    "date": video_date.get(code, ""),
                    "filename": img.get("filename", ""),
                    "kind": img.get("kind", ""),
                    "app": img.get("app", ""),
                    "screenshot_type": img.get("screenshot_type", ""),
                    "stock_name": stock,
                    "type": tp,
                    "time": t,
                    "quantity": a.get("quantity"),
                    "price": p,
                    "cost": a.get("cost"),
                    "pnl_amount": a.get("pnl_amount"),
                    "pnl_pct": a.get("pnl_pct"),
                    "position_pct": a.get("position_pct"),
                    "account_snapshot": img.get("account_snapshot", {}),
                    "note": a.get("note", ""),
                    "evidence_level": img.get("evidence_level", ""),
                })

records.sort(key=lambda x: (x["date"], x["time"] or ""))
out = os.path.join(base, "data/ks_video_trade_actions.json")
json.dump(records, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

# Markdown
md = ["# 多空博弈社 快手视频交易动作明细（截图 OCR）\n",
      "| 日期 | 视频 | App | 截图类型 | 标的 | 动作 | 时间 | 价格 | 盈亏/仓位 | 证据 | 备注 |",
      "|------|------|-----|----------|------|------|------|------|-----------|------|------|"]
for r in records:
    price = f"{r['price']:.3f}" if isinstance(r['price'], (int, float)) else ""
    pnl = ""
    if isinstance(r['pnl_amount'], (int, float)):
        pnl += f"{r['pnl_amount']:+.2f}"
    if isinstance(r['pnl_pct'], (int, float)):
        pnl += f"/{r['pnl_pct']*100:.2f}%"
    if isinstance(r['position_pct'], (int, float)):
        pnl += f" 仓位{r['position_pct']*100:.1f}%"
    md.append(
        f"| {r['date']} | {r['video_code']} | {r['app']} | {r['screenshot_type']} | {r['stock_name']} | {r['type']} | {r['time']} | {price} | {pnl} | {r['evidence_level']} | {r['note'][:55]} |"
    )

stocks = Counter(r["stock_name"] for r in records if r["stock_name"])
types = Counter(r["type"] for r in records)
md.append(f"\n\n## 聚合统计\n")
md.append(f"- 总交易动作记录: **{len(records)}** 条（去重后）\n")
md.append(f"- 涉及标的: {dict(stocks.most_common(15))}\n")
md.append(f"- 动作类型: {dict(types.most_common())}\n")

md_path = os.path.join(base, "data/ks_video_trade_actions_table.md")
open(md_path, "w", encoding="utf-8").write("\n".join(md))
print(f"records: {len(records)}")
print(f"saved {out}")
print(f"saved {md_path}")
print("top stocks:", stocks.most_common(10))
print("action types:", dict(types))
