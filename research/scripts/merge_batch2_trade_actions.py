#!/usr/bin/env python3
"""合并 batch2 快手视频封面 OCR 提取的交易动作到总表。"""
import json
import os
from collections import Counter
from datetime import datetime

base = "/Users/hr.wh/WorkBuddy/2026-10-13-00-21-29"
meta = json.load(open(os.path.join(base, "raw/ks_videos_meta.json"), encoding="utf-8"))
video_date = {r["code"]: int(r.get("video", {}).get("timestamp", 0)) / 1000 for r in meta}

def d(code, override=None):
    if override:
        return override
    ts = video_date.get(code)
    if ts:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
    return ""

# batch2 从 10 个交易类视频封面手动提取（10 个视频，3 个非交易：K1T1YQiR/71McZuyt/nY2vguA9 已剔除）
new_records = []

def add(code, hint, date, filename, screenshot_type, stock, action, time, price,
        note, evidence="高", qty=None):
    new_records.append({
        "video_code": code,
        "hint": hint,
        "date": date,
        "filename": filename,
        "kind": "cover",
        "app": "同花顺",
        "screenshot_type": screenshot_type,
        "stock_name": stock,
        "type": action,
        "time": time,
        "quantity": qty,
        "price": price,
        "cost": None,
        "pnl_amount": None,
        "pnl_pct": None,
        "position_pct": None,
        "account_snapshot": {"总资产": None, "当日盈亏": None, "持仓盈亏": None, "仓位": None, "可用": None},
        "note": note,
        "evidence_level": evidence,
    })

# K0zjoFdn 2026-09-11 同花顺/江海证券 **0857 成交明细
add("K0zjoFdn", "花6000开个新号第一天来个暴击！大号也是惨不忍睹",
    d("K0zjoFdn"), "K0zjoFdn_03.jpg", "成交明细", "爱丽家居", "买入", "10:14:00", 27.010,
    "同花顺（江海证券 **0857）买入成交")
add("K0zjoFdn", "花6000开个新号第一天来个暴击！大号也是惨不忍睹",
    d("K0zjoFdn"), "K0zjoFdn_03.jpg", "成交明细", "香江控股", "卖出", "10:13:21", 4.710,
    "同花顺（江海证券 **0857）卖出成交（OCR 复核：该行显示为卖出/蓝色）")
add("K0zjoFdn", "花6000开个新号第一天来个暴击！大号也是惨不忍睹",
    d("K0zjoFdn"), "K0zjoFdn_03.jpg", "成交明细", "爱丽家居", "买入", "10:10:55", 27.150,
    "同花顺（江海证券 **0857）该价/时间出现两条，可能分笔成交")

# nhaozyYa 2026-09-10
add("nhaozyYa", "每一步都算数，配速是努力的刻度 | 换一下仓位了",
    d("nhaozyYa"), "nhaozyYa_00.jpg", "成交明细", "香江控股", "买入", "09:31:40", 5.090,
    "同花顺（江海证券 **0857）买入")
add("nhaozyYa", "每一步都算数，配速是努力的刻度 | 换一下仓位了",
    d("nhaozyYa"), "nhaozyYa_00.jpg", "成交明细", "爱丽家居", "卖出", "09:31:29", 26.280,
    "同花顺（江海证券 **0857）该价/时间出现两条，可能分笔成交")

# nTqrGbW8 2026-09-09
add("nTqrGbW8", "数据不会骗人，每一分努力都算数",
    d("nTqrGbW8"), "nTqrGbW8_00.jpg", "成交明细", "康欣新材", "买入", "14:06:55", 2.550,
    "同花顺（江海证券 **0857）买入")
add("nTqrGbW8", "数据不会骗人，每一分努力都算数",
    d("nTqrGbW8"), "nTqrGbW8_00.jpg", "成交明细", "金螳螂", "买入", "10:19:39", 5.320,
    "同花顺（江海证券 **0857）买入")
add("nTqrGbW8", "数据不会骗人，每一分努力都算数",
    d("nTqrGbW8"), "nTqrGbW8_00.jpg", "成交明细", "百合花", "买入", "10:19:00", 60.010,
    "同花顺（江海证券 **0857）买入")
add("nTqrGbW8", "数据不会骗人，每一分努力都算数",
    d("nTqrGbW8"), "nTqrGbW8_00.jpg", "成交明细", "香江控股", "卖出", "10:18:47", 5.370,
    "同花顺（江海证券 **0857）卖出")
add("nTqrGbW8", "数据不会骗人，每一分努力都算数",
    d("nTqrGbW8"), "nTqrGbW8_00.jpg", "成交明细", "金螳螂", "买入", "09:39:56", 5.440,
    "同花顺（江海证券 **0857）买入")
add("nTqrGbW8", "数据不会骗人，每一分努力都算数",
    d("nTqrGbW8"), "nTqrGbW8_00.jpg", "成交明细", "香江控股", "卖出", "09:39:47", 5.360,
    "同花顺（江海证券 **0857）卖出")

# Kt13VNcF 2026-09-08  已完成委托列表，无成交时间，仅有委托/现价
kt_date = d("Kt13VNcF")
kt_note = "同花顺（江海证券 **0857）'已完成'委托列表，无成交时间；价格为委托价/现价"
add("Kt13VNcF", "数据不会骗人，身体在悄悄提醒你",
    kt_date, "Kt13VNcF_03.jpg", "已完成委托列表", "香江控股", "买入", "", 5.36,
    kt_note, "中")
add("Kt13VNcF", "数据不会骗人，身体在悄悄提醒你",
    kt_date, "Kt13VNcF_03.jpg", "已完成委托列表", "金螳螂", "卖出", "", 5.41,
    kt_note, "中")
add("Kt13VNcF", "数据不会骗人，身体在悄悄提醒你",
    kt_date, "Kt13VNcF_03.jpg", "已完成委托列表", "金螳螂", "买入", "", 5.30,
    kt_note, "中")
add("Kt13VNcF", "数据不会骗人，身体在悄悄提醒你",
    kt_date, "Kt13VNcF_03.jpg", "已完成委托列表", "百合花", "买入", "", 63.80,
    kt_note, "中")
add("Kt13VNcF", "数据不会骗人，身体在悄悄提醒你",
    kt_date, "Kt13VNcF_03.jpg", "已完成委托列表", "新金路", "卖出", "", 17.10,
    kt_note, "中")
add("Kt13VNcF", "数据不会骗人，身体在悄悄提醒你",
    kt_date, "Kt13VNcF_03.jpg", "已完成委托列表", "百合花", "卖出", "", 62.80,
    kt_note, "中")
add("Kt13VNcF", "数据不会骗人，身体在悄悄提醒你",
    kt_date, "Kt13VNcF_03.jpg", "已完成委托列表", "百合花", "卖出", "", 62.79,
    kt_note, "中")
add("Kt13VNcF", "数据不会骗人，身体在悄悄提醒你",
    kt_date, "Kt13VNcF_03.jpg", "已完成委托列表", "新金路", "卖出", "", 17.40,
    kt_note, "中")

# K04yw219 2026-09-04  已完成委托列表，无成交时间
k04_date = d("K04yw219")
k04_note = "同花顺（江海证券 **0857）'已完成'委托列表，无成交时间；价格为委托价/现价"
add("K04yw219", "数字不会骗人，每一步都算数",
    k04_date, "K04yw219_03.jpg", "已完成委托列表", "贤丰控股", "买入", "", 6.73,
    k04_note, "中")
add("K04yw219", "数字不会骗人，每一步都算数",
    k04_date, "K04yw219_03.jpg", "已完成委托列表", "百合花", "买入", "", 53.69,
    k04_note, "中")
add("K04yw219", "数字不会骗人，每一步都算数",
    k04_date, "K04yw219_03.jpg", "已完成委托列表", "香江控股", "卖出", "", 4.76,
    k04_note, "中")
add("K04yw219", "数字不会骗人，每一步都算数",
    k04_date, "K04yw219_03.jpg", "已完成委托列表", "香江控股", "卖出", "", 4.78,
    k04_note, "中")
add("K04yw219", "数字不会骗人，每一步都算数",
    k04_date, "K04yw219_03.jpg", "已完成委托列表", "金螳螂", "买入", "", 5.18,
    k04_note, "中")
add("K04yw219", "数字不会骗人，每一步都算数",
    k04_date, "K04yw219_03.jpg", "已完成委托列表", "博纳影业", "卖出", "", 5.86,
    k04_note, "中")

# nJNnQl71 2026-09-03
add("nJNnQl71", "每一步都算数，配速是努力的刻度",
    d("nJNnQl71"), "nJNnQl71_02.jpg", "成交明细", "黄河旋风", "买入", "14:33:08", 16.140,
    "同花顺（江海证券 **0857）买入")
add("nJNnQl71", "每一步都算数，配速是努力的刻度",
    d("nJNnQl71"), "nJNnQl71_02.jpg", "成交明细", "香江控股", "卖出", "14:32:16", 4.800,
    "同花顺（江海证券 **0857）卖出")
add("nJNnQl71", "每一步都算数，配速是努力的刻度",
    d("nJNnQl71"), "nJNnQl71_02.jpg", "成交明细", "香江控股", "卖出", "14:32:15", 4.800,
    "同花顺（江海证券 **0857）卖出")
add("nJNnQl71", "每一步都算数，配速是努力的刻度",
    d("nJNnQl71"), "nJNnQl71_02.jpg", "成交明细", "香江控股", "卖出", "14:32:13", 4.800,
    "同花顺（江海证券 **0857）卖出")
add("nJNnQl71", "每一步都算数，配速是努力的刻度",
    d("nJNnQl71"), "nJNnQl71_02.jpg", "成交明细", "香江控股", "买入", "10:03:20", 4.780,
    "同花顺（江海证券 **0857）该价/时间出现两条，可能分笔成交")
add("nJNnQl71", "每一步都算数，配速是努力的刻度",
    d("nJNnQl71"), "nJNnQl71_02.jpg", "成交明细", "香江控股", "买入", "10:03:18", 4.780,
    "同花顺（江海证券 **0857）买入")

# npiWKRV1 2026-08-31 历史成交（截图自带日期）
npi_date = "2026-08-31"
add("npiWKRV1", "跑完十五公里，汗水没白流，身体在悄悄变强",
    npi_date, "npiWKRV1_00.jpg", "历史委托", "红宝丽", "卖出", "14:46:16", 8.630,
    "同花顺（江海证券 **0857）历史委托，日期 2026-08-31；该条均价 8.630，已成交")
add("npiWKRV1", "跑完十五公里，汗水没白流，身体在悄悄变强",
    npi_date, "npiWKRV1_00.jpg", "历史委托", "香江控股", "卖出", "14:41:26", 5.050,
    "同花顺（江海证券 **0857）历史委托，日期 2026-08-31；截图中均价 0.000，可能未成交/废单")
add("npiWKRV1", "跑完十五公里，汗水没白流，身体在悄悄变强",
    npi_date, "npiWKRV1_00.jpg", "历史委托", "香江控股", "卖出", "14:40:57", 5.090,
    "同花顺（江海证券 **0857）历史委托，日期 2026-08-31；截图中均价 0.000，可能未成交/废单")
add("npiWKRV1", "跑完十五公里，汗水没白流，身体在悄悄变强",
    npi_date, "npiWKRV1_00.jpg", "历史委托", "香江控股", "买入", "14:03:03", 5.000,
    "同花顺（江海证券 **0857）历史委托，日期 2026-08-31；该条均价 5.000，已成交")
add("npiWKRV1", "跑完十五公里，汗水没白流，身体在悄悄变强",
    npi_date, "npiWKRV1_00.jpg", "历史委托", "香江控股", "卖出", "13:53:26", 5.060,
    "同花顺（江海证券 **0857）历史委托，日期 2026-08-31；该条均价 5.060，已成交")

# JkxoMpoR 2026-08-28
# 校正说明：2026-10-13 子代理独立 OCR 复核发现：
#   JkxoMpoR_02.jpg 实际是“已完成委托(5)”列表，并非成交明细；
#   原硬编码的红宝丽/爱丽家居记录系误归属，已剔除。
# 实际交易内容：JkxoMpoR_00.jpg 已完成委托 + JkxoMpoR_01.jpg 成交明细。
jk_note = "同花顺（江海证券 **0857）"
add("JkxoMpoR", "数据不会骗人，身体在悄悄提醒你，有几个账号今天清了香江...",
    d("JkxoMpoR"), "JkxoMpoR_00.jpg", "已完成委托", "津药药业", "买入", "", 5.61,
    f"{jk_note}已完成委托，委托价 5.61 / 现价 5.60", "中")
add("JkxoMpoR", "数据不会骗人，身体在悄悄提醒你，有几个账号今天清了香江...",
    d("JkxoMpoR"), "JkxoMpoR_00.jpg", "已完成委托", "百合花", "买入", "", 66.04,
    f"{jk_note}已完成委托，委托价 66.04 / 现价 66.12", "中")
add("JkxoMpoR", "数据不会骗人，身体在悄悄提醒你，有几个账号今天清了香江...",
    d("JkxoMpoR"), "JkxoMpoR_00.jpg", "已完成委托", "香江控股", "卖出", "", 5.36,
    f"{jk_note}已完成委托，委托价 5.36 / 现价 5.36", "中")
add("JkxoMpoR", "数据不会骗人，身体在悄悄提醒你，有几个账号今天清了香江...",
    d("JkxoMpoR"), "JkxoMpoR_00.jpg", "已完成委托", "华电辽能", "买入", "", 14.46,
    f"{jk_note}已完成委托，委托价 14.46 / 现价 14.17", "中")
add("JkxoMpoR", "数据不会骗人，身体在悄悄提醒你，有几个账号今天清了香江...",
    d("JkxoMpoR"), "JkxoMpoR_00.jpg", "已完成委托", "贤丰控股", "卖出", "", 6.42,
    f"{jk_note}已完成委托，委托价 6.42 / 现价 6.45", "中")
add("JkxoMpoR", "数据不会骗人，身体在悄悄提醒你，有几个账号今天清了香江...",
    d("JkxoMpoR"), "JkxoMpoR_01.jpg", "成交明细", "香江控股", "买入", "09:32:41", 5.150,
    f"{jk_note}成交明细")
add("JkxoMpoR", "数据不会骗人，身体在悄悄提醒你，有几个账号今天清了香江...",
    d("JkxoMpoR"), "JkxoMpoR_01.jpg", "成交明细", "香江控股", "卖出", "09:31:58", 5.220,
    f"{jk_note}成交明细")

# KMvecutb 2026-08-20 历史成交
km_date = "2026-08-20"
add("KMvecutb", "账户数字悄然上扬，心也跟着轻快起来",
    km_date, "KMvecutb_00.jpg", "历史成交", "立新能源", "买入", "13:30:06", 12.330,
    "同花顺（江海证券 **0857）历史成交，日期 2026-08-20")
add("KMvecutb", "账户数字悄然上扬，心也跟着轻快起来",
    km_date, "KMvecutb_00.jpg", "历史成交", "金螳螂", "卖出", "13:29:08", 5.570,
    "同花顺（江海证券 **0857）历史成交，日期 2026-08-20")
add("KMvecutb", "账户数字悄然上扬，心也跟着轻快起来",
    km_date, "KMvecutb_00.jpg", "历史成交", "金螳螂", "买入", "13:04:42", 5.460,
    "同花顺（江海证券 **0857）历史成交，日期 2026-08-20")
add("KMvecutb", "账户数字悄然上扬，心也跟着轻快起来",
    km_date, "KMvecutb_00.jpg", "历史成交", "立新能源", "卖出", "13:03:46", 12.490,
    "同花顺（江海证券 **0857）历史成交，日期 2026-08-20")
add("KMvecutb", "账户数字悄然上扬，心也跟着轻快起来",
    km_date, "KMvecutb_00.jpg", "历史成交", "香江控股", "买入", "09:37:32", 4.140,
    "同花顺（江海证券 **0857）历史成交，日期 2026-08-20")
add("KMvecutb", "账户数字悄然上扬，心也跟着轻快起来",
    km_date, "KMvecutb_00.jpg", "历史成交", "香江控股", "卖出", "09:30:42", 4.210,
    "同花顺（江海证券 **0857）历史成交，日期 2026-08-20")

# KcNsTpS2 2026-08-17
add("KcNsTpS2", "炒股日记 实盘记录 我的主场时刻",
    d("KcNsTpS2"), "KcNsTpS2_00.jpg", "成交明细", "立新能源", "卖出", "14:04:38", 14.380,
    "同花顺（江海证券 **0857）卖出")
add("KcNsTpS2", "炒股日记 实盘记录 我的主场时刻",
    d("KcNsTpS2"), "KcNsTpS2_00.jpg", "成交明细", "立新能源", "买入", "13:44:10", 14.220,
    "同花顺（江海证券 **0857）买入")
add("KcNsTpS2", "炒股日记 实盘记录 我的主场时刻",
    d("KcNsTpS2"), "KcNsTpS2_00.jpg", "成交明细", "立新能源", "卖出", "13:43:09", 14.270,
    "同花顺（江海证券 **0857）卖出")
add("KcNsTpS2", "炒股日记 实盘记录 我的主场时刻",
    d("KcNsTpS2"), "KcNsTpS2_00.jpg", "成交明细", "立新能源", "买入", "10:53:51", 14.060,
    "同花顺（江海证券 **0857）买入")
add("KcNsTpS2", "炒股日记 实盘记录 我的主场时刻",
    d("KcNsTpS2"), "KcNsTpS2_00.jpg", "成交明细", "香江控股", "卖出", "10:53:36", 4.300,
    "同花顺（江海证券 **0857）卖出")
add("KcNsTpS2", "炒股日记 实盘记录 我的主场时刻",
    d("KcNsTpS2"), "KcNsTpS2_00.jpg", "成交明细", "香江控股", "买入", "09:32:58", 4.600,
    "同花顺（江海证券 **0857）买入")
add("KcNsTpS2", "炒股日记 实盘记录 我的主场时刻",
    d("KcNsTpS2"), "KcNsTpS2_00.jpg", "成交明细", "贤丰控股", "卖出", "09:32:46", 6.690,
    "同花顺（江海证券 **0857）卖出")

# 加载旧记录并合并
old = json.load(open(os.path.join(base, "data/ks_video_trade_actions.json"), encoding="utf-8"))
records = old + new_records
records.sort(key=lambda x: (x["date"], x.get("time") or ""))

# 去重键: code+stock+type+time+price
deduped = []
seen = set()
for r in records:
    key = (r["video_code"], r["stock_name"], r["type"], str(r.get("time") or ""), str(r.get("price") or ""))
    if key in seen:
        continue
    seen.add(key)
    deduped.append(r)

out_json = os.path.join(base, "data/ks_video_trade_actions.json")
json.dump(deduped, open(out_json, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
print(f"merged records: old={len(old)} new={len(new_records)} total={len(records)} deduped={len(deduped)}")

# Markdown 表
md = ["# 多空博弈社 快手视频交易动作明细（截图 OCR）\n",
      "| 日期 | 视频 | App | 截图类型 | 标的 | 动作 | 时间 | 价格 | 盈亏/仓位 | 证据 | 备注 |",
      "|------|------|-----|----------|------|------|------|------|-----------|------|------|"]
for r in deduped:
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

stocks = Counter(r["stock_name"] for r in deduped if r["stock_name"])
types = Counter(r["type"] for r in deduped)
md.append(f"\n\n## 聚合统计\n")
md.append(f"- 总交易动作记录: **{len(deduped)}** 条（去重后）\n")
md.append(f"- 涉及标的: {dict(stocks.most_common(20))}\n")
md.append(f"- 动作类型: {dict(types.most_common())}\n")

md_path = os.path.join(base, "data/ks_video_trade_actions_table.md")
open(md_path, "w", encoding="utf-8").write("\n".join(md))
print(f"saved {out_json} ({len(records)} rows)")
print(f"saved {md_path} ({len(deduped)} deduped rows)")
print("top stocks:", stocks.most_common(10))
print("action types:", dict(types))

# 同时保存 batch2 原始记录便于追溯
batch2_path = os.path.join(base, "data/ks_video_trade_actions_batch2_raw.json")
json.dump(new_records, open(batch2_path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
print(f"saved batch2 raw: {batch2_path} ({len(new_records)} rows)")
