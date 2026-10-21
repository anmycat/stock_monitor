#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""3M-v2 验真分析：定稿口径（v1.4采纳+剔R06）完整统计 + 月度分布 + 尾部截断影响 + bull日明细。
输入：data/signals_3m_v2.csv + data/validation_3m_v2.json + raw/index_daily.json
"""
import csv, json, os, sys
from collections import defaultdict

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE, "scripts"))
import validate_6m_all_rules as v6
stat = v6.stat
fmt = v6.fmt

rows = list(csv.DictReader(open(os.path.join(BASE, "data/signals_3m_v2.csv"), encoding="utf-8-sig")))
for r in rows:
    r["pnl_pct"] = float(r["pnl_pct"])
    r["idx_bias20"] = float(r["idx_bias20"]) if r["idx_bias20"] else None
    r["pre_runup_5d"] = float(r["pre_runup_5d"]) if r["pre_runup_5d"] else None
    r["pre_runup_10d"] = float(r["pre_runup_10d"]) if r["pre_runup_10d"] else None

def pass_r12(s):
    if s["pre_runup_5d"] is not None and s["pre_runup_5d"] > 15: return False
    if s["pre_runup_10d"] is not None and s["pre_runup_10d"] > 25: return False
    return True
def pass_r13v(s):
    if s["rule"] == "R05" and s["idx_bias20"] is not None and s["idx_bias20"] <= -2: return False
    return True

combo = [s for s in rows if pass_r12(s) and pass_r13v(s)]
final = [s for s in combo if s["rule"] != "R06"]

print("=== v1.5 定稿口径（v1.4采纳 R12+R13v + 剔R06）===")
print("定稿:      " + fmt(stat([s["pnl_pct"] for s in final])))
print("  按规则:", {r: sum(1 for s in final if s["rule"] == r) for r in sorted({s["rule"] for s in final})})
print("v1.4采纳: " + fmt(stat([s["pnl_pct"] for s in combo])))
print("无过滤:    " + fmt(stat([s["pnl_pct"] for s in rows])))

print("\n=== 定稿口径月度稳定性（对照 PRD v1: 7月22笔72.7%/+8.24% 8月79笔59.5%/+2.57% 9月96笔27.1%/-1.99% 10月半月4笔0%/-4.39%）===")
months = defaultdict(list)
for s in final:
    months[s["signal_date"][:7]].append(s["pnl_pct"])
for m, v in sorted(months.items()):
    print(f"  {m}: " + fmt(stat(v)))

print("\n=== R02 明细（19笔，检查窗口尾部截断影响）===")
r02 = sorted([s for s in rows if s["rule"] == "R02"], key=lambda x: x["signal_date"])
for s in r02:
    print(f"  {s['signal_date']} {s['code']} {s['stock']:6} 买{s['buy_date']}({s['buy_price']}) 卖{s['sell_date']}({s['sell_price']}) {s['pnl_pct']:+.2f}% {s['reason']}")

print("\n=== 各规则信号日期范围与末5日占比 ===")
for rid in ("R01", "R02", "R04", "R05", "R06"):
    sigs = sorted([s for s in rows if s["rule"] == rid], key=lambda x: x["signal_date"])
    tail = [s for s in sigs if s["signal_date"] >= "2026-10-14"]
    tail_pnl = sum(s["pnl_pct"] for s in tail)
    print(f"  {rid}: n={len(sigs)} 范围{sigs[0]['signal_date']}~{sigs[-1]['signal_date']} 尾部(>=10-14){len(tail)}笔 合计{tail_pnl:+.2f}%")

print("\n=== 窗口内 regime 日期明细（bull/最后几日）===")
IDX_REGIME = v6.IDX_REGIME
days = [d for d in sorted(IDX_REGIME) if "2026-07-21" <= d <= "2026-10-20"]
from collections import Counter
print("  regime分布:", dict(Counter(IDX_REGIME[d]["idx_regime"] for d in days)))
print("  bull日:", [d for d in days if IDX_REGIME[d]["idx_regime"] == "bull"])
print("  最后5日指数:", [(d, IDX_REGIME[d]["idx_close"], IDX_REGIME[d]["idx_bias20"], IDX_REGIME[d]["idx_regime"]) for d in days[-5:]])

print("\n=== 基准对比（PRD 诚实边界：基准 vs 规则）===")
print("  v2基准 48.5%/+1.02%（PRD v1: 51.4%/+1.87%）；定稿口径胜率/均盈亏见上——差距仍在")
