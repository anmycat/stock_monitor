#!/usr/bin/env python3
"""D53 信号回归双跑对比（基线 HEAD vs 修复后）

用法：python3 research/scripts/compare_regression.py BASE.json NEW.json [--csv OUT]
逐字段对比 action/score/strategy/position/kelly/reason，输出差异统计与明细。
"""
import argparse
import json
import sys


def load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("base")
    ap.add_argument("new")
    ap.add_argument("--csv")
    args = ap.parse_args()

    b = load(args.base)
    n = load(args.new)
    bres, nres = b["results"], n["results"]

    fields = ["action", "score", "strategy", "position", "kelly_position", "reason"]
    field_diff = {f: 0 for f in fields}
    action_diff = []
    detail = []
    total_signals_b = sum(1 for d in bres.values() for v in d.values() if v)
    total_signals_n = sum(1 for d in nres.values() for v in d.values() if v)

    for date in sorted(set(bres) | set(nres)):
        bd = bres.get(date, {})
        nd = nres.get(date, {})
        for code in sorted(set(bd) | set(nd)):
            bv, nv = bd.get(code), nd.get(code)
            if bv is None and nv is None:
                continue
            if bv is None or nv is None:
                field_diff["action"] += 1
                ob = bv.get("action") if bv else "无信号"
                on = nv.get("action") if nv else "无信号"
                detail.append({
                    "date": date, "code": code, "field": "existence",
                    "base": ob, "new": on,
                })
                action_diff.append((date, code, ob, on))
                continue
            if "error" in bv or "error" in nv:
                detail.append({"date": date, "code": code, "field": "error",
                               "base": bv.get("error"), "new": nv.get("error")})
                continue
            diffs = {}
            for f in fields:
                if bv.get(f) != nv.get(f):
                    field_diff[f] += 1
                    diffs[f] = (bv.get(f), nv.get(f))
            if diffs:
                detail.append({"date": date, "code": code, "diffs": diffs})
                if "action" in diffs:
                    action_diff.append((date, code, diffs["action"][0], diffs["action"][1]))

    print("=" * 68)
    print("D53 信号回归双跑对比（基线=HEAD / 新=修复后）")
    print("=" * 68)
    print(f"窗口: {b['meta']['window']}  日期数 {b['meta']['dates']}  票数 {b['meta']['codes']}")
    print(f"基线信号数: {total_signals_b}    修复后信号数: {total_signals_n}")
    print(f"差异总数(票×日): {len(detail)}")
    print("-" * 68)
    print("逐字段差异计数:")
    for f in fields:
        print(f"  {f:16s} {field_diff[f]}")
    print("-" * 68)
    print(f"action 变化明细（前 30 条，共 {len(action_diff)}）:")
    for d, c, ob, on in action_diff[:30]:
        print(f"  {d} {c}: {ob} → {on}")
    if len(action_diff) > 30:
        print(f"  ... 其余 {len(action_diff) - 30} 条")
    print("-" * 68)
    # action 变化统计
    from collections import Counter
    trans = Counter((ob, on) for _, _, ob, on in action_diff)
    print("action 迁移矩阵:")
    for (ob, on), cnt in trans.most_common():
        print(f"  {ob:6s} → {on:6s} : {cnt}")
    # 仓位变化统计
    pos_rows = [d for d in detail if "diffs" in d and "position" in d["diffs"]]
    if pos_rows:
        ratios = []
        for d in pos_rows:
            ob, on = d["diffs"]["position"]
            if ob:
                ratios.append(on / ob)
        ratios.sort()
        print("-" * 68)
        print(f"仓位变化 {len(pos_rows)} 条，倍率 中位 {ratios[len(ratios)//2]:.3f} "
              f"最小 {ratios[0]:.3f} 最大 {ratios[-1]:.3f}")

    if args.csv:
        import csv
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "code", "field", "base", "new"])
            for d in detail:
                if "diffs" in d:
                    for k, (ob, on) in d["diffs"].items():
                        w.writerow([d["date"], d["code"], k, ob, on])
                else:
                    w.writerow([d["date"], d["code"], d.get("field"), d.get("base"), d.get("new")])
        print(f"\n明细已写出: {args.csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
