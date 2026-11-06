#!/usr/bin/env python3
"""盘中日K 实时修正 A/B 复盘（D65 取证工具）

读 Redis 键 `intraday_kline:ab`（worker 每轮扫描写入，滚动 200 条 / TTL 7 天），
输出「修正前 vs 修正后」的信号差异统计。

用法（ECS 上）：
  cd /stock_monitor && python3 research/scripts/ab_intraday_report.py
  cd /stock_monitor && python3 research/scripts/ab_intraday_report.py --json
  cd /stock_monitor && python3 research/scripts/ab_intraday_report.py --days 3

本地读法（研究机上无 Redis 时）：把 redis-cli 的输出存成 JSON 再用 --file 读。
"""
import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

KEY = 'intraday_kline:ab'


def load_from_redis():
    try:
        from quant_engine.api.dependencies import datacenter
        items = datacenter.cache.get(KEY) or []
        return items if isinstance(items, list) else []
    except Exception as e:  # noqa: BLE001
        print(f'读取 Redis 失败：{e}', file=sys.stderr)
        return []


def load_from_file(path):
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    return data if isinstance(data, list) else data.get('items', [])


def summarize(items, days=None):
    if days:
        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        items = [i for i in items if str(i.get('ts', ''))[:10] >= cutoff]
    if not items:
        print('无 A/B 记录（明日交易时段扫描后即产生）')
        return {}

    changed = [i for i in items if i.get('changed')]
    print(f'总记录 {len(items)} 条｜口径变更 {len(changed)} 条'
          f'（{len(changed)/len(items):.1%}）\n')

    print('=== 变更明细（按跳空幅度排序）===')
    for i in sorted(changed, key=lambda x: x.get('gap_pct') or 0):
        print(f"  {i.get('ts', '')[11:16]} {i.get('code')} {i.get('name', ''):<8} "
              f"跳空 {i.get('gap_pct'):+.2%} "
              f"base {i.get('base_action')}/{i.get('base_score')} → "
              f"corr {i.get('corr_action')}/{i.get('corr_score')}")

    print('\n=== 变更方向统计 ===')
    trans = Counter()
    for i in changed:
        trans[(i.get('base_action'), i.get('corr_action'))] += 1
    for (b, c), n in trans.most_common():
        print(f'  {b} → {c}: {n} 条')

    print('\n=== 跳空分档 vs 变更率 ===')
    buckets = {'<=-5%': [], '-5%~-3%': [], '-3%~-1%': [], '-1%~0': [],
               '>=0': []}
    for i in items:
        g = i.get('gap_pct')
        if g is None:
            continue
        if g <= -0.05:
            k = '<=-5%'
        elif g <= -0.03:
            k = '-5%~-3%'
        elif g <= -0.01:
            k = '-3%~-1%'
        elif g < 0:
            k = '-1%~0'
        else:
            k = '>=0'
        buckets[k].append(i)
    for k, arr in buckets.items():
        if not arr:
            continue
        ch = sum(1 for i in arr if i.get('changed'))
        print(f'  跳空 {k:>9}: {len(arr):>3} 条，变更 {ch:>3} 条（{ch/len(arr):.0%}）')

    return {'total': len(items), 'changed': len(changed),
            'transitions': {f'{k[0]}->{k[1]}': v for k, v in trans.items()}}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--file', help='从本地 JSON 文件读（无 Redis 时）')
    ap.add_argument('--days', type=int, default=None, help='只看最近 N 天')
    ap.add_argument('--json', action='store_true', help='输出 JSON')
    args = ap.parse_args()

    items = load_from_file(args.file) if args.file else load_from_redis()
    if args.json:
        print(json.dumps(items, ensure_ascii=False, indent=2))
        return
    summarize(items, args.days)


if __name__ == '__main__':
    main()
