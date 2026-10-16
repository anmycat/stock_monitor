import json, os, collections, re
base='/Users/hr.wh/WorkBuddy/2026-10-13-00-21-29'
all_records=[]
for i in range(7):
    f=os.path.join(base,f'data/ks_analysis_batch_{i}.json')
    if not os.path.exists(f):
        print('missing',f); continue
    d=json.load(open(f,encoding='utf-8'))
    print(f'batch {i}: {len(d)} records')
    all_records.extend(d)
print('total',len(all_records))

# 截图类型分布
sc=collections.Counter(r.get('screenshot_type','unknown') for r in all_records)
print('screenshot_type:', dict(sc))

# 交易相关记录
trade_types={'持仓','成交','委托','日K买卖点','分时卖点','收益曲线','账户资产'}
trade=[r for r in all_records if r.get('screenshot_type') in trade_types]
print('trade-related:', len(trade))

# 标的分布
stocks=collections.Counter((r.get('stock_code',''), r.get('stock_name','')) for r in trade if r.get('stock_name'))
print('stocks:', dict(stocks.most_common(15)))

# evidence level
ev=collections.Counter(r.get('evidence_level','') for r in trade)
print('evidence:', dict(ev))

# 保存合并结果
with open(os.path.join(base,'data/ks_trade_actions_all.json'),'w',encoding='utf-8') as f:
    json.dump(all_records, f, ensure_ascii=False, indent=2)

# 生成摘要
summary={
    "total_screenshots": len(all_records),
    "trade_related": len(trade),
    "screenshot_type_distribution": dict(sc),
    "stock_distribution": [ {"code":c,"name":n,"count":cnt} for (c,n),cnt in stocks.most_common() ],
    "evidence_level_distribution": dict(ev),
}
with open(os.path.join(base,'data/ks_trade_actions_summary.json'),'w',encoding='utf-8') as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)

print('saved all & summary')
