import json, os, re
base='/Users/hr.wh/WorkBuddy/2026-10-13-00-21-29'
recs=json.load(open(os.path.join(base,'data/ks_trade_actions_all.json'),encoding='utf-8'))
trade_types={'持仓','成交','委托','日K买卖点','分时卖点','收益曲线','账户资产'}
trade=[r for r in recs if r.get('screenshot_type') in trade_types]

def fmt_action(a):
    parts=[]
    if a.get('type'): parts.append(a['type'])
    if a.get('time'): parts.append(f"时间 {a['time']}")
    if a.get('quantity') is not None: parts.append(f"量 {a['quantity']}")
    if a.get('price') is not None: parts.append(f"价 {a['price']}")
    if a.get('cost') is not None: parts.append(f"成本 {a['cost']}")
    if a.get('pnl_amount') is not None: parts.append(f"盈亏额 {a['pnl_amount']}")
    if a.get('pnl_pct') is not None: parts.append(f"盈亏比 {a['pnl_pct']}")
    if a.get('position_pct') is not None: parts.append(f"仓位 {a['position_pct']}")
    return '; '.join(parts)

lines=[]
lines.append('# 多空博弈社 交易截图提取明细（25条）\n')
lines.append(f"来源：东方财富 多空博弈社（uid 7924037455558900）\n")
lines.append(f"截图总数：69 张 | 交易相关：{len(trade)} 条\n\n")
lines.append("| 日期 | 标的 | 类型 | App | 动作与关键数字 | 文字上下文 | 证据 |\n")
lines.append("|------|------|------|-----|----------------|------------|------|\n")
for r in trade:
    acts=' <br> '.join(fmt_action(a) for a in r.get('actions',[]))
    txt=(r.get('text_context') or '').replace('|','/').replace('\n',' ')
    lines.append(f"| {r.get('date','')} | {r.get('stock_name','')} {r.get('stock_code','')} | {r.get('screenshot_type','')} | {r.get('app','')} | {acts} | {txt[:90]} | {r.get('evidence_level','')} |\n")
    if r.get('note'):
        lines.append(f"| | | | | 备注：{r.get('note').replace('|','/').replace(chr(10),' ')} | | |\n")

open(os.path.join(base,'data/ks_trade_actions_table.md'),'w',encoding='utf-8').writelines(lines)
print('saved data/ks_trade_actions_table.md')
