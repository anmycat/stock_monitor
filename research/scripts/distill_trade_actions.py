import json, os, re, collections, datetime
base='/Users/hr.wh/WorkBuddy/2026-10-13-00-21-29'
recs=json.load(open(os.path.join(base,'data/ks_trade_actions_all.json'),encoding='utf-8'))
trade_types={'持仓','成交','委托','日K买卖点','分时卖点','收益曲线','账户资产'}
trade=[r for r in recs if r.get('screenshot_type') in trade_types]

# 按时间排序
trade.sort(key=lambda x:x.get('date',''))

# 标的统计
stock_cnt=collections.Counter((r.get('stock_code',''), r.get('stock_name','')) for r in trade if r.get('stock_name'))
# 类型统计
type_cnt=collections.Counter(r.get('screenshot_type','') for r in trade)

# 辅助：把 actions 列表转成可读字符串
def actions_summary(r):
    parts=[]
    for a in r.get('actions',[]):
        s=a.get('type','')
        if a.get('time'): s+=f" {a['time']}"
        if a.get('quantity') is not None: s+=f" 量{a['quantity']}"
        if a.get('price') is not None: s+=f" 价{a['price']}"
        if a.get('cost') is not None: s+=f" 成本{a['cost']}"
        if a.get('pnl_pct') is not None: s+=f" 盈亏{a['pnl_pct']*100:.2f}%"
        if a.get('position_pct') is not None: s+=f" 仓位{a['position_pct']*100:.1f}%"
        parts.append(s)
    return ' | '.join(parts) if parts else '—'

md=[]
md.append('# 多空博弈社交易动作蒸馏（基于东方财富截图）\n')
md.append(f"**更新时间**：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
md.append('**来源**：东方财富股吧用户「多空博弈社」（uid 7924037455558900）\n')
md.append('**覆盖范围**：从 299 篇帖子中识别出 56 篇带截图，共下载 69 张原图；经视觉 OCR 提取后，25 张与交易动作直接相关（持仓/成交/委托/买卖点/收益曲线）。\n\n')

md.append('## 一、数据处理方法\n')
md.append('1. 从东方财富 `UserPost/CenterList` 接口抓取全部 299 篇帖子，提取 `post_pic_url` 图片字段。\n')
md.append('2. 对 56 篇带图帖子的 69 张图片，使用 https + 原图 URL（去掉 `/size500/` 段）全量下载，总大小 28.7MB。\n')
md.append('3. 将 69 张图按时间分为 7 批，每批约 10 张，调用多模态子代理并行视觉 OCR，输出统一 schema 的 JSON。\n')
md.append('4. 合并 7 批结果，筛选 `screenshot_type` 为持仓/成交/委托/日K买卖点/分时卖点/收益曲线/账户资产的 25 条作为交易动作证据。\n')
md.append('5. 对同一标的、同日的多条记录进行交叉验证；对异常口径（如同花顺盈亏分析 551.33%）单独标注，不直接作为真实收益率使用。\n\n')

md.append('## 二、核心发现摘要\n')
md.append(f'- **交易相关截图**：{len(trade)} / 69 张（占比 {len(trade)*100//69}%）\n')
md.append(f'- **时间跨度**：{trade[0]["date"]} 至 {trade[-1]["date"]}\n')
md.append(f'- **涉及标的**：{len(stock_cnt)} 只（按出现次数排序）\n')
for (c,n),cnt in stock_cnt.most_common():
    md.append(f'  - {n} {c}：{cnt} 次\n')
md.append(f'- **截图类型分布**：{dict(type_cnt)}\n')
md.append('- **证据强度**：高 10 条、中 15 条（无低证据交易截图）\n\n')

md.append('## 三、可验证交易动作时间线\n')
md.append('| 日期 | 标的 | 类型 | App | 动作摘要 | 文字上下文一致性 | 证据 |\n')
md.append('|------|------|------|-----|----------|------------------|------|\n')
for r in trade:
    txt=(r.get('text_context') or '').replace('|','/').replace('\n',' ')[:60]
    consist='—'
    # 一致性判断
    stock=r.get('stock_name','')
    if stock and stock in txt: consist='一致'
    elif stock and txt and stock not in txt: consist='不一致/未提'
    md.append(f"| {r['date']} | {r.get('stock_name','')} {r.get('stock_code','')} | {r.get('screenshot_type','')} | {r.get('app','')} | {actions_summary(r)} | {consist} | {r.get('evidence_level','')} |\n")
md.append('\n')

md.append('## 四、持仓与仓位特征\n')
md.append('### 4.1 单票持仓案例\n')
md.append('- **2025-06-05 中毅达（600610）**：持仓 8,200 股，成本 9.336，现价 15.190，当日仅亏 11.28 元，累计浮盈约 **62.7%**。这说明其通过早期低成本建仓，在股价高位时仍能承受单日小跌。\n')
md.append('- **2025-08-14 *ST宇顺（002289）**：持仓约 2,200 股，成本 19.536，现价 24.990，浮盈 **+27.9%**（约 12,000 元）。\n')
md.append('- **2025-12-03 东方财富持仓页**：同时持有 4 只股票，**总仓位 84.7%**，显示高仓位运行。个股包括茂业商业、海欣食品、航天动力、*ST东易；数量被隐藏。\n')
md.append('- **2026-08-28 香江控股（600162）**：浮动盈亏 **+117.82%**，持股 56 天，**个股仓位 42.0%**。当日做 T：先卖 5.220，再买 5.150。\n\n')
md.append('### 4.2 仓位特征\n')
md.append('- **单票重仓**：香江控股在 2026-08-03 至 08-27 期间个股仓位 17.6%–40.1%，2026-08-28 达到 42.0%。\n')
md.append('- **总仓位高**：2025-12-03 持仓页显示总仓位 84.7%，接近满仓。\n')
md.append('- **高周转做 T**：同一只票在同一交易日内多次反向成交（如 2026-08-28 香江控股 09:31:58 卖出 5.220，09:32:41 买回 5.150）。\n\n')

md.append('## 五、成交与做T行为特征\n')
md.append('### 5.1 交易时段\n')
md.append('- 大量成交集中在 **09:30–10:30 早盘**（如香江控股 08-03、08-27、08-28 的买卖均在 09:31–09:52 完成）。\n')
md.append('- 存在尾盘或盘中做 T（如 2026-08-14 香江控股 13:47、14:31 买入）。\n')
md.append('### 5.2 做T与反向交易\n')
md.append('- **香江控股**：8-28 先卖后买（5.220→5.150），8-27 先买后卖（4.620→4.870），是典型的日内/隔日 T。\n')
md.append('- **中毅达**：日 K 截图上布满 B/S/T 买卖点标记，显示长期反复交易。\n')
md.append('- **津药药业（600488）**：买 5.67、卖 5.92，隔日短差。\n')
md.append('- **嘉美包装（002969）**：买 12.87、卖 12.74，该笔做 T 亏损（-1% 左右）。\n')
md.append('### 5.3 成交数量\n')
md.append('- 同花顺成交列表中**数量列普遍被 `**` 隐藏**，只能确认价格和时间。\n')
md.append('- 文字中自称：*ST东易 "7000 手已卖出"、"挂单的一万手被吞"，但截图未显示对应数量的成交明细。\n')
md.append('- 可见的明确数量：中毅达 8,200 股、*ST宇顺 2,200 股、凯美特气 500 股。\n\n')

md.append('## 六、收益与盈亏口径\n')
md.append('| 日期 | 标的/账户 | 口径 | 数值 | 说明 |\n')
md.append('|------|----------|------|------|------|\n')
md.append('| 2025-05-30 | 账户资产趋势 | 期末资产 | 255,136.39 元 | 从约 28 万冲高到 33 万后回撤，曲线含出入金，非单纯投资收益 |\n')
md.append('| 2025-07-31 | *ST宇顺 | 做T收益 | +1,296.60 元 | 4 次做T，成功率 100% |\n')
md.append('| 2025-08-14 | *ST宇顺 | 持仓浮盈 | +27.92% | 约 12,000 元 |\n')
md.append('| 2025-12-24 | 账户 | 月收益率 | +35.49% | 跑赢上证 34.44%，战胜 99.44% 股友 |\n')
md.append('| 2026-08-03 | 香江控股 | 浮动盈亏 | +55.87% | 持股 37 天，仓位 24.4% |\n')
md.append('| 2026-08-18 | 香江控股 | 浮动盈亏 | **+551.33%** | **异常口径**，疑似同花顺多次做T累计盈亏/投入本金的统计方式，非单次或真实总收益率 |\n')
md.append('| 2026-08-27 | 香江控股 | 浮动盈亏 | +97.66% | 仓位 40.1% |\n')
md.append('| 2026-08-28 | 香江控股 | 浮动盈亏 | +117.82% | 持股 56 天，仓位 42.0% |\n')
md.append('\n')

md.append('## 七、文字喊单 vs 截图交易的一致性\n')
md.append('| 维度 | 观察 | 案例 |\n')
md.append('|------|------|------|\n')
md.append('| 一致 | 文字提到的标的与截图持仓一致 | 中毅达 8,200 股、*ST东易逃顶、香江控股持仓 |\n')
md.append('| **不一致** | 文字说加仓 A，截图却是买入 B | **2025-09-08 帖子谈吉视传媒加仓，截图成交为凯美特气 500 股@17.30** |\n')
md.append('| 无法验证 | 文字说卖出数量巨大，截图无对应明细 | "7000 手已卖出"、"挂单一万手"只有分时卖点/委托截图，无成交数量 |\n')
md.append('| 选择性披露 | 只晒盈利/逃顶截图 | 2025-12-04 *ST东易逃顶、2025-12-24 月收益 35.49% 均属正向展示 |\n')
md.append('\n')

md.append('## 八、行为模式蒸馏\n')
md.append('基于 25 条交易截图证据，可提炼出以下可计算/可识别特征：\n\n')
md.append('1. **标的选择：偏爱 ST、低价、重组/保壳、题材高波动票**\n')
md.append('   - 高频标的：*ST东易、*ST宇顺、ST中迪、中毅达（监管/停牌博弈）、香江控股（重组/低价）。\n')
md.append('2. **持仓风格：单票重仓 + 高总仓位**\n')
md.append('   - 个股仓位可达 40%+，总仓位 80%+，属于激进型。\n')
md.append('3. **交易风格：高频做 T，早盘活跃**\n')
md.append('   - 同一标的日 K 上布满 B/S/T 标记；成交时间集中在 09:30–10:30。\n')
md.append('4. **晒单模式：正向选择性披露 + 挑衅式表达**\n')
md.append('   - 常见「逃顶小能手」「做T超神」「随便控盘」等自我标榜；亏损截图极少。\n')
md.append('5. **多账户/分仓迹象**\n')
md.append('   - 文字自称「十几个账号随便玩」；2026-08-03 同花顺成交列表显示不同证券账户片段。\n')
md.append('6. **收益口径风险**\n')
md.append('   - 平台盈亏分析页面的高百分比（如 551.33%）可能是累计做T收益/投入本金口径，不能直接等同于账户真实收益率。\n\n')

md.append('## 九、可作为项目开发依据的信号\n')
md.append('若要将此账号作为量化/监控信号源，建议关注以下可结构化字段：\n\n')
md.append('- **持仓暴露**：标的、数量、成本、现价、仓位比例、持股天数。\n')
md.append('- **成交动作**：方向（买/卖）、价格、时间、是否做 T（同一标的前后反向成交）。\n')
md.append('- **收益展示**：浮盈百分比、月收益率、做T成功率（需注意口径）。\n')
md.append('- **语言-行为一致性**：对比文字喊单方向与截图实际成交方向，识别「口嗨/反向操作」。\n')
md.append('- **晒单真实性评估**：数量是否被隐藏、是否与文字一致、是否只晒盈利。\n\n')

md.append('## 十、局限与风险提示\n')
md.append('1. **幸存者偏差**：用户只晒对自己有利的截图，亏损、割肉、踏空几乎不展示。\n')
md.append('2. **数量隐藏**：同花顺 App 默认隐藏数量/金额（显示 `**`），无法还原真实资金规模。\n')
md.append('3. **口径不一致**：文字「7000 手」「一万手」与截图无法一一对应。\n')
md.append('4. **异常收益**：2026-08-18 香江控股 +551.33% 属于平台统计口径，不能直接当作真实收益率。\n')
md.append('5. **样本偏少**：25 条交易截图相对 299 篇文字仍偏少，不能完整刻画其交易体系。\n')
md.append('6. **法律合规风险**：文字中「控盘」「十几个账号」「资金优势」等表述涉及操纵市场/账户出借风险，项目引用时建议仅作为行为识别样本，不作为投资建议或跟单依据。\n')

open(os.path.join(base,'docs/04_多空博弈社_交易动作蒸馏.md'),'w',encoding='utf-8').write(''.join(md))
print('saved docs/04_多空博弈社_交易动作蒸馏.md')

# 生成结构化 JSON
distilled={
    "meta": {
        "source": "东方财富 多空博弈社",
        "uid": "7924037455558900",
        "total_screenshots": 69,
        "trade_related": len(trade),
        "date_range": [trade[0]['date'], trade[-1]['date']],
        "generated_at": datetime.datetime.now().isoformat()
    },
    "summary": {
        "stock_distribution": [ {"code":c,"name":n,"count":cnt} for (c,n),cnt in stock_cnt.most_common() ],
        "type_distribution": dict(type_cnt),
        "evidence": {"高":10,"中":15}
    },
    "patterns": [
        {"pattern":"偏爱ST/低价/高波动题材","evidence":["*ST东易","*ST宇顺","中毅达","香江控股"]},
        {"pattern":"单票重仓+高总仓位","evidence":["香江控股仓位17.6%-42.0%","总仓位84.7%"]},
        {"pattern":"高频做T+早盘活跃","evidence":["香江控股8-27/8-28反向成交","中毅达K线BS标记"]},
        {"pattern":"选择性晒盈+自我标榜","evidence":["逃顶小能手","做T超神","月收益35.49%"]},
        {"pattern":"多账户/分仓迹象","evidence":["文字自称十几个账号","成交列表多账户片段"]},
    ],
    "inconsistencies": [
        {"date":"2025-09-08","issue":"文字说吉视传媒加仓，截图是凯美特气买入500股@17.30"},
        {"date":"2025-12-04","issue":"文字说7000手已卖出，截图只有分时卖点，无成交数量"},
        {"date":"2026-08-18","issue":"香江控股浮动盈亏+551.33%异常高，可能是平台统计口径"}
    ],
    "records": trade
}
open(os.path.join(base,'data/trade_actions_distilled.json'),'w',encoding='utf-8').write(json.dumps(distilled, ensure_ascii=False, indent=2))
print('saved data/trade_actions_distilled.json')
