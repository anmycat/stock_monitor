#!/usr/bin/env python3
"""新发现高赞作者画像分析（雪球+知乎）。

复用东财头部用户的分析维度：时段分布、多空倾向、提股集中度、主题分布、
方法论关键词密度、带图率；新增知乎维度：高赞回答主题、赞同分布。
输出 data/new_authors_analysis.json/.txt
"""
import json
import os
import re
import sys
from collections import Counter, defaultdict

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(BASE, "raw", "new_authors", "new_authors_content.json")

# 复用东财分析的关键词体系
BULL_KW = ["看多", "牛市", "上涨", "大涨", "涨停", "反弹", "突破", "加仓", "抄底", "买入", "布局", "机会", "看好", "上涨空间", "主升", "龙头", "赚钱", "盈利", "翻倍", "降息", "利好"]
BEAR_KW = ["看空", "熊市", "下跌", "大跌", "跌停", "回调", "破位", "减仓", "卖出", "风险", "泡沫", "割肉", "套牢", "利空", "崩盘", "暴雷", "退市", "亏损", "爆仓"]
THEMES = {
    "大盘策略": ["大盘", "指数", "上证", "深证", "创业板", "科创", "沪深", "板块", "轮动", "风格"],
    "个股研究": ["个股", "持股", "持仓", "建仓", "加仓", "减仓", "换股"],
    "方法论": ["方法", "体系", "框架", "纪律", "策略", "模型", "指标", "均线", "K线", "缠论", "波浪", "背离"],
    "基本面": ["财报", "业绩", "营收", "利润", "估值", "PE", "PB", "ROE", "现金流", "分红", "股息"],
    "资金情绪": ["资金", "主力", "北向", "游资", "筹码", "情绪", "量能", "成交", "放量", "缩量"],
    "宏观": ["宏观", "政策", "利率", "汇率", "通胀", "GDP", "央行", "美联储", "关税", "财政"],
    "风险教训": ["风险", "亏损", "教训", "爆仓", "杠杆", "配资", "骗局", "杀猪盘", "退市"],
}
STOCK_RE = re.compile(r"([\u4e00-\u9fa5]{2,4})(?:\(|（)?(?:SH|SZ|BJ)?\d{6}|([\u4e00-\u9fa5]{2,4})")
METHOD_KW = ["方法", "体系", "纪律", "策略", "模型", "指标", "均线", "K线", "缠论", "波浪", "背离", "估值", "财报",
             "止损", "止盈", "仓位", "复盘", "逻辑", "战法", "模式", "共振", "周期", "情绪", "龙头", "打板",
             "低吸", "回调", "突破", "分歧", "一致", "补涨", "切换", "轮动", "主线", "题材"]


def zh_time(ts):
    import datetime
    if not ts:
        return ""
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def classify_hour(hhmm):
    """hhmm: 'HH:MM' 或 'HH'"""
    parts = hhmm.split(":")
    h = int(parts[0])
    minute = int(parts[1]) if len(parts) > 1 else 0
    if h < 9:
        return "盘前(<09:00)"
    if h == 9 and minute <= 30:
        return "早盘集合(09:00-09:30)"
    if h < 11 or (h == 11 and minute <= 30):
        return "早盘(09:31-11:30)"
    if h < 15:
        return "午后(13:00-15:00)"
    if h < 18:
        return "盘后(15:01-18:00)"
    return "晚间(>18:00)"


def analyze_author(rec):
    plat = rec["platform"]
    out = {
        "platform": plat, "name": rec["name"],
        "followers": rec.get("followers") or rec.get("fans"),
        "status_count": rec.get("status_count"),
        "description": (rec.get("description") or "")[:100],
    }
    texts = []
    votes = []
    times = []
    if plat == "xueqiu":
        for p in rec["posts"]:
            t = (p.get("full_text") or p.get("text") or "")
            if p.get("title"):
                t = p["title"] + " " + t
            texts.append(t)
            votes.append(p.get("fav_count") or 0)
            ca = p.get("created_at")
            if ca:
                import datetime
                times.append(datetime.datetime.fromtimestamp(ca / 1000).strftime("%H:%M"))
    else:
        for a in rec["answers"]:
            t = a.get("full_text") or a.get("excerpt") or ""
            texts.append(a["title"] + " " + t)
            votes.append(a.get("upvote") or 0)

    joined = " ".join(texts)
    out["count"] = len(texts)
    out["avg_fav"] = round(sum(votes) / len(votes), 1) if votes else 0
    out["max_fav"] = max(votes) if votes else 0
    out["total_fav"] = sum(votes)

    bull = sum(joined.count(k) for k in BULL_KW)
    bear = sum(joined.count(k) for k in BEAR_KW)
    out["bull_kw"] = bull
    out["bear_kw"] = bear
    out["bull_ratio"] = round(bull / (bull + bear), 2) if (bull + bear) else None

    themes = {}
    for th, kws in THEMES.items():
        c = sum(joined.count(k) for k in kws)
        themes[th] = c
    out["themes"] = themes
    out["top_theme"] = max(themes, key=themes.get) if any(themes.values()) else "-"

    m_cnt = sum(joined.count(k) for k in METHOD_KW)
    out["method_density"] = round(m_cnt / max(len(texts), 1), 2)
    out["method_kw"] = m_cnt

    # 提股
    stocks = Counter()
    for m in re.finditer(r"([\u4e00-\u9fa5]{2,4})(?:\(|（)?(?:SH|SZ|BJ)?(\d{6})", joined):
        stocks[m.group(1)] += 1
    for m in re.finditer(r"(?:SH|SZ|BJ)(\d{6})", joined):
        stocks[m.group(0)] += 1
    top_stocks = stocks.most_common(8)
    out["top_stocks"] = top_stocks
    out["distinct_stocks"] = len(stocks)
    top3 = sum(n for _, n in top_stocks[:3])
    total_s = sum(stocks.values()) or 1
    out["top3_concentration"] = round(top3 / total_s, 2)

    # 时段（雪球有 created_at；知乎无发布时间戳，跳过）
    if times:
        dist = Counter(classify_hour(t) for t in times)
        tot = sum(dist.values()) or 1
        out["time_dist"] = {k: round(v / tot * 100, 1) for k, v in dist.most_common()}
        out["top_period"] = dist.most_common(1)[0][0]
    else:
        out["time_dist"] = {}
        out["top_period"] = "-"

    out["avg_len"] = round(sum(len(t) for t in texts) / max(len(texts), 1))
    return out


def main():
    if not os.path.exists(SRC):
        print("未找到", SRC)
        return
    data = json.load(open(SRC, encoding="utf-8"))
    users = []
    for rec in data.get("xueqiu", []):
        users.append(analyze_author(rec))
    for rec in data.get("zhihu", []):
        users.append(analyze_author(rec))

    # 分组排序
    users.sort(key=lambda u: -(u.get("total_fav") or 0))
    out = {"users": users}

    with open(os.path.join(BASE, "data", "new_authors_analysis.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)

    lines = []
    P = lines.append
    P(f"新作者画像分析：{len(users)} 人")
    P(f"  雪球 {sum(1 for u in users if u['platform']=='xueqiu')} 人 / 知乎 {sum(1 for u in users if u['platform']=='zhihu')} 人")
    P("")
    for u in users:
        P(f"【{u['platform']}】{u['name']}  粉{u['followers']}  内容{u['count']}条 均赞{u['avg_fav']} 最高{u['max_fav']}")
        P(f"  多空: 看多{u['bull_kw']} 看空{u['bear_kw']} (多占比{u['bull_ratio']}) | 主题: {u['top_theme']} | 方法论密度 {u['method_density']}")
        P(f"  提股: {u['distinct_stocks']}只 Top3集中{u['top3_concentration']*100:.0f}% | 高频: {', '.join(s for s,_ in u['top_stocks'][:5])}")
        if u["top_period"] != "-":
            P(f"  时段: {u['top_period']} | 均长 {u['avg_len']} 字")
        P("")
    txt = "\n".join(lines)
    with open(os.path.join(BASE, "data", "new_authors_analysis.txt"), "w", encoding="utf-8") as f:
        f.write(txt)
    print(txt)


if __name__ == "__main__":
    main()
