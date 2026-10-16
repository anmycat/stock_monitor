#!/usr/bin/env python3
"""分析东方财富 2025 年度榜单 19 位头部用户的 16767 条内容，产出画像并与多空博弈社交叉对比。

维度：
  1. 基础统计：条数、时间跨度、时段分布、均阅读/评论/点赞、带图率
  2. 内容主题：大盘/板块/个股/基金/方法论/晒单 分类
  3. 语言特征：多空词、挑衅词、感叹号密度、文本长度
  4. 标的集中度：$股票(SH/SZ...)$ 提及次数 Top
  5. 与多空博弈社（em_duokong.json）交叉对比
产出：data/em_top_users_analysis.json / .txt
"""
import json
import os
import re
from collections import Counter, defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOP_DIR = os.path.join(ROOT, "raw", "em_top_users")

# ---------- 词典 ----------
BULL_WORDS = ["看多", "牛市", "大涨", "涨停", "反弹", "突破", "加仓", "抄底", "买入", "上车",
              "主升", "龙头", "暴涨", "新高", "逼空", "低吸", "布局", "机会", "目标价"]
BEAR_WORDS = ["看空", "熊市", "大跌", "跌停", "回调", "破位", "减仓", "清仓", "卖出", "下车",
              "风险", "套牢", "割肉", "新低", "暴跌", "阴跌", "出货", "砸盘", "利空"]
PROVOKE_WORDS = ["韭菜", "散户", "傻", "蠢", "脑子", "醒醒", "别不信", "不信等着", "打脸", "杠精"]
PROFIT_WORDS = ["收益", "盈利", "浮盈", "落袋", "晒", "今天赚", "又赚", "满仓", "仓位", "做T", "做t"]
METHOD_WORDS = ["体系", "方法论", "逻辑", "规律", "纪律", "战法", "模式", "体系内", "研究", "估值",
                 "基本面", "技术面", "仓位管理", "止损", "止盈", "复盘"]
MARKET_WORDS = ["大盘", "上证", "指数", "沪指", "创业板", "科创", "两市", "成交额", "北向"]
SECTOR_WORDS = ["板块", "题材", "概念", "赛道", "行业", "主线", "轮动", "热点"]
FUND_WORDS = ["基金", "ETF", "etf", "净值", "定投", "债基", "混合"]

STOCK_RE = re.compile(r"\$([^$()（）]{1,12})\((?:SH|SZ|HK|BJ|sh|sz)[0-9]{6}\)")
TIME_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})")


def classify_time(hhmm):
    h = int(hhmm[:2])
    mi = int(hhmm[3:5])
    if h < 9:
        return "盘前(<09:00)"
    if h == 9 and mi <= 30:
        return "早盘集合(09:00-09:30)"
    if h < 11 or (h == 11 and mi <= 30):
        return "早盘(09:31-11:30)"
    if h < 15:
        return "午后(13:00-15:00)"
    if h < 18:
        return "盘后(15:01-18:00)"
    return "晚间(>18:00)"


def text_of(item):
    t = item.get("post_title") or ""
    c = item.get("post_content") or ""
    return (t + "\n" + c).strip()


def count_kw(text, kws):
    return sum(text.count(k) for k in kws)


def analyze_user(rec):
    items = []
    for t, lst in rec.get("items", {}).items():
        items.extend(lst)
    if not items:
        return None
    n = len(items)
    stats = {"name": rec["name"], "category": rec.get("category", ""), "count": n}

    # 时间分布
    times = []
    clicks, comments, likes, pics = [], [], [], 0
    texts = []
    stock_counter = Counter()
    for it in items:
        tm = it.get("post_publish_time") or ""
        m = TIME_RE.search(tm)
        if m:
            times.append(tm)
        clicks.append(it.get("post_click_count") or 0)
        comments.append(it.get("post_comment_count") or 0)
        likes.append(it.get("post_like_count") or 0)
        if it.get("post_pic_url"):
            pics += 1
        tx = text_of(it)
        texts.append(tx)
        for s in STOCK_RE.findall(tx):
            stock_counter[s.strip()] += 1

    dates = sorted({t[:10] for t in times})
    stats["date_from"], stats["date_to"] = (dates[0], dates[-1]) if dates else ("", "")
    stats["days_span"] = len(dates)
    stats["posts_per_active_day"] = round(n / max(len(dates), 1), 2)
    stats["avg_click"] = int(sum(clicks) / max(n, 1))
    stats["avg_comment"] = round(sum(comments) / max(n, 1), 2)
    stats["avg_like"] = round(sum(likes) / max(n, 1), 2)
    stats["pic_ratio"] = round(pics / max(n, 1) * 100, 1)

    tdist = Counter(classify_time(t[11:13] + ":" + t[14:16]) for t in times)
    stats["time_dist"] = dict(tdist)

    # 语言特征
    all_text = "\n".join(texts)
    stats["avg_len"] = round(len(all_text) / max(n, 1))
    stats["bull_kw"] = count_kw(all_text, BULL_WORDS)
    stats["bear_kw"] = count_kw(all_text, BEAR_WORDS)
    stats["provoke_kw"] = count_kw(all_text, PROVOKE_WORDS)
    stats["profit_kw"] = count_kw(all_text, PROFIT_WORDS)
    stats["method_kw"] = count_kw(all_text, METHOD_WORDS)
    stats["market_kw"] = count_kw(all_text, MARKET_WORDS)
    stats["sector_kw"] = count_kw(all_text, SECTOR_WORDS)
    stats["fund_kw"] = count_kw(all_text, FUND_WORDS)
    stats["exclaim_per_post"] = round(all_text.count("！") / max(n, 1), 2)
    stats["bull_ratio"] = round(stats["bull_kw"] / max(stats["bull_kw"] + stats["bear_kw"], 1) * 100)

    # 主题分类（按条）
    topic = Counter()
    for tx in texts:
        tags = []
        if count_kw(tx, MARKET_WORDS):
            tags.append("大盘")
        if count_kw(tx, SECTOR_WORDS):
            tags.append("板块题材")
        if STOCK_RE.search(tx):
            tags.append("个股")
        if count_kw(tx, FUND_WORDS):
            tags.append("基金")
        if count_kw(tx, METHOD_WORDS):
            tags.append("方法论")
        if count_kw(tx, PROFIT_WORDS) and re.search(r"(今天|昨日|上周|本月|今年).{0,12}(赚|收益|浮盈|落袋)", tx):
            tags.append("晒收益")
        if not tags:
            tags.append("其他")
        for tg in tags:
            topic[tg] += 1
    stats["topics"] = {k: round(v / n * 100, 1) for k, v in topic.most_common()}

    # 标的集中度
    stats["top_stocks"] = stock_counter.most_common(10)
    total_mentions = sum(stock_counter.values())
    stats["stock_mention_total"] = total_mentions
    stats["distinct_stocks"] = len(stock_counter)
    stats["top3_concentration"] = round(
        sum(c for _, c in stock_counter.most_common(3)) / max(total_mentions, 1) * 100, 1)

    # 最高阅读帖
    top_posts = sorted(items, key=lambda x: x.get("post_click_count") or 0, reverse=True)[:3]
    stats["top_posts"] = [
        {"title": (p.get("post_title") or "")[:60], "click": p.get("post_click_count"),
         "comment": p.get("post_comment_count"), "time": p.get("post_publish_time")}
        for p in top_posts
    ]
    return stats


def main():
    meta = json.load(open(os.path.join(TOP_DIR, "_all.json"), encoding="utf-8"))
    results = []
    for name in meta:
        path = os.path.join(TOP_DIR, f"{name}.json")
        if not os.path.exists(path):
            continue
        rec = json.load(open(path, encoding="utf-8"))
        s = analyze_user(rec)
        if s:
            results.append(s)

    results.sort(key=lambda x: -x["count"])

    out = {"users": results}
    with open(os.path.join(ROOT, "data", "em_top_users_analysis.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)

    lines = []
    P = lines.append
    P("=" * 100)
    P("东方财富 2025 年度榜单头部用户内容画像（19 人 / 16767 条）")
    P("=" * 100)
    for s in results:
        P(f"\n【{s['name']}】（{s['category']}）  {s['count']} 条  {s['date_from']}~{s['date_to']}（活跃 {s['days_span']} 天，日均 {s['posts_per_active_day']} 条）")
        P(f"  均阅读 {s['avg_click']} | 均评论 {s['avg_comment']} | 均赞 {s['avg_like']} | 带图率 {s['pic_ratio']}% | 均文本 {s['avg_len']} 字")
        P(f"  多空: 看多词 {s['bull_kw']} vs 看空词 {s['bear_kw']}（多占比 {s['bull_ratio']}%）| 挑衅词 {s['provoke_kw']} | 感叹号/条 {s['exclaim_per_post']}")
        P(f"  主题%: " + " ".join(f"{k} {v}" for k, v in s["topics"].items()))
        P(f"  提股 {s['stock_mention_total']} 次 / {s['distinct_stocks']} 只，Top3集中度 {s['top3_concentration']}%："
          + "、".join(f"{n}({c})" for n, c in s["top_stocks"][:5]))
        td = " ".join(f"{k}:{v}" for k, v in sorted(s["time_dist"].items(), key=lambda x: -x[1])[:4])
        P(f"  时段Top4: {td}")
        for p in s["top_posts"]:
            P(f"  热帖[{p['click']}阅/{p['comment']}评] {p['time'][:16]} 《{p['title']}》")

    txt = "\n".join(lines)
    with open(os.path.join(ROOT, "data", "em_top_users_analysis.txt"), "w", encoding="utf-8") as f:
        f.write(txt)
    print(txt)


if __name__ == "__main__":
    main()
