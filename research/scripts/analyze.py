#!/usr/bin/env python3
"""对语料做结构化统计：个股提及频次、方法论关键词、时间分布、代表性文本。"""
import collections
import json
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = os.path.join(ROOT, "raw")

STOCK_RE = re.compile(r"\$?([一-龥A-Z]{2,8})\((?:SH|SZ|BJ|HK)?(\d{6})\)|\$([一-龥]{2,8})\$")

KEYWORDS = {
    "业绩超预期": ["超预期", "业绩超预期", "超预期财报"],
    "净利润断层": ["断层", "净利润断层", "缺口"],
    "景气度": ["景气", "景气度", "产业趋势", "赛道"],
    "供需缺口": ["供需", "缺口", "涨价", "产能", "扩产"],
    "估值": ["估值", "PE", "pe", "PEG", "peg", "市盈率", "市净率"],
    "止损": ["止损", "割肉", "风控", "回撤", "仓位"],
    "买点卖点": ["买点", "卖点", "介入", "减仓", "清仓", "加仓"],
    "左侧右侧": ["左侧", "右侧"],
    "资金面": ["主力", "资金", "北向", "游资", "龙虎榜", "换手", "放量", "缩量"],
    "技术面": ["均线", "K线", "形态", "突破", "支撑", "压力", "趋势线", "MACD"],
    "仓位管理": ["仓位", "满仓", "空仓", "分仓", "金字塔"],
    "短线": ["短线", "隔夜", "打板", "龙头", "情绪", "连板", "接力"],
}

STOP = set(
    "我们 他们 自己 这个 那个 什么 怎么 因为 所以 但是 如果 可以 已经 就是 不是 一个 没有 很多 一些 时候 现在 之后 之前 这些 那些 觉得 知道 感觉 应该 或者 以及 对于 关于 而且 然后 还有 这样 那样 公司 市场 问题 情况 东西 事情 可能 需要 进行 通过 由于 其中 目前 同时 另外 因此 例如 比较 非常 特别 一定 主要 部分 方面 方式 不同 各种 出现 表示 认为 看到 说明 影响 提高 增加 下降 上涨 下跌".split()
)


def text_of(path):
    return open(path, encoding="utf-8").read() if os.path.exists(path) else ""


def stats(name, items, topn=25):
    """items: list of (date, text)"""
    print(f"\n{'='*60}\n【{name}】共 {len(items)} 篇\n{'='*60}")
    stocks = collections.Counter()
    kw = collections.Counter()
    years = collections.Counter()
    for date, txt in items:
        if date:
            years[date[:4]] += 1
        for m in STOCK_RE.finditer(txt):
            nm = m.group(1) or m.group(3)
            if nm:
                stocks[nm] += 1
        for cat, words in KEYWORDS.items():
            if any(w in txt for w in words):
                kw[cat] += 1
    print("— 年度分布：", dict(sorted(years.items())))
    print("\n— 个股提及 TOP%d：" % topn)
    for s, c in stocks.most_common(topn):
        print(f"   {s:<10} {c}")
    print("\n— 方法论关键词覆盖（篇数）：")
    for k, c in kw.most_common():
        print(f"   {k:<10} {c}/{len(items)}  ({c*100//max(len(items),1)}%)")


def main():
    # 雪球
    p = os.path.join(RAW, "xueqiu_jingshui2008.json")
    if os.path.exists(p):
        d = json.load(open(p, encoding="utf-8"))
        import datetime
        items = [
            (
                datetime.datetime.fromtimestamp(x["created_at"] / 1000).strftime("%Y-%m-%d"),
                x.get("full_text") or x.get("description") or "",
            )
            for x in d["posts"]
        ]
        stats("雪球 @静水2008", items, 30)

    # 知乎
    p = os.path.join(RAW, "zhihu_jingshui2008.json")
    if os.path.exists(p):
        d = json.load(open(p, encoding="utf-8"))
        import datetime
        ans = [
            (
                datetime.datetime.fromtimestamp(x["created_time"]).strftime("%Y-%m-%d"),
                ((x.get("question") or {}).get("title", "")) + "\n" + (x.get("content") or ""),
                x.get("voteup_count") or 0,
            )
            for x in d["answers"]
        ]
        stats("知乎 @静水2008 回答", [(a, b) for a, b, _ in ans], 30)
        print("\n— 高赞回答 TOP15：")
        for a, b, v in sorted(ans, key=lambda x: -x[2])[:15]:
            print(f"   {v:>6}赞 | {a} | {b.splitlines()[0][:44]}")

        art = [
            (
                datetime.datetime.fromtimestamp(x.get("created") or 0).strftime("%Y-%m-%d"),
                (x.get("title", "")) + "\n" + (x.get("content") or ""),
            )
            for x in d["articles"]
        ]
        stats("知乎 @静水2008 文章", art, 30)
        print("\n— 文章清单：")
        for a, b in art:
            print(f"   {a} | {b.splitlines()[0][:60]}")

    # 东财
    p = os.path.join(RAW, "em_duokong.json")
    if os.path.exists(p):
        d = json.load(open(p, encoding="utf-8"))
        seen, items = set(), []
        for _, lst in d["items"].items():
            for x in lst:
                pid = str(x.get("post_id"))
                if pid in seen:
                    continue
                seen.add(pid)
                t = x.get("post_publish_time") or ""
                body = (x.get("post_title") or "") + "\n" + (x.get("post_content") or "")
                items.append((t[:10], body))
        stats("东方财富 @多空博弈社", items, 30)


if __name__ == "__main__":
    main()
