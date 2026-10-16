#!/usr/bin/env python3
"""雪球高赞作者发现 v2：热门股票页 SSR → 讨论区作者卡片（含粉丝数/讨论数）。

路线：CDP 访问热门股票页 → DOM 提取 /u/<uid> 卡片（文本含"XXX粉丝、N条相关讨论"）
     → 多股票聚合 → 按粉丝规模与讨论活跃度输出候选作者。
"""
import json
import os
import re
import sys
import time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cdp import Tab, new_tab, close_tab  # noqa: E402

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(BASE, "raw", "xq_author_discovery.json")

# 热门活跃股（覆盖 价值白马/赛道/券商/题材，多风格讨论区）
STOCKS = [
    ("SH600519", "贵州茅台"), ("SZ300750", "宁德时代"), ("SZ002594", "比亚迪"),
    ("SH601318", "中国平安"), ("SZ000858", "五粮液"), ("SH600036", "招商银行"),
    ("SZ300059", "东方财富"), ("SH601899", "紫金矿业"), ("SH600030", "中信证券"),
    ("SZ002460", "赣锋锂业"), ("SH688498", "源杰科技"), ("SZ002131", "利欧股份"),
    ("SH600276", "恒瑞医药"), ("SZ000063", "中兴通讯"), ("SH688981", "中芯国际"),
    ("SH601127", "赛力斯"), ("SZ300308", "中际旭创"), ("SH603259", "药明康德"),
]

EXTRACT_JS = """
(() => {
  const out = [];
  document.querySelectorAll('a[href*="/u/"]').forEach(a => {
    const t = a.textContent.replace(/\\s+/g, ' ').trim();
    const h = a.getAttribute('href') || '';
    const m = h.match(/\\/u\\/(\\d+)/);
    if (m && t) out.push({uid: m[1], text: t.slice(0, 80)});
  });
  return out;
})()
"""

CARD_RE = re.compile(r"^(.*?)(\d+)粉丝、(\d+)条相关讨论")


def main():
    t = new_tab("https://xueqiu.com/S/SH600519")
    time.sleep(1.5)
    tab = Tab(t["webSocketDebuggerUrl"])

    authors = defaultdict(lambda: {"name": "", "followers": 0, "discuss": 0, "stocks": set()})
    for i, (code, name) in enumerate(STOCKS):
        try:
            tab.navigate(f"https://xueqiu.com/S/{code}", wait=7)
            rows = tab.eval(EXTRACT_JS, timeout=30) or []
            got = 0
            for r in rows:
                m = CARD_RE.match(r["text"])
                if not m:
                    continue
                uid = r["uid"]
                e = authors[uid]
                e["name"] = m.group(1).strip() or e["name"]
                e["followers"] = max(e["followers"], int(m.group(2)))
                e["discuss"] = max(e["discuss"], int(m.group(3)))
                e["stocks"].add(code)
                got += 1
            print(f"  [{i+1}/{len(STOCKS)}] {name}({code}) → {got} 作者卡片", flush=True)
        except Exception as ex:
            print(f"  [{i+1}/{len(STOCKS)}] {code} ERR {str(ex)[:60]}", flush=True)
        time.sleep(0.8)

    close_tab(t["id"])

    out = []
    for uid, e in authors.items():
        if e["followers"] < 800:  # 过滤小号
            continue
        out.append({
            "uid": uid,
            "name": e["name"],
            "url": f"https://xueqiu.com/u/{uid}",
            "followers": e["followers"],
            "discuss_max": e["discuss"],
            "stock_cnt": len(e["stocks"]),
            "stocks": sorted(e["stocks"]),
        })
    out.sort(key=lambda x: -(x["followers"] + x["discuss_max"] * 50 + x["stock_cnt"] * 2000))
    result = {"stocks_scanned": len(STOCKS), "authors": out}
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=1)
    print(f"\n===== 合格作者 {len(out)} 人 → {OUT} =====")
    for a in out[:30]:
        print(f"  粉{a['followers']:>7} 讨论{a['discuss_max']:>4} 覆盖{a['stock_cnt']:>2}只 | {a['name'][:20]:22} | {a['url']}")


if __name__ == "__main__":
    main()
