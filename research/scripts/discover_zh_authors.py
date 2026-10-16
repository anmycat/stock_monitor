#!/usr/bin/env python3
"""知乎高赞作者发现：抓股票/投资类问题页 SSR，提取回答者（people链接+赞同数）。

链路：
  1. 问题 ID 池（本地静水回答过的 103 个股票类问题 + 经典热题）
  2. CDP 访问问题页，滚动加载回答，DOM 提取 作者people链接/名字/赞同数
  3. 聚合：按 总赞同、回答数、问题覆盖数 排序输出候选作者
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
OUT = os.path.join(BASE, "raw", "zh_author_discovery.json")

EXTRACT_JS = """
(() => {
  const out = [];
  document.querySelectorAll('.ContentItem').forEach(it => {
    const zop = it.getAttribute('data-zop');
    let authorName = '';
    try { authorName = JSON.parse(zop.replace(/&quot;/g, '"')).authorName || ''; } catch (e) {}
    let upvote = 0, comment = 0;
    try {
      const za = JSON.parse((it.getAttribute('data-za-extra-module') || '{}').replace(/&quot;/g, '"'));
      const c = za.card && za.card.content;
      if (c) { upvote = c.upvote_num || 0; comment = c.comment_num || 0; }
    } catch (e) {}
    const a = it.querySelector('a[href*="/people/"], a[href*="/org/"]');
    if (!a) return;
    const contentEl = it.querySelector('.RichContent-inner, .RichText');
    const excerpt = contentEl ? contentEl.textContent.trim().replace(/\\s+/g, ' ').slice(0, 100) : '';
    out.push({
      href: a.getAttribute('href'),
      name: authorName || a.textContent.trim(),
      vote: upvote,
      comment: comment,
      excerpt: excerpt,
    });
  });
  return out;
})()
"""


def load_questions(limit=40):
    """从本地静水知乎数据取股票类问题，按标题热度启发式排序"""
    d = json.load(open(os.path.join(BASE, "raw/zhihu_jingshui2008.json"), encoding="utf-8"))
    kw = ["股", "A股", "基金", "投资", "理财", "财报", "证券", "上市", "估值", "牛市", "熊市"]
    qs = {}
    for a in d["answers"]:
        q = a.get("question") or {}
        t = q.get("title") or ""
        if any(k in t for k in kw) and q.get("id"):
            qs[str(q["id"])] = t
    # 经典热题优先（年份老的问题通常回答多）
    def heat(t):
        if any(k in t for k in ["如何系统地学习", "如何看懂财报", "大家是怎么选股", "新手", "基本知识", "价值投资"]):
            return 2
        return 1
    items = sorted(qs.items(), key=lambda kv: -heat(kv[1]))
    return items[:limit]


def main():
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 40
    questions = load_questions(limit)
    print(f"目标问题 {len(questions)} 个", flush=True)

    t = new_tab("https://www.zhihu.com/")
    time.sleep(1.5)
    tab = Tab(t["webSocketDebuggerUrl"])

    authors = defaultdict(lambda: {
        "name": "", "token": "", "votes": [], "questions": set(), "excerpts": [],
    })
    ok_q = 0
    for i, (qid, title) in enumerate(questions):
        try:
            tab.navigate(f"https://www.zhihu.com/question/{qid}", wait=6)
            # 滚动触发懒加载
            for k in range(4):
                tab.eval("window.scrollTo(0, document.body.scrollHeight)", timeout=20)
                time.sleep(1.2)
            rows = tab.eval(EXTRACT_JS, timeout=30) or []
            if not rows:
                print(f"  [{i+1}/{len(questions)}] {qid} {title[:30]} → 0 条", flush=True)
                continue
            ok_q += 1
            for r in rows:
                m = re.search(r"/people/([a-z0-9\-]+)", r["href"] or "")
                if not m:
                    continue
                token = m.group(1)
                e = authors[token]
                e["name"] = r["name"] or e["name"]
                e["token"] = token
                e["questions"].add(qid)
                v = r.get("vote") or 0
                if v:
                    e["votes"].append(int(v))
                if r["excerpt"]:
                    e["excerpts"].append({"q": title[:40], "t": r["excerpt"]})
            print(f"  [{i+1}/{len(questions)}] {qid} {title[:28]:30} → {len(rows)} 回答", flush=True)
        except Exception as ex:
            print(f"  [{i+1}/{len(questions)}] {qid} ERR {str(ex)[:60]}", flush=True)
        time.sleep(1.0)

    close_tab(t["id"])

    # 汇总输出
    out = []
    for token, e in authors.items():
        total_votes = sum(e["votes"])
        out.append({
            "token": token,
            "name": e["name"],
            "url": f"https://www.zhihu.com/people/{token}",
            "answer_cnt": len(e["questions"]),
            "vote_sum": total_votes,
            "vote_max": max(e["votes"]) if e["votes"] else 0,
            "vote_avg": round(total_votes / len(e["votes"]), 1) if e["votes"] else 0,
            "question_titles": [t for t in e["questions"]],
            "excerpts": e["excerpts"][:3],
        })
    out.sort(key=lambda x: -(x["vote_sum"] + x["vote_max"] * 3 + x["answer_cnt"] * 500))
    result = {"questions_scanned": len(questions), "questions_ok": ok_q, "authors": out}
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=1)
    print(f"\n===== 发现作者 {len(out)} 人（有效问题 {ok_q}）→ {OUT} =====")
    for a in out[:30]:
        print(f"  总赞{a['vote_sum']:>7} 最高{a['vote_max']:>6} 答题{a['answer_cnt']:>2} | {a['name'][:20]:22} | {a['url']}")


if __name__ == "__main__":
    main()
