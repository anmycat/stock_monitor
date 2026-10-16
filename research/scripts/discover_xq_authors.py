#!/usr/bin/env python3
"""雪球高赞作者发现：从大V（静水2008）帖子评论区提取活跃用户，再用其时间线验证质量。

链路：
  1. 抓静水2008 最近原创帖（user_timeline type=11/0）
  2. 抓每条帖的评论（comments.json 可用）→ 聚合评论者
  3. 对候选评论者抓 user_timeline 首页 → 拿 user.followers_count/status_count
  4. 筛选：粉丝数≥1000 且发帖≥50 的候选，按 粉丝×ln(赞) 排序输出
"""
import json
import math
import os
import sys
import time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cdp import Tab, new_tab, close_tab  # noqa: E402

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SEED_UID = "5661156172"  # 静水2008
OUT = os.path.join(BASE, "raw", "xq_author_discovery.json")

JS_FETCH = """
(async (args) => {
  const [url] = args;
  const r = await fetch(url, {headers: {'Accept':'application/json','X-Requested-With':'XMLHttpRequest'}, credentials:'include'});
  return await r.text();
})
"""


def fetch_json(tab, url, retries=2):
    for _ in range(retries + 1):
        try:
            raw = tab.eval(
                "(%s)(%s)" % (JS_FETCH.strip(), json.dumps([url], ensure_ascii=False)),
                timeout=60,
            )
            return json.loads(raw)
        except Exception as e:
            print("    retry:", str(e)[:60], flush=True)
            time.sleep(2)
    return None


def main():
    t = new_tab(f"https://xueqiu.com/u/{SEED_UID}")
    time.sleep(1.5)
    tab = Tab(t["webSocketDebuggerUrl"])
    tab.navigate(f"https://xueqiu.com/u/{SEED_UID}", wait=10)

    # 1. 种子用户最近帖子
    posts = {}
    for ptype in ("11", "0"):
        for page in range(1, 11):
            d = fetch_json(tab, f"/v4/statuses/user_timeline.json?user_id={SEED_UID}&page={page}&type={ptype}")
            if not d or not d.get("statuses"):
                break
            for s in d["statuses"]:
                posts[s["id"]] = {
                    "id": s["id"], "title": (s.get("title") or "")[:60],
                    "created_at": s.get("created_at"),
                    "fav_count": s.get("fav_count") or 0,
                    "reply_count": s.get("reply_count") or 0,
                    "retweet_count": s.get("retweet_count") or 0,
                }
            print(f"  timeline type={ptype} page={page}: total {len(posts)}", flush=True)
            if len(d["statuses"]) < 10:
                break
            time.sleep(0.8)

    print(f"\n种子帖子 {len(posts)} 条，开始抓评论...", flush=True)

    # 2. 评论者聚合（优先抓高互动帖）
    commenters = defaultdict(lambda: {"name": "", "cnt": 0, "posts_touched": set(), "as_author": 0})
    sorted_posts = sorted(posts.values(), key=lambda x: -(x["fav_count"] + x["reply_count"]))
    target_posts = sorted_posts[:80]
    for i, p in enumerate(target_posts):
        for page in range(1, 3):
            d = fetch_json(tab, f"/statuses/comments.json?id={p['id']}&count=20&page={page}")
            if not d or not d.get("comments"):
                break
            for c in d["comments"]:
                uid = str(c.get("user_id"))
                e = commenters[uid]
                e["name"] = c.get("screen_name") or e["name"]
                e["cnt"] += 1
                e["posts_touched"].add(p["id"])
            if len(d["comments"]) < 20:
                break
            time.sleep(0.5)
        if (i + 1) % 20 == 0:
            print(f"  进度 {i+1}/{len(target_posts)}，候选 {len(commenters)} 人", flush=True)
        time.sleep(0.3)

    print(f"\n候选评论者 {len(commenters)} 人，开始验证质量...", flush=True)

    # 3. 候选验证：抓 user_timeline 首页拿 user 信息
    # 先按出现频率粗排，取 Top 120 深验
    rough = sorted(commenters.items(), key=lambda kv: -len(kv[1]["posts_touched"]))[:120]
    authors = []
    for j, (uid, e) in enumerate(rough):
        d = fetch_json(tab, f"/v4/statuses/user_timeline.json?user_id={uid}&page=1&type=11")
        u = None
        favs, cnt = [], 0
        if d and d.get("statuses"):
            st = d["statuses"]
            u = st[0].get("user") or {}
            favs = [s.get("fav_count") or 0 for s in st]
            cnt = len(st)
        if not u:
            continue
        followers = u.get("followers_count") or 0
        statuses = u.get("status_count") or 0
        if followers < 500 or statuses < 30:
            continue
        avg_fav = sum(favs) / len(favs) if favs else 0
        authors.append({
            "uid": uid,
            "screen_name": u.get("screen_name") or e["name"],
            "followers": followers,
            "status_count": statuses,
            "friends_count": u.get("friends_count") or 0,
            "description": (u.get("description") or "")[:120],
            "verified": u.get("verified") or False,
            "verified_type": u.get("verified_type"),
            "avg_fav_sample": round(avg_fav, 1),
            "max_fav_sample": max(favs) if favs else 0,
            "comment_cnt_on_seed": e["cnt"],
            "seed_posts_touched": len(e["posts_touched"]),
        })
        if (j + 1) % 20 == 0:
            print(f"  验证进度 {j+1}/{len(rough)}，合格 {len(authors)}", flush=True)
        time.sleep(0.4)

    # 4. 综合评分：粉丝规模 × 内容质量 × 与种子相关性
    for a in authors:
        a["score"] = round(
            math.log10(max(a["followers"], 1)) * 2
            + math.log10(max(a["avg_fav_sample"], 1) + 1) * 1.5
            + math.log10(max(a["max_fav_sample"], 1) + 1) * 1.5
            + min(a["seed_posts_touched"], 10) * 0.3,
            2,
        )
    authors.sort(key=lambda x: -x["score"])

    result = {
        "seed_uid": SEED_UID,
        "seed_posts": len(posts),
        "commenter_pool": len(commenters),
        "authors": authors,
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=1)
    close_tab(t["id"])

    print(f"\n===== 合格作者 {len(authors)} 人 → {OUT} =====")
    for a in authors[:30]:
        print(f"  {a['score']:>6} | 粉{a['followers']:>7} 帖{a['status_count']:>6} 均赞{a['avg_fav_sample']:>7} | "
              f"{a['screen_name'][:18]:20} | {a['description'][:40]}")


if __name__ == "__main__":
    main()
