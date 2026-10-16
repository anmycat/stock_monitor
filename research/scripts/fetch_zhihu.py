#!/usr/bin/env python3
"""抓取知乎用户(静水2008 / ban-ma-ban-ma-30-2)全部回答与文章正文。"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cdp import Tab, new_tab, close_tab  # noqa: E402

TOKEN = "ban-ma-ban-ma-30-2"
OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "raw")

ANSWER_INC = (
    "data[*].is_normal,admin_closed_comment,reward_info,is_collapsed,annotation_action,"
    "annotation_detail,collapse_reason,is_sticky,collapsed_by,suggest_edit,comment_count,"
    "can_comment,content,editable_content,attachment,voteup_count,reshipment_settings,"
    "comment_permission,created_time,updated_time,review_info,relevant_info,question,"
    "excerpt,is_labeled,paid_info,paid_info_content,relationship.is_authorized,"
    "is_author,voting,is_thanked,is_nothelp,is_recognized;data[*].mark_infos[*].url;"
    "data[*].author.follower_count,badge[*].topics"
)
ARTICLE_INC = (
    "data[*].comment_count,suggest_edit,is_normal,thumbnail_extra_info,thumbnail,"
    "description,updated_time,upvoted_count,voting,content,created_time,voteup_count,"
    "comment_permission,author.is_following,author.badge[*].topics"
)


def api(tab, path):
    js = "(async()=>{const r=await fetch('%s',{headers:{'Accept':'*/*'}});return await r.text()})()"
    for attempt in range(3):
        try:
            raw = tab.eval(js % path, timeout=60)
            return json.loads(raw)
        except Exception as e:
            print("   retry:", str(e)[:70], flush=True)
            time.sleep(3)
    return None


def paged(tab, kind, inc):
    items, offset, limit = [], 0, 20
    while True:
        path = (
            f"/api/v4/members/{TOKEN}/{kind}?include={inc}&offset={offset}"
            f"&limit={limit}&sort_by=created"
        )
        d = api(tab, path)
        if not d or "data" not in d:
            break
        data = d["data"]
        items.extend(data)
        print(f"  {kind} offset={offset} +{len(data)} (total {len(items)})", flush=True)
        if d.get("paging", {}).get("is_end") or not data:
            break
        offset += limit
        time.sleep(1.2)
    return items


def main():
    t = new_tab("https://www.zhihu.com/people/" + TOKEN)
    time.sleep(1)
    tab = Tab(t["webSocketDebuggerUrl"])
    tab.navigate("https://www.zhihu.com/people/" + TOKEN, wait=10)

    print("== 抓取回答 ==", flush=True)
    answers = paged(tab, "answers", ANSWER_INC)
    print("== 抓取文章 ==", flush=True)
    articles = paged(tab, "articles", ARTICLE_INC)
    print("== 抓取提问/想法 ==", flush=True)
    pins = paged(tab, "pins", "data[*].content,created_time,upvoted_count;data[*].author.follower_count")

    out = {"token": TOKEN, "name": "静水2008", "answers": answers, "articles": articles, "pins": pins}
    jp = os.path.join(OUT_DIR, "zhihu_jingshui2008.json")
    with open(jp, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)
    print(f"\n回答 {len(answers)} / 文章 {len(articles)} / 想法 {len(pins)} -> {jp}", flush=True)

    tab.close()
    close_tab(t["id"])


if __name__ == "__main__":
    main()
