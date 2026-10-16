#!/usr/bin/env python3
"""把 raw/ 下的 JSON 语料转为纯文本/Markdown 语料，便于后续蒸馏分析。"""
import datetime
import html
import json
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = os.path.join(ROOT, "raw")
OUT = os.path.join(ROOT, "corpus")
os.makedirs(OUT, exist_ok=True)


def clean(s):
    if not s:
        return ""
    s = re.sub(r"<br\s*/?>", "\n", s)
    s = re.sub(r"</p>", "\n", s)
    s = re.sub(r"<[^>]+>", "", s)
    s = html.unescape(s)
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def ts(ms):
    try:
        return datetime.datetime.fromtimestamp(int(ms)).strftime("%Y-%m-%d")
    except Exception:
        return ""


def build_xueqiu():
    p = os.path.join(RAW, "xueqiu_jingshui2008.json")
    if not os.path.exists(p):
        return ""
    d = json.load(open(p, encoding="utf-8"))
    lines = ["# 雪球 @静水2008 全部帖子（%d 篇）\n" % len(d["posts"])]
    for s in sorted(d["posts"], key=lambda x: x["created_at"]):
        t = ts(s["created_at"] / 1000)
        lines.append(f"\n## {t}｜{s.get('title') or ''}\n")
        lines.append(f"- 链接：https://xueqiu.com/{s['user_id']}/{s['id']}")
        lines.append(f"- 转发{s.get('retweet_count')} 评论{s.get('reply_count', 0)} 赞{s.get('fav_count')}\n")
        body = s.get("full_text") or s.get("description") or ""
        lines.append(clean(body))
        lines.append("")
    return "\n".join(lines)


def build_zhihu():
    p = os.path.join(RAW, "zhihu_jingshui2008.json")
    if not os.path.exists(p):
        return "", ""
    d = json.load(open(p, encoding="utf-8"))
    ans = sorted(d["answers"], key=lambda x: x["created_time"])
    art = sorted(d["articles"], key=lambda x: x.get("created") or x.get("updated") or 0)

    la = ["# 知乎 @静水2008 全部回答（%d 篇）\n" % len(ans)]
    for a in ans:
        q = (a.get("question") or {}).get("title", "")
        la.append(f"\n## {ts(a['created_time'])}｜问题：{q}\n")
        la.append(f"- 赞同 {a.get('voteup_count')}｜评论 {a.get('comment_count')}")
        la.append(f"- 链接：https://www.zhihu.com/question/{(a.get('question') or {}).get('id','')}/answer/{a.get('id','')}\n")
        la.append(clean(a.get("content")))
        la.append("")

    lt = ["# 知乎 @静水2008 全部文章（%d 篇）\n" % len(art)]
    for a in art:
        lt.append(f"\n## {ts(a.get('created') or a.get('updated') or 0)}｜{a.get('title','')}\n")
        lt.append(f"- 赞同 {a.get('voteup_count')}｜评论 {a.get('comment_count')}")
        lt.append(f"- 链接：https://zhuanlan.zhihu.com/p/{a.get('id','')}\n")
        lt.append(clean(a.get("content")))
        lt.append("")
    return "\n".join(la), "\n".join(lt)


def build_em():
    p = os.path.join(RAW, "em_duokong.json")
    if not os.path.exists(p):
        return ""
    d = json.load(open(p, encoding="utf-8"))
    lines = ["# 东方财富 @多空博弈社 全部内容\n"]
    for key, items in d["items"].items():
        lines.append(f"\n## 分类：{key}（{len(items)} 条）\n")
        for it in items:
            t = it.get("post_publish_time") or it.get("publish_time") or ""
            title = it.get("post_title") or ""
            body = it.get("post_content") or it.get("post_content_text") or ""
            stocks = it.get("post_stock") or []
            sn = ",".join(
                (s.get("stock_name") or s.get("name") or "")
                for s in stocks
                if isinstance(s, dict)
            )
            lines.append(f"\n### {t}｜{title}\n")
            if sn:
                lines.append(f"- 关联股票：{sn}")
            lines.append(f"- 阅读 {it.get('post_click_count')}｜评论 {it.get('post_comment_count')}")
            lines.append("")
            lines.append(clean(body))
            lines.append("")
    return "\n".join(lines)


def main():
    x = build_xueqiu()
    if x:
        open(os.path.join(OUT, "01_雪球_静水2008.md"), "w", encoding="utf-8").write(x)
        print("雪球:", len(x), "字符")
    a, t = build_zhihu()
    if a:
        open(os.path.join(OUT, "02_知乎_静水2008_回答.md"), "w", encoding="utf-8").write(a)
        open(os.path.join(OUT, "03_知乎_静水2008_文章.md"), "w", encoding="utf-8").write(t)
        print("知乎回答:", len(a), "字符；文章:", len(t), "字符")
    e = build_em()
    if e:
        open(os.path.join(OUT, "04_东方财富_多空博弈社.md"), "w", encoding="utf-8").write(e)
        print("东方财富:", len(e), "字符")


if __name__ == "__main__":
    main()
