#!/usr/bin/env python3
"""抓取新发现的雪球/知乎高赞作者内容。

雪球：user_timeline（type=11/0，最新约7条/型）+ /statuses/show.json 全文 + user 权威粉丝数
知乎：/people/<token>/answers（40条元数据：问题标题+赞数+问题ID）+ Top 高赞回答的问题页全文

用法：python3 fetch_new_authors.py <xq_top_n> <zh_top_n>
"""
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cdp import Tab, new_tab, close_tab  # noqa: E402

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(BASE, "raw", "new_authors")
os.makedirs(OUT_DIR, exist_ok=True)

JS_FETCH = """
(async (args) => {
  const [url] = args;
  const r = await fetch(url, {headers: {'Accept':'application/json','X-Requested-With':'XMLHttpRequest'}, credentials:'include'});
  return await r.text();
})
"""

# 知乎作者页提取：回答卡片元数据
ZH_ANSWERS_JS = """
(() => {
  const out = [];
  document.querySelectorAll('.ContentItem').forEach(it => {
    const zop = it.getAttribute('data-zop');
    let title = '', authorName = '';
    try { const z = JSON.parse(zop.replace(/&quot;/g, '"')); title = z.title || ''; authorName = z.authorName || ''; } catch (e) {}
    let upvote = 0, comment = 0, qid = '', aid = '';
    try {
      const za = JSON.parse((it.getAttribute('data-za-extra-module') || '{}').replace(/&quot;/g, '"'));
      const c = za.card && za.card.content;
      if (c) { upvote = c.upvote_num || 0; comment = c.comment_num || 0; qid = c.parent_token || ''; aid = c.token || ''; }
    } catch (e) {}
    const contentEl = it.querySelector('.RichContent-inner, .RichText');
    const excerpt = contentEl ? contentEl.textContent.trim().replace(/\\s+/g, ' ').slice(0, 200) : '';
    if (qid) out.push({qid: qid, aid: aid, title: title, upvote: upvote, comment: comment, excerpt: excerpt});
  });
  return out;
})()
"""

# 知乎问题页提取：指定回答的全文（按 data-zop itemId 匹配）
ZH_FULLTEXT_JS = """
(() => {
  const out = [];
  document.querySelectorAll('.ContentItem').forEach(it => {
    const zop = it.getAttribute('data-zop');
    let itemId = '', title = '';
    try { const z = JSON.parse(zop.replace(/&quot;/g, '"')); itemId = z.itemId || ''; title = z.title || ''; } catch (e) {}
    let upvote = 0;
    try {
      const za = JSON.parse((it.getAttribute('data-za-extra-module') || '{}').replace(/&quot;/g, '"'));
      const c = za.card && za.card.content;
      if (c) upvote = c.upvote_num || 0;
    } catch (e) {}
    const contentEl = it.querySelector('.RichContent-inner, .RichText');
    const text = contentEl ? contentEl.textContent.trim().replace(/\\s+/g, ' ') : '';
    if (text.length > 50) out.push({aid: itemId, title: title, upvote: upvote, text: text.slice(0, 4000)});
  });
  return out;
})()
"""


def fetch_json(tab, url, retries=2):
    for _ in range(retries + 1):
        try:
            raw = tab.eval("(%s)(%s)" % (JS_FETCH.strip(), json.dumps([url], ensure_ascii=False)), timeout=60)
            return json.loads(raw)
        except Exception as e:
            print("    retry:", str(e)[:60], flush=True)
            time.sleep(2)
    return None


def fetch_xq_author(tab, uid, name):
    """雪球：时间线 + 全文"""
    posts = {}
    for ptype in ("11", "0"):
        d = fetch_json(tab, f"/v4/statuses/user_timeline.json?user_id={uid}&page=1&type={ptype}")
        if not d or not d.get("statuses"):
            continue
        for s in d["statuses"]:
            posts[str(s["id"])] = s
        time.sleep(0.6)
    if not posts:
        return None
    user = None
    for s in posts.values():
        if s.get("user"):
            user = s["user"]
            break
    # 全文
    items = []
    for i, (sid, s) in enumerate(posts.items()):
        rec = {
            "id": sid,
            "title": (s.get("title") or s.get("_title") or "")[:120],
            "created_at": s.get("created_at"),
            "fav_count": s.get("fav_count") or 0,
            "reply_count": s.get("reply_count") or 0,
            "retweet_count": s.get("retweet_count") or 0,
            "type": s.get("type"),
            "text": (s.get("description") or s.get("text") or "")[:600],
        }
        if s.get("type") in ("3", "2") or s.get("title"):
            d = fetch_json(tab, f"/statuses/show.json?id={sid}")
            if d and d.get("text"):
                rec["full_text"] = d["text"][:6000]
                rec["title"] = (d.get("title") or rec["title"])[:120]
            time.sleep(0.6)
        items.append(rec)
    return {
        "platform": "xueqiu", "uid": uid, "name": name,
        "followers": (user or {}).get("followers_count"),
        "status_count": (user or {}).get("status_count"),
        "description": (user or {}).get("description") or "",
        "posts": items,
    }


def fetch_zh_author(tab, token, name):
    """知乎：回答列表 + Top 高赞全文（回答独立页 /question/x/answer/y 保全文）"""
    tab.navigate(f"https://www.zhihu.com/people/{token}/answers", wait=8)
    time.sleep(1.5)
    url = tab.eval("location.href", timeout=15) or ""
    if "/answers" not in url:
        print(f"    账号不可用（重定向到 {url[:60]}）", flush=True)
        return None
    rows = tab.eval(ZH_ANSWERS_JS, timeout=40) or []
    if not rows:
        return None
    # 用户信息从页面抓
    prof = tab.eval("(()=>{const n=document.querySelector('.ProfileHeader-name');const d=document.querySelector('.ProfileHeader-infoValue, .ProfileHeader-headline');const f=document.querySelector('[class*=FollowButton] .NumberBoard-itemValue, .FollowButton .NumberBoard-itemValue');return JSON.stringify({name:n?n.textContent.trim():'',desc:d?d.textContent.trim().slice(0,150):'',fans:f?f.textContent.trim():''})})()", timeout=20) or "{}"
    try:
        prof = json.loads(prof)
    except Exception:
        prof = {}

    answers = sorted(rows, key=lambda x: -x["upvote"])
    items = []
    for r in answers[:40]:
        items.append({
            "qid": r["qid"], "aid": r["aid"], "title": r["title"],
            "upvote": r["upvote"], "comment": r["comment"],
            "excerpt": r["excerpt"],
        })
    # Top 高赞抓全文（回答独立页）
    top = [a for a in answers if a["upvote"] >= 20][:6] or answers[:3]
    for a in top:
        try:
            tab.navigate(f"https://www.zhihu.com/question/{a['qid']}/answer/{a['aid']}", wait=6)
            time.sleep(1.2)
            fulls = tab.eval(ZH_FULLTEXT_JS, timeout=30) or []
            for f in fulls:
                if f["aid"] == a["aid"]:
                    for it in items:
                        if it["aid"] == a["aid"]:
                            it["full_text"] = f["text"]
                    break
        except Exception as e:
            print(f"    fulltext ERR {a['qid']}: {str(e)[:50]}", flush=True)
        time.sleep(1.5)
    return {
        "platform": "zhihu", "token": token, "name": prof.get("name") or name,
        "fans": prof.get("fans"), "description": prof.get("desc"),
        "answers": items,
    }


def main():
    xq_n = int(sys.argv[1]) if len(sys.argv) > 1 else 12
    zh_n = int(sys.argv[2]) if len(sys.argv) > 2 else 12

    # 名单：雪球发现 + 知乎发现
    xq_authors = []
    p = os.path.join(BASE, "raw", "xq_author_discovery.json")
    if os.path.exists(p):
        xq_authors = json.load(open(p, encoding="utf-8"))["authors"][:xq_n]
    zh_authors = []
    p2 = os.path.join(BASE, "raw", "zh_author_discovery.json")
    if os.path.exists(p2):
        zh_authors = json.load(open(p2, encoding="utf-8"))["authors"][:zh_n]

    print(f"雪球 {len(xq_authors)} 人 + 知乎 {len(zh_authors)} 人", flush=True)
    only = os.environ.get("ONLY", "")

    t = new_tab("https://xueqiu.com/")
    time.sleep(1.5)
    tab = Tab(t["webSocketDebuggerUrl"])
    tab.navigate("https://xueqiu.com/", wait=8)

    results = {"xueqiu": [], "zhihu": []}
    # 若已有雪球结果，保留
    existing = os.path.join(OUT_DIR, "new_authors_content.json")
    if os.path.exists(existing) and only == "zhihu":
        prev = json.load(open(existing, encoding="utf-8"))
        results["xueqiu"] = prev.get("xueqiu", [])
        print(f"保留既有雪球结果 {len(results['xueqiu'])} 人", flush=True)

    if only != "zhihu":
        for i, a in enumerate(xq_authors):
            print(f"[雪球 {i+1}/{len(xq_authors)}] {a['name']} ({a['uid']})", flush=True)
            try:
                tab.navigate(f"https://xueqiu.com/u/{a['uid']}", wait=5)
                rec = fetch_xq_author(tab, a["uid"], a["name"])
                if rec:
                    results["xueqiu"].append(rec)
                    print(f"  → {len(rec['posts'])} 帖，粉 {rec['followers']}", flush=True)
            except Exception as e:
                print(f"  ERR {str(e)[:70]}", flush=True)
            time.sleep(0.8)

    # 知乎部分：每个作者独立 tab（避免跨域验证与 ws 断裂互相影响），排除已蒸馏的静水2008
    zh_authors = [a for a in zh_authors if a["token"] != "ban-ma-ban-ma-30-2"]
    # 合并已抓过的（按 token 去重）
    done_tokens = {r["token"] for r in results.get("zhihu", [])}
    if os.path.exists(existing):
        try:
            prev = json.load(open(existing, encoding="utf-8"))
            for r in prev.get("zhihu", []):
                if r["token"] not in done_tokens:
                    results["zhihu"].append(r)
                    done_tokens.add(r["token"])
        except Exception:
            pass
    print(f"知乎已有 {len(results['zhihu'])} 人，待抓 {len(zh_authors) - len([a for a in zh_authors if a['token'] in done_tokens])} 人", flush=True)

    if only != "xueqiu":
        for i, a in enumerate(zh_authors):
            if a["token"] in done_tokens:
                print(f"[知乎 {i+1}/{len(zh_authors)}] {a['name']} 已抓过，跳过", flush=True)
                continue
            print(f"[知乎 {i+1}/{len(zh_authors)}] {a['name']} ({a['token']})", flush=True)
            rec = None
            for attempt in range(2):
                tt = None
                try:
                    tt = new_tab(f"https://www.zhihu.com/people/{a['token']}/answers")
                    time.sleep(1.5)
                    tab2 = Tab(tt["webSocketDebuggerUrl"])
                    rec = fetch_zh_author(tab2, a["token"], a["name"])
                except Exception as e:
                    print(f"  ERR {str(e)[:70]}", flush=True)
                    rec = None
                finally:
                    if tt:
                        try:
                            close_tab(tt["id"])
                        except Exception:
                            pass
                if rec:
                    break
                time.sleep(10)  # 软风控冷却
            if rec:
                results["zhihu"].append(rec)
                print(f"  → {len(rec['answers'])} 回答，{sum(1 for x in rec['answers'] if x.get('full_text'))} 篇全文", flush=True)
                # 增量保存（防中断丢失）
                with open(existing, "w", encoding="utf-8") as f:
                    json.dump(results, f, ensure_ascii=False, indent=1)
            time.sleep(5)

    close_tab(t["id"])
    out = os.path.join(OUT_DIR, "new_authors_content.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=1)
    total_xq = sum(len(r["posts"]) for r in results["xueqiu"])
    total_zh = sum(len(r["answers"]) for r in results["zhihu"])
    print(f"\n完成：雪球 {len(results['xueqiu'])} 人 {total_xq} 帖；知乎 {len(results['zhihu'])} 人 {total_zh} 回答 → {out}")


if __name__ == "__main__":
    main()
