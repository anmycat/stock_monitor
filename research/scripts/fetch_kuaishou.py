#!/usr/bin/env python3
"""
快手抓取器 —— 基于「真实浏览器页面驱动 + 网络响应拦截」。

背景：快手 /rest/v/* 接口带参数绑定签名（__NS_hxfalcon），无法脱离页面直接构造请求，
必须让页面 JS 自己发请求（签名由页面生成），再用 CDP Network 域拦截响应体。
移动浏览器 UA 会被强制跳转到 App 下载页，因此内容抓取统一走桌面指纹。

  python3 fetch_kuaishou.py profile <快手号> [--scroll N] [--out raw/ks_xxx.json]
  python3 fetch_kuaishou.py search <关键词>
  python3 fetch_kuaishou.py urls <url>            # 列出某页触发的全部接口
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ks_mobile as K  # noqa: E402

RAW = K.RAW

# 关注的响应 URL 模式 -> 语义标签
PATTERNS = [
    ("/rest/v/profile/photolist", "photolist"),
    ("/rest/v/profile/get", "profile"),
    ("/rest/v/search/user", "search_user"),
    ("/rest/v/search/feed", "search_feed"),
    ("/rest/v/feed/profile", "feed_profile"),
    ("/graphql", "graphql"),
    ("photoList", "photolist"),
    ("photolist", "photolist"),
]


def label_of(url):
    for pat, lab in PATTERNS:
        if pat in url:
            return lab
    return None


def collect(tab, store):
    """把新拦截到的响应按标签归集（去重 by url）。"""
    for e in tab.net:
        lab = label_of(e["url"])
        if not lab:
            continue
        body = e.get("body") or ""
        if not body or len(body) < 20:
            continue
        store.setdefault(lab, {})
        if e["url"] in store[lab]:
            continue
        store[lab][e["url"]] = body


def parse_feeds(body):
    """从各类响应里尽力抽 feeds 数组。"""
    try:
        j = json.loads(body)
    except Exception:
        return []
    if not isinstance(j, dict):
        return []
    for key in ("feeds", "data", "visionProfilePhotoList", "result"):
        v = j.get(key)
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return v
        if isinstance(v, dict):
            f = v.get("feeds")
            if isinstance(f, list):
                return f
    return []


def norm(f):
    return {
        "photo_id": f.get("photoId") or f.get("id") or f.get("photo_id"),
        "caption": f.get("caption") or f.get("text") or f.get("desc") or "",
        "timestamp": f.get("timestamp") or f.get("time") or f.get("createTime"),
        "view_count": f.get("viewCount") or f.get("view_count"),
        "like_count": f.get("realLikeCount") or f.get("likeCount") or f.get("like_count"),
        "comment_count": f.get("commentCount") or f.get("comment_count"),
        "duration": f.get("duration"),
        "cover": f.get("coverUrl") or f.get("thumbnailUrl"),
        "raw": f,
    }


def profile(ks_id, scroll=25, out=None, device="desktop"):
    url = f"https://www.kuaishou.com/profile/{ks_id}"
    print(f"[open] {url}", flush=True)
    tid, tab = K.open_mobile(url, device=device, wait=10)
    store = {}
    try:
        collect(tab, store)
        print(f"  首屏接口: { {k: len(v) for k, v in store.items()} }", flush=True)
        for i in range(scroll):
            tab.eval("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.8)
            collect(tab, store)
            feeds_n = sum(len(parse_feeds(b)) for b in store.get("photolist", {}).values())
            if i % 5 == 0:
                print(f"  scroll {i}: photolist响应{len(store.get('photolist',{}))} 条, 作品≈{feeds_n}", flush=True)
            if "服务器" in tab.text()[:50]:
                pass
    finally:
        feeds = []
        for b in store.get("photolist", {}).values():
            for f in parse_feeds(b):
                feeds.append(norm(f))
        seen, uniq = set(), []
        for f in feeds:
            pid = str(f["photo_id"])
            if pid in seen:
                continue
            seen.add(pid)
            uniq.append(f)
        payload = {"source": "快手", "account": ks_id, "fetched_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                   "works_count": len(uniq), "works": uniq,
                   "raw_profile": list(store.get("profile", {}).values())[:2],
                   "api_urls": sorted(store.get("photolist", {}))[:5]}
        p = out or os.path.join(RAW, f"ks_{ks_id}.json")
        with open(p, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=1)
        print(f"\n[done] 作品 {len(uniq)} 条 -> {p}")
        print(f"  接口标签: { {k: len(v) for k, v in store.items()} }")
        if not uniq:
            print("  提示: 未拿到作品列表，可能是 profile 接口风控。可试 --device iphone 或先完成登录。")
        tab.close()
    return payload


def search(kw, device="desktop"):
    from urllib.parse import quote
    url = f"https://www.kuaishou.com/search/user?searchKey={quote(kw)}"
    tid, tab = K.open_mobile(url, device=device, wait=10)
    store = {}
    try:
        tab.scroll(3)
        collect(tab, store)
    finally:
        tab.close()
    p = os.path.join(RAW, "ks_search_dump.json")
    with open(p, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False, indent=1)
    print(f"已保存 { {k: len(v) for k, v in store.items()} } -> {p}")
    return store


def urls(url, device="desktop"):
    tid, tab = K.open_mobile(url, device=device, wait=10)
    try:
        tab.scroll(2)
        for e in tab.net:
            b = e.get("body") or ""
            print(f"[{e.get('status')}] {len(b):>7}B {e['url'][:120]}")
    finally:
        tab.close()


if __name__ == "__main__":
    a = sys.argv[1:]
    if not a:
        print(__doc__)
        raise SystemExit
    cmd = a[0]

    def opt(n, d=None):
        return a[a.index(n) + 1] if n in a else d

    if cmd == "profile":
        profile(a[1], int(opt("--scroll", 25)), opt("--out"), opt("--device", "desktop"))
    elif cmd == "search":
        search(a[1], opt("--device", "desktop"))
    elif cmd == "urls":
        urls(a[1], opt("--device", "desktop"))
    else:
        print(__doc__)
