#!/usr/bin/env python3
"""抓取雪球用户(5661156172 静水2008)全部帖子正文，输出 JSON + Markdown。"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cdp import Tab, new_tab, close_tab  # noqa: E402

USER_ID = 5661156172
OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "raw")
os.makedirs(OUT_DIR, exist_ok=True)

JS_FETCH = """
(async (args) => {
  const [url] = args;
  const r = await fetch(url, {headers: {'Accept':'application/json','X-Requested-With':'XMLHttpRequest'}});
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
            print("  retry:", str(e)[:80], flush=True)
            time.sleep(2)
    return None


def main():
    t = new_tab("https://xueqiu.com/u/5661156172")
    time.sleep(1)
    tab = Tab(t["webSocketDebuggerUrl"])
    tab.navigate("https://xueqiu.com/u/5661156172", wait=10)

    all_posts = {}
    for ptype in ("11", "0"):
        for page in range(1, 15):
            url = f"/v4/statuses/user_timeline.json?user_id={USER_ID}&page={page}&type={ptype}"
            data = fetch_json(tab, url)
            if not data or not data.get("statuses"):
                print(f"type={ptype} page={page}: empty, stop", flush=True)
                break
            st = data["statuses"]
            for s in st:
                all_posts[s["id"]] = s
            print(f"type={ptype} page={page}: +{len(st)} (total {len(all_posts)})", flush=True)
            if len(st) < 10:
                break
            time.sleep(1)

    print(f"\n== 共 {len(all_posts)} 条，开始取全文 ==", flush=True)
    for i, (sid, s) in enumerate(all_posts.items(), 1):
        if s.get("type") in ("3", "2") or s.get("title"):
            d = fetch_json(tab, f"/statuses/show.json?id={sid}")
            if d and d.get("text"):
                s["full_text"] = d["text"]
                s["_title"] = d.get("title") or s.get("title", "")
            time.sleep(0.8)
        if i % 5 == 0:
            print(f"  full-text {i}/{len(all_posts)}", flush=True)

    out = {"user_id": USER_ID, "screen_name": "静水2008", "posts": list(all_posts.values())}
    jp = os.path.join(OUT_DIR, "xueqiu_jingshui2008.json")
    with open(jp, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
    print(f"\n已保存 {jp}")

    tab.close()
    close_tab(t["id"])


if __name__ == "__main__":
    main()
