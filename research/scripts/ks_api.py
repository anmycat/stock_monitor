#!/usr/bin/env python3
"""
快手 REST 接口探测器（在已登录浏览器上下文内 fetch，自带 cookie）。

  python3 ks_api.py search <关键词>        # 搜用户 + 搜视频
  python3 ks_api.py profile <快手号|userId>
  python3 ks_api.py photolist <userId>     # 作品列表(分页)
  python3 ks_api.py raw <path>             # 自定义路径, 如 /rest/v/search/user?keyword=x
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ks_mobile as K  # noqa: E402

RAW = K.RAW
FALCON = "__NS_hxfalcon=HUDR_sFnX-DtsGUFXsbDPT"

FETCH_JS = """
(async (url, opts) => {
  try {
    const r = await fetch(url, Object.assign({credentials:'include', headers:{
      'Accept':'application/json, text/plain, */*',
      'X-Requested-With':'XMLHttpRequest'
    }}, opts||{}));
    const t = await r.text();
    return JSON.stringify({status:r.status, url:r.url, body:t.slice(0,120000)});
  } catch(e) { return JSON.stringify({error:String(e)}); }
})(%s, %s)
"""


def call(tab, path):
    url = path if path.startswith("http") else f"https://www.kuaishou.com{path}"
    js = FETCH_JS % (json.dumps(url), "null")
    return json.loads(tab.eval(js, timeout=60))


def get_tab():
    t = K.find_tab("kuaishou")
    if not t:
        raise SystemExit("未找到快手标签页，请在 Chrome 打开 www.kuaishou.com")
    return K.MobileTab(t["webSocketDebuggerUrl"], device="desktop")


def save(name, data):
    p = os.path.join(RAW, name)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    print("saved:", p, os.path.getsize(p), "B")
    return p


def brief(res, keys=("userId", "user_name", "name", "caption", "photoId", "id")):
    b = res.get("body", "")
    try:
        j = json.loads(b)
    except Exception:
        return b[:300]
    s = json.dumps(j, ensure_ascii=False)
    return s[:400]


def main():
    args = sys.argv[1:]
    cmd = args[0] if args else "help"
    tab = get_tab()
    try:
        if cmd == "search":
            kw = args[1]
            from urllib.parse import quote
            q = quote(kw)
            for label, path in [
                ("user", f"/rest/v/search/user?keyword={q}&pcursor=&{FALCON}"),
                ("feed", f"/rest/v/search/feed?keyword={q}&pcursor=&{FALCON}"),
            ]:
                r = call(tab, path)
                print(f"\n### search/{label} status={r.get('status')} len={len(r.get('body',''))}")
                print(brief(r)[:600])
                save(f"ks_search_{label}.json", r)

        elif cmd == "profile":
            who = args[1]
            for label, path in [
                ("byid", f"/rest/v/profile/get?userId={who}&{FALCON}"),
                ("bykw", f"/rest/v/profile/get?keyword={who}&{FALCON}"),
            ]:
                r = call(tab, path)
                print(f"\n### profile/{label} status={r.get('status')}")
                print(r.get("body", "")[:600])
                save(f"ks_profile_{label}.json", r)

        elif cmd == "photolist":
            uid = args[1]
            pcursor = ""
            allf = []
            for page in range(int(args[2]) if len(args) > 2 else 5):
                path = f"/rest/v/profile/photolist?userId={uid}&pcursor={pcursor}&page=profile&{FALCON}"
                r = call(tab, path)
                print(f"\n--- page {page} status={r.get('status')} len={len(r.get('body',''))}")
                print(r.get("body", "")[:300])
                save(f"ks_photolist_{page}.json", r)
                try:
                    j = json.loads(r.get("body", ""))
                except Exception:
                    break
                feeds = (j.get("feeds") or j.get("visionProfilePhotoList", {}).get("feeds")) or []
                allf.extend(feeds)
                pcursor = j.get("pcursor") or j.get("visionProfilePhotoList", {}).get("pcursor") or ""
                if not feeds or not pcursor:
                    break
                time.sleep(1)
            save("ks_photolist_all.json", allf)
            print("总作品:", len(allf))

        elif cmd == "raw":
            r = call(tab, args[1])
            print(json.dumps(r, ensure_ascii=False)[:2000])
            save("ks_raw.json", r)

        else:
            print(__doc__)
    finally:
        tab.close()


if __name__ == "__main__":
    main()
