#!/usr/bin/env python3
"""
下载快手视频/图文的封面图集(atlas)与视频封面(upic)，供视觉分析。
对每个 v.kuaishou.com 短链：打开桌面登录态页面 -> 提取所有 atlas/upic 图 ->
下载到 raw/ks_video_covers/ -> 保存 manifest。
"""
import json
import os
import re
import sys
import time
import ssl
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ks_mobile as K

WORK = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = os.path.join(WORK, "raw")
COVER_DIR = os.path.join(RAW, "ks_video_covers")
os.makedirs(COVER_DIR, exist_ok=True)

LINKS = [
    ("JKn0ajlI", "新开的小号亏损，大号赚钱，互相颠倒"),
    ("Jdg4jr6z", "数据不会骗人，每一分努力都算数"),
    ("nZTKyjXA", "今天被砸的头晕眼花"),
    ("JgZ1IFSc", "炒股日记 股票交流 实盘记录"),
    ("nmP9R9Lt", "@多空博弈社 发了一个快手作品"),
]
BASE_URL = "https://v.kuaishou.com/{}"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
REF = "https://v.kuaishou.com/"


def download(url, path):
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    for u in (url, url.replace("https://", "http://")):
        try:
            req = urllib.request.Request(u, headers={"User-Agent": UA, "Referer": REF})
            data = urllib.request.urlopen(req, timeout=30, context=ctx).read()
            if len(data) > 2000:
                open(path, "wb").write(data)
                return len(data)
        except Exception:
            pass
    return 0


def handle_one(code, hint):
    url = BASE_URL.format(code)
    print(f"\n{'='*70}\n处理 {code} | {hint}\n{'='*70}", flush=True)
    tid, tab = K.open_mobile(url, device="desktop", wait=10, net=True)
    rec = {"code": code, "hint": hint, "images": []}
    try:
        js = r"""(() => {
          const set = new Set();
          document.querySelectorAll('img').forEach(i => {
            const s = i.src || i.getAttribute('data-src') || i.getAttribute('srcset') || '';
            if (s) s.split(/\s+/).forEach(x => set.add(x));
          });
          document.querySelectorAll('[style*="url("]').forEach(e => {
            const m = (e.getAttribute('style')||'').match(/url\(['"]?(https?[^'")]+)['"]?\)/g);
            if (m) m.forEach(x => set.add(x.replace(/^url\(['"]?/,'').replace(/['"]?\)$/,'')));
          });
          return JSON.stringify([...set]);
        })()"""
        urls = json.loads(tab.eval(js))
        # 仅保留图集(atlas)和视频封面(upic, 带 clientCacheKey)
        atlas = sorted({u for u in urls if "ufile/atlas/" in u},
                       key=lambda u: int(re.search(r"_(\d+)\.", u).group(1)) if re.search(r"_(\d+)\.", u) else 0)
        covers = [u for u in urls if "upic/" in u and "clientCacheKey" in u]
        print(f"  atlas 图: {len(atlas)} 张, 视频封面: {len(covers)} 张", flush=True)
        all_imgs = atlas + covers
        for i, u in enumerate(all_imgs):
            p = os.path.join(COVER_DIR, f"{code}_{i:02d}.jpg")
            sz = download(u, p)
            kind = "atlas" if "atlas" in u else "cover"
            if sz:
                rec["images"].append({"local": p, "kind": kind, "url": u, "bytes": sz})
                print(f"    下载 {kind}: {os.path.basename(p)} ({sz//1024}KB)", flush=True)
    finally:
        tab.close()
    return rec


def main():
    results = []
    for code, hint in LINKS:
        try:
            results.append(handle_one(code, hint))
        except Exception as ex:
            print(f"  !! {code} 失败: {ex}", flush=True)
            results.append({"code": code, "hint": hint, "error": str(ex)})
        time.sleep(1)
    out = os.path.join(COVER_DIR, "frames_manifest.json")
    json.dump(results, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print(f"\n完成，保存: {out}")
    total = sum(len(r.get("images", [])) for r in results)
    print(f"共下载 {total} 张图（来自 {len(results)} 个视频）")


if __name__ == "__main__":
    main()
