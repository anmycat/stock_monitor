#!/usr/bin/env python3
"""探测单个快手视频页的封面图 / 视频直链 / meta 信息。"""
import json
import os
import sys
import time
import ssl
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ks_mobile as K

WORK = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = os.path.join(WORK, "raw")
os.makedirs(os.path.join(RAW, "ks_video_covers"), exist_ok=True)

CODE = sys.argv[1] if len(sys.argv) > 1 else "JKn0ajlI"
URL = f"https://v.kuaishou.com/{CODE}"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")


def main():
    tid, tab = K.open_mobile(URL, device="desktop", wait=10, net=True)
    try:
        js = r"""(() => {
          const m = {};
          document.querySelectorAll('meta').forEach(e => {
            const k = e.getAttribute('property') || e.getAttribute('name');
            if (k) m[k] = e.getAttribute('content');
          });
          const imgs = [...document.querySelectorAll('img')]
            .map(i => i.src || i.getAttribute('data-src') || i.getAttribute('srcset'))
            .filter(Boolean).slice(0, 40);
          const vids = [...document.querySelectorAll('video')]
            .map(v => v.src || (v.querySelector('source') && v.querySelector('source').src))
            .filter(Boolean);
          const bg = [];
          document.querySelectorAll('[style*="background"], [style*="url("]').forEach(e => {
            const s = e.getAttribute('style') || '';
            const mm = s.match(/url\(['"]?(https?[^'")]+)['"]?\)/);
            if (mm) bg.push(mm[1]);
          });
          return JSON.stringify({meta: m, imgs: imgs, vids: vids, bg: bg.slice(0,20)});
        })()"""
        data = json.loads(tab.eval(js))
        print("=== META ===")
        for k, v in data.get("meta", {}).items():
            if any(w in k.lower() for w in ("image", "video", "title", "description", "url")):
                print(f"  {k}: {v[:200]}")
        print("\n=== VIDEO TAGS ===")
        for v in data.get("vids", []):
            print("  ", v[:200])
        print("\n=== IMG SRCS (前20) ===")
        for u in data.get("imgs", [])[:20]:
            print("  ", u[:200])
        print("\n=== BACKGROUND IMAGES (前10) ===")
        for u in data.get("bg", [])[:10]:
            print("  ", u[:200])
        # 尝试从拦截到的网络里找媒体资源
        print("\n=== 拦截到的媒体类网络请求 ===")
        for e in tab.net:
            u = e["url"]
            if any(d in u.lower() for d in ("kwimgs", "chenzhongtech", ".jpg", ".png", ".mp4", "cover", "poster", "blur")):
                print(f"  [{e.get('status')}] {e.get('mime')} {u[:160]}")
    finally:
        tab.close()


if __name__ == "__main__":
    main()
