#!/usr/bin/env python3
"""
批量2：解析 13 个新快手短链，下载视频 atlas(关键帧拼图)+封面(upic)，
并从拦截到的 API 响应体抽取 发布时间戳/playUrl/标题/播放量，写入 batch2 manifest
与 ks_videos_meta.json(追加)，不覆盖旧数据。
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

# (short code, 用户给出的提示/标题)
LINKS = [
    ("K0zjoFdn", "花6000开个新号第一天来个暴击！大号也是惨不忍睹"),
    ("nhaozyYa", "每一步都算数配速是努力的刻度 | 换一下仓位了"),
    ("nTqrGbW8", "数据不会骗人每一分努力都算数"),
    ("Kt13VNcF", "数据不会骗人身体在悄悄提醒你"),
    ("K1T1YQiR", "数据不会骗人身体在悄悄提醒你"),
    ("K04yw219", "数字不会骗人每一步都算数"),
    ("nJNnQl71", "每一步都算数配速是努力的刻度"),
    ("nY2vguA9", "@多空博弈社 发了一个快手作品"),
    ("npiWKRV1", "跑完十五公里汗水没白流身体在悄悄变强"),
    ("JkxoMpoR", "清了香江新金路百合红宝丽"),
    ("71McZuyt", "股票交流炒股日记实盘记录股票"),
    ("KMvecutb", "账户数字悄然上扬心也跟着轻快起来"),
    ("KcNsTpS2", "炒股日记实盘记录我的主场时刻"),
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


def grab_meta(body):
    """从拦截响应体抽视频元信息。"""
    info = {}
    for key in ("photoId", "caption", "viewCount", "likeCount", "timestamp",
                "publishTime", "playUrl", "videoUrl", "coverUrl", "commentCount"):
        m = re.search(r'"%s"\s*:\s*("?[^,}\]]+)' % key, body)
        if m:
            info[key] = m.group(1).strip().strip('"')
    cap = re.search(r'"caption"\s*:\s*"((?:[^"\\]|\\.)*)"', body)
    if cap and not info.get("caption"):
        try:
            info["caption"] = cap.group(1).encode().decode("unicode_escape", "ignore")
        except Exception:
            pass
    # 抓取第一个出现的 playUrl (可能是转义嵌套)
    if "playUrl" not in info:
        pm = re.search(r'"playUrl"\s*:\s*"((?:[^"\\]|\\.)*)"', body)
        if pm:
            info["playUrl"] = pm.group(1)
    return info


def handle_one(code, hint):
    url = BASE_URL.format(code)
    print(f"\n{'='*70}\n处理 {code} | {hint}\n{'='*70}", flush=True)
    tid, tab = K.open_mobile(url, device="desktop", wait=10, net=True)
    rec = {"code": code, "hint": hint, "images": [], "meta": {}}
    try:
        # 元信息：扫描拦截响应体
        for e in tab.net:
            b = e.get("body") or ""
            if len(b) < 50:
                continue
            low = e["url"].lower()
            if any(k in low for k in ("vision", "video", "photo", "graphql", "short-video")) or \
               ("playUrl" in b or "viewCount" in b or "timestamp" in b):
                info = grab_meta(b)
                if info:
                    for k, v in info.items():
                        rec["meta"].setdefault(k, v)
        print("  meta:", {k: str(v)[:50] for k, v in rec["meta"].items()}, flush=True)

        # 收集 atlas + 封面
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
        try:
            urls = json.loads(tab.eval(js))
        except Exception:
            urls = []
        atlas = sorted({u for u in urls if "ufile/atlas/" in u},
                       key=lambda u: int(re.search(r"_(\d+)\.", u).group(1)) if re.search(r"_(\d+)\.", u) else 0)
        covers = [u for u in urls if "upic/" in u and "clientCacheKey" in u]
        print(f"  atlas: {len(atlas)} 张, 封面: {len(covers)} 张", flush=True)
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
        for attempt in range(2):
            try:
                results.append(handle_one(code, hint))
                break
            except Exception as ex:
                print(f"  !! {code} 第{attempt+1}次失败: {type(ex).__name__}: {ex}", flush=True)
                if attempt == 1:
                    results.append({"code": code, "hint": hint, "error": str(ex)})
                time.sleep(2)
        time.sleep(1)
    out = os.path.join(COVER_DIR, "frames_manifest_b2.json")
    json.dump(results, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print(f"\n完成，保存: {out}")
    total = sum(len(r.get("images", [])) for r in results)
    print(f"共下载 {total} 张图（来自 {len(results)} 个视频）")

    # 追加时间戳到 ks_videos_meta.json（不覆盖旧 5 条）
    meta_path = os.path.join(RAW, "ks_videos_meta.json")
    old = []
    if os.path.exists(meta_path):
        try:
            old = json.load(open(meta_path, encoding="utf-8"))
        except Exception:
            old = []
    old_codes = {r.get("code") for r in old}
    for r in results:
        if r.get("code") and r["code"] not in old_codes:
            old.append({
                "code": r["code"], "hint": r.get("hint", ""),
                "url": BASE_URL.format(r["code"]),
                "final_url": None, "page_text": "", "api_hits": [],
                "video": r.get("meta", {}), "cover_local": None, "batch": 2,
            })
    json.dump(old, open(meta_path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print(f"ks_videos_meta.json 现有 {len(old)} 条")


if __name__ == "__main__":
    main()
