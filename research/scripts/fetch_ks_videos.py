#!/usr/bin/env python3
"""
解析快手短视频分享短链 (v.kuaishou.com/xxx)，在桌面登录态 Chrome 中打开，
拦截视频详情接口，提取 标题/描述/标签/播放量/发布时间/封面图/视频直链。

依赖：scripts/ks_mobile.py
运行：KS_PORT=9222 python3 scripts/fetch_ks_videos.py
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
REF = "https://www.kuaishou.com/"


def download(url, path):
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    for u in (url.replace("http://", "https://"), url):
        try:
            req = urllib.request.Request(u, headers={"User-Agent": UA, "Referer": REF})
            data = urllib.request.urlopen(req, timeout=30, context=ctx).read()
            if len(data) > 2000:
                open(path, "wb").write(data)
                return len(data)
        except Exception as e:
            last = e
    return 0


def parse_video_response(body):
    """从拦截到的响应体里抽取视频详情字段。"""
    info = {}
    # 常见 JSON 字段
    for key in ("videoUrl", "playUrl", "coverUrl", "poster", "caption",
                "viewCount", "likeCount", "commentCount", "photoId",
                "timestamp", "publishTime", "label", "tags", "cityName"):
        m = re.search(r'"%s"\s*:\s*("?[^,}\]]+)' % key, body)
        if m:
            v = m.group(1).strip().strip('"')
            info[key] = v
    # 标题/正文可能在 caption 或 vision 结构
    cap = re.search(r'"caption"\s*:\s*"((?:[^"\\]|\\.)*)"', body)
    if cap and not info.get("caption"):
        info["caption"] = cap.group(1).encode().decode("unicode_escape", "ignore")
    return info


def handle_one(code, hint):
    url = BASE_URL.format(code)
    print(f"\n{'='*70}\n处理 {code} | {hint}\nURL: {url}\n{'='*70}", flush=True)
    tid, tab = K.open_mobile(url, device="desktop", wait=12, net=True)
    rec = {"code": code, "hint": hint, "url": url, "final_url": None,
           "page_text": "", "api_hits": [], "video": {}, "cover_local": None}
    try:
        rec["final_url"] = str(tab.eval("location.href"))
        print("  最终URL:", rec["final_url"][:120], flush=True)
        # 滚动触发视频/评论加载
        tab.scroll(times=6, pause=1.5)
        rec["page_text"] = (tab.text() or "").replace("\n", " | ")[:1200]
        print("  页面文本:", rec["page_text"][:200], flush=True)
        # 遍历拦截到的接口
        for e in tab.net:
            b = e.get("body") or ""
            if len(b) < 50:
                continue
            low = e["url"].lower()
            is_video_api = any(k in low for k in ("vision", "video", "photo", "short-video", "graphql"))
            if is_video_api or "videoUrl" in b or "coverUrl" in b or "viewCount" in b:
                info = parse_video_response(b)
                if info:
                    rec["api_hits"].append({"url": e["url"][:160], "info": info})
                    # 合并到 video 字段
                    for k, v in info.items():
                        rec["video"].setdefault(k, v)
        # 优先取第一个 api hit 的完整 info
        if rec["api_hits"]:
            rec["video"] = rec["api_hits"][0]["info"]
        print("  视频字段:", {k: str(v)[:60] for k, v in rec["video"].items()}, flush=True)
        # 下载封面
        cover = rec["video"].get("coverUrl") or rec["video"].get("poster")
        if cover:
            cov_path = os.path.join(COVER_DIR, f"{code}.jpg")
            sz = download(cover, cov_path)
            if sz:
                rec["cover_local"] = cov_path
                print(f"  封面已下载: {cov_path} ({sz//1024}KB)", flush=True)
        # 视频直链也尝试下载一帧（可选，先看封面）
    finally:
        tab.close()
    return rec


def main():
    results = []
    for code, hint in LINKS:
        try:
            results.append(handle_one(code, hint))
        except Exception as ex:
            print(f"  !! {code} 失败: {type(ex).__name__}: {ex}", flush=True)
            results.append({"code": code, "hint": hint, "error": str(ex)})
        time.sleep(1.5)
    out = os.path.join(RAW, "ks_videos_meta.json")
    json.dump(results, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print(f"\n全部完成，保存: {out}")
    # 汇总
    for r in results:
        v = r.get("video", {})
        print(f"  {r['code']}: cover={'Y' if r.get('cover_local') else 'N'} "
              f"views={v.get('viewCount')} caption={str(v.get('caption'))[:40]}")


if __name__ == "__main__":
    main()
