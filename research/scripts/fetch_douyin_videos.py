#!/usr/bin/env python3
"""抖音短视频元信息抓取（CDP 拦截，D53，2026-11-01）

背景：抖音短链（v.douyin.com/xxx）由 JS 渲染 + 接口带 a_bogus 签名，
普通 HTTP 抓取拿不到内容；本脚本复用项目既有的 ks_mobile.CDP 框架，
在真实 Chrome 里打开短链 → 页面自身发出 aweme detail 请求 → 拦截响应
提取 元信息 + 视频直链（供后续抽帧 OCR，见 fetch_ks_video_frames.py）。

用法：
  # 1) 启动带调试端口的 Chrome（已开着普通 Chrome 时先完全退出）
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
      --remote-debugging-port=9222 --user-data-dir=/tmp/dy_chrome
  # 2) 抓取
  python3 research/scripts/fetch_douyin_videos.py \
      https://v.douyin.com/erMBW19z8OM/ https://v.douyin.com/HpmFEMuqqGs/
  # 3) 自检（端口是否通）
  python3 research/scripts/fetch_douyin_videos.py --probe

输出：research/raw/douyin_videos.json（逐条含 desc/author/stats/直链/落地 URL）
"""
from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from ks_mobile import MobileTab, list_tabs, new_tab
except ImportError as e:
    print(f'依赖缺失：{e}（需 websocket-client 与 research/scripts/ks_mobile.py）')
    raise SystemExit(2) from e

RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'raw')
OUT_PATH = os.path.join(RAW_DIR, 'douyin_videos.json')

# 抖音详情接口（桌面 web 版）
DETAIL_RE = re.compile(r'/aweme/v1/web/aweme/(detail|post)/', re.IGNORECASE)


def _probe() -> int:
    """CDP 端口自检"""
    try:
        tabs = list_tabs()
    except Exception as e:
        print(f'❌ CDP 不通：{type(e).__name__}: {e}')
        print('   启动："Google Chrome" --remote-debugging-port=9222 --user-data-dir=/tmp/dy_chrome')
        return 1
    print(f'✅ CDP 可用，当前标签页 {len(tabs)} 个')
    return 0


def _parse_detail(body: str) -> dict | None:
    """从 aweme detail 响应里提取元信息"""
    try:
        data = json.loads(body)
    except (TypeError, ValueError):
        return None
    detail = (data.get('aweme_detail')
              or (data.get('aweme_list') or [{}])[0]
              or data.get('data'))
    if not isinstance(detail, dict) or not detail:
        return None
    video = detail.get('video') or {}
    play = video.get('play_addr') or {}
    urls = play.get('url_list') or []
    stats = detail.get('statistics') or {}
    author = detail.get('author') or {}
    return {
        'aweme_id': str(detail.get('aweme_id') or ''),
        'desc': (detail.get('desc') or '').strip(),
        'create_time': detail.get('create_time'),
        'author': author.get('nickname') or '',
        'author_id': str(author.get('uid') or author.get('sec_uid') or ''),
        'duration_ms': video.get('duration'),
        'digg_count': stats.get('digg_count'),
        'comment_count': stats.get('comment_count'),
        'share_count': stats.get('share_count'),
        'collect_count': stats.get('collect_count'),
        'cover': ((video.get('cover') or {}).get('url_list') or [None])[0],
        'play_url': urls[0] if urls else None,
        'play_urls': urls,
    }


def fetch_one(url: str, wait: int = 10) -> dict:
    """打开一个抖音短链并抓取元信息"""
    tab = new_tab('about:blank')
    mtab = MobileTab(tab['webSocketDebuggerUrl'], device='desktop')
    try:
        mtab.enable_net()
        mtab.navigate(url, wait=wait)
        mtab.drain(3.0)
        landed = ''
        try:
            landed = mtab.cmd('Runtime.evaluate', {
                'expression': 'location.href', 'returnByValue': True}) \
                .get('result', {}).get('value', '')
        except (RuntimeError, OSError, ValueError, KeyError):
            landed = ''
        result = None
        for entry in mtab.net:
            if not DETAIL_RE.search(entry.get('url', '')):
                continue
            body = entry.get('body') or ''
            if not body or body.startswith('<err'):
                continue
            parsed = _parse_detail(body)
            if parsed:
                result = parsed
                break
        if result is None:
            # 兜底：从页面 DOM 抓标题
            try:
                title = mtab.cmd('Runtime.evaluate', {
                    'expression': 'document.title', 'returnByValue': True}) \
                    .get('result', {}).get('value', '')
            except Exception:  # noqa: BLE001
                title = ''
            result = {'aweme_id': '', 'desc': title, 'author': '',
                      'play_url': None, 'play_urls': []}
        result['source_url'] = url
        result['landed_url'] = landed
        result['captured_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
        return result
    finally:
        with contextlib.suppress(Exception):
            mtab.cmd('Page.close')
            mtab.ws.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('urls', nargs='*', help='抖音短链或视频页 URL')
    ap.add_argument('--probe', action='store_true', help='仅做 CDP 端口自检')
    ap.add_argument('--wait', type=int, default=10, help='页面加载等待秒数')
    ap.add_argument('--out', default=OUT_PATH)
    args = ap.parse_args()

    if args.probe:
        return _probe()
    if not args.urls:
        ap.print_help()
        return 2

    records = []
    for url in args.urls:
        print(f'→ {url}')
        try:
            rec = fetch_one(url, wait=args.wait)
        except Exception as e:  # noqa: BLE001 单条失败不影响其余
            rec = {'source_url': url, 'error': f'{type(e).__name__}: {e}'}
        records.append(rec)
        desc = (rec.get('desc') or '')[:60]
        print(f'  ✅ {desc!r} | 作者 {rec.get("author", "")} '
              f'| 赞 {rec.get("digg_count")} | 直链 {"有" if rec.get("play_url") else "无"}')
        if rec.get('error'):
            print(f'  ❌ {rec["error"]}')

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=1)
    print(f'已写出: {args.out}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
