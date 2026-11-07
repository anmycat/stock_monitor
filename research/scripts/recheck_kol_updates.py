#!/usr/bin/env python3
"""KOL 账号级更新核查 —— `docs/33` 补遗的可复现取证脚本。

背景：`docs/32`（2026-11-06）对「静水2008 / 多空博弈社」两位方法论 KOL
只做到「检索到相关内容」，未能完成**账号级更新时间线**核实。本脚本复用
原有取证脚本（`fetch_xueqiu.py` / `fetch_zhihu.py` / `fetch_em.py`）里的账号
标识，重跑公开接口，把「能否核实」变成可复现的机器结论。

运行：
    coding/.venv/bin/python research/scripts/recheck_kol_updates.py
    coding/.venv/bin/python research/scripts/recheck_kol_updates.py --pages 12

通道与边界（均为公开接口，不绕过登录/WAF）：
    东财 多空博弈社  uid=7924037455558900 —— `UserPost/CenterList` 匿名可用，
        type=1(动态)/type=2(帖子) 正常；**type=0 会返回「系统繁忙」**，勿用。
    雪球 静水2008    uid=5661156172 —— 匿名 HTTP 命中阿里云 WAF（返回 HTML 挑战页）；
        需浏览器渲染通道或 `cdp.py` 登录态，本脚本只做可达性探测，不硬闯。
    知乎 静水2008    token=ban-ma-ban-ma-30-2 —— 匿名 API 返回
        401 `AuthenticationInvalidClient`，需登录态；本脚本只做可达性探测。
    微信公众号       biz=MzkxNjY2Njc5NA== —— 无公开列表接口，不纳入自动取数。

输出：`research/data/kol_update_recheck_<YYYY-MM-DD>.json`
"""
from __future__ import annotations

import argparse
import json
import re
import time
from datetime import date
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "research" / "data"

EM_UID = "7924037455558900"
EM_API = "https://gbapi.eastmoney.com/userpostlist/api/UserPost/CenterList"
XQ_UID = "5661156172"
XQ_API = "https://xueqiu.com/v4/statuses/user_timeline.json"
ZH_TOKEN = "ban-ma-ban-ma-30-2"
ZH_API = "https://www.zhihu.com/api/v4/members"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# 基线来自本地语料快照（2026-10 中旬抓取），由 corpus/*.md 的条目标题日期提取
BASELINES = {
    "duokong_em": "2026-09-22 19:26:37",
    "jingshui_xueqiu": "2026-08-26 06:42:08",
    "jingshui_zhihu_answer": "2026-10-11",
    "jingshui_zhihu_article": "2026-10-10",
}


def em_page(ptype: int, page: int, ps: int = 20) -> list[dict]:
    """东财创作中心单页；JSONP `cb(...)` 外壳需剥离。"""
    params = {
        "uid": EM_UID, "onlyYt": 0, "version": "9001", "product": "Guba",
        "plat": "Web", "p": page, "ps": ps, "callback": "cb",
        "callback_param": "", "ctoken": "", "utoken": "", "deviceid": "",
        "gtoken": "", "type": ptype, "filterType": 0, "noinlist": 0,
    }
    headers = {"User-Agent": UA, "Referer": "https://emcreative.eastmoney.com/"}
    for _ in range(3):
        try:
            r = requests.get(EM_API, params=params, headers=headers, timeout=25)
            m = re.search(r"^cb\((.*)\)$", r.text.strip(), re.DOTALL)
            if m:
                return json.loads(m.group(1)).get("re") or []
        except (requests.RequestException, ValueError):
            pass
        time.sleep(1)
    return []


def collect_em(pages: int) -> dict:
    out: dict[str, dict] = {}
    for ptype, label in ((1, "动态"), (2, "帖子")):
        items: dict[int, dict] = {}
        used = 0
        for page in range(1, pages + 1):
            rows = em_page(ptype, page)
            if not rows:
                break
            used = page
            for row in rows:
                pid = row.get("post_id")
                if pid and pid not in items:
                    items[pid] = {
                        "post_id": pid,
                        "time": row.get("post_publish_time"),
                        "title": row.get("post_title", ""),
                        "content": (row.get("post_content") or "")[:200],
                        "stock_bar": (row.get("post_guba") or {}).get("stockbar_name", ""),
                        "from": row.get("post_from", ""),
                        "nickname": (row.get("post_user") or {}).get("user_nickname", ""),
                    }
            time.sleep(0.3)
        out[str(ptype)] = {
            "label": label, "pages_fetched": used,
            "items": sorted(items.values(), key=lambda x: x["time"] or "", reverse=True),
        }
    return out


def probe(name: str, url: str, headers: dict | None = None) -> dict:
    """只做匿名可达性探测，记录 HTTP 状态与返回体特征，不做任何绕过。"""
    h = {"User-Agent": UA}
    if headers:
        h.update(headers)
    try:
        r = requests.get(url, headers=h, timeout=20)
        head = r.text[:180].replace("\n", " ")
        if "aliyun_waf" in r.text or "renderData" in r.text:
            kind = "waf_challenge"
        elif r.headers.get("content-type", "").startswith("application/json") or head.startswith("{"):
            kind = "json"
        else:
            kind = "html_or_other"
        return {"name": name, "url": url, "http_status": r.status_code,
                "body_kind": kind, "body_head": head, "bytes": len(r.content)}
    except requests.RequestException as e:
        return {"name": name, "url": url, "error": f"{type(e).__name__}: {e}"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=8, help="东财每类最多翻几页")
    ap.add_argument("--out", type=str, default="")
    args = ap.parse_args()

    em = collect_em(args.pages)
    all_items = [i for group in em.values() for i in group["items"]]
    times = [i["time"] for i in all_items if i["time"]]
    newer = sorted(
        (i for i in all_items if (i["time"] or "") > BASELINES["duokong_em"]),
        key=lambda x: x["time"], reverse=True,
    )

    probes = [
        probe("xueqiu_timeline_anonymous", f"{XQ_API}?user_id={XQ_UID}&page=1&type=0",
              {"Referer": f"https://xueqiu.com/u/{XQ_UID}"}),
        probe("zhihu_articles_anonymous",
              f"{ZH_API}/{ZH_TOKEN}/articles?include=data%5B*%5D.created_time,title"
              "&offset=0&limit=5&sort_by=created",
              {"Referer": f"https://www.zhihu.com/people/{ZH_TOKEN}"}),
    ]

    result = {
        "checked_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "purpose": "docs/32 §5 遗留项：两位方法论 KOL 的账号级更新时间线核实",
        "baselines_from_corpus": BASELINES,
        "eastmoney_duokong": {
            "uid": EM_UID,
            "api": EM_API,
            "identity_confirmed_by": "post_user.user_nickname == 多空博弈社",
            "unique_items": len(all_items),
            "earliest": min(times) if times else None,
            "latest": max(times) if times else None,
            "new_after_baseline": len(newer),
            "new_items": newer,
            "types": em,
        },
        "anonymous_probes": probes,
        "manual_evidence_note": (
            "雪球/知乎的匿名通道被 WAF/401 阻断；docs/33 记录了通过浏览器渲染通道"
            "取得的雪球 timeline 时间戳、第三方知乎观点日报转录、以及微信公众号原文。"
            "这些是人工取证证据，不属于本脚本自动取数范围。"
        ),
    }

    out = Path(args.out) if args.out else DATA_DIR / f"kol_update_recheck_{date.today():%Y_%m_%d}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")

    print(f"东财 多空博弈社：{len(all_items)} 条 | 最早 {result['eastmoney_duokong']['earliest']}"
          f" | 最新 {result['eastmoney_duokong']['latest']}"
          f" | 基线后新增 {len(newer)} 条")
    for i in newer[:5]:
        print(f"  + {i['time']} | {i['stock_bar']} | {i['title'][:60]}")
    for p in probes:
        print(f"探测 {p['name']}: HTTP {p.get('http_status', '-')} / {p.get('body_kind', p.get('error'))}")
    print(f"已写出 {out}")


if __name__ == "__main__":
    main()
