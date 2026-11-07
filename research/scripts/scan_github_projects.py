#!/usr/bin/env python3
"""GitHub 项目调研扫描：复现 docs/34 的元数据层证据（匿名 REST API）。

用法：
    coding/.venv/bin/python research/scripts/scan_github_projects.py --out research/data/github_project_scan_2026_11_07.json

说明：
- 只用 GitHub 匿名 REST API 与 raw 源码，不需要任何 token；匿名限额 60 次/小时，
  脚本按需请求，必要时可稍后重跑。
- 输出为**精简取证数据**（对象元数据、近期提交/release 摘要、关键提交文件统计、
  两条检索式命中），不含 API 响应全文，避免大文件与无关内容入库。
- 源码级结论（函数实现、字段契约）见 docs/34 正文与其中列出的 raw 文件/commit SHA。
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen

API = "https://api.github.com"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# docs/32 的 5 个既有调研对象 + docs/34 新发现的 6 个高相关项目
REPOS = [
    "ZhuLinsen/daily_stock_analysis",
    "hsliuping/TradingAgents-CN",
    "wbh604/UZI-Skill",
    "shy3130/tick-stock-panel",
    "oficcejo/aiagents-stock",
    "electkismet/eltdx",
    "rootSunc/CNEquity",
    "1nchaos/adata",
    "hello245m/free-stockdb",
    "qusong0627/QuantMind",
    "jundizhou/easy-stock",
]

# docs/34 引用到的关键提交：只存统计与文件清单，不存 patch 全文
KEY_COMMITS = {
    "ZhuLinsen/daily_stock_analysis": [
        "77e419281d",  # #2389 日线缓存新鲜度
        "ebab2ba9f5",  # #2304 data capability center
        "5fa42fd9",    # #2305 evidence provenance
        "e0e0b723",    # #2331 decision review aggregation
    ],
    "shy3130/tick-stock-panel": [
        "8fb23fcf",    # 日K自检 / 精准修补 / 数据版本管理
    ],
}

SEARCH_QUERIES = [
    "stock analysis AI pushed:>2026-10-20",
    "A股 量化 pushed:>2026-10-01",
]


def get_json(path: str) -> dict | list | None:
    """匿名 GET，失败返回 None（不中断整轮扫描）。"""
    req = Request(
        f"{API}{path}",
        headers={"User-Agent": UA, "Accept": "application/vnd.github+json"},
    )
    for attempt in range(3):
        try:
            with urlopen(req, timeout=25) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:  # noqa: BLE001 单个端点失败不影响其余取证
            if attempt == 2:
                print(f"  ! {path} -> {type(e).__name__}: {str(e)[:80]}")
            time.sleep(1)
    return None


def brief_repo(full_name: str) -> dict:
    d = get_json(f"/repos/{full_name}")
    if not isinstance(d, dict):
        return {"full_name": full_name, "error": "metadata unavailable"}
    return {
        "full_name": d.get("full_name"),
        "stars": d.get("stargazers_count"),
        "forks": d.get("forks_count"),
        "language": d.get("language"),
        "pushed_at": d.get("pushed_at"),
        "created_at": d.get("created_at"),
        "archived": d.get("archived"),
        "open_issues": d.get("open_issues_count"),
        "default_branch": d.get("default_branch"),
        "topics": d.get("topics", [])[:10],
        "description": (d.get("description") or "")[:200],
    }


def brief_commits(full_name: str, limit: int = 12) -> list[dict]:
    d = get_json(f"/repos/{full_name}/commits?per_page={limit}")
    if not isinstance(d, list):
        return []
    return [
        {
            "sha": c.get("sha", "")[:8],
            "date": c.get("commit", {}).get("author", {}).get("date", "")[:10],
            "message": c.get("commit", {}).get("message", "").splitlines()[0][:120],
        }
        for c in d
    ]


def brief_releases(full_name: str, limit: int = 3) -> list[dict]:
    d = get_json(f"/repos/{full_name}/releases?per_page={limit}")
    if not isinstance(d, list):
        return []
    return [
        {
            "tag": r.get("tag_name"),
            "published_at": (r.get("published_at") or "")[:16],
            "body_excerpt": (r.get("body") or "")[:600],
        }
        for r in d
    ]


def brief_key_commits(full_name: str) -> list[dict]:
    out = []
    for sha in KEY_COMMITS.get(full_name, []):
        d = get_json(f"/repos/{full_name}/commits/{sha}")
        if not isinstance(d, dict):
            out.append({"sha": sha, "error": "unavailable"})
            continue
        files = d.get("files") or []
        out.append(
            {
                "sha": sha,
                "date": d.get("commit", {}).get("author", {}).get("date", "")[:10],
                "message": d.get("commit", {}).get("message", "").splitlines()[0][:120],
                "files_changed": len(files),
                "additions": sum(f.get("additions", 0) for f in files),
                "deletions": sum(f.get("deletions", 0) for f in files),
                "files": [
                    {
                        "name": f.get("filename"),
                        "add": f.get("additions"),
                        "del": f.get("deletions"),
                    }
                    for f in files[:20]
                ],
            }
        )
    return out


def brief_search(query: str) -> dict:
    d = get_json(f"/search/repositories?q={quote(query)}&sort=stars&order=desc&per_page=12")
    if not isinstance(d, dict):
        return {"query": query, "error": "unavailable"}
    return {
        "query": query,
        "total_count": d.get("total_count"),
        "top": [
            {
                "full_name": x.get("full_name"),
                "stars": x.get("stargazers_count"),
                "pushed_at": (x.get("pushed_at") or "")[:10],
                "description": (x.get("description") or "")[:140],
            }
            for x in (d.get("items") or [])[:12]
        ],
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--out",
        default="research/data/github_project_scan_2026_11_07.json",
        help="输出 JSON 路径",
    )
    ap.add_argument("--with-commits", action="store_true", help="一并抓取每个项目近期提交列表")
    args = ap.parse_args()

    result: dict = {
        "scan_date": time.strftime("%Y-%m-%d %H:%M:%S"),
        "method": "GitHub REST API v3 (anonymous) + raw.githubusercontent.com",
        "note": "精简取证数据；源码级实现与结论见 research/docs/34",
        "projects": {},
        "search_queries": [],
    }
    for repo in REPOS:
        print(f"scan {repo} ...")
        entry = brief_repo(repo)
        entry["releases"] = brief_releases(repo)
        entry["key_commits"] = brief_key_commits(repo)
        if args.with_commits:
            entry["recent_commits"] = brief_commits(repo)
        result["projects"][repo] = entry
    for q in SEARCH_QUERIES:
        print(f"search {q} ...")
        result["search_queries"].append(brief_search(q))

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\nwritten: {out} ({out.stat().st_size} bytes, {len(REPOS)} projects)")


if __name__ == "__main__":
    main()
