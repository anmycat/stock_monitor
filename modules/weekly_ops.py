import json
import os
import re
import shutil
import subprocess
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree

import requests
from modules.market import now_bj

CN_CHAR_RE = re.compile(r"[\u4e00-\u9fff]")
A_SHARE_KEYWORDS = (
    "a股",
    "沪深",
    "上证",
    "深证",
    "沪指",
    "深成指",
    "创业板",
    "科创板",
    "北向资金",
    "两市",
    "涨停",
    "跌停",
    "主力资金",
    "量化",
    "公募",
    "券商",
    "中证",
    "同花顺",
    "东方财富",
    "财联社",
    "雪球",
    "ETF",
)
POLICY_KEYWORDS = (
    "国务院",
    "国常会",
    "发改委",
    "证监会",
    "央行",
    "财政部",
    "金融监管总局",
    "工信部",
    "商务部",
    "政策",
    "新规",
    "征求意见稿",
    "指导意见",
    "措施",
    "方案",
)
GLOBAL_KEYWORDS = (
    "美联储",
    "fed",
    "us",
    "nasdaq",
    "dow",
    "s&p",
    "日经",
    "欧洲",
    "欧央行",
    "ecb",
    "boj",
    "日本央行",
    "中东",
    "原油",
    "gold",
    "美元",
    "美股",
    "港股",
)


_GIT_BIN = os.getenv("GIT_BIN") or shutil.which("git")


def _run_cmd(args, timeout=30):
    if args and args[0] == "git":
        if _GIT_BIN:
            args = [_GIT_BIN] + args[1:]
        else:
            return subprocess.CompletedProcess(args, 127, "", "cmd_not_found:git")
    try:
        return subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError as exc:
        return subprocess.CompletedProcess(args, 127, "", f"cmd_not_found:{exc}")


def _daily_quote_paths():
    primary = os.getenv("DAILY_STOCK_ANALYSIS_QUOTE_PATH", "data/daily_stock_analysis/latest_quote.json").strip()
    repo_path = os.getenv("DAILY_STOCK_ANALYSIS_REPO_PATH", "/daily_stock_analysis").strip()
    fallback = os.path.join(repo_path, "output", "latest_quote.json")
    return [primary, fallback]


def _check_daily_quote_data():
    for path in _daily_quote_paths():
        if not path or not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if isinstance(payload, dict) and len(payload) > 0:
                return True, "dict_non_empty"
            if isinstance(payload, list) and len(payload) > 0:
                return True, "list_non_empty"
            return False, "empty_payload"
        except Exception as exc:
            return False, f"invalid_json:{exc}"
    return False, "missing_or_empty"


def check_daily_repo_status():
    repo_path = os.getenv("DAILY_STOCK_ANALYSIS_REPO_PATH", "/daily_stock_analysis").strip()
    branch = os.getenv("DAILY_STOCK_ANALYSIS_BRANCH", "main").strip()
    auto_upgrade = os.getenv("DAILY_STOCK_ANALYSIS_AUTO_UPGRADE", "false").lower() == "true"
    sync_mode = os.getenv("DAILY_STOCK_ANALYSIS_SYNC_MODE", "ff_only").strip().lower()

    result = {
        "repo_path": repo_path,
        "branch": branch,
        "repo_exists": False,
        "is_git_repo": False,
        "behind": None,
        "upgraded": False,
        "sync_mode": sync_mode,
        "daily_data_ok": False,
        "daily_data_reason": "",
        "error": "",
        "checked_at": now_bj().isoformat(timespec="seconds"),
    }

    if os.getenv("DAILY_STOCK_ANALYSIS_AUX_ENABLED", "true").lower() != "true":
        result["error"] = "daily_aux_disabled"
        return result

    if not os.path.isdir(repo_path):
        result["error"] = "repo_path_not_found"
        return result
    result["repo_exists"] = True

    is_git = _run_cmd(["git", "-C", repo_path, "rev-parse", "--is-inside-work-tree"])
    if is_git.returncode != 0:
        if "cmd_not_found:git" in (is_git.stderr or ""):
            result["error"] = "git_not_found"
        else:
            result["error"] = "not_git_repo"
        return result
    if is_git.stdout.strip() != "true":
        result["error"] = "not_git_repo"
        return result
    result["is_git_repo"] = True

    fetch = _run_cmd(["git", "-C", repo_path, "fetch", "origin", branch])
    if fetch.returncode != 0:
        result["error"] = f"git_fetch_failed:{fetch.stderr.strip()[:120]}"
        return result

    behind_cmd = _run_cmd(["git", "-C", repo_path, "rev-list", "--count", f"HEAD..origin/{branch}"])
    if behind_cmd.returncode != 0:
        result["error"] = f"git_rev_list_failed:{behind_cmd.stderr.strip()[:120]}"
        return result
    behind = int(behind_cmd.stdout.strip() or "0")
    result["behind"] = behind

    if auto_upgrade and behind > 0:
        if sync_mode == "hard_reset":
            commands = [
                ["git", "-C", repo_path, "fetch", "origin", branch],
                ["git", "-C", repo_path, "checkout", "-B", branch, f"origin/{branch}"],
                ["git", "-C", repo_path, "reset", "--hard", f"origin/{branch}"],
                ["git", "-C", repo_path, "clean", "-fd"],
            ]
            ok = True
            for cmd in commands:
                rs = _run_cmd(cmd, timeout=90)
                if rs.returncode != 0:
                    ok = False
                    result["error"] = f"git_sync_failed:{rs.stderr.strip()[:120]}"
                    break
            result["upgraded"] = ok
        elif sync_mode == "rebase":
            pull = _run_cmd(["git", "-C", repo_path, "pull", "--rebase", "origin", branch], timeout=90)
            result["upgraded"] = pull.returncode == 0
            if pull.returncode != 0:
                result["error"] = f"git_pull_failed:{pull.stderr.strip()[:120]}"
        else:
            pull = _run_cmd(["git", "-C", repo_path, "pull", "--ff-only", "origin", branch], timeout=60)
            result["upgraded"] = pull.returncode == 0
            if pull.returncode != 0:
                result["error"] = f"git_pull_failed:{pull.stderr.strip()[:120]}"

    ok, reason = _check_daily_quote_data()
    result["daily_data_ok"] = ok
    result["daily_data_reason"] = reason
    return result


def _weekly_state_file():
    return os.getenv("WEEKLY_STATE_FILE", "logs/weekly_ops_state.json")


def save_weekly_state(payload):
    path = _weekly_state_file()
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2)


def load_weekly_state():
    path = _weekly_state_file()
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _normalize_title(title):
    title = re.sub(r"\s+", " ", (title or "").strip())
    return title.replace("\u3000", " ")[:180]


def _dedupe_news(items):
    seen = set()
    out = []
    for row in items:
        title = _normalize_title(row.get("title", ""))
        if not title:
            continue
        key = title.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "title": title,
                "link": (row.get("link") or "").strip(),
                "source_url": (row.get("source_url") or "").strip(),
                "source": (row.get("source") or "").strip(),
                "published_at": (row.get("published_at") or "").strip(),
                "category": (row.get("category") or "").strip(),
            }
        )
    return out


def _parse_datetime_any(value):
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        pass
    try:
        return parsedate_to_datetime(text)
    except Exception:
        return None


def _filter_news_by_age(items):
    max_age_days = int(os.getenv("NEWS_MAX_AGE_DAYS", "3"))
    now = now_bj()
    out = []
    for row in items:
        dt = _parse_datetime_any(row.get("published_at"))
        if dt is None:
            out.append(row)
            continue
        # Normalize naive/aware datetime differences.
        if dt.tzinfo is None:
            delta = now.replace(tzinfo=None) - dt
        else:
            delta = now.astimezone(dt.tzinfo) - dt
        if delta <= timedelta(days=max_age_days):
            out.append(row)
    return out


def _contains_cn(text):
    return bool(CN_CHAR_RE.search(text or ""))


def _is_a_share_related(text):
    lower = (text or "").lower()
    return any(keyword.lower() in lower for keyword in A_SHARE_KEYWORDS)


def _is_policy_related(text):
    lower = (text or "").lower()
    return any(keyword.lower() in lower for keyword in POLICY_KEYWORDS)


def _is_global_related(text):
    lower = (text or "").lower()
    return any(keyword.lower() in lower for keyword in GLOBAL_KEYWORDS)


def _tag_news_category(row):
    title = row.get("title", "")
    source = (row.get("source") or "").lower()
    if _is_policy_related(title):
        return "policy"
    if _is_global_related(title) or source in {"wallstreetcn", "reuters", "bloomberg", "cnbc", "bbc", "marketwatch"}:
        return "global"
    return "domestic"


def _filter_news(items, require_chinese=True, require_a_share=True):
    out = []
    for row in items:
        title = row.get("title", "")
        category = row.get("category", "")
        if require_chinese and not _contains_cn(title):
            continue
        if require_a_share and category == "domestic" and not _is_a_share_related(title):
            continue
        out.append(row)
    return _dedupe_news(out)


def _fetch_news_from_rss(limit):
    raw = os.getenv(
        "FINANCE_NEWS_RSS_URLS",
        "",
    )
    urls = [x.strip() for x in raw.split(",") if x.strip()]
    items = []
    for url in urls:
        try:
            response = requests.get(url, timeout=8)
            response.raise_for_status()
            root = ElementTree.fromstring(response.content)
            for node in root.findall(".//item"):
                title = _normalize_title(node.findtext("title") or "")
                link = (node.findtext("link") or "").strip()
                published_at = (node.findtext("pubDate") or node.findtext("published") or "").strip()
                if title:
                    items.append(
                        {
                            "title": title,
                            "link": link,
                            "source_url": url,
                            "source": "rss",
                            "published_at": published_at,
                            "category": "",
                        }
                    )
                if len(items) >= limit * 3:
                    break
            if len(items) >= limit * 3:
                break
        except Exception:
            continue
    return _dedupe_news(items)[:limit]


def _extract_json_news_items(payload):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    if isinstance(payload.get("result"), dict) and isinstance(payload["result"].get("data"), list):
        return payload["result"]["data"]
    for key in ["articles", "data", "results", "items", "news"]:
        if isinstance(payload.get(key), list):
            return payload[key]
    return []


def _fetch_news_from_json(limit):
    raw = os.getenv(
        "FINANCE_NEWS_JSON_URLS",
        "",
    )
    urls = [x.strip() for x in raw.split(",") if x.strip()]
    items = []
    for url in urls:
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            payload = response.json()
            rows = _extract_json_news_items(payload)
            for row in rows:
                if not isinstance(row, dict):
                    continue
                title = _normalize_title(
                    row.get("title")
                    or row.get("name")
                    or row.get("headline")
                    or row.get("description")
                    or ""
                )
                link = (
                    row.get("url")
                    or row.get("link")
                    or row.get("sourceurl")
                    or row.get("sourceUrl")
                    or ""
                )
                ctime = row.get("ctime")
                if ctime:
                    try:
                        from datetime import datetime
                        published_at = datetime.fromtimestamp(int(ctime)).strftime("%Y-%m-%d %H:%M")
                    except Exception:
                        published_at = str(ctime)
                else:
                    published_at = (
                        row.get("publishedAt")
                        or row.get("published_at")
                        or row.get("pubDate")
                        or row.get("datetime")
                        or row.get("date")
                        or ""
                    )
                source = (
                    row.get("media_name")
                    or row.get("mediaName")
                    or row.get("source")
                    or "json"
                )
                if title:
                    items.append(
                        {
                            "title": title,
                            "link": str(link).strip(),
                            "source_url": url,
                            "source": source,
                            "published_at": str(published_at).strip() if published_at else "",
                            "category": "",
                        }
                    )
                if len(items) >= limit * 3:
                    break
            if len(items) >= limit * 3:
                break
        except Exception:
            continue
    return _dedupe_news(items)[:limit]


def _fetch_news_from_tushare(limit):
    token = os.getenv("TUSHARE_TOKEN", "").strip()
    if not token:
        return []
    try:
        import tushare as ts
    except Exception:
        return []
    items = []
    end_dt = now_bj()
    start_dt = end_dt - timedelta(days=2)
    start = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    end = end_dt.strftime("%Y-%m-%d %H:%M:%S")
    sources = ["10jqka", "eastmoney", "sina", "wallstreetcn"]
    try:
        pro = ts.pro_api(token)
        for src in sources:
            try:
                df = pro.news(
                    src=src,
                    start_date=start,
                    end_date=end,
                    fields="datetime,title,content,src",
                )
            except TypeError:
                df = pro.news(src=src, start_date=start, end_date=end)
            except Exception:
                continue
            if df is None or getattr(df, "empty", True):
                continue
            for _, row in df.iterrows():
                title = _normalize_title(str(row.get("title", "") or ""))
                if not title:
                    continue
                items.append(
                    {
                        "title": title,
                        "link": "",
                        "source_url": "tushare:news",
                        "source": str(row.get("src", src) or src),
                        "published_at": str(row.get("datetime", "") or ""),
                        "category": "",
                    }
                )
                if len(items) >= limit * 4:
                    break
            if len(items) >= limit * 4:
                break
    except Exception:
        return []
    return _dedupe_news(items)[: limit * 3]


def _fetch_news_from_eastmoney(limit):
    items = []
    try:
        url = "https://np-anotice-stock.eastmoney.com/api/security/ann"
        params = {
            "sr": -1,
            "page_size": limit,
            "page_index": 1,
            "ann_type": "A,SHSZ,SH,SZ",
            "client_source": "web",
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.eastmoney.com",
        }
        response = requests.get(url, timeout=10, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        rows = data.get("data", {}).get("list", [])
        for row in rows:
            title = _normalize_title(str(row.get("title", "") or ""))
            if title:
                items.append({
                    "title": title,
                    "link": f"https://np-anotice-stock.eastmoney.com/api/security/ann/detail?ann_id={row.get('art_id')}",
                    "source_url": "eastmoney",
                    "source": "东方财富公告",
                    "published_at": str(row.get("notice_date", "") or ""),
                    "category": "policy",
                })
    except Exception:
        pass
    return items


def _fetch_news_from_xueqiu(limit):
    items = []
    try:
        url = "https://xueqiu.com/query/v1/search/status.json"
        params = {
            "q": "A股 OR 沪指 OR 深成指 OR 创业板",
            "count": limit,
            "page": 1,
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        }
        response = requests.get(url, timeout=10, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        for item in (data.get("statuses") or [])[:limit]:
            title = _normalize_title(str(item.get("title") or item.get("text") or ""))
            if title and len(title) > 5:
                sid = item.get("id", "")
                items.append({
                    "title": title[:200],
                    "link": f"https://xueqiu.com/{sid}",
                    "source_url": "xueqiu",
                    "source": "雪球",
                    "published_at": "",
                    "category": "",
                })
    except Exception:
        pass
    return items


def _fetch_news_from_sina(limit):
    items = []
    raw_urls = os.getenv("FINANCE_NEWS_JSON_URLS", "").strip()
    if not raw_urls:
        return items
    urls = [x.strip() for x in raw_urls.split(",") if x.strip()]
    for url in urls:
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": "https://finance.sina.com.cn",
            }
            response = requests.get(url, timeout=10, headers=headers)
            response.raise_for_status()
            payload = response.json()
            rows = _extract_json_news_items(payload)
            for row in rows:
                if not isinstance(row, dict):
                    continue
                title = _normalize_title(
                    row.get("title") or row.get("intro") or ""
                )
                link = row.get("url") or row.get("wapurl") or ""
                ctime = row.get("ctime")
                published_at = ""
                if ctime:
                    try:
                        from datetime import datetime
                        published_at = datetime.fromtimestamp(int(ctime)).strftime("%Y-%m-%d %H:%M")
                    except Exception:
                        published_at = str(ctime)
                media_name = row.get("media_name") or "新浪财经"
                if title:
                    items.append({
                        "title": title,
                        "link": str(link).strip(),
                        "source_url": "sina",
                        "source": str(media_name).strip(),
                        "published_at": published_at,
                        "category": "",
                    })
                if len(items) >= limit * 3:
                    break
            if len(items) >= limit * 3:
                break
        except Exception:
            continue
    return items


def _fetch_news_from_tonghuashun(limit):
    items = []
    try:
        url = "https://news.10jqka.com.cn/public/nc/article/list/"
        params = {"page": 1, "limit": limit}
        response = requests.get(url, timeout=10, params=params)
        response.raise_for_status()
        data = response.json()
        for item in data.get("data", []):
            title = _normalize_title(item.get("title", ""))
            if title:
                items.append({
                    "title": title,
                    "link": item.get("url", ""),
                    "source_url": "tonghuashun",
                    "source": "同花顺",
                    "published_at": item.get("create_time", ""),
                    "category": "",
                })
    except Exception:
        pass
    return items


def _fetch_news_from_policy_json(limit):
    raw = os.getenv("POLICY_NEWS_JSON_URLS", "").strip()
    urls = [x.strip() for x in raw.split(",") if x.strip()]
    items = []
    for url in urls:
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            payload = response.json()
            rows = _extract_json_news_items(payload)
            for row in rows:
                if not isinstance(row, dict):
                    continue
                title = _normalize_title(
                    row.get("title")
                    or row.get("name")
                    or row.get("headline")
                    or row.get("description")
                    or ""
                )
                if not title:
                    continue
                items.append(
                    {
                        "title": title,
                        "link": str(row.get("url") or row.get("link") or "").strip(),
                        "source_url": url,
                        "source": str(row.get("source") or row.get("src") or "policy-json").strip(),
                        "published_at": str(
                            row.get("publishedAt")
                            or row.get("published_at")
                            or row.get("pubDate")
                            or row.get("datetime")
                            or row.get("date")
                            or ""
                        ).strip(),
                        "category": "policy",
                    }
                )
                if len(items) >= limit * 3:
                    break
        except Exception:
            continue
    return _dedupe_news(items)[: limit * 2]


def _assign_categories(items):
    out = []
    for row in items:
        new_row = dict(row)
        if not new_row.get("category"):
            new_row["category"] = _tag_news_category(new_row)
        out.append(new_row)
    return out


def fetch_finance_news(limit=5):
    require_cn = os.getenv("NEWS_REQUIRE_CHINESE", "true").lower() == "true"
    require_a_share = os.getenv("NEWS_REQUIRE_A_SHARE", "true").lower() == "true"
    relax_to_cn_only = os.getenv("NEWS_RELAX_TO_CN_ONLY", "true").lower() == "true"

    all_items = []
    
    news_fetchers = [
        _fetch_news_from_sina,
        _fetch_news_from_eastmoney,
        _fetch_news_from_xueqiu,
        _fetch_news_from_rss,
    ]
    
    for fetcher in news_fetchers:
        try:
            rows = fetcher(limit=limit)
            if rows:
                all_items.extend(rows)
        except Exception:
            continue
    
    if not all_items:
        return []
    
    all_items = _dedupe_news(all_items)
    all_items = _filter_news_by_age(all_items)
    all_items = _assign_categories(all_items)
    
    strict = _filter_news(
        all_items,
        require_chinese=require_cn,
        require_a_share=require_a_share,
    )
    if strict:
        return strict[:limit]

    if relax_to_cn_only:
        cn_only = _filter_news(all_items, require_chinese=require_cn, require_a_share=False)
        if cn_only:
            return cn_only[:limit]

    return all_items[:limit]


def build_news_digest(news_items):
    if not news_items:
        return "未获取到财经新闻（请检查 FINANCE_NEWS_RSS_URLS、FINANCE_NEWS_JSON_URLS、TUSHARE_TOKEN）"
    sections = [
        ("domestic", "国内财经"),
        ("global", "国际财经"),
        ("policy", "政策新闻"),
    ]
    lines = []
    for key, label in sections:
        bucket = [row for row in news_items if row.get("category") == key]
        if not bucket:
            continue
        lines.append(f"【{label}】")
        for idx, row in enumerate(bucket, start=1):
            source = (row.get("source") or "").strip()
            if source:
                lines.append(f"{idx}. [{source}] {row['title']}")
            else:
                lines.append(f"{idx}. {row['title']}")
        lines.append("")
    if lines:
        return "\n".join(lines).strip()
    lines = []
    for idx, row in enumerate(news_items[:], start=1):
        source = (row.get("source") or "").strip()
        if source:
            lines.append(f"{idx}. [{source}] {row['title']}")
        else:
            lines.append(f"{idx}. {row['title']}")
    return "\n".join(lines)
