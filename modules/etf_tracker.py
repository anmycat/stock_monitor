import os
import time
import logging
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

from .utils import request_with_throttle, to_float, get_cache_ttl

logger = logging.getLogger("guardian")


_ETF_HOLDINGS_CACHE = {}
_PREV_CLOSE_CACHE = {"sh000001": None, "sz399001": None}
_ETF_KLINE_CACHE = {}
_ETF_STOCK_ALERT_CACHE = {}
_ETF_STATE_CACHE = {}
_ETF_HOLDCAP_BLOCKED_UNTIL = {}
_ETF_DIFF_STATE_CACHE = {"data": None, "ts": 0.0}


def _normalize_code(code: str) -> str:
    text = str(code or "").strip().lower()
    if text.startswith(("sh", "sz")):
        return text
    if not text:
        return text
    return f"sh{text}" if text.startswith(("5", "6", "9")) else f"sz{text}"


def _normalize_component_code(code: str) -> str:
    text = _normalize_code(code)
    if len(text) < 8:
        return text
    code6 = text[2:]
    if code6.startswith(("5", "6", "9")):
        return f"sh{code6}"
    if code6.startswith(("0", "1", "2", "3")):
        return f"sz{code6}"
    return text


def _infer_market_from_code(code6: str, market_hint: Optional[str] = None) -> str:
    code = str(code6 or "").strip()
    if code.startswith(("5", "6", "9")):
        return "sh"
    if code.startswith(("0", "1", "2", "3")):
        return "sz"

    hint = str(market_hint or "").strip().lower()
    if hint.startswith(("sh", "sz")):
        return hint[:2]
    if hint.startswith("1.") or hint in {"1", "ss"}:
        return "sh"
    if hint.startswith("0.") or hint in {"0", "sz"}:
        return "sz"

    return "sz"


def _extract_market_hint(raw: str) -> Optional[str]:
    text = str(raw or "").strip().lower()
    if not text:
        return None
    if text.startswith(("sh", "sz")):
        return text[:2]
    if text.startswith(("1.", "0.")):
        return "sh" if text.startswith("1.") else "sz"
    if ".ss" in text:
        return "sh"
    if ".sz" in text:
        return "sz"
    if "sh" in text:
        return "sh"
    if "sz" in text:
        return "sz"
    return None


def _looks_like_code_name(name: str, code: str) -> bool:
    label = str(name or "").strip().lower()
    if not label:
        return True
    norm = _normalize_code(code)
    code6 = norm[2:] if len(norm) >= 8 else norm
    return label in {norm, code6, str(code or "").strip().lower()}


def _to_secid(code: str) -> str:
    norm = _normalize_code(code)
    if norm.startswith("sh"):
        return f"1.{norm[2:]}"
    if norm.startswith("sz"):
        return f"0.{norm[2:]}"
    return norm


def _format_flow_text(main_total: Optional[float]) -> str:
    if main_total is None:
        return "主力流向NA"
    flow_yi = float(main_total) / 100000000.0
    if flow_yi > 0:
        return f"主力净流入{flow_yi:.2f}亿"
    if flow_yi < 0:
        return f"主力净流出{abs(flow_yi):.2f}亿"
    return "主力流向持平"


def _holdcap_is_blocked(etf_code: str) -> bool:
    now = time.time()
    until = float(_ETF_HOLDCAP_BLOCKED_UNTIL.get(etf_code, 0.0) or 0.0)
    return until > now


def _block_holdcap(etf_code: str, seconds: Optional[int] = None):
    cooldown = seconds
    if cooldown is None:
        cooldown = int(os.getenv("ETF_HOLDCAP_404_COOLDOWN_SECONDS", "21600"))
    _ETF_HOLDCAP_BLOCKED_UNTIL[etf_code] = time.time() + max(300, int(cooldown))


def _holdings_state_file() -> str:
    log_dir = os.getenv("LOG_DIR", "logs")
    os.makedirs(log_dir, exist_ok=True)
    return os.getenv(
        "ETF_HOLDINGS_STATE_FILE", os.path.join(log_dir, "etf_holdings_state.json")
    )


def _load_holdings_state() -> Dict:
    now = time.time()
    cache_ttl = max(10, int(os.getenv("ETF_HOLDINGS_STATE_CACHE_SECONDS", "60")))
    if (
        _ETF_DIFF_STATE_CACHE.get("data") is not None
        and (now - float(_ETF_DIFF_STATE_CACHE.get("ts", 0.0))) < cache_ttl
    ):
        return _ETF_DIFF_STATE_CACHE.get("data") or {}
    path = _holdings_state_file()
    if not os.path.exists(path):
        _ETF_DIFF_STATE_CACHE["data"] = {}
        _ETF_DIFF_STATE_CACHE["ts"] = now
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            data = {}
    except Exception:
        data = {}
    _ETF_DIFF_STATE_CACHE["data"] = data
    _ETF_DIFF_STATE_CACHE["ts"] = now
    return data


def _save_holdings_state(state: Dict):
    if not isinstance(state, dict):
        return
    path = _holdings_state_file()
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
    except Exception:
        return
    _ETF_DIFF_STATE_CACHE["data"] = state
    _ETF_DIFF_STATE_CACHE["ts"] = time.time()


def _snapshot_hold_pct_map(holdings: List[Dict]) -> Dict[str, float]:
    snapshot = {}
    for row in holdings or []:
        code = _normalize_component_code(row.get("code", ""))
        hold_pct = to_float(row.get("hold_pct"))
        if not code or hold_pct is None:
            continue
        snapshot[code] = float(hold_pct)
    return snapshot


def get_etf_holdings(etf_code: str) -> List[Dict]:
    """获取ETF持仓股
    注意：ECS环境可能无法访问EastMoney API，会返回空列表
    增加重试机制和动态缓存
    """
    # 盘中5分钟缓存，盘后1小时缓存
    from .market import now_bj

    now = now_bj()
    is_trading_hours = 9 <= now.hour <= 15
    cache_ttl = 300 if is_trading_hours else 3600  # 盘中5分钟，盘后1小时

    cache_key = f"etf_{etf_code}"
    current_time = time.time()

    if (
        _ETF_HOLDINGS_CACHE.get(cache_key)
        and (current_time - _ETF_HOLDINGS_CACHE.get(f"{cache_key}_ts", 0)) < cache_ttl
    ):
        return _ETF_HOLDINGS_CACHE.get(cache_key, [])

    if _holdcap_is_blocked(etf_code):
        logger.debug("etf_holdcap_skipped cooldown etf=%s", etf_code)
        return _ETF_HOLDINGS_CACHE.get(cache_key, [])

    # 重试机制：最多重试2次
    max_retries = max(1, int(os.getenv("ETF_HOLDINGS_MAX_RETRIES", "1")))
    timeout_seconds = max(3, int(os.getenv("ETF_HOLDINGS_TIMEOUT_SECONDS", "8")))
    last_error = None

    for attempt in range(max_retries):
        try:
            secid = _to_secid(etf_code)

            url = "https://push2.eastmoney.com/api/qt/stock/holdcap/list"
            params = {
                "secid": secid,
                "pn": 1,
                "pz": 50,
                "po": 1,
                "np": 1,
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                "fid": "f3",
                "fields": "f2,f3,f4,f12,f14,f21,f22,f23,f24,f25,f26",
            }
            response = request_with_throttle(
                url, timeout=timeout_seconds, params=params
            )
            response.raise_for_status()
            payload = response.json()

            if not payload or not payload.get("data", {}).get("diff"):
                logger.warning(f"etf_holdings_api_empty etf={etf_code}")
                return []
            diff = ((payload.get("data") or {}).get("diff")) or []

            holdings = []
            for row in diff:
                if not isinstance(row, dict):
                    continue
                code = str(row.get("f12") or "").strip()
                name = str(row.get("f14") or "").strip()
                if not code or not name:
                    continue

                holdings.append(
                    {
                        "code": code,
                        "name": name,
                        "hold_pct": to_float(row.get("f2")),
                        "hold_amount": to_float(row.get("f4")),
                        "change_pct": to_float(row.get("f3"), scale=100),
                        "change_amount": to_float(row.get("f24")),
                        "market_value": to_float(row.get("f21")),
                        "type": row.get("f23"),
                    }
                )

            _ETF_HOLDINGS_CACHE[cache_key] = holdings
            _ETF_HOLDINGS_CACHE[f"{cache_key}_ts"] = current_time
            return holdings

        except Exception as e:
            last_error = e
            text = str(e)
            if "404" in text or "Not Found" in text:
                _block_holdcap(etf_code)
                logger.info("etf_holdcap_404_blocked etf=%s", etf_code)
                break
            if attempt < max_retries - 1:
                logger.info(
                    f"etf_holdings_retry etf={etf_code} attempt={attempt + 1}/{max_retries}"
                )
                time.sleep(1)  # 重试前等待1秒

    # 所有重试都失败，记录错误并返回缓存数据
    logger.warning(f"etf_holdings_failed {etf_code}: {last_error}")
    return _ETF_HOLDINGS_CACHE.get(cache_key, [])


# Stock name mapping for fallback when API fails
_ETF_FALLBACK_STOCK_NAMES = {
    # 银行 sector
    "sh600036": "招商银行",
    "sh601988": "中国银行",
    "sh600015": "华夏银行",
    "sz000001": "平安银行",
    # 白酒 sector
    "sh600519": "贵州茅台",
    "sz000858": "五粮液",
    "sz000568": "泸州老窖",
    "sh603589": "江小白",
    # 新能源汽车 sector
    "sz002594": "比亚迪",
    "sh600418": "比亚迪电子",
    "sz300750": "宁德时代",
    "sh601238": "广汽集团",
    # 保险 sector
    "sh601318": "中国平安",
    "sh601319": "平安保险",
    "sh601601": "中国人寿",
    # 电力 sector
    "sh600900": "长江电力",
    "sh600021": "国电电力",
    "sh600795": "国投电力",
    "sz003816": "沪电股份",
    # 常用成分股
    "sh600000": "浦发银行",
    "sh600016": "民生银行",
    "sh600030": "中信银行",
    "sh600519": "贵州茅台",
    "sh600887": "伊利股份",
    "sh600898": "东方财富",
    "sh600900": "长江电力",
    "sh600905": "东吴证券",
    "sh601166": "兴业银行",
    "sh601318": "中国平安",
    "sh601328": "交通银行",
    "sh601988": "中国银行",
}

_ETF_FALLBACK_COMPONENTS = {
    "sh510300": [
        "sh600519",
        "sh600036",
        "sh601318",
        "sh601988",
        "sh600016",
        "sh600030",
        "sh601166",
        "sh600887",
        "sh600000",
        "sh601328",
    ],
    "sh510500": [
        "sh600519",
        "sh600036",
        "sh600016",
        "sh600030",
        "sh600887",
        "sh600009",
        "sh600019",
        "sh600028",
        "sh600050",
        "sh600104",
    ],
    "sh512880": [
        "sh600030",
        "sh600837",
        "sh601066",
        "sh601688",
        "sh600999",
        "sh601788",
        "sh600958",
        "sh600369",
        "sh601555",
        "sh601881",
    ],
    "sh513050": [
        "sh600703",
        "sh600522",
        "sh600183",
        "sh600468",
        "sh600875",
        "sh601012",
        "sh600089",
        "sh600438",
        "sh600276",
        "sh600547",
    ],
    "sz159919": [
        "sz300750",
        "sz300059",
        "sz300015",
        "sz300002",
        "sz300003",
        "sz300124",
        "sz300014",
        "sz300122",
        "sz300408",
        "sz300676",
    ],
    "sz159995": [
        "sz000001",
        "sz000002",
        "sz000009",
        "sz000012",
        "sz000021",
        "sz000023",
        "sz000025",
        "sz000027",
        "sz000028",
        "sz000030",
    ],
    "sz159941": [
        "sz000001",
        "sz000002",
        "sz000009",
        "sz000012",
        "sz000021",
    ],
    "sh588000": [
        "sh600519",
        "sh600036",
        "sh601318",
        "sh601988",
        "sh600016",
    ],
}


def _get_etf_holdings_akshare(etf_code: str) -> List[Dict]:
    """使用EastMoney获取ETF成分股"""
    import re

    etf_num = etf_code.replace("sh", "").replace("sz", "")
    holdings = []

    try:
        url = f"https://fund.eastmoney.com/pingzhongdata/{etf_num}.js"
        from .utils import request_with_throttle

        resp = request_with_throttle(url, timeout=10)
        resp.encoding = "gbk"
        text = resp.text

        stock_codes_pattern = r"var stockCodes\s*=\s*\[([^\]]+)\]"
        match = re.search(stock_codes_pattern, text)
        if not match:
            raise ValueError(f"No holdings data for ETF {etf_num}")

        codes_str = match.group(1)
        codes = re.findall(r'"(\d+)"', codes_str)

        stock_new_pattern = r"var stockCodesNew\s*=\s*\[([^\]]+)\]"
        match_new = re.search(stock_new_pattern, text)

        if match_new:
            codes_new = re.findall(r'"([^"]+)"', match_new.group(1))
        else:
            codes_new = []

        for i, code_full in enumerate(codes):
            code_raw = code_full.strip().strip('"')

            if not code_raw:
                continue

            if code_raw.isdigit():
                if len(code_raw) == 8:
                    code_num = code_raw[:6]
                elif len(code_raw) == 7:
                    code_num = code_raw[:-1]
                elif len(code_raw) == 6:
                    code_num = code_raw
                else:
                    continue
            else:
                continue

            if len(code_num) != 6:
                continue

            hint = _extract_market_hint(codes_new[i]) if i < len(codes_new) else None
            market = _infer_market_from_code(code_num, hint)
            stock_code = f"{market}{code_num}"

            hold_pct = max(10 - i, 1) * 0.5

            try:
                from .market import get_quote

                q = get_quote(stock_code)
                stock_name = q.get("name", code_num) if q else code_num
            except Exception:
                stock_name = _ETF_FALLBACK_STOCK_NAMES.get(stock_code, code_num)

            holdings.append(
                {
                    "code": stock_code,
                    "name": stock_name,
                    "hold_pct": hold_pct,
                    "change_pct": 0,
                    "change_amount": 0,
                }
            )

        logger.info(f"etf_holdings_eastmoney {etf_code}: {len(holdings)} stocks")

    except Exception as e:
        logger.warning(f"etf_holdings_failed {etf_code}: {e}")
        return []

    return holdings


def _get_etf_fallback_holdings(etf_code: str) -> List[Dict]:
    """使用预设成分股列表作为备选"""
    from .market import get_quote

    holdings = _get_etf_holdings_akshare(etf_code)
    if holdings:
        return holdings

    logger.info(f"etf_using_fallback_components {etf_code}")

    components = _ETF_FALLBACK_COMPONENTS.get(etf_code, [])
    holdings = []

    for code in components:
        try:
            q = get_quote(code)
            if q and q.get("price"):
                # Use fallback name mapping if available, otherwise use quote name or code
                fallback_name = _ETF_FALLBACK_STOCK_NAMES.get(code)
                name = (
                    fallback_name
                    if fallback_name
                    else (q.get("name", code) if q else code)
                )
                holdings.append(
                    {
                        "code": code,
                        "name": name,
                        "hold_pct": 0,
                        "change_pct": q.get("pct_change", 0) or 0,
                        "change_amount": 0,
                    }
                )
        except Exception:
            continue

    return holdings


def get_etf_holdings_with_fallback(etf_code: str) -> List[Dict]:
    """获取ETF持仓，优先使用EastMoney API，失败时使用预设成分股"""
    cache_ttl = get_cache_ttl("etf_holdings", 3600)
    now = time.time()
    cache_key = f"etf_fallback_{etf_code}"

    if (
        _ETF_HOLDINGS_CACHE.get(cache_key)
        and (now - _ETF_HOLDINGS_CACHE.get(f"{cache_key}_ts", 0)) < cache_ttl
    ):
        return _ETF_HOLDINGS_CACHE.get(cache_key, [])

    holdings = get_etf_holdings(etf_code)
    if holdings:
        return holdings

    holdings = _get_etf_fallback_holdings(etf_code)

    _ETF_HOLDINGS_CACHE[cache_key] = holdings
    _ETF_HOLDINGS_CACHE[f"{cache_key}_ts"] = now

    return holdings


def scan_etf_holdings_changes(etf_codes: List[str]) -> Dict:
    """扫描ETF持仓股变化
    优先使用EastMoney API，失败时使用预设成分股+实时行情作为备选
    """
    changes = {
        "increased": [],
        "decreased": [],
        "new": [],
        "neutral": [],
    }

    max_codes = max(1, int(os.getenv("ETF_HOLDINGS_SCAN_MAX_CODES", "20")))
    etf_codes = list(etf_codes or [])[:max_codes]
    workers = max(1, int(os.getenv("ETF_HOLDINGS_SCAN_WORKERS", "2")))

    has_change_data = False
    state = _load_holdings_state()
    diff_min_pct = float(os.getenv("ETF_HOLDINGS_DIFF_MIN_PCT", "0.02"))
    new_min_pct = float(os.getenv("ETF_HOLDINGS_NEW_MIN_PCT", "0.50"))

    holdings_map = {}
    if workers <= 1 or len(etf_codes) <= 1:
        for code in etf_codes:
            holdings_map[code] = get_etf_holdings_with_fallback(code)
    else:
        with ThreadPoolExecutor(max_workers=min(workers, len(etf_codes))) as executor:
            future_map = {
                executor.submit(get_etf_holdings_with_fallback, code): code
                for code in etf_codes
            }
            for future in as_completed(future_map):
                code = future_map[future]
                try:
                    holdings_map[code] = future.result()
                except Exception:
                    holdings_map[code] = []

    for etf_code in etf_codes:
        holdings = holdings_map.get(etf_code, [])
        has_change_data_etf = False
        prev_snapshot = {}
        prev_entry = state.get(etf_code)
        if isinstance(prev_entry, dict):
            prev_snapshot = prev_entry.get("holds") or {}

        for stock in holdings:
            change_pct = to_float(stock.get("change_pct")) or 0
            change_amount = to_float(stock.get("change_amount")) or 0

            if change_amount > 0:
                has_change_data = True
                has_change_data_etf = True
                changes["increased"].append(
                    {
                        "etf": etf_code,
                        "code": stock.get("code"),
                        "name": stock.get("name"),
                        "change_pct": change_pct,
                        "change_amount": change_amount,
                        "hold_pct": stock.get("hold_pct"),
                        "change_mode": "amount",
                    }
                )
            elif change_amount < 0:
                has_change_data = True
                has_change_data_etf = True
                changes["decreased"].append(
                    {
                        "etf": etf_code,
                        "code": stock.get("code"),
                        "name": stock.get("name"),
                        "change_pct": change_pct,
                        "change_amount": change_amount,
                        "hold_pct": stock.get("hold_pct"),
                        "change_mode": "amount",
                    }
                )

        # 变更字段缺失时，使用前后快照差分识别增减持
        if not has_change_data_etf and prev_snapshot:
            for stock in holdings:
                code = _normalize_component_code(stock.get("code", ""))
                if not code:
                    continue
                hold_pct = to_float(stock.get("hold_pct"))
                if hold_pct is None:
                    continue
                prev_hold = to_float(prev_snapshot.get(code))
                if prev_hold is None:
                    # 新进持仓（按权重最小阈值过滤）
                    if hold_pct >= new_min_pct:
                        has_change_data = True
                        has_change_data_etf = True
                        changes["new"].append(
                            {
                                "etf": etf_code,
                                "code": stock.get("code"),
                                "name": stock.get("name"),
                                "change_pct": 0,
                                "change_amount": hold_pct,
                                "hold_pct": hold_pct,
                                "change_mode": "hold_pct_new",
                            }
                        )
                    continue
                diff = float(hold_pct) - float(prev_hold)
                if abs(diff) < diff_min_pct:
                    continue
                row = {
                    "etf": etf_code,
                    "code": stock.get("code"),
                    "name": stock.get("name"),
                    "change_pct": 0,
                    "change_amount": diff,
                    "hold_pct": hold_pct,
                    "change_mode": "hold_pct_delta",
                }
                has_change_data = True
                has_change_data_etf = True
                if diff > 0:
                    changes["increased"].append(row)
                else:
                    changes["decreased"].append(row)

        if not has_change_data_etf:
            top_holdings = sorted(
                holdings, key=lambda x: x.get("hold_pct") or 0, reverse=True
            )[:10]
            for stock in top_holdings:
                changes["neutral"].append(
                    {
                        "etf": etf_code,
                        "code": stock.get("code"),
                        "name": stock.get("name"),
                        "change_pct": 0,
                        "change_amount": stock.get("hold_pct") or 0,
                        "hold_pct": stock.get("hold_pct"),
                        "_fallback": True,
                        "_reason": "no_rebalance_data",
                        "change_mode": "fallback",
                    }
                )

        state[etf_code] = {
            "ts": int(time.time()),
            "holds": _snapshot_hold_pct_map(holdings),
        }

    _save_holdings_state(state)
    changes["increased"].sort(key=lambda x: x.get("change_amount", 0), reverse=True)
    changes["decreased"].sort(key=lambda x: x.get("change_amount", 0))
    changes["new"].sort(key=lambda x: x.get("change_amount", 0), reverse=True)

    return changes


def get_etf_daily_klines(etf_code: str, days: int = 30) -> List[Dict]:
    cache_key = f"kline_{etf_code}_{days}"
    cache_ttl = get_cache_ttl("etf_kline", 1800)
    now = time.time()
    if (
        cache_key in _ETF_KLINE_CACHE
        and (now - _ETF_KLINE_CACHE[cache_key]["ts"]) < cache_ttl
    ):
        return _ETF_KLINE_CACHE[cache_key]["data"]

    try:
        end_date = time.strftime("%Y%m%d")
        start_date = time.strftime(
            "%Y%m%d", time.localtime(time.time() - max(days, 30) * 24 * 3600)
        )
        url = "https://push2.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": _to_secid(etf_code),
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": "101",
            "fqt": "1",
            "beg": start_date,
            "end": end_date,
            "lmt": days + 10,
        }
        response = request_with_throttle(url, timeout=10, params=params)
        response.raise_for_status()
        payload = response.json()
        rows = []
        for line in (payload.get("data") or {}).get("klines") or []:
            parts = line.split(",")
            if len(parts) < 6:
                continue
            rows.append(
                {
                    "date": parts[0],
                    "open": to_float(parts[1]),
                    "close": to_float(parts[2]),
                    "high": to_float(parts[3]),
                    "low": to_float(parts[4]),
                    "amount": to_float(parts[5]),
                }
            )
        _ETF_KLINE_CACHE[cache_key] = {"ts": now, "data": rows}
        return rows
    except Exception:
        return _ETF_KLINE_CACHE.get(cache_key, {}).get("data", [])


def get_etf_support_snapshot(etf_code: str) -> Dict:
    rows = get_etf_daily_klines(
        etf_code, days=int(os.getenv("ETF_SUPPORT_LOOKBACK_DAYS", "30"))
    )
    closes = [row.get("close") for row in rows if row.get("close") is not None]
    lows = [row.get("low") for row in rows[-10:] if row.get("low") is not None]
    if not closes:
        return {
            "support_price": None,
            "support_basis": "NA",
            "ma5": None,
            "ma10": None,
            "ma20": None,
        }

    def _avg(window: int):
        if len(closes) < window:
            return None
        return sum(closes[-window:]) / float(window)

    ma5 = _avg(5)
    ma10 = _avg(10)
    ma20 = _avg(20)
    low10 = min(lows) if lows else None
    support_candidates = [value for value in (ma20, ma10, low10) if value is not None]
    support_price = max(support_candidates) if support_candidates else None
    return {
        "support_price": support_price,
        "support_basis": "MA20/MA10/10日低点",
        "ma5": ma5,
        "ma10": ma10,
        "ma20": ma20,
        "low10": low10,
        "closes": closes,
    }


def _estimate_component_flow(etf_code: str, days: int, topn: int) -> Dict:
    from .fund_flow import get_stock_fund_flow_days

    holdings = get_etf_holdings(etf_code)
    if not holdings:
        return {"trend": "unknown", "main_total": None, "source": "components"}

    ranked = sorted(holdings, key=lambda x: x.get("hold_pct") or 0, reverse=True)[:topn]
    total_weight = 0.0
    total_flow = 0.0
    used = 0
    for row in ranked:
        code = _normalize_component_code(row.get("code"))
        weight = float(row.get("hold_pct") or 0.0)
        flow = get_stock_fund_flow_days(code, days=days)
        if flow.get("trend") == "error":
            continue
        main_total = flow.get("main_total")
        if main_total is None:
            continue
        if weight <= 0:
            weight = 1.0
        total_weight += weight
        total_flow += float(main_total) * weight
        used += 1

    if used == 0:
        return {"trend": "unknown", "main_total": None, "source": "components"}

    weighted_flow = total_flow / total_weight if total_weight > 0 else total_flow
    trend = (
        "inflow" if weighted_flow > 0 else "outflow" if weighted_flow < 0 else "neutral"
    )
    return {
        "trend": trend,
        "main_total": weighted_flow,
        "source": f"components_top{used}",
    }


def get_etf_flow_snapshot(etf_code: str, days: int = 3) -> Dict:
    from .fund_flow import get_stock_fund_flow_days

    flow = get_stock_fund_flow_days(etf_code, days=days)
    if (
        flow.get("trend") not in {"error", "unknown"}
        and flow.get("main_total") is not None
    ):
        return {
            "trend": flow.get("trend"),
            "main_total": flow.get("main_total"),
            "source": "etf",
            "flow_text": _format_flow_text(flow.get("main_total")),
        }

    fallback = _estimate_component_flow(
        etf_code,
        days=days,
        topn=int(os.getenv("ETF_COMPONENT_FLOW_TOPN", "8")),
    )
    fallback["flow_text"] = _format_flow_text(fallback.get("main_total"))
    return fallback


def _safe_float(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def evaluate_etf_trade_state(etf_code: str, flow_days: int = 3) -> Dict:
    cache_key = f"trade_state_{etf_code}_{flow_days}"
    cache_ttl = get_cache_ttl("etf_panel", 300)
    now = time.time()
    cached = _ETF_STATE_CACHE.get(cache_key)
    if cached and (now - cached.get("ts", 0.0)) < cache_ttl:
        return cached.get("data", {})

    from .market import get_quote

    try:
        quote = get_quote(etf_code) or {}
    except Exception:
        quote = {}
    support = get_etf_support_snapshot(etf_code)
    flow = get_etf_flow_snapshot(etf_code, days=flow_days)

    name = quote.get("name", etf_code)
    price = _safe_float(quote.get("price"))
    pct_change = _safe_float(quote.get("pct_change"))

    support_price = _safe_float(support.get("support_price"))
    ma5 = _safe_float(support.get("ma5"))
    ma10 = _safe_float(support.get("ma10"))
    ma20 = _safe_float(support.get("ma20"))
    distance_pct = None
    if price is not None and support_price not in (None, 0):
        distance_pct = (price - support_price) / support_price * 100.0

    buy_distance = float(os.getenv("ETF_SUPPORT_MAX_DISTANCE_BUY", "3.0"))
    chase_distance = float(os.getenv("ETF_SUPPORT_MAX_DISTANCE_CHASE", "6.0"))
    buy_score = float(os.getenv("ETF_STATE_BUY_SCORE", "70"))
    watch_score = float(os.getenv("ETF_STATE_WATCH_SCORE", "50"))
    avoid_chase_pct = float(os.getenv("ETF_STATE_AVOID_CHASE_PCT", "4.0"))

    score = 50.0
    reasons = []

    if ma5 is not None and ma10 is not None and ma20 is not None:
        if ma5 > ma10 > ma20:
            score += 18
            reasons.append("均线多头")
        elif ma5 < ma10 < ma20:
            score -= 18
            reasons.append("均线空头")
        else:
            reasons.append("均线震荡")

    flow_trend = str(flow.get("trend") or "unknown")
    if flow_trend == "inflow":
        score += 15
        reasons.append("资金净流入")
    elif flow_trend == "outflow":
        score -= 18
        reasons.append("资金净流出")
    elif flow_trend == "neutral":
        reasons.append("资金中性")
    else:
        reasons.append("资金数据不足")

    if distance_pct is not None:
        if distance_pct <= buy_distance:
            score += 15
            reasons.append(f"接近支撑({distance_pct:+.2f}%)")
        elif distance_pct <= chase_distance:
            score += 4
            reasons.append(f"离支撑适中({distance_pct:+.2f}%)")
        else:
            score -= 12
            reasons.append(f"偏离支撑较大({distance_pct:+.2f}%)")
    else:
        reasons.append("支撑位不足")

    if pct_change is not None:
        if pct_change >= avoid_chase_pct:
            score -= 8
            reasons.append("短线不追高")
        elif pct_change <= -2 and flow_trend == "outflow":
            score -= 6
            reasons.append("下跌且流出")

    score = max(0.0, min(100.0, score))
    if score >= buy_score and flow_trend != "outflow":
        status = "BUYABLE"
        status_text = "可买"
        action = "买"
    elif score >= watch_score and flow_trend != "outflow":
        status = "WATCH"
        status_text = "观望"
        action = "等"
    else:
        status = "AVOID"
        status_text = "X不买"
        action = "不买"

    result = {
        "code": etf_code,
        "name": name,
        "price": price,
        "pct_change": pct_change,
        "status": status,
        "status_text": status_text,
        "action": action,
        "score": round(score, 1),
        "distance_to_support_pct": distance_pct,
        "support_price": support_price,
        "flow_trend": flow_trend,
        "flow_text": flow.get("flow_text", _format_flow_text(flow.get("main_total"))),
        "flow_source": flow.get("source", "unknown"),
        "reasons": reasons[:3],
    }
    _ETF_STATE_CACHE[cache_key] = {"ts": now, "data": result}
    return result


def get_etf_trade_states(
    etf_codes: List[str], topn: int = 3, flow_days: int = 3
) -> List[Dict]:
    if not etf_codes:
        return []
    unique_codes = []
    seen = set()
    for code in etf_codes:
        norm = _normalize_code(code)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        unique_codes.append(norm)

    states = []
    for code in unique_codes:
        try:
            states.append(evaluate_etf_trade_state(code, flow_days=flow_days))
        except Exception:
            continue

    rank = {"BUYABLE": 2, "WATCH": 1, "AVOID": 0}
    states.sort(
        key=lambda row: (
            rank.get(str(row.get("status", "WATCH")), 1),
            float(row.get("score") or 0.0),
        ),
        reverse=True,
    )
    return states[: max(1, int(topn))]


def get_etf_stock_alerts(
    etf_codes: List[str], topn: int = 5, quote_source: str = "auto"
) -> Dict:
    """根据ETF持仓变化，生成成分股操作建议

    通过追踪ETF对成分股的增减持变化，判断大资金动向，
    对ETF增持的股票考虑买入，ETF减持的股票考虑卖出。
    """
    cache_key = f"stock_alerts_{'_'.join(etf_codes)}_{topn}"
    cache_ttl = get_cache_ttl("etf_stock_alerts", 300)
    now = time.time()
    if (
        cache_key in _ETF_STOCK_ALERT_CACHE
        and (now - _ETF_STOCK_ALERT_CACHE[cache_key]["ts"]) < cache_ttl
    ):
        return _ETF_STOCK_ALERT_CACHE[cache_key]["data"]

    from .market import get_quote

    changes = scan_etf_holdings_changes(etf_codes)
    increased = changes.get("increased", [])
    decreased = changes.get("decreased", [])
    neutral = changes.get("neutral", [])

    increased_stocks = []
    decreased_stocks = []
    neutral_stocks = []

    def _fetch_stock_quote(stock_item: Dict) -> Dict:
        code = _normalize_component_code(stock_item.get("code", ""))
        raw_name = str(stock_item.get("name", "") or "").strip()
        source_name = str(quote_source or "auto").strip().lower() or "auto"
        try:
            quote = get_quote(code, source=source_name)
        except Exception:
            if source_name != "auto":
                try:
                    quote = get_quote(code, source="auto")
                except Exception:
                    quote = {}
            else:
                quote = {}
        quote_name = str(quote.get("name", "") or "").strip()
        fallback_name = _ETF_FALLBACK_STOCK_NAMES.get(code, code[2:] if len(code) >= 8 else code)
        if quote_name and not _looks_like_code_name(quote_name, code):
            final_name = quote_name
        elif raw_name and not _looks_like_code_name(raw_name, code):
            final_name = raw_name
        else:
            final_name = fallback_name
        change_amount = stock_item.get("change_amount") or 0
        change_mode = stock_item.get("change_mode") or "amount"
        hold_pct = stock_item.get("hold_pct") or 0
        etf_code = stock_item.get("etf", "")
        return {
            "code": code,
            "name": final_name,
            "etf": etf_code,
            "etf_hold_pct": hold_pct,
            "etf_change_amount": change_amount,
            "etf_change_mode": change_mode,
            "price": quote.get("price"),
            "pct_change": quote.get("pct_change"),
            "turnover_rate": quote.get("turnover_rate"),
            "volume": quote.get("volume"),
            "fund_flow": quote.get("main_net_inflow", quote.get("fund_flow")),
            "ma5": quote.get("ma5"),
            "ma10": quote.get("ma10"),
            "ma20": quote.get("ma20"),
        }

    max_workers = int(os.getenv("ETF_SCAN_WORKERS", "5"))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        inc_limit = min(len(increased), topn)
        dec_limit = min(len(decreased), topn)
        neutral_limit = min(len(neutral), topn) if (inc_limit == 0 and dec_limit == 0) else 0

        if inc_limit > 0:
            futures_inc = []
            for item in sorted(
                increased, key=lambda x: x.get("change_amount") or 0, reverse=True
            )[:inc_limit]:
                futures_inc.append(executor.submit(_fetch_stock_quote, item))
            for f in futures_inc:
                increased_stocks.append(f.result())

        if dec_limit > 0:
            futures_dec = []
            for item in sorted(decreased, key=lambda x: x.get("change_amount") or 0)[
                :dec_limit
            ]:
                futures_dec.append(executor.submit(_fetch_stock_quote, item))
            for f in futures_dec:
                decreased_stocks.append(f.result())

        if neutral_limit > 0:
            futures_neutral = []
            for item in sorted(
                neutral, key=lambda x: x.get("hold_pct") or 0, reverse=True
            )[:neutral_limit]:
                futures_neutral.append(executor.submit(_fetch_stock_quote, item))
            for f in futures_neutral:
                neutral_stocks.append(f.result())

    def _build_alert(stock: Dict, direction: str) -> Dict:
        code = stock.get("code", "")
        name = stock.get("name", "")
        etf = stock.get("etf", "")
        change_amt = stock.get("etf_change_amount") or 0
        change_mode = stock.get("etf_change_mode") or "amount"
        hold_pct = stock.get("etf_hold_pct") or 0
        price = to_float(stock.get("price"))
        pct = to_float(stock.get("pct_change"))
        turnover = to_float(stock.get("turnover_rate"))
        fund_flow = stock.get("fund_flow")
        turnover_eval = turnover if turnover is not None else 0.0

        if direction == "buy":
            action = "关注"
            if turnover_eval >= 5 and pct is not None and 0 <= pct <= 9.5:
                suggestion = "ETF增持+量价配合，可关注"
            elif turnover_eval >= 3:
                suggestion = "ETF增持，量能放大"
            else:
                suggestion = "ETF增持，跟踪观察"
        else:
            action = "减仓"
            if turnover_eval >= 5 and pct is not None and pct <= -2:
                suggestion = "ETF减持+放量下跌，建议减仓"
            elif turnover_eval >= 3:
                suggestion = "ETF减持，注意风险"
            else:
                suggestion = "ETF减持，持续观察"

        flow_text = "NA"
        if fund_flow is not None:
            flow_yi = float(fund_flow) / 100000000.0
            if flow_yi > 0:
                flow_text = f"主力净流入{flow_yi:.2f}亿"
            elif flow_yi < 0:
                flow_text = f"主力净流出{abs(flow_yi):.2f}亿"

        price_text = "NA" if price is None else f"{float(price):.3f}"
        pct_text = "NA" if pct is None else f"{float(pct):+.2f}%"
        turnover_text = "NA" if turnover is None else f"{float(turnover):.2f}%"

        return {
            "code": code,
            "name": name,
            "etf": etf,
            "direction": direction,
            "action": action,
            "suggestion": suggestion,
            "price": price_text,
            "pct_change": pct_text,
            "turnover_rate": turnover_text,
            "fund_flow_text": flow_text,
            "etf_hold_pct": (
                f"{float(hold_pct):.2f}%"
                if hold_pct not in (None, "", "NA")
                else "NA"
            ),
            "etf_change_amount": change_amt,
            "etf_change_mode": change_mode,
        }

    def _build_neutral(stock: Dict) -> Dict:
        base = _build_alert(stock, "buy")
        base["direction"] = "watch"
        base["action"] = "观察"
        base["suggestion"] = "暂无调仓明细，按权重成分股观察"
        return base

    result = {
        "increased": [_build_alert(s, "buy") for s in increased_stocks],
        "decreased": [_build_alert(s, "sell") for s in decreased_stocks],
        "neutral": [_build_neutral(s) for s in neutral_stocks],
        "total_increased": len(increased),
        "total_decreased": len(decreased),
    }

    _ETF_STOCK_ALERT_CACHE[cache_key] = {"ts": now, "data": result}
    return result


def get_index_open_price(code: str) -> Optional[Dict]:
    """获取指数开盘价"""
    try:
        secid = code
        if code == "sh000001":
            secid = "1.000001"
        elif code == "sz399001":
            secid = "0.399001"

        url = "https://push2.eastmoney.com/api/qt/stock/get"
        params = {
            "secid": secid,
            "fields": "f43,f44,f45,f46,f50,f58,f170",
        }
        response = request_with_throttle(url, timeout=5, params=params)
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data", {})

        # f46 = 今开, f43 = 最新价, f50 = 昨收
        open_price = to_float(data.get("f46"))
        current_price = to_float(data.get("f43"))
        prev_close = to_float(data.get("f50"))

        if open_price is None:
            return None

        if prev_close is None:
            prev_close = _PREV_CLOSE_CACHE.get(code)

        change = None
        if prev_close and prev_close != 0:
            change = ((current_price - prev_close) / prev_close) * 100

        # 缓存昨收价
        if prev_close:
            _PREV_CLOSE_CACHE[code] = prev_close

        return {
            "code": code,
            "name": data.get("f58", code),
            "open": open_price,
            "current": current_price,
            "prev_close": prev_close,
            "change_pct": change,
        }

    except Exception:
        return None


def get_market_index_summary(indexes: List[str] = None) -> List[Dict]:
    """获取市场主要指数开盘情况"""
    if indexes is None:
        indexes = ["sh000001", "sz399001"]
    results = []

    for code in indexes:
        data = get_index_open_price(code)
        if data:
            results.append(data)

    return results
