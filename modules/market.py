import os
import json
import random
import time
import logging
import io
import threading
from datetime import datetime, timedelta
from contextlib import redirect_stderr, redirect_stdout
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal

from .utils import request_with_throttle, to_float

logger = logging.getLogger("guardian")
_TRADING_DAY_CACHE = {}
_ENRICH_CACHE = {}
_AKSHARE_SPOT_CACHE = {"data": None, "ts": 0.0}
_SOURCE_HEALTH = {}
_CALL_AUCTION_CACHE = {"data": None, "ts": 0.0}
_KLINE_CACHE = {}
_DSA_AVAILABILITY_CACHE = {"available": None, "ts": 0.0}
_BAOSTOCK_LOCK = threading.Lock()
_BAOSTOCK_SESSION = {"ready": False}


def _call_auction_cache_ttl():
    return max(60, int(os.getenv("AUCTION_CACHE_SECONDS", "600")))


def _akshare_spot_cache_ttl():
    return max(5, float(os.getenv("AKSHARE_SPOT_CACHE_SECONDS", "30")))


def _health_state(name):
    if name not in _SOURCE_HEALTH:
        _SOURCE_HEALTH[name] = {
            "success": 0,
            "fail": 0,
            "last_success": 0.0,
            "last_error": 0.0,
            "score": 1.0,
            "consecutive_failures": 0,
            "open_until": 0.0,
            "half_open": False,
            "probe_inflight": False,
        }
    return _SOURCE_HEALTH[name]


def _record_source_result(name, ok):
    state = _health_state(name)
    now = time.time()
    reward = float(os.getenv("SOURCE_SUCCESS_REWARD", "0.05"))
    penalty = float(os.getenv("SOURCE_FAILURE_PENALTY", "0.15"))
    failure_threshold = max(
        1, int(os.getenv("SOURCE_CIRCUIT_FAILURE_THRESHOLD", "3"))
    )
    cooldown_seconds = max(
        30,
        int(
            os.getenv(
                "SOURCE_CIRCUIT_COOLDOWN_SECONDS",
                os.getenv("SOURCE_COOLDOWN_SECONDS", "120"),
            )
        ),
    )

    state["probe_inflight"] = False

    if ok:
        state["success"] += 1
        state["last_success"] = now
        state["score"] = min(1.0, state["score"] + reward)
        state["consecutive_failures"] = 0
        state["open_until"] = 0.0
        state["half_open"] = False
    else:
        state["fail"] += 1
        state["last_error"] = now
        state["score"] = max(0.0, state["score"] - penalty)
        state["consecutive_failures"] = int(state.get("consecutive_failures", 0)) + 1
        if (
            state.get("half_open")
            or int(state.get("consecutive_failures", 0)) >= failure_threshold
        ):
            state["open_until"] = now + cooldown_seconds
            state["half_open"] = False
    return state["score"]


def _should_skip_source(name, mutate=True):
    state = _health_state(name)
    now = time.time()

    # Circuit breaker: OPEN state
    open_until = float(state.get("open_until", 0.0) or 0.0)
    if open_until > now:
        return True

    # Circuit breaker: HALF_OPEN probe (one request only)
    if open_until > 0 and open_until <= now:
        if not mutate:
            return False
        if not state.get("probe_inflight", False):
            state["probe_inflight"] = True
            state["half_open"] = True
            return False
        return True

    min_score = float(os.getenv("SOURCE_MIN_SCORE", "0.2"))
    cooldown = int(os.getenv("SOURCE_COOLDOWN_SECONDS", "120"))
    if state["score"] < min_score and state["last_error"] > 0:
        if (now - state["last_error"]) < cooldown:
            return True
    return False


def _rank_sources(fetchers):
    ranked = []
    for fetcher in fetchers:
        name = fetcher.__name__
        if _should_skip_source(name, mutate=False):
            continue
        state = _health_state(name)
        jitter = random.uniform(0.0, 0.05)
        ranked.append((state["score"] + jitter, fetcher))
    ranked.sort(key=lambda x: x[0], reverse=True)
    return [f for _, f in ranked] or list(fetchers)


def get_source_health_snapshot():
    snapshot = []
    now = time.time()
    for name, state in _SOURCE_HEALTH.items():
        last_success = state.get("last_success", 0.0)
        last_error = state.get("last_error", 0.0)
        open_until = float(state.get("open_until", 0.0) or 0.0)
        if open_until > now:
            circuit_state = "open"
            open_remaining = int(open_until - now)
        elif state.get("half_open"):
            circuit_state = "half_open"
            open_remaining = 0
        else:
            circuit_state = "closed"
            open_remaining = 0
        snapshot.append(
            {
                "name": name,
                "score": round(float(state.get("score", 0.0)), 3),
                "success": int(state.get("success", 0)),
                "fail": int(state.get("fail", 0)),
                "consecutive_failures": int(state.get("consecutive_failures", 0)),
                "last_success_ago": None
                if last_success <= 0
                else int(now - last_success),
                "last_error_ago": None if last_error <= 0 else int(now - last_error),
                "cooldown": _should_skip_source(name, mutate=False),
                "circuit_state": circuit_state,
                "open_remaining": open_remaining,
            }
        )
    snapshot.sort(key=lambda x: x.get("score", 0), reverse=True)
    return snapshot


def _market_tz():
    name = os.getenv("MARKET_TIMEZONE", "Asia/Shanghai").strip() or "Asia/Shanghai"
    try:
        return ZoneInfo(name)
    except Exception:
        return ZoneInfo("Asia/Shanghai")


def now_bj():
    return datetime.now(_market_tz())


def _today_bj():
    return now_bj().date()


def _in_cn_trading_session(dt=None):
    dt = dt or now_bj()
    hm = dt.strftime("%H:%M")
    return ("09:30" <= hm <= "11:30") or ("13:00" <= hm <= "15:00")


# 使用 utils.to_float 统一版本


def _is_trading_day_tushare(target_date):
    token = os.getenv("TUSHARE_TOKEN", "").strip()
    if not token:
        raise RuntimeError("missing TUSHARE_TOKEN")
    try:
        import tushare as ts
    except Exception as exc:
        raise RuntimeError(f"tushare unavailable: {exc}") from exc
    ts.set_token(token)
    pro = ts.pro_api()
    date_str = target_date.strftime("%Y%m%d")
    frame = pro.trade_cal(
        exchange="SSE",
        start_date=date_str,
        end_date=date_str,
        fields="exchange,cal_date,is_open",
    )
    if frame is None or frame.empty:
        raise RuntimeError("tushare trade_cal empty")
    return str(frame.iloc[0].get("is_open", "0")) == "1"


def _is_trading_day_pandas(target_date, calendar_name="SSE"):
    calendar = mcal.get_calendar(calendar_name)
    schedule = calendar.schedule(start_date=target_date, end_date=target_date)
    return not schedule.empty


def is_trading_day(calendar_name="SSE"):
    today = _today_bj()
    key = today.strftime("%Y%m%d")
    if key in _TRADING_DAY_CACHE:
        return _TRADING_DAY_CACHE[key]["is_open"]

    provider = os.getenv("TRADING_CALENDAR_PROVIDER", "auto").strip().lower()
    result = None
    if provider in {"auto", "tushare"}:
        try:
            result = _is_trading_day_tushare(today)
        except Exception:
            if provider == "tushare":
                raise
    if result is None:
        result = _is_trading_day_pandas(today, calendar_name=calendar_name)

    _TRADING_DAY_CACHE[key] = {"is_open": bool(result)}
    return bool(result)


def _normalize_symbol_plain(code):
    text = code.lower()
    if text.startswith("sh") or text.startswith("sz"):
        return text[2:]
    return code


def normalize_symbol(code):
    text = str(code or "").strip().lower()
    if not text:
        return text
    if text.startswith(("sh", "sz")):
        return text
    return f"sh{text}" if text.startswith(("5", "6", "9")) else f"sz{text}"


def _normalize_symbol_tushare(code):
    text = code.lower()
    if text.startswith("sh"):
        return f"{text[2:]}.SH"
    if text.startswith("sz"):
        return f"{text[2:]}.SZ"
    return code


def _extract_quote_row(payload, code):
    text = code.lower()
    # dict: {"sh000001": {"name": "...", "price": 123.4}}
    if isinstance(payload, dict):
        row = (
            payload.get(text)
            or payload.get(code)
            or payload.get(text[2:])
            or payload.get(code[2:])
        )
        if isinstance(row, dict) and ("price" in row or "close" in row):
            return row
    # list: [{"symbol":"sh000001","name":"...","price":123.4}, ...]
    if isinstance(payload, list):
        for row in payload:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or row.get("code") or "").lower()
            if symbol in {text, code.lower(), text[2:]}:
                return row
    return None


def _normalize_quote(result):
    out = dict(result)
    out["price"] = float(out["price"])
    out.setdefault("pct_change", None)
    out.setdefault("turnover_rate", None)
    out.setdefault("volume", None)
    out.setdefault("amount", None)
    out.setdefault("sector", None)
    return out


def _daily_aux_enabled():
    return os.getenv("DAILY_STOCK_ANALYSIS_AUX_ENABLED", "true").lower() == "true"


def _quote_enrich_enabled():
    return os.getenv("QUOTE_ENRICH_MISSING_FIELDS", "true").lower() == "true"


def _needs_enrich(quote):
    return any(
        quote.get(k) is None
        for k in ("pct_change", "turnover_rate", "volume", "amount")
    )


def _merge_quote(primary, fallback):
    merged = dict(primary)
    primary_name = str(merged.get("name") or "").strip().lower()
    fallback_name = fallback.get("name")
    if fallback_name:
        if (
            not primary_name
            or primary_name.replace(".", "")
            .replace("sh", "")
            .replace("sz", "")
            .isdigit()
        ):
            merged["name"] = fallback_name
    if merged.get("sector") is None and fallback.get("sector") is not None:
        merged["sector"] = fallback.get("sector")
    for key in ("pct_change", "turnover_rate", "volume", "amount"):
        if merged.get(key) is None and fallback.get(key) is not None:
            merged[key] = fallback[key]
    if merged.get("source") == "daily_stock_analysis_local" and fallback.get("source"):
        merged["source"] = f"{merged['source']}+{fallback['source']}"
    return merged


def _enrich_cache_ttl_seconds():
    return max(1, int(os.getenv("QUOTE_ENRICH_CACHE_SECONDS", "30")))


def _cache_key(code):
    return code.lower().strip()


def _read_enrich_cache(code):
    row = _ENRICH_CACHE.get(_cache_key(code))
    if not row:
        return None
    if (time.time() - float(row.get("ts", 0))) > _enrich_cache_ttl_seconds():
        return None
    return row.get("fields")


def _write_enrich_cache(code, quote):
    _ENRICH_CACHE[_cache_key(code)] = {
        "ts": time.time(),
        "fields": {
            "pct_change": quote.get("pct_change"),
            "turnover_rate": quote.get("turnover_rate"),
            "volume": quote.get("volume"),
            "amount": quote.get("amount"),
            "source": quote.get("source"),
        },
    }


def _get_quote_daily_local(code):
    cache_ttl = 300
    now = time.time()
    if (
        _DSA_AVAILABILITY_CACHE["available"] is False
        and (now - _DSA_AVAILABILITY_CACHE["ts"]) < cache_ttl
    ):
        raise FileNotFoundError("daily_stock_analysis unavailable (cached)")

    primary = os.getenv(
        "DAILY_STOCK_ANALYSIS_QUOTE_PATH", "data/daily_stock_analysis/latest_quote.json"
    ).strip()
    repo_path = os.getenv(
        "DAILY_STOCK_ANALYSIS_REPO_PATH", "/daily_stock_analysis"
    ).strip()
    fallback = os.path.join(repo_path, "output", "latest_quote.json")
    paths = [primary, fallback]
    last_error = "missing"
    for path in paths:
        if not path:
            continue
        if not os.path.exists(path):
            last_error = f"missing:{path}"
            continue
        max_staleness = int(
            os.getenv("DAILY_STOCK_ANALYSIS_MAX_STALENESS_SECONDS", "900")
        )
        allow_stale = (
            os.getenv("DAILY_STOCK_ANALYSIS_ALLOW_STALE", "false").lower() == "true"
        )
        file_age = None
        try:
            file_age = time.time() - os.path.getmtime(path)
        except OSError:
            file_age = None
        if (
            max_staleness > 0
            and not allow_stale
            and _in_cn_trading_session()
            and file_age is not None
            and file_age > max_staleness
        ):
            last_error = f"stale_file:{path}"
            continue
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        row = _extract_quote_row(payload, code)
        if not row:
            last_error = f"symbol_missing:{path}"
            continue
        name = str(row.get("name") or row.get("stock_name") or code)
        price = row.get("price", row.get("close"))
        _DSA_AVAILABILITY_CACHE["available"] = True
        _DSA_AVAILABILITY_CACHE["ts"] = time.time()
        return _normalize_quote(
            {
                "name": name,
                "price": float(price),
                "source": "daily_stock_analysis_local",
                "pct_change": to_float(
                    row.get("pct_change", row.get("change_percent"))
                ),
                "turnover_rate": to_float(row.get("turnover_rate")),
                "volume": to_float(row.get("volume", row.get("vol"))),
                "amount": to_float(row.get("amount")),
            }
        )
    _DSA_AVAILABILITY_CACHE["available"] = False
    _DSA_AVAILABILITY_CACHE["ts"] = time.time()
    raise FileNotFoundError(f"daily_stock_analysis quote not available: {last_error}")


def _get_quote_qtimg(code):
    url = f"http://qt.gtimg.cn/q={code}"
    response = request_with_throttle(url, timeout=5)
    response.raise_for_status()
    data = response.text.split("~")
    if len(data) < 32:
        raise ValueError(f"Invalid quote payload for {code}: {response.text[:100]}")
    name = data[1] if len(data) > 1 else code
    price_str = data[3] if len(data) > 3 else None
    pct_str = data[31] if len(data) > 31 else None
    turnover_str = data[38] if len(data) > 38 else None
    if price_str is None:
        raise ValueError(f"Missing price in quote for {code}")
    try:
        price = float(price_str)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid price value: {price_str}")

    quote_data = {"name": name, "price": price, "source": "qtimg"}

    if pct_str:
        try:
            quote_data["pct_change"] = float(pct_str) / 100.0
        except (ValueError, TypeError):
            pass

    if turnover_str:
        try:
            quote_data["turnover_rate"] = float(turnover_str)
        except (ValueError, TypeError):
            pass

    return _normalize_quote(quote_data)


def _get_quote_akshare(code):
    try:
        import akshare as ak
    except Exception as exc:
        raise RuntimeError(f"akshare unavailable: {exc}") from exc
    symbol = _normalize_symbol_plain(code)

    cache_ttl = _akshare_spot_cache_ttl()
    now = time.time()
    if (
        _AKSHARE_SPOT_CACHE["data"] is not None
        and (now - _AKSHARE_SPOT_CACHE["ts"]) < cache_ttl
    ):
        frame = _AKSHARE_SPOT_CACHE["data"]
    else:
        frame = ak.stock_zh_a_spot_em()
        _AKSHARE_SPOT_CACHE["data"] = frame
        _AKSHARE_SPOT_CACHE["ts"] = now
    row = frame[frame["代码"] == symbol]
    if row.empty:
        raise ValueError(f"akshare symbol not found: {symbol}")
    first = row.iloc[0]
    return _normalize_quote(
        {
            "name": str(first["名称"]),
            "price": float(first["最新价"]),
            "source": "akshare",
            "pct_change": to_float(first.get("涨跌幅")),
            "turnover_rate": to_float(first.get("换手率")),
            "volume": to_float(first.get("成交量")),
            "amount": to_float(first.get("成交额")),
            "sector": str(first.get("行业") or "").strip() or None,
        }
    )


def _get_quote_tushare(code):
    token = os.getenv("TUSHARE_TOKEN", "").strip()
    if not token:
        raise RuntimeError("missing TUSHARE_TOKEN")
    try:
        import tushare as ts
    except Exception as exc:
        raise RuntimeError(f"tushare unavailable: {exc}") from exc
    ts.set_token(token)
    pro = ts.pro_api()
    ts_code = _normalize_symbol_tushare(code)
    frame = pro.daily(ts_code=ts_code, limit=1)
    if frame.empty:
        raise ValueError(f"tushare no daily data: {ts_code}")
    first = frame.iloc[0]
    return _normalize_quote(
        {
            "name": ts_code,
            "price": float(first["close"]),
            "source": "tushare_daily",
            "pct_change": to_float(first.get("pct_chg")),
            "volume": to_float(first.get("vol")),
            "amount": to_float(first.get("amount")),
        }
    )


def _get_quote_sina(code):
    symbol = code.lower()
    url = f"http://hq.sinajs.cn/list={symbol}"
    response = request_with_throttle(url, timeout=5)
    response.raise_for_status()
    text = response.text
    if '"' not in text:
        raise ValueError(f"invalid sina payload: {text[:100]}")
    payload = text.split('"')[1]
    data = payload.split(",")
    if len(data) < 4:
        raise ValueError(f"invalid sina fields: {payload[:100]}")
    current_price = to_float(data[3])
    prev_close = to_float(data[2])
    pct_change = None
    if current_price is not None and prev_close not in (None, 0.0):
        pct_change = (current_price - prev_close) / prev_close * 100.0
    return _normalize_quote(
        {
            "name": data[0],
            "price": float(data[3]),
            "source": "sina",
            "pct_change": pct_change,
            "volume": to_float(data[8]),
            "amount": to_float(data[9]),
        }
    )


def _to_eastmoney_secid(code):
    text = normalize_symbol(code)
    if text.startswith("sh"):
        return f"1.{text[2:]}"
    if text.startswith("sz"):
        return f"0.{text[2:]}"
    return f"1.{text}" if text.startswith(("5", "6", "9")) else f"0.{text}"


def _to_baostock_symbol(code):
    text = normalize_symbol(code)
    if text.startswith("sh"):
        return f"sh.{text[2:]}"
    if text.startswith("sz"):
        return f"sz.{text[2:]}"
    return f"sh.{text}" if text.startswith(("5", "6", "9")) else f"sz.{text}"


def _ensure_baostock_login():
    try:
        import baostock as bs
    except Exception as exc:
        raise RuntimeError(f"baostock unavailable: {exc}") from exc

    with _BAOSTOCK_LOCK:
        if _BAOSTOCK_SESSION.get("ready"):
            return bs
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            login = bs.login()
        if login.error_code != "0":
            raise RuntimeError(f"baostock login failed: {login.error_msg}")
        _BAOSTOCK_SESSION["ready"] = True
        return bs


def _get_daily_klines_eastmoney(code, start_date, end_date, days):
    secid = _to_eastmoney_secid(code)
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "1",
        "beg": start_date,
        "end": end_date,
        "lmt": max(int(days) + 10, 30),
    }
    response = request_with_throttle(url, timeout=8, params=params)
    response.raise_for_status()
    payload = response.json()
    rows = []
    for line in (payload.get("data") or {}).get("klines") or []:
        parts = line.split(",")
        if len(parts) < 11:
            continue
        rows.append(
            {
                "date": parts[0],
                "open": to_float(parts[1]),
                "close": to_float(parts[2]),
                "high": to_float(parts[3]),
                "low": to_float(parts[4]),
                "volume": to_float(parts[5]),
                "amount": to_float(parts[6]),
                "amplitude": to_float(parts[7]),
                "pct_change": to_float(parts[8]),
                "change": to_float(parts[9]),
                "turnover_rate": to_float(parts[10]),
            }
        )
    return rows


def _get_daily_klines_akshare(code, start_date, end_date):
    import akshare as ak

    symbol = normalize_symbol(code)[2:]
    with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
        frame = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="",
        )
    rows = []
    if frame is None or getattr(frame, "empty", True):
        return rows
    for _, row in frame.iterrows():
        rows.append(
            {
                "date": str(row.get("日期") or row.get("date") or ""),
                "open": to_float(row.get("开盘") or row.get("open")),
                "close": to_float(row.get("收盘") or row.get("close")),
                "high": to_float(row.get("最高") or row.get("high")),
                "low": to_float(row.get("最低") or row.get("low")),
                "volume": to_float(row.get("成交量") or row.get("volume")),
                "amount": to_float(row.get("成交额") or row.get("amount")),
                "amplitude": to_float(row.get("振幅") or row.get("amplitude")),
                "pct_change": to_float(row.get("涨跌幅") or row.get("pct_change")),
                "change": to_float(row.get("涨跌额") or row.get("change")),
                "turnover_rate": to_float(row.get("换手率") or row.get("turnover_rate")),
            }
        )
    return rows


def _get_daily_klines_baostock(code, start_date, end_date):
    bs = _ensure_baostock_login()
    with _BAOSTOCK_LOCK:
        rs = bs.query_history_k_data_plus(
            _to_baostock_symbol(code),
            "date,open,high,low,close,volume,amount,pctChg,turn",
            start_date=datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d"),
            end_date=datetime.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d"),
            frequency="d",
            adjustflag="3",
        )
        if rs.error_code != "0":
            if "未登录" in str(rs.error_msg):
                _BAOSTOCK_SESSION["ready"] = False
                bs = _ensure_baostock_login()
                rs = bs.query_history_k_data_plus(
                    _to_baostock_symbol(code),
                    "date,open,high,low,close,volume,amount,pctChg,turn",
                    start_date=datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d"),
                    end_date=datetime.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d"),
                    frequency="d",
                    adjustflag="3",
                )
        if rs.error_code != "0":
            raise RuntimeError(f"baostock query failed: {rs.error_msg}")
        rows = []
        while rs.next():
            item = rs.get_row_data()
            rows.append(
                {
                    "date": item[0],
                    "open": to_float(item[1]),
                    "high": to_float(item[2]),
                    "low": to_float(item[3]),
                    "close": to_float(item[4]),
                    "volume": to_float(item[5]),
                    "amount": to_float(item[6]),
                    "pct_change": to_float(item[7]),
                    "turnover_rate": to_float(item[8]),
                }
            )
        return rows


def get_daily_klines(code, days=40):
    cache_key = f"{normalize_symbol(code)}_{int(days)}"
    cache_ttl = max(60, int(os.getenv("DAILY_KLINE_CACHE_SECONDS", "300")))
    now = time.time()
    cached = _KLINE_CACHE.get(cache_key)
    if cached and (now - cached.get("ts", 0)) < cache_ttl:
        return cached.get("data", [])

    end_date = _today_bj().strftime("%Y%m%d")
    start_date = (now_bj().date() - timedelta(days=max(30, int(days) * 3))).strftime(
        "%Y%m%d"
    )
    rows = []
    last_error = None
    loaders = (
        lambda: _get_daily_klines_eastmoney(code, start_date, end_date, days),
        lambda: _get_daily_klines_akshare(code, start_date, end_date),
        lambda: _get_daily_klines_baostock(code, start_date, end_date),
    )
    for loader in loaders:
        try:
            rows = loader()
            if rows:
                break
        except Exception as exc:
            last_error = exc
            rows = []
    if not rows and last_error is not None:
        raise last_error
    rows = rows[-max(1, int(days)) :]
    _KLINE_CACHE[cache_key] = {"ts": now, "data": rows}
    return rows


def _get_quote_eastmoney(code):
    secid = _to_eastmoney_secid(code)
    url = "https://push2.eastmoney.com/api/qt/stock/get"
    params = {"secid": secid, "fields": "f58,f43,f170,f168,f47,f48,f100,f127,f173,f174"}
    response = request_with_throttle(url, timeout=5, params=params)
    response.raise_for_status()
    payload = response.json()
    data = payload.get("data") or {}
    name = str(data.get("f58") or code)
    price_raw = data.get("f43")
    if price_raw is None:
        raise ValueError(f"eastmoney missing price for {secid}")
    return _normalize_quote(
        {
            "name": name,
            "price": float(price_raw),
            "source": "eastmoney",
            "pct_change": to_float(data.get("f170"), scale=100),
            "turnover_rate": to_float(data.get("f168"), scale=100),
            "volume": to_float(data.get("f47")),
            "amount": to_float(data.get("f48")),
            "sector": str(data.get("f127") or data.get("f100") or "").strip() or None,
            "fund_flow": to_float(data.get("f173")),
            "fund_flow_rate": to_float(data.get("f174"), scale=100),
        }
    )


def _to_yfinance_symbol(code):
    text = code.lower()
    if text.startswith("sh"):
        return f"{text[2:]}.SS"
    if text.startswith("sz"):
        return f"{text[2:]}.SZ"
    return code


def _get_quote_yfinance(code):
    if os.getenv("YFINANCE_SKIP_DURING_SESSION", "true").lower() == "true":
        if _in_cn_trading_session():
            raise RuntimeError("yfinance skipped during CN session")
    try:
        import yfinance as yf
    except Exception as exc:
        raise RuntimeError(f"yfinance unavailable: {exc}") from exc
    symbol = _to_yfinance_symbol(code)
    ticker = yf.Ticker(symbol)
    timeout = max(2, int(os.getenv("YFINANCE_TIMEOUT_SECONDS", "5")))
    hist = ticker.history(period="5d", interval="1d", timeout=timeout)
    if hist is None or hist.empty:
        raise ValueError(f"yfinance no data: {symbol}")
    price = float(hist["Close"].iloc[-1])
    info = getattr(ticker, "fast_info", {}) or {}
    name = info.get("shortName") or symbol
    return _normalize_quote({"name": str(name), "price": price, "source": "yfinance"})


def _get_quote_mkts(code):
    """使用 mkts.io API 获取行情数据（需要 OPENCLAW_API_KEY）"""
    api_key = os.getenv("OPENCLAW_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("mkts API key not configured (OPENCLAW_API_KEY)")

    # 转换代码格式
    text = code.lower()
    if text.startswith("sh"):
        symbol = f"{text[2:]}.SS"  # 上海
    elif text.startswith("sz"):
        symbol = f"{text[2:]}.SZ"  # 深圳
    else:
        # 尝试自动判断
        num = text.strip("shsz")
        if num.startswith(("5", "6", "9")):
            symbol = f"{num}.SS"
        else:
            symbol = f"{num}.SZ"

    import requests

    url = f"https://mkts.io/api/v1/asset/{symbol}/live"
    headers = {"X-API-Key": api_key}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not data.get("success"):
            raise RuntimeError(f"mkts API error: {data.get('message', 'unknown')}")

        asset = data.get("data", {})
        return _normalize_quote(
            {
                "name": asset.get("name", code),
                "price": asset.get("price"),
                "pct_change": asset.get("changePct"),
                "volume": asset.get("volume"),
                "source": "mkts",
            }
        )
    except Exception as exc:
        raise RuntimeError(f"mkts request failed: {exc}") from exc


def _split_market_code(code):
    text = code.lower()
    if text.startswith("sh"):
        return 1, text[2:]
    if text.startswith("sz"):
        return 0, text[2:]
    return (1 if text.startswith(("5", "6", "9")) else 0), text


def _get_quote_pytdx(code):
    try:
        from pytdx.hq import TdxHq_API
    except Exception as exc:
        raise RuntimeError(f"pytdx unavailable: {exc}") from exc
    host = os.getenv("PYTDX_HOST", "119.147.212.81").strip()
    port = int(os.getenv("PYTDX_PORT", "7709"))
    market, code6 = _split_market_code(code)
    api = TdxHq_API()
    try:
        if not api.connect(host, port):
            raise RuntimeError(f"pytdx connect failed: {host}:{port}")
        bars = api.get_security_bars(9, market, code6, 0, 1)
        if not bars:
            raise ValueError(f"pytdx no bars: {code6}")
        price = float(bars[0].get("close", 0))
        if price <= 0:
            raise ValueError(f"pytdx invalid close: {bars[0]}")
        return _normalize_quote(
            {"name": code.upper(), "price": price, "source": "pytdx"}
        )
    finally:
        try:
            api.disconnect()
        except Exception:
            pass


def _get_quote_baostock(code):
    bs = _ensure_baostock_login()
    symbol = _to_baostock_symbol(code)
    with _BAOSTOCK_LOCK:
        end_date = _today_bj()
        start_date = end_date - timedelta(days=14)
        rs = bs.query_history_k_data_plus(
            symbol,
            "code,close,date",
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            frequency="d",
            adjustflag="3",
        )
        if rs.error_code != "0":
            if "未登录" in str(rs.error_msg):
                _BAOSTOCK_SESSION["ready"] = False
                bs = _ensure_baostock_login()
                rs = bs.query_history_k_data_plus(
                    symbol,
                    "code,close,date",
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d"),
                    frequency="d",
                    adjustflag="3",
                )
        if rs.error_code != "0":
            raise RuntimeError(f"baostock query failed: {rs.error_msg}")
        rows = []
        while rs.next():
            rows.append(rs.get_row_data())
        if not rows:
            raise ValueError(f"baostock no daily rows: {symbol}")
        last = rows[-1]
        price = float(last[1])
        return _normalize_quote({"name": symbol, "price": price, "source": "baostock"})


def _build_remote_pool():
    configured = (
        os.getenv(
            "QUOTE_SOURCE_POOL",
            "qtimg,baostock,eastmoney,sina,akshare,tushare,pytdx,yfinance,mkts",
        )
        .strip()
        .lower()
    )
    names = [name.strip() for name in configured.split(",") if name.strip()]
    mapping = {
        "qtimg": _get_quote_qtimg,
        "sina": _get_quote_sina,
        "eastmoney": _get_quote_eastmoney,
        "yfinance": _get_quote_yfinance,
        "pytdx": _get_quote_pytdx,
        "baostock": _get_quote_baostock,
        "akshare": _get_quote_akshare,
        "tushare": _get_quote_tushare,
        "mkts": _get_quote_mkts,
    }

    skip_slow_in_session = (
        os.getenv("QUOTE_SKIP_SLOW_DURING_SESSION", "true").strip().lower() == "true"
    )
    slow_sources = {
        s.strip()
        for s in os.getenv("QUOTE_SLOW_SOURCES", "yfinance,pytdx").lower().split(",")
        if s.strip()
    }
    in_session = _in_cn_trading_session()

    pool = []
    for name in names:
        fetcher = mapping.get(name)
        if not fetcher:
            continue
        if skip_slow_in_session and in_session and name in slow_sources:
            continue
        if name == "tushare" and not os.getenv("TUSHARE_TOKEN", "").strip():
            continue
        if name == "mkts" and not os.getenv("OPENCLAW_API_KEY", "").strip():
            continue
        pool.append(fetcher)

    if not pool:
        pool = [_get_quote_qtimg, _get_quote_eastmoney]
    return pool


def get_quote(code, source=None):
    # 优先使用DSA本地数据（如果可用），然后按健康度排序远程数据源
    selected = (source or os.getenv("QUOTE_SOURCE", "auto")).lower().strip()
    remote_pool = _build_remote_pool()
    auto_sequence = (
        remote_pool[:]
        if not _daily_aux_enabled()
        else [_get_quote_daily_local] + remote_pool
    )
    fetchers = {
        "daily": [_get_quote_daily_local],
        "qtimg": [_get_quote_qtimg],
        "sina": [_get_quote_sina],
        "eastmoney": [_get_quote_eastmoney],
        "yfinance": [_get_quote_yfinance],
        "pytdx": [_get_quote_pytdx],
        "baostock": [_get_quote_baostock],
        "akshare": [_get_quote_akshare],
        "tushare": [_get_quote_tushare],
        "mkts": [_get_quote_mkts],
        "auto": auto_sequence,
        "random": auto_sequence,
    }
    if selected not in fetchers:
        raise ValueError(f"unsupported quote source: {selected}")

    sequence = list(fetchers[selected])
    if selected in {"auto", "random"}:
        if sequence and sequence[0] is _get_quote_daily_local:
            local_head = sequence[:1]
            tail = _rank_sources(sequence[1:])
            if selected == "random":
                random.shuffle(tail)
            sequence = local_head + tail
        else:
            sequence = _rank_sources(sequence)
            if selected == "random":
                random.shuffle(sequence)

    errors = []
    for fetcher in sequence:
        if _should_skip_source(fetcher.__name__, mutate=True):
            errors.append(f"{fetcher.__name__}:cooldown")
            continue
        try:
            quote = fetcher(code)
            _record_source_result(fetcher.__name__, True)
            if (
                selected in {"auto", "random"}
                and _quote_enrich_enabled()
                and _needs_enrich(quote)
            ):
                cached = _read_enrich_cache(code)
                if cached:
                    quote = _merge_quote(quote, cached)
                else:
                    max_enrich_sources = max(
                        1, int(os.getenv("QUOTE_ENRICH_MAX_SOURCES", "3"))
                    )
                    enrich_tries = 0
                    for fallback_fetcher in _rank_sources(remote_pool):
                        if fallback_fetcher is fetcher:
                            continue
                        if _should_skip_source(fallback_fetcher.__name__, mutate=True):
                            continue
                        try:
                            remote_quote = fallback_fetcher(code)
                            if remote_quote:
                                _record_source_result(fallback_fetcher.__name__, True)
                                _write_enrich_cache(code, remote_quote)
                                quote = _merge_quote(quote, remote_quote)
                                enrich_tries += 1
                                if (
                                    not _needs_enrich(quote)
                                    or enrich_tries >= max_enrich_sources
                                ):
                                    break
                        except Exception as exc:
                            _record_source_result(fallback_fetcher.__name__, False)
                            errors.append(f"{fallback_fetcher.__name__}:{exc}")
            return quote
        except Exception as exc:
            _record_source_result(fetcher.__name__, False)
            errors.append(f"{fetcher.__name__}:{exc}")

    if selected in {"auto", "random"}:
        last_probe = []
        seen = set()
        for fetcher in (_get_quote_qtimg, _get_quote_eastmoney, _get_quote_baostock):
            if fetcher.__name__ in seen:
                continue
            seen.add(fetcher.__name__)
            last_probe.append(fetcher)
        for fetcher in last_probe:
            try:
                quote = fetcher(code)
                _record_source_result(fetcher.__name__, True)
                return quote
            except Exception as exc:
                _record_source_result(fetcher.__name__, False)
                errors.append(f"{fetcher.__name__}:last_probe:{exc}")
    raise RuntimeError(f"all quote sources failed for {code}: {' | '.join(errors)}")


def _get_call_auction_top10_qtimg(limit=10):
    """QT/GT咪数据 集合竞价/实时行情"""
    codes = [
        "sh000001",
        "sh000002",
        "sh000003",
        "sh000004",
        "sh000005",
        "sh000006",
        "sh000007",
        "sh000008",
        "sh000009",
        "sh000010",
        "sz000001",
        "sz000002",
        "sz000003",
        "sz000004",
        "sz000005",
        "sz000006",
        "sz000007",
        "sz000008",
        "sz000009",
        "sz000010",
    ][: limit + 10]

    url = f"http://qt.gtimg.cn/q={','.join(codes)}"
    response = request_with_throttle(url, timeout=10)
    if response.status_code != 200:
        raise RuntimeError(f"qtimg unavailable: {response.status_code}")

    text = response.text.strip()
    if not text:
        raise RuntimeError("qtimg empty response")

    stocks = text.split(";")
    rows = []
    for stock_data in stocks:
        if not stock_data or "=" not in stock_data:
            continue
        try:
            parts = stock_data.split("=")
            code = parts[0].replace("sz", "").replace("sh", "")
            data = parts[1].split("~")
            if len(data) < 32:
                continue
            name = data[1] if len(data) > 1 else ""
            price = float(data[3]) if data[3] and data[3] != "-" else None
            pct_change = (
                float(data[31]) / 100.0 if data[31] and data[31] != "-" else None
            )
            volume = float(data[5]) if data[5] and data[5] != "-" else None
            amount = float(data[6]) if data[6] and data[6] != "-" else None
            turnover = float(data[38]) if data[38] and data[38] != "-" else None

            if code and name and price is not None:
                rows.append(
                    {
                        "code": "sz" + code if code.startswith("000") else "sh" + code,
                        "name": name,
                        "price": price,
                        "pct_change": pct_change,
                        "volume": volume,
                        "amount": amount,
                        "turnover_rate": turnover,
                        "sector": None,
                        "source": "qtimg",
                    }
                )
        except (ValueError, IndexError):
            continue

        if len(rows) >= limit:
            break

    return rows


def _get_call_auction_top10_sina(limit=10):
    """Sina财经集合竞价数据"""
    url = "https://hq.sinajs.cn/list=sh000001,sz399001"
    response = request_with_throttle(url, timeout=5)
    if response.status_code != 200:
        raise RuntimeError(f"Sina unavailable: {response.status_code}")
    text = response.text
    lines = text.strip().split("\n")
    rows = []
    for line in lines:
        if "=" not in line:
            continue
        parts = line.split("=")
        if len(parts) < 2:
            continue
        code = parts[0].split("_")[-1]
        data = parts[1].strip().split(",")
        if len(data) < 32:
            continue
        try:
            name = data[0]
            open_price = float(data[1]) if data[1] else 0
            pre_close = float(data[2]) if data[2] else 0
            if open_price and pre_close:
                pct_change = ((open_price - pre_close) / pre_close) * 100
                rows.append(
                    {
                        "code": code,
                        "name": name,
                        "price": open_price,
                        "pct_change": pct_change,
                        "volume": None,
                        "amount": None,
                        "turnover_rate": None,
                        "sector": None,
                    }
                )
        except (ValueError, IndexError):
            continue
    return rows[:limit]


def _get_call_auction_top10_akshare(limit=10):
    """AkShare 集合竞价数据（通过akshare库）"""
    try:
        import akshare as ak

        df = ak.stock_zh_a_spot_em()
        if df is None or df.empty:
            raise RuntimeError("AkShare spot empty")
        df = df.head(limit)
        rows = []
        for _, row in df.iterrows():
            code = str(row.get("代码", ""))
            name = str(row.get("名称", ""))
            price = row.get("最新价")
            pct = row.get("涨跌幅")
            vol = row.get("成交量")
            amt = row.get("成交额")
            turnover = row.get("换手率")
            if code and name:
                rows.append(
                    {
                        "code": code,
                        "name": name,
                        "price": float(price) if price else None,
                        "pct_change": float(pct) if pct else None,
                        "volume": float(vol) if vol else None,
                        "amount": float(amt) if amt else None,
                        "turnover_rate": float(turnover) if turnover else None,
                        "sector": None,
                    }
                )
        return rows
    except ImportError:
        raise RuntimeError("akshare not installed")
    except Exception as exc:
        raise RuntimeError(f"akshare failed: {exc}")


def _get_call_auction_top10_daily_analysis(limit=10):
    """daily_stock_analysis API 集合竞价数据"""
    try:
        import requests

        resp = requests.get("http://127.0.0.1:8000/api/auction/top10", timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("data", [])[:limit]
        raise RuntimeError(f"daily_analysis API: {resp.status_code}")
    except Exception as exc:
        raise RuntimeError(f"daily_analysis unavailable: {exc}")


def _get_call_auction_top10_eastmoney(limit=10):
    # A-share universe ordered by amount descending. At 09:25 this reflects call-auction activity.
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": 1,
        "pz": max(1, int(limit)),
        "po": 1,
        "np": 1,
        "fid": "f6",
        "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
        "fields": "f12,f14,f2,f3,f5,f6,f8,f100",
    }

    # Retry with longer timeout
    last_exc = None
    for attempt in range(3):
        try:
            response = request_with_throttle(url, timeout=15, params=params)
            response.raise_for_status()
            break
        except Exception as exc:
            last_exc = exc
            logger.warning("call_auction_eastmoney_attempt_%s err=%s", attempt + 1, exc)
            time.sleep(2)
    else:
        raise RuntimeError(f"EastMoney failed after 3 attempts: {last_exc}")

    payload = response.json()
    diff = ((payload.get("data") or {}).get("diff")) or []
    rows = []
    for row in diff[:limit]:
        if not isinstance(row, dict):
            continue
        code = str(row.get("f12") or "").strip()
        name = str(row.get("f14") or "").strip()
        price = to_float(row.get("f2"))
        pct_change = to_float(row.get("f3"), scale=100)
        volume = to_float(row.get("f5"))
        amount = to_float(row.get("f6"))
        turnover_rate = to_float(row.get("f8"), scale=100)
        sector = str(row.get("f100") or "").strip() or None
        if not code or not name:
            continue
        rows.append(
            {
                "code": code,
                "name": name,
                "price": price,
                "pct_change": pct_change,
                "volume": volume,
                "amount": amount,
                "turnover_rate": turnover_rate,
                "sector": sector,
            }
        )
    return rows


def get_call_auction_top10_with_status(limit=10):
    err = ""
    stale = False
    rows = []

    fetchers = [
        ("qtimg", lambda: _get_call_auction_top10_qtimg(limit=limit)),
        ("eastmoney", lambda: _get_call_auction_top10_eastmoney(limit=limit)),
        ("akshare", lambda: _get_call_auction_top10_akshare(limit=limit)),
    ]

    for source, fetcher in fetchers:
        try:
            rows = fetcher()
            if rows:
                _CALL_AUCTION_CACHE["data"] = rows
                _CALL_AUCTION_CACHE["ts"] = time.time()
                stale = False
                logger.info(
                    "call_auction_success source=%s count=%s", source, len(rows)
                )
                break
        except Exception as exc:
            logger.warning("call_auction_%s_failed err=%s", source, exc)
            err = f"{source}:{exc}"

    if not rows:
        cached = _CALL_AUCTION_CACHE.get("data") or []
        if (
            cached
            and (time.time() - _CALL_AUCTION_CACHE.get("ts", 0))
            < _call_auction_cache_ttl()
        ):
            rows = cached
            stale = True
            logger.info("call_auction_using_stale_cache")
        else:
            return [], False, err or "no data source available"

    min_amount = float(os.getenv("AUCTION_TOP10_MIN_AMOUNT", "0"))
    if min_amount > 0:
        rows = [r for r in rows if (r.get("amount") or 0) >= min_amount]
    return rows[:limit], stale, err
