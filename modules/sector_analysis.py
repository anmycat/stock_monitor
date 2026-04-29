import os
import time
import json
import io
from contextlib import redirect_stderr, redirect_stdout

from .utils import request_with_throttle, to_float


_SECTOR_CACHE = {"data": None, "ts": 0.0}
_TOP_STOCKS_CACHE = {}
_MARKET_BREADTH_CACHE = {"data": None, "ts": 0.0}
_MARKET_ACTIVE_CACHE = {}


def get_cache_ttl(key: str, default_ttl: int = 60) -> int:
    """获取缓存TTL"""
    return int(os.getenv(f"{key.upper()}_CACHE_SECONDS", str(default_ttl)))


def _cache_file(name: str) -> str:
    log_dir = os.getenv("LOG_DIR", "logs")
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, f"{name}.json")


def _read_persisted_cache(name: str, ttl: int):
    path = _cache_file(name)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        data = payload.get("data")
        ts = float(payload.get("ts", 0))
        if data and (time.time() - ts) <= ttl:
            return data
    except Exception:
        return None
    return None


def _write_persisted_cache(name: str, data):
    if not data:
        return
    path = _cache_file(name)
    payload = {"ts": time.time(), "data": data}
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
    except Exception:
        pass


def _to_rows_from_spot_frame(frame, limit=None):
    rows = []
    if frame is None or getattr(frame, "empty", True):
        return rows
    for _, row in frame.iterrows():
        code = str(
            row.get("代码") or row.get("股票代码") or row.get("code") or ""
        ).strip()
        name = str(
            row.get("名称") or row.get("股票名称") or row.get("name") or ""
        ).strip()
        if not code or not name:
            continue
        rows.append(
            {
                "code": code,
                "name": name,
                "price": to_float(
                    row.get("最新价")
                    or row.get("close")
                    or row.get("最新交易价")
                    or row.get("trade")
                ),
                "pct_change": to_float(
                    row.get("涨跌幅") or row.get("pct_chg") or row.get("changepercent")
                ),
                "turnover_rate": to_float(
                    row.get("换手率") or row.get("turnoverratio")
                ),
                "volume": to_float(row.get("成交量") or row.get("volume")),
                "amount": to_float(row.get("成交额") or row.get("amount")),
                "total_mv": to_float(
                    row.get("总市值")
                    or row.get("总市值(元)")
                    or row.get("total_mv")
                    or row.get("market_value")
                ),
            }
        )
    rows.sort(key=lambda x: float(x.get("amount") or 0.0), reverse=True)
    return rows[:limit] if limit else rows


def _calc_market_breadth_from_rows(rows):
    if not rows:
        return {}
    up = 0
    down = 0
    flat = 0
    limit_up = 0
    limit_down = 0
    amount_total = 0.0
    turnover_sum = 0.0
    turnover_cnt = 0
    for row in rows:
        pct = row.get("pct_change")
        amount = row.get("amount")
        turnover = row.get("turnover_rate")
        if pct is None:
            continue
        pct = float(pct)
        if pct > 0:
            up += 1
        elif pct < 0:
            down += 1
        else:
            flat += 1
        if pct >= 9.8:
            limit_up += 1
        if pct <= -9.8:
            limit_down += 1
        if amount is not None:
            amount_total += float(amount)
        if turnover is not None:
            turnover_sum += float(turnover)
            turnover_cnt += 1
    return {
        "sample_size": up + down + flat,
        "up": up,
        "down": down,
        "flat": flat,
        "limit_up": limit_up,
        "limit_down": limit_down,
        "amount_total": amount_total,
        "avg_turnover": (turnover_sum / turnover_cnt) if turnover_cnt else None,
    }


def _merge_spot_enrichment(primary_rows, extra_rows):
    if not primary_rows or not extra_rows:
        return primary_rows, 0

    def _plain_code(value):
        text = str(value or "").strip().lower()
        if text.startswith(("sh", "sz")):
            return text[2:]
        return text

    extra_map = {}
    for row in extra_rows:
        code = _plain_code(row.get("code"))
        if code:
            extra_map[code] = row

    turnover_hits = 0
    for row in primary_rows:
        code = _plain_code(row.get("code"))
        extra = extra_map.get(code)
        if not extra:
            continue
        before_turnover = row.get("turnover_rate")
        for key in ("price", "pct_change", "turnover_rate", "volume", "amount"):
            if row.get(key) is None and extra.get(key) is not None:
                row[key] = extra.get(key)
        if before_turnover is None and row.get("turnover_rate") is not None:
            turnover_hits += 1
    return primary_rows, turnover_hits


def _batch_enrich_rows_with_qtimg(rows, batch_size=60):
    if not rows:
        return rows, 0
    batch_size = max(
        1,
        int(
            batch_size
            or os.getenv("MARKET_QTIMG_BATCH_SIZE", "20")
            or 20
        ),
    )

    def _qt_code(code):
        text = str(code or "").strip().lower()
        if text.startswith(("sh", "sz")):
            return text
        return f"sh{text}" if text.startswith(("5", "6", "9")) else f"sz{text}"

    def _plain_code(code):
        text = str(code or "").strip().lower()
        if text.startswith(("sh", "sz")):
            return text[2:]
        return text

    enrich_map = {}
    codes = [_qt_code(row.get("code")) for row in rows if row.get("code")]
    for start in range(0, len(codes), batch_size):
        chunk = codes[start : start + batch_size]
        if not chunk:
            continue
        try:
            url = f"http://qt.gtimg.cn/q={','.join(chunk)}"
            response = request_with_throttle(url, timeout=10)
            response.raise_for_status()
            text = response.text.strip()
        except Exception:
            continue
        if not text:
            continue
        for stock_data in text.split(";"):
            if not stock_data or "=" not in stock_data:
                continue
            parts = stock_data.split("=", 1)
            raw_code = parts[0].replace("v_", "").strip().lower()
            code = raw_code[2:] if raw_code.startswith(("sh", "sz")) else raw_code
            data = parts[1].split("~")
            if len(data) < 39:
                continue
            enrich_map[code] = {
                "turnover_rate": to_float(data[38]),
            }

    turnover_hits = 0
    for row in rows:
        code = _plain_code(row.get("code"))
        extra = enrich_map.get(code)
        if not extra:
            continue
        before_turnover = row.get("turnover_rate")
        if row.get("turnover_rate") is None and extra.get("turnover_rate") is not None:
            row["turnover_rate"] = extra.get("turnover_rate")
        if before_turnover is None and row.get("turnover_rate") is not None:
            turnover_hits += 1
    return rows, turnover_hits


def _get_market_spot_efinance():
    import efinance as ef

    frame = ef.stock.get_realtime_quotes()
    return _to_rows_from_spot_frame(frame)


def _get_market_spot_akshare():
    import akshare as ak

    with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
        try:
            frame = ak.stock_zh_a_spot_em()
        except Exception:
            frame = ak.stock_zh_a_spot()
    return _to_rows_from_spot_frame(frame)


def _get_sector_rows_efinance():
    import efinance as ef

    frame = ef.stock.get_realtime_quotes(["行业板块"])
    rows = []
    if frame is None or getattr(frame, "empty", True):
        return rows
    for _, row in frame.iterrows():
        name = str(row.get("股票名称") or row.get("名称") or "").strip()
        pct = to_float(row.get("涨跌幅") or row.get("pct_chg"))
        if name:
            rows.append({"code": "", "name": name, "pct_change": pct})
    rows.sort(key=lambda x: float(x.get("pct_change") or 0.0), reverse=True)
    return rows


def _get_sector_rows_akshare():
    import akshare as ak

    with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
        try:
            frame = ak.stock_board_industry_name_em()
        except Exception:
            frame = ak.stock_sector_spot(indicator="新浪行业")
    rows = []
    if frame is None or getattr(frame, "empty", True):
        return rows
    for _, row in frame.iterrows():
        name = str(
            row.get("板块名称") or row.get("板块") or row.get("名称") or ""
        ).strip()
        pct = to_float(row.get("涨跌幅"))
        code = str(
            row.get("板块代码") or row.get("代码") or row.get("label") or ""
        ).strip()
        label = str(row.get("label") or code).strip()
        if name:
            rows.append({"code": code, "name": name, "pct_change": pct, "label": label})
    rows.sort(key=lambda x: float(x.get("pct_change") or 0.0), reverse=True)
    return rows


def _get_sector_constituents_akshare(sector_name, limit=10):
    import akshare as ak

    frame = None
    for row in _get_sector_rows_akshare():
        if str(row.get("name") or "").strip() == str(sector_name or "").strip():
            label = row.get("label")
            if label:
                try:
                    with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                        frame = ak.stock_sector_detail(sector=label)
                    break
                except Exception:
                    frame = None
                    break
    if frame is None:
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            frame = ak.stock_board_industry_cons_em(symbol=sector_name)
    return _to_rows_from_spot_frame(frame, limit=limit)


def get_sector_list():
    cache_ttl = get_cache_ttl("sector", 300)
    now = time.time()
    if _SECTOR_CACHE["data"] and (now - _SECTOR_CACHE["ts"]) < cache_ttl:
        return _SECTOR_CACHE["data"]
    persisted = _read_persisted_cache("sector_list_cache", ttl=max(cache_ttl, 6 * 3600))
    if persisted:
        _SECTOR_CACHE["data"] = persisted
        _SECTOR_CACHE["ts"] = now

    for loader in (_get_sector_rows_akshare, _get_sector_rows_efinance):
        try:
            sectors = loader()
            if sectors:
                _SECTOR_CACHE["data"] = sectors
                _SECTOR_CACHE["ts"] = now
                _write_persisted_cache("sector_list_cache", sectors)
                return sectors
        except Exception:
            continue

    try:
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": 1,
            "pz": 100,
            "po": 1,
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fid": "f3",
            "fs": "m:90+t:2",
            "fields": "f1,f2,f3,f4,f12,f13,f14",
        }
        response = request_with_throttle(url, timeout=10, params=params)
        response.raise_for_status()
        payload = response.json()
        diff = ((payload.get("data") or {}).get("diff")) or []
        sectors = []
        for row in diff:
            if not isinstance(row, dict):
                continue
            code = str(row.get("f12") or "")
            name = str(row.get("f14") or "").strip()
            pct = to_float(row.get("f3"), scale=100)
            if code and name:
                sectors.append({"code": code, "name": name, "pct_change": pct})
        if not sectors:
            raise ValueError("eastmoney sector list empty")
        _SECTOR_CACHE["data"] = sectors
        _SECTOR_CACHE["ts"] = now
        _write_persisted_cache("sector_list_cache", sectors)
        return sectors
    except Exception:
        return _SECTOR_CACHE.get("data") or persisted or []


def get_sector_top_stocks(sector_name=None, limit=10):
    cache_ttl = int(os.getenv("SECTOR_STOCKS_CACHE_SECONDS", "60"))
    now = time.time()
    cache_key = f"sector_{sector_name or 'all'}"

    cached = _TOP_STOCKS_CACHE.get(cache_key)
    if cached:
        cached_ts = _TOP_STOCKS_CACHE.get(f"{cache_key}_ts", 0)
        if (now - cached_ts) < cache_ttl:
            return cached

    if sector_name:
        try:
            stocks = _get_sector_constituents_akshare(sector_name, limit=limit)
            if stocks:
                _TOP_STOCKS_CACHE[cache_key] = stocks
                _TOP_STOCKS_CACHE[f"{cache_key}_ts"] = now
                return stocks
        except Exception:
            pass

    try:
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        if sector_name:
            sector_map = {
                "白酒": "m:0+t:6,m:1:t:23",
                "新能源汽车": "m:0+t:80,m:1:t:27",
                "银行": "m:0+t:6,m:1:t:2",
                "保险": "m:0+t:6,m:1:t:28",
                "电力": "m:0+t:6,m:1:t:26",
                "芯片": "m:0+t:6,m:1:t:24",
                "光伏": "m:0+t:6,m:1:t:29",
                "医药": "m:0+t:6,m:1:t:21",
            }
            fs = sector_map.get(sector_name, "m:0+t:6,m:1+t:2")
        else:
            fs = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
        params = {
            "pn": 1,
            "pz": limit,
            "po": 1,
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fid": "f6",
            "fs": fs,
            "fields": "f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f14,f15,f16,f17",
        }
        response = request_with_throttle(url, timeout=10, params=params)
        response.raise_for_status()
        payload = response.json()
        diff = ((payload.get("data") or {}).get("diff")) or []
        stocks = []
        for row in diff:
            if not isinstance(row, dict):
                continue
            code = str(row.get("f12") or "").strip()
            name = str(row.get("f14") or "").strip()
            price = to_float(row.get("f2"))
            pct = to_float(row.get("f3"), scale=100)
            turnover = to_float(row.get("f8"), scale=100)
            volume = to_float(row.get("f5"))
            amount = to_float(row.get("f6"))
            if code and name:
                stocks.append(
                    {
                        "code": code,
                        "name": name,
                        "price": price,
                        "pct_change": pct,
                        "turnover_rate": turnover,
                        "volume": volume,
                        "amount": amount,
                    }
                )
        if not stocks:
            raise ValueError("eastmoney sector stocks empty")
        _TOP_STOCKS_CACHE[cache_key] = stocks
        _TOP_STOCKS_CACHE[f"{cache_key}_ts"] = now
        return stocks
    except Exception:
        if not sector_name:
            for loader in (_get_market_spot_efinance, _get_market_spot_akshare):
                try:
                    stocks = loader()[:limit]
                    if stocks:
                        _TOP_STOCKS_CACHE[cache_key] = stocks
                        _TOP_STOCKS_CACHE[f"{cache_key}_ts"] = now
                        return stocks
                except Exception:
                    continue
        return _TOP_STOCKS_CACHE.get(cache_key) or []


def analyze_sector_momentum(sector_name):
    stocks = get_sector_top_stocks(sector_name, limit=15)
    if not stocks:
        return None

    def _safe_pct(s):
        val = s.get("pct_change")
        return val if val is not None else 0

    rising_count = sum(1 for s in stocks if _safe_pct(s) > 0)
    avg_pct = sum(_safe_pct(s) for s in stocks) / len(stocks) if stocks else 0
    turnover_values = [
        s.get("turnover_rate") for s in stocks if s.get("turnover_rate") is not None
    ]
    avg_turnover = (
        (sum(turnover_values) / len(turnover_values)) if turnover_values else 0.0
    )

    if rising_count >= len(stocks) * 0.7 and avg_pct > 2:
        signal = "STRONG_RISING"
    elif rising_count >= len(stocks) * 0.5 and avg_pct > 0:
        signal = "RISING"
    elif rising_count <= len(stocks) * 0.3 and avg_pct < -2:
        signal = "WEAK_FALLING"
    else:
        signal = "MIXED"

    return {
        "sector": sector_name,
        "signal": signal,
        "stock_count": len(stocks),
        "rising_count": rising_count,
        "avg_pct_change": avg_pct,
        "avg_turnover": avg_turnover,
        "top_stocks": stocks[:10],
    }


def auto_discover_hot_sectors(min_stocks_rising=5, min_avg_pct=2.0):
    hot_sectors = []
    sectors = get_sector_list()
    scan_limit = max(5, int(os.getenv("HOT_SECTOR_SCAN_LIMIT", "12")))
    ranked_sectors = sorted(
        sectors,
        key=lambda x: float(x.get("pct_change") or 0.0),
        reverse=True,
    )[:scan_limit]

    for sector in ranked_sectors:
        sector_name = sector.get("name", "")
        if not sector_name:
            continue

        pct_change = sector.get("pct_change", 0) or 0
        if pct_change < min_avg_pct:
            continue

        sector_data = analyze_sector_momentum(sector_name)
        if not sector_data:
            continue

        rising = sector_data.get("rising_count", 0)
        if rising < min_stocks_rising:
            continue

        if sector_data["signal"] in ("STRONG_RISING", "RISING"):
            hot_sectors.append(sector_data)

    hot_sectors.sort(key=lambda x: x["avg_pct_change"], reverse=True)
    return hot_sectors


def _market_fs():
    return "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"


def _fetch_market_diff_pages(sample_size, fid, fields):
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    page_size = max(100, min(500, int(os.getenv("MARKET_PAGE_SIZE", "500"))))
    max_pages = max(1, int(os.getenv("MARKET_MAX_PAGES", "20")))
    target = max(200, int(sample_size))
    page_count = min(max_pages, (target + page_size - 1) // page_size)

    rows = []
    for page in range(1, page_count + 1):
        params = {
            "pn": page,
            "pz": page_size,
            "po": 1,
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fid": fid,
            "fs": _market_fs(),
            "fields": fields,
        }
        response = request_with_throttle(url, timeout=10, params=params)
        response.raise_for_status()
        payload = response.json()
        diff = ((payload.get("data") or {}).get("diff")) or []
        if not diff:
            break
        rows.extend(diff)
        if len(rows) >= target:
            break
    return rows[:target]


def get_market_breadth(sample_size=5000):
    cache_ttl = get_cache_ttl("market_breadth", 60)
    now = time.time()
    if (
        _MARKET_BREADTH_CACHE["data"]
        and (now - _MARKET_BREADTH_CACHE["ts"]) < cache_ttl
    ):
        return _MARKET_BREADTH_CACHE["data"]

    try:
        diff = _fetch_market_diff_pages(
            sample_size=sample_size,
            fid="f3",
            fields="f2,f3,f6,f8,f12,f14",
        )
        total = len(diff)
        min_sample = min(
            max(200, int(os.getenv("MARKET_BREADTH_MIN_SAMPLE", "800"))),
            max(200, int(sample_size)),
        )
        if total < min_sample:
            raise ValueError(f"eastmoney market breadth sample too small: {total}")
        up = 0
        down = 0
        flat = 0
        limit_up = 0
        limit_down = 0
        amount_total = 0.0
        turnover_sum = 0.0
        turnover_cnt = 0

        for row in diff:
            if not isinstance(row, dict):
                continue
            pct = to_float(row.get("f3"), scale=100)
            amount = to_float(row.get("f6"))
            turnover = to_float(row.get("f8"), scale=100)
            if pct is None:
                continue
            if pct > 0:
                up += 1
            elif pct < 0:
                down += 1
            else:
                flat += 1
            if pct >= 9.8:
                limit_up += 1
            if pct <= -9.8:
                limit_down += 1
            if amount is not None:
                amount_total += float(amount)
            if turnover is not None:
                turnover_sum += float(turnover)
                turnover_cnt += 1

        out = {
            "sample_size": total,
            "up": up,
            "down": down,
            "flat": flat,
            "limit_up": limit_up,
            "limit_down": limit_down,
            "amount_total": amount_total,
            "avg_turnover": (turnover_sum / turnover_cnt) if turnover_cnt else None,
        }
        if total <= 0:
            raise ValueError("eastmoney market breadth empty")
        _MARKET_BREADTH_CACHE["data"] = out
        _MARKET_BREADTH_CACHE["ts"] = now
        return out
    except Exception:
        for loader in (_get_market_spot_efinance, _get_market_spot_akshare):
            try:
                rows = loader()
                if rows:
                    out = _calc_market_breadth_from_rows(
                        rows[: max(200, int(sample_size))]
                    )
                    _MARKET_BREADTH_CACHE["data"] = out
                    _MARKET_BREADTH_CACHE["ts"] = now
                    return out
            except Exception:
                continue
        return _MARKET_BREADTH_CACHE.get("data") or {}


def get_market_active_top(limit=10, force_refresh=False):
    cache_ttl = get_cache_ttl("market_active", 60)
    now = time.time()
    cache_key = f"active_{int(limit)}"
    if (
        not force_refresh
        and (
        _MARKET_ACTIVE_CACHE.get(cache_key)
        and (now - _MARKET_ACTIVE_CACHE.get(f"{cache_key}_ts", 0)) < cache_ttl
        )
    ):
        return _MARKET_ACTIVE_CACHE.get(cache_key, [])
    persisted = None
    if not force_refresh:
        persisted = _read_persisted_cache(
            f"market_active_{int(limit)}", ttl=max(cache_ttl, 600)
        )
    if persisted:
        if not any(row.get("turnover_rate") is not None for row in persisted):
            try:
                persisted, _ = _batch_enrich_rows_with_qtimg(persisted)
            except Exception:
                pass
        _MARKET_ACTIVE_CACHE[cache_key] = persisted
        _MARKET_ACTIVE_CACHE[f"{cache_key}_ts"] = now

    try:
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": 1,
            "pz": max(1, int(limit)),
            "po": 1,
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fid": "f6",
            "fs": _market_fs(),
            "fields": "f2,f3,f6,f8,f12,f14,f15,f16,f17,f20",
        }
        response = request_with_throttle(url, timeout=10, params=params)
        response.raise_for_status()
        payload = response.json()
        diff = ((payload.get("data") or {}).get("diff")) or []
        rows = []
        for row in diff:
            if not isinstance(row, dict):
                continue
            code = str(row.get("f12") or "").strip()
            name = str(row.get("f14") or "").strip()
            if not code or not name:
                continue
            rows.append(
                {
                    "code": code,
                    "name": name,
                    "price": to_float(row.get("f2")),
                    "pct_change": to_float(row.get("f3"), scale=100),
                    "amount": to_float(row.get("f6")),
                    "turnover_rate": to_float(row.get("f8"), scale=100),
                    "total_mv": to_float(row.get("f20")),
                }
            )
        if not rows:
            raise ValueError("eastmoney market active empty")
        if not any(row.get("turnover_rate") is not None for row in rows):
            enriched_hits = 0
            try:
                rows, enriched_hits = _batch_enrich_rows_with_qtimg(rows)
            except Exception:
                enriched_hits = 0
            if enriched_hits <= 0:
                for loader in (_get_market_spot_efinance, _get_market_spot_akshare):
                    try:
                        extra_rows = loader()
                    except Exception:
                        continue
                    rows, enriched_hits = _merge_spot_enrichment(rows, extra_rows)
                    if enriched_hits > 0:
                        break
            if not any(row.get("turnover_rate") is not None for row in rows):
                raise ValueError("eastmoney market active turnover empty")
        if not any(row.get("amount") is not None for row in rows):
            for loader in (_get_market_spot_efinance, _get_market_spot_akshare):
                try:
                    extra_rows = loader()
                    rows, _ = _merge_spot_enrichment(rows, extra_rows)
                    break
                except Exception:
                    continue
        _MARKET_ACTIVE_CACHE[cache_key] = rows
        _MARKET_ACTIVE_CACHE[f"{cache_key}_ts"] = now
        _write_persisted_cache(f"market_active_{int(limit)}", rows)
        return rows
    except Exception:
        for loader in (_get_market_spot_efinance, _get_market_spot_akshare):
            try:
                rows = loader()
                if rows:
                    rows = rows[: max(1, int(limit))]
                    if not any(row.get("turnover_rate") is not None for row in rows):
                        try:
                            rows, _ = _batch_enrich_rows_with_qtimg(rows)
                        except Exception:
                            pass
                    _MARKET_ACTIVE_CACHE[cache_key] = rows
                    _MARKET_ACTIVE_CACHE[f"{cache_key}_ts"] = now
                    _write_persisted_cache(f"market_active_{int(limit)}", rows)
                    return rows
            except Exception:
                continue
        return _MARKET_ACTIVE_CACHE.get(cache_key) or persisted or []


def load_watchlist():
    path = os.getenv("WATCHLIST_PATH", "config/watchlist.json")
    if not os.path.exists(path):
        return {"watch_stocks": [], "sectors": {}, "scan_rules": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"watch_stocks": [], "sectors": {}, "scan_rules": {}}
