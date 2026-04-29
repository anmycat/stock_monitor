import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

from .utils import request_with_throttle, to_float


_FUND_FLOW_CACHE = {}
_DRAGON_CACHE = {"data": None, "ts": 0.0}


def get_stock_fund_flow_days(code: str, days: int = 3) -> Dict:
    """获取股票多日资金流向"""
    cache_key = f"fund_flow_{code}_{days}"
    cache_ttl = int(os.getenv("FUND_FLOW_CACHE_SECONDS", "300"))
    now = time.time()
    
    if cache_key in _FUND_FLOW_CACHE:
        cached = _FUND_FLOW_CACHE[cache_key]
        if (now - cached.get("ts", 0)) < cache_ttl:
            return cached.get("data", {})
    
    try:
        secid = code
        if code.startswith("sh"):
            secid = f"1.{code[2:]}"
        elif code.startswith("sz"):
            secid = f"0.{code[2:]}"
        
        url = "https://push2.eastmoney.com/api/qt/stock/fflow/daykline/get"
        params = {
            "lmt": days,
            "klt": "101",
            "secid": secid,
            "fields1": "f1,f2,f3,f7",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
        }
        response = request_with_throttle(url, timeout=10, params=params)
        response.raise_for_status()
        payload = response.json()
        
        data = payload.get("data", {}).get("klines", [])
        if not data:
            result = {"code": code, "days": days, "data": [], "trend": "unknown"}
            _FUND_FLOW_CACHE[cache_key] = {"ts": now, "data": result}
            return result
        
        day_data = []
        for line in data:
            parts = line.split(",")
            if len(parts) >= 4:
                day_data.append({
                    "date": parts[0],
                    "main_net_in": to_float(parts[1]),
                    "super_net_in": to_float(parts[2]),
                    "large_net_in": to_float(parts[3]),
                })
        
        main_total = sum(d.get("main_net_in", 0) or 0 for d in day_data)
        
        if main_total > 0:
            daily_avg = main_total / len(day_data) if day_data else 0
            if daily_avg > 0 and daily_avg < abs(main_total) / days * 1.5:
                trend = "slow_inflow"
            else:
                trend = "inflow"
        elif main_total < 0:
            trend = "outflow"
        else:
            trend = "neutral"
        
        result = {
            "code": code,
            "days": days,
            "data": day_data,
            "trend": trend,
            "main_total": main_total,
            "daily_avg": main_total / len(day_data) if day_data else 0,
        }
        
        _FUND_FLOW_CACHE[cache_key] = {"ts": now, "data": result}
        return result
        
    except Exception as e:
        return {"code": code, "days": days, "data": [], "trend": "error", "error": str(e)}


def get_dragon_tiger_list(limit: int = 10) -> List[Dict]:
    """获取龙虎榜数据（游资关注股票）"""
    cache_ttl = int(os.getenv("DRAGON_CACHE_SECONDS", "300"))
    now = time.time()
    
    if _DRAGON_CACHE["data"] is not None and (now - _DRAGON_CACHE["ts"]) < cache_ttl:
        return _DRAGON_CACHE["data"]
    
    try:
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": 1,
            "pz": limit,
            "po": 1,
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fid": "f62",
            "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
            "fields": "f2,f3,f4,f8,f12,f14,f62,f184,f66,f69,f75,f78,f124,f136,f128",
        }
        response = request_with_throttle(url, timeout=10, params=params)
        response.raise_for_status()
        payload = response.json()
        diff = ((payload.get("data") or {}).get("diff")) or []
        
        results = []
        for row in diff:
            if not isinstance(row, dict):
                continue
            code = str(row.get("f12") or "").strip()
            name = str(row.get("f14") or "").strip()
            if not code or not name:
                continue
            
            results.append({
                "code": code,
                "name": name,
                "price": to_float(row.get("f2")),
                "pct_change": to_float(row.get("f3"), scale=100),
                "turnover_rate": to_float(row.get("f8"), scale=100),
                "buy_amount": to_float(row.get("f62")),
                "sell_amount": to_float(row.get("f184")),
                "net_amount": to_float(row.get("f66")),
                "buy_rate": to_float(row.get("f69"), scale=100),
            })
        
        _DRAGON_CACHE["data"] = results
        _DRAGON_CACHE["ts"] = now
        if results:
            return results
    except Exception:
        pass

    # Fallback: tushare top_list
    results = _get_dragon_tiger_list_tushare(limit=limit)
    if results:
        _DRAGON_CACHE["data"] = results
        _DRAGON_CACHE["ts"] = now
        return results

    return _DRAGON_CACHE.get("data") or []


def _normalize_ts_code(code: str) -> str:
    text = (code or "").lower()
    if text.endswith(".sh"):
        return f"sh{text[:-3]}"
    if text.endswith(".sz"):
        return f"sz{text[:-3]}"
    return text


def _get_dragon_tiger_list_tushare(limit: int = 10) -> List[Dict]:
    token = os.getenv("TUSHARE_TOKEN", "").strip()
    if not token:
        return []
    try:
        import tushare as ts
    except Exception:
        return []
    ts.set_token(token)
    pro = ts.pro_api()
    try:
        from .market import now_bj
    except Exception:
        return []

    # Try recent trading days (up to 3) to avoid empty same-day results.
    trade_date = now_bj().date()
    for _ in range(3):
        date_str = trade_date.strftime("%Y%m%d")
        df = None
        try:
            df = pro.top_list(trade_date=date_str)
        except Exception:
            try:
                df = ts.top_list(date_str)
            except Exception:
                df = None
        if df is None or getattr(df, "empty", True):
            trade_date = trade_date.fromordinal(trade_date.toordinal() - 1)
            continue

        results = []
        for _, row in df.iterrows():
            if len(results) >= limit:
                break
            code = _normalize_ts_code(str(row.get("ts_code") or row.get("code") or ""))
            name = str(row.get("name") or "").strip()
            if not code or not name:
                continue
            pchange = to_float(row.get("pchange"))
            buy = to_float(row.get("buy"))
            sell = to_float(row.get("sell"))
            net_amount = None
            if buy is not None and sell is not None:
                net_amount = (buy - sell) * 10000.0  # 万 -> 元
            results.append(
                {
                    "code": code,
                    "name": name,
                    "price": None,
                    "pct_change": pchange,
                    "turnover_rate": None,
                    "buy_amount": None if buy is None else buy * 10000.0,
                    "sell_amount": None if sell is None else sell * 10000.0,
                    "net_amount": net_amount,
                    "buy_rate": to_float(row.get("bratio")),
                }
            )
        return results
    return []


def _fetch_quote_for_trader(code: str) -> Optional[Dict]:
    """并行获取单只股票行情"""
    try:
        from .market import get_quote
        return get_quote(code)
    except Exception:
        return None


def get_famous_trader_stocks(max_workers: int = 5) -> List[Dict]:
    """获取游资常用席位关注的股票（并行行情获取）"""
    dragon_list = get_dragon_tiger_list(limit=20)
    
    results = []
    for item in dragon_list:
        net_amount = item.get("net_amount", 0) or 0
        buy_rate = item.get("buy_rate", 0) or 0
        
        if net_amount > 0 and buy_rate > 5:
            results.append({
                "code": item.get("code"),
                "name": item.get("name"),
                "price": item.get("price"),
                "pct_change": item.get("pct_change"),
                "turnover_rate": item.get("turnover_rate"),
                "net_amount": net_amount,
                "buy_rate": buy_rate,
                "reason": "龙虎榜净买入",
            })

    if not results:
        return results

    # 并行获取行情数据
    codes_to_fetch = []
    for row in results:
        code = row.get("code")
        if not code:
            continue
        need_turnover = row.get("turnover_rate") in (None, 0)
        need_pct = row.get("pct_change") is None
        need_price = row.get("price") is None
        if need_turnover or need_pct or need_price:
            codes_to_fetch.append(code)
    
    if codes_to_fetch:
        quote_map = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_fetch_quote_for_trader, code): code for code in codes_to_fetch}
            for future in as_completed(futures):
                code = futures[future]
                quote = future.result()
                if quote:
                    quote_map[code] = quote
        
        for row in results:
            code = row.get("code")
            if code not in quote_map:
                continue
            quote = quote_map[code]
            if row.get("turnover_rate") in (None, 0):
                row["turnover_rate"] = quote.get("turnover_rate")
            if row.get("pct_change") is None:
                row["pct_change"] = quote.get("pct_change")
            if row.get("price") is None:
                row["price"] = quote.get("price")

    return results
