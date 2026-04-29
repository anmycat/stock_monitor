import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

from .utils import request_with_throttle, to_float


_FACTOR_CACHE = {}


def _get_factor_cache_ttl():
    return max(60, int(os.getenv("FACTOR_CACHE_SECONDS", "300")))


def _to_secid(code: str) -> str:
    text = code.lower()
    if text.startswith("sh"):
        return f"1.{text[2:]}"
    if text.startswith("sz"):
        return f"0.{text[2:]}"
    return f"1.{text}" if text.startswith(("5", "6", "9")) else f"0.{text}"


def get_stock_factors(code: str) -> Dict:
    """获取股票多维度因子数据"""
    cache_key = f"factors_{code}"
    cache_ttl = _get_factor_cache_ttl()
    now = time.time()
    
    if cache_key in _FACTOR_CACHE:
        cached = _FACTOR_CACHE[cache_key]
        if (now - cached.get("ts", 0)) < cache_ttl:
            return cached.get("data", {})
    
    try:
        secid = _to_secid(code)
        
        url = "https://push2.eastmoney.com/api/qt/stock/get"
        params = {
            "secid": secid,
            "fields": "f2,f3,f4,f8,f9,f10,f12,f14,f20,f21,f23,f24,f25,f37,f38,f39,f40,f41,f42,f43,f44,f45,f46,f47,f48,f49,f50,f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f62,f115,f117,f128,f140,f141,f142,f143,f144,f145,f146,f147,f148,f149,f150,f151,f152,f153,f154,f155,f156,f157,f158,f159,f160,f161,f162,f163,f164,f165,f166,f167,f168,f169,f170,f171,f172,f173,f174,f175,f176,f177,f178,f179,f180,f181,f182,f183,f184,f185,f186,f187,f188,f189,f190,f191,f192,f193,f194,f195,f196,f197,f198,f199,f200,f201,f202,f203,f204,f205,f206,f207,f208,f209,f210",
        }
        response = request_with_throttle(url, timeout=10, params=params)
        response.raise_for_status()
        payload = response.json()
        
        data = payload.get("data", {})
        if not data:
            raise ValueError(f"no data for {code}")
        
        result = {
            "code": code,
            "name": data.get("f58"),
            "price": to_float(data.get("f43")),
            "pct_change": to_float(data.get("f170"), scale=100),
            "turnover_rate": to_float(data.get("f168"), scale=100),
            "volume": to_float(data.get("f47")),
            "amount": to_float(data.get("f48")),
            "pe": to_float(data.get("f162")),
            "pb": to_float(data.get("f167")),
            "main_net_inflow": to_float(data.get("f173")),
            "super_net_inflow": to_float(data.get("f174")),
            "large_net_inflow": to_float(data.get("f175")),
            "medium_net_inflow": to_float(data.get("f176")),
            "small_net_inflow": to_float(data.get("f177")),
            "turnover_rate_5d": to_float(data.get("f187")),
            "turnover_rate_10d": to_float(data.get("f188")),
            "amplitude": to_float(data.get("f49"), scale=100),
            "high_limit_count": to_float(data.get("f92")),
            "float_share": to_float(data.get("f84")),
            "total_share": to_float(data.get("f83")),
            "float_mv": to_float(data.get("f116")),
            "total_mv": to_float(data.get("f117")),
        }
        
        _FACTOR_CACHE[cache_key] = {"ts": now, "data": result}
        return result
        
    except Exception as e:
        fallback_result = _get_factors_fallback(code)
        if fallback_result and "error" not in fallback_result:
            _FACTOR_CACHE[cache_key] = {"ts": now, "data": fallback_result}
        return fallback_result


def _get_factors_fallback(code: str) -> Dict:
    """Fallback: 使用market.py获取基本数据"""
    try:
        from .market import get_quote
        quote = get_quote(code)
        if quote:
            return {
                "code": code,
                "name": quote.get("name"),
                "price": quote.get("price"),
                "pct_change": quote.get("pct_change"),
                "turnover_rate": quote.get("turnover_rate"),
                "volume": quote.get("volume"),
                "amount": quote.get("amount"),
                "pe": None,
                "pb": None,
                "main_net_inflow": None,
                "source": "fallback_market",
            }
    except Exception:
        pass
    return {"code": code, "error": "EastMoney API unavailable, fallback failed"}


def get_volatility_factor(code: str, days: int = 20) -> Optional[float]:
    """获取波动率因子 (20日收益率标准差)"""
    cache_key = f"volatility_{code}_{days}"
    cache_ttl = _get_factor_cache_ttl()
    now = time.time()
    
    if cache_key in _FACTOR_CACHE:
        cached = _FACTOR_CACHE[cache_key]
        if (now - cached.get("ts", 0)) < cache_ttl:
            return cached.get("data", {}).get("volatility")
    
    try:
        secid = _to_secid(code)
        end_date = time.strftime("%Y%m%d")
        start_date = time.strftime("%Y%m%d", time.localtime(time.time() - days * 24 * 3600))
        
        url = "https://push2.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": secid,
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65,f66,f67,f68",
            "klt": "101",
            "fqt": "1",
            "beg": start_date,
            "end": end_date,
            "lmt": days + 10,
        }
        response = request_with_throttle(url, timeout=10, params=params)
        response.raise_for_status()
        payload = response.json()
        
        klines = (payload.get("data") or {}).get("klines", [])
        if not klines or len(klines) < 5:
            return None
        
        closes = []
        for line in klines[-days:]:
            parts = line.split(",")
            if len(parts) >= 4:
                close = to_float(parts[2])
                if close is not None:
                    closes.append(close)
        
        if len(closes) < 5:
            return None
        
        import statistics
        if len(closes) >= 2:
            returns = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(1, len(closes))]
            volatility = statistics.stdev(returns) if len(returns) > 1 else 0.0
        else:
            volatility = 0.0
        
        result = {"volatility": volatility}
        _FACTOR_CACHE[cache_key] = {"ts": now, "data": result}
        return volatility
        
    except Exception:
        return None


def calculate_factor_score(factors: Dict) -> Dict:
    """计算多因子综合评分 (BigQuant风格)"""
    if not factors or "error" in factors:
        return {"score": 0, "factors": {}, "signal": "INVALID", "details": {}}
    
    scores = {}
    details = {}
    
    pe = factors.get("pe")
    if pe is not None and pe > 0:
        if pe > 500:
            pe = pe / 100
        if pe < 0:
            pe_score = 40
        elif pe < 15:
            pe_score = 100 - (pe / 15 * 30)
        elif pe < 30:
            pe_score = 70 - ((pe - 15) / 15 * 30)
        else:
            pe_score = max(0, 40 - ((pe - 30) / 30 * 40))
        scores["pe"] = pe_score
        details["pe"] = {"value": pe, "score": round(pe_score, 1)}
    else:
        scores["pe"] = 50
        details["pe"] = {"value": pe, "score": 50}
    
    pb = factors.get("pb")
    if pb is not None and pb > 0:
        if pb > 100:
            pb = pb / 100
        if pb < 0:
            pb_score = 40
        elif pb < 2:
            pb_score = 100 - (pb / 2 * 30)
        elif pb < 5:
            pb_score = 70 - ((pb - 2) / 3 * 30)
        else:
            pb_score = max(0, 40 - ((pb - 5) / 5 * 40))
        scores["pb"] = pb_score
        details["pb"] = {"value": pb, "score": round(pb_score, 1)}
    else:
        scores["pb"] = 50
        details["pb"] = {"value": pb, "score": 50}
    
    turnover = factors.get("turnover_rate")
    if turnover is not None:
        if 3 <= turnover <= 10:
            turnover_score = 100
        elif turnover > 10:
            turnover_score = max(0, 100 - (turnover - 10) * 5)
        elif turnover > 0:
            turnover_score = turnover / 3 * 80
        else:
            turnover_score = 30
        scores["turnover"] = turnover_score
        details["turnover"] = {"value": turnover, "score": round(turnover_score, 1)}
    else:
        scores["turnover"] = 50
        details["turnover"] = {"value": turnover, "score": 50}
    
    main_inflow = factors.get("main_net_inflow")
    if main_inflow is not None:
        if main_inflow > 10000:
            inflow_score = 100
        elif main_inflow > 0:
            inflow_score = 50 + (main_inflow / 10000 * 50)
        elif main_inflow > -5000:
            inflow_score = 50 + (main_inflow / 5000 * 30)
        else:
            inflow_score = max(0, 20 + (main_inflow / 20000 * 20))
        scores["fund_flow"] = inflow_score
        details["fund_flow"] = {"value": main_inflow, "score": round(inflow_score, 1)}
    else:
        scores["fund_flow"] = 50
        details["fund_flow"] = {"value": main_inflow, "score": 50}
    
    pct_change = factors.get("pct_change")
    if pct_change is not None:
        if 0 < pct_change <= 5:
            momentum_score = 100
        elif pct_change > 5:
            momentum_score = max(0, 100 - (pct_change - 5) * 3)
        elif pct_change > -3:
            momentum_score = 50 + (pct_change + 3) / 3 * 30
        else:
            momentum_score = max(0, 30 + pct_change)
        scores["momentum"] = momentum_score
        details["momentum"] = {"value": pct_change, "score": momentum_score}
    else:
        scores["momentum"] = 50
        details["momentum"] = {"value": pct_change, "score": 50}
    
    weights = {
        "pe": float(os.getenv("FACTOR_WEIGHT_PE", "0.15")),
        "pb": float(os.getenv("FACTOR_WEIGHT_PB", "0.10")),
        "turnover": float(os.getenv("FACTOR_WEIGHT_TURNOVER", "0.20")),
        "fund_flow": float(os.getenv("FACTOR_WEIGHT_FUND_FLOW", "0.25")),
        "momentum": float(os.getenv("FACTOR_WEIGHT_MOMENTUM", "0.30")),
    }
    
    total_score = sum(scores.get(k, 50) * v for k, v in weights.items())
    
    if total_score >= 75:
        signal = "STRONG_BUY"
    elif total_score >= 60:
        signal = "BUY"
    elif total_score >= 45:
        signal = "HOLD"
    elif total_score >= 30:
        signal = "SELL"
    else:
        signal = "STRONG_SELL"
    
    return {
        "score": round(total_score, 2),
        "signal": signal,
        "weights": weights,
        "details": details,
        "factors": factors,
    }


def _fetch_single_factor(code: str) -> Optional[Dict]:
    """并行获取单只股票因子"""
    try:
        factors = get_stock_factors(code)
        if "error" in factors:
            return None
        
        score_result = calculate_factor_score(factors)
        return {
            "code": code,
            "name": factors.get("name"),
            "score": score_result["score"],
            "signal": score_result["signal"],
            "pe": factors.get("pe"),
            "pb": factors.get("pb"),
            "turnover_rate": factors.get("turnover_rate"),
            "main_net_inflow": factors.get("main_net_inflow"),
            "pct_change": factors.get("pct_change"),
            "factor_details": score_result.get("details"),
        }
    except Exception:
        return None


def score_stocks_by_factors(codes: List[str], min_score: float = 60, max_workers: int = 10) -> List[Dict]:
    """对股票列表进行多因子评分筛选（并行处理）"""
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_single_factor, code): code for code in codes}
        for future in as_completed(futures):
            result = future.result()
            if result and result["score"] >= min_score:
                results.append(result)
    
    results.sort(key=lambda x: x["score"], reverse=True)
    return results


def get_factor_ranking(stocks: List[Dict], max_workers: int = 10) -> List[Dict]:
    """对股票进行因子排名分析（并行处理）"""
    if not stocks:
        return []
    
    codes = [s.get("code", "") for s in stocks if s.get("code")]
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_single_factor, code): code for code in codes}
        factor_map = {}
        for future in as_completed(futures):
            result = future.result()
            if result:
                factor_map[result["code"]] = result
    
    for stock in stocks:
        code = stock.get("code", "")
        if code in factor_map:
            factor_data = factor_map[code]
            stock["factor_score"] = factor_data["score"]
            stock["factor_signal"] = factor_data["signal"]
            stock["factor_details"] = factor_data.get("factor_details")
    
    stocks.sort(key=lambda x: x.get("factor_score", 0), reverse=True)
    return stocks
