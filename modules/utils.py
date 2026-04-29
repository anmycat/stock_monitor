import os
import time
import random
from typing import Any, Callable, Dict, Optional

import requests


_REQUEST_STATE = {"last_call": 0.0}
_CACHE_TTL_CONFIG = {
    "default": 300,
    "sector": 300,
    "sector_stocks": 60,
    "fund_flow": 300,
    "dragon": 300,
    "sentiment": 300,
    "etf_holdings": 3600,
    "etf_kline": 1800,
    "etf_panel": 300,
    "factors": 300,
    "kline": 300,
    "trading_day": 86400,
    "auction": 600,
}


def get_cache_ttl(cache_type: str = "default", default_ttl: Optional[int] = None) -> int:
    """获取缓存TTL"""
    if default_ttl is None:
        default_ttl = int(_CACHE_TTL_CONFIG.get(cache_type, _CACHE_TTL_CONFIG["default"]))
    raw = os.getenv(f"{cache_type.upper()}_CACHE_SECONDS", str(default_ttl))
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        value = int(default_ttl)
    return max(1, value)


class CacheManager:
    """统一缓存管理器"""
    
    def __init__(self, cache_type: str = "default"):
        self._cache: Dict[str, Dict] = {}
        self._cache_type = cache_type
    
    def get(self, key: str) -> Optional[Any]:
        """获取缓存值"""
        if key not in self._cache:
            return None
        cached = self._cache[key]
        ttl = get_cache_ttl(self._cache_type)
        if time.time() - cached.get("ts", 0) > ttl:
            del self._cache[key]
            return None
        return cached.get("data")
    
    def set(self, key: str, data: Any):
        """设置缓存值"""
        self._cache[key] = {"data": data, "ts": time.time()}
    
    def get_or_set(self, key: str, fetcher: Callable, *args, **kwargs) -> Any:
        """获取缓存或调用fetcher获取并缓存"""
        cached = self.get(key)
        if cached is not None:
            return cached
        data = fetcher(*args, **kwargs)
        self.set(key, data)
        return data
    
    def clear(self, key: Optional[str] = None):
        """清除缓存"""
        if key:
            self._cache.pop(key, None)
        else:
            self._cache.clear()
    
    def size(self) -> int:
        """获取缓存大小"""
        return len(self._cache)


def request_with_throttle(url: str, timeout: int = 8, params: Optional[dict] = None) -> requests.Response:
    """带限频的HTTP请求"""
    min_interval = float(os.getenv("REQUEST_MIN_INTERVAL_SECONDS", "1.2"))
    jitter_max = float(os.getenv("REQUEST_JITTER_MAX_SECONDS", "0.8"))
    
    elapsed = time.time() - _REQUEST_STATE["last_call"]
    wait = max(0.0, min_interval - elapsed) + random.uniform(0.0, max(0.0, jitter_max))
    if wait > 0:
        time.sleep(wait)
    
    headers = {
        "User-Agent": "stock-monitor/1.0 (+https://github.com/anmycat/stock_monitor)",
        "Accept": "*/*",
    }
    
    response = requests.get(url, timeout=timeout, params=params, headers=headers)
    _REQUEST_STATE["last_call"] = time.time()
    return response


def to_float(value, scale: float = 1.0) -> Optional[float]:
    """安全转换为浮点数"""
    if value is None or value == "":
        return None
    try:
        if isinstance(value, str):
            value = (
                value.strip()
                .strip('"')
                .strip("'")
                .replace(",", "")
                .replace("%", "")
            )
            if value in {"", "-", "--", "NA", "N/A", "None", "null"}:
                return None
        return float(value) / float(scale)
    except (ValueError, TypeError, ZeroDivisionError):
        return None
