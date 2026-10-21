"""行情数据仓储层（P1 重构，2026-10-22，prd/15 §2.2）

由 DataCenter 拆分而来，保留其取数编排 + 缓存门面职责：
公开方法签名、缓存 key/TTL、线程锁保护、B5 卫生断言全部逐一对齐
（调用方零行为变化）。数据校验已移至 utils/market_sanity.validate_dataframe
（B5 单一事实源）；源注册与容错仍在 data/source.py + source_manager.py。
"""
import threading
import time
from typing import List, Optional, Dict

import pandas as pd
from loguru import logger

from ..cache import RedisCache
from ..source import CompositeSource
from ...config import get_settings
from ...utils.calendar import is_trading_time, get_market_phase
from ...utils.market_sanity import (
    validate_dataframe,
    sanitize_quotes,
    is_overview_sane,
    DROP_RATIO_SUSPECT,
)


class MarketDataRepository:
    """行情数据仓储：多源取数 + Redis/进程内双层缓存 + 市场概览（B5 断言）"""

    def __init__(self):
        settings = get_settings()
        self.cache = RedisCache(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD if settings.REDIS_PASSWORD else None,
            ssl=settings.REDIS_SSL
        )
        self.source = CompositeSource()
        self.source.datacenter = self  # 设置引用（CompositeSource 缓存写入用）
        self.cache.connect()
        self._validation_errors = []
        # 线程锁保护共享缓存
        self._cache_lock = threading.Lock()
        self._all_quotes_cache = None
        self._all_quotes_cache_time = 0

    def get_stock_list(self) -> pd.DataFrame:
        cache_key = "stock_list"
        cached = self.cache.get(cache_key)
        if cached is not None:
            df = pd.DataFrame(cached)
            if validate_dataframe(df, "stock_list"):
                return df
        df = self.source.get_stock_list()
        if validate_dataframe(df, "stock_list"):
            self.cache.set(cache_key, df.to_dict(orient='records'), ttl=86400)
        return df

    def get_daily_data(self, code: str, days: int = 120) -> pd.DataFrame:
        cache_key = f"daily:{code}:{days}"
        cached = self.cache.get(cache_key)
        if cached is not None:
            df = pd.DataFrame(cached)
            if validate_dataframe(df, f"daily:{code}"):
                return df
        df = self.source.get_daily_data(code, days)
        if df is not None and not df.empty:
            if validate_dataframe(df, f"daily:{code}"):
                self.cache.set(cache_key, df.to_dict(orient='records'), ttl=3600)
        return df

    def get_minute_data(self, code: str, period: str = "5") -> pd.DataFrame:
        cache_key = f"scan:{code}:{period}"
        cached = self.cache.get(cache_key)
        if cached is not None:
            df = pd.DataFrame(cached)
            if validate_dataframe(df, f"minute:{code}"):
                return df
        df = self.source.get_minute_data(code, period)
        if df is not None and not df.empty:
            if validate_dataframe(df, f"minute:{code}"):
                self.cache.set(cache_key, df.to_dict(orient='records'), ttl=300)
        return df

    def get_realtime_quote(self, codes: List[str]) -> pd.DataFrame:
        cached_results = []
        uncached_codes = []
        for code in codes:
            cached = self.get_cached_quote(code)
            if cached:
                cached_results.append(cached)
            else:
                uncached_codes.append(code)
        if uncached_codes:
            df = self.source.get_realtime_quote(uncached_codes)
            if df is not None and not df.empty:
                records = df.to_dict('records')
                for row in records:
                    code = row.get('code')
                    if code:
                        self.set_cached_quote(code, row)
                cached_results.extend(records)
        result = pd.DataFrame(cached_results) if cached_results else pd.DataFrame()
        return result

    def get_all_realtime_quotes(self) -> pd.DataFrame:
        # 非交易时间返回缓存数据
        if not is_trading_time():
            cached = self.cache.get("all_realtime_quotes")
            if cached:
                logger.debug("Non-trading time, using cached data")
                return pd.DataFrame(cached)
            return pd.DataFrame()

        current_time = time.time()
        # 使用线程锁保护共享缓存访问
        with self._cache_lock:
            if self._all_quotes_cache is not None and current_time - self._all_quotes_cache_time < 30:
                return self._all_quotes_cache

        cache_key = "all_realtime_quotes"
        cached = self.cache.get(cache_key)
        if cached is not None:
            df = pd.DataFrame(cached)
            if not df.empty:
                with self._cache_lock:
                    self._all_quotes_cache = df
                    self._all_quotes_cache_time = current_time
                return df

        df = self.source.get_all_realtime_quotes()
        if df is not None and not df.empty:
            self.cache.set(cache_key, df.to_dict(orient='records'), ttl=30)
            with self._cache_lock:
                self._all_quotes_cache = df
                self._all_quotes_cache_time = current_time
        return df

    def get_cached_quote(self, code: str) -> Optional[Dict]:
        cache_key = f"quote:{code}"
        return self.cache.get(cache_key)

    def set_cached_quote(self, code: str, data: Dict):
        cache_key = f"quote:{code}"
        self.cache.set(cache_key, data, ttl=60)

    def get_market_overview(self) -> Dict:
        cache_key = "market_overview"
        cached = self.cache.get(cache_key)

        # 非交易时间直接返回缓存数据，不更新
        phase = get_market_phase()
        if phase in ["MORNING_CLOSED", "AFTER_HOURS", "WEEKEND", "PRE_MARKET"]:
            if cached is not None:
                return cached
            # 如果缓存为空，获取一次数据
            pass

        # 交易时间内且缓存有效，直接返回
        if cached is not None:
            return cached

        # 获取所有行情数据
        df = self.get_all_realtime_quotes()
        if df.empty:
            logger.warning("No realtime quotes available for market overview")
            return {'total': 0, 'up': 0, 'down': 0, 'flat': 0, 'avg_change': 0, 'suspect': True}

        # B5 数据卫生断言（2026-10-21）：剔除 change_pct 超物理上限 / price<=0 / 非有限行；
        # 剔除率 >20% 视为数据源系统性损坏 → 记 error 并出厂 suspect 空结果（不缓存脏值）
        df, sanity = sanitize_quotes(df)
        if df.empty or sanity['drop_ratio'] > DROP_RATIO_SUSPECT:
            logger.error(f"[B5] market overview source data suspect: {sanity} — refusing to publish dirty stats")
            return {'total': 0, 'up': 0, 'down': 0, 'flat': 0, 'avg_change': 0, 'suspect': True}

        overview = {
            'total': int(len(df)),
            'up': int(len(df[df['change_pct'] > 0])),
            'down': int(len(df[df['change_pct'] < 0])),
            'flat': int(len(df[df['change_pct'] == 0])),
            'avg_change': round(float(df['change_pct'].mean()), 2),
            'suspect': False,
        }
        # 出口断言不过 → 同样不出厂（理论不可达，防御性保留）
        if not is_overview_sane({k: v for k, v in overview.items() if k != 'suspect'}):
            logger.error(f"[B5] market overview failed exit assertion: {overview}")
            return {'total': 0, 'up': 0, 'down': 0, 'flat': 0, 'avg_change': 0, 'suspect': True}
        # 交易时间内缓存60秒，非交易时间缓存到下一个交易周期
        if phase in ["MORNING_SESSION", "AFTERNOON_SESSION"]:
            self.cache.set(cache_key, overview, ttl=60)
        else:
            self.cache.set(cache_key, overview, ttl=86400)  # 24小时
        logger.info(f"Market overview updated: {overview}")
        return overview
