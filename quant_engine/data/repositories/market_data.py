"""行情数据仓储层（P1 重构，2026-10-22，prd/15 §2.2）

由 DataCenter 拆分而来，保留其取数编排 + 缓存门面职责：
公开方法签名、缓存 key/TTL、线程锁保护、B5 卫生断言全部逐一对齐
（调用方零行为变化）。数据校验已移至 utils/market_sanity.validate_dataframe
（B5 单一事实源）；源注册与容错仍在 data/source.py + source_manager.py。

2026-10-25（docs/25 §5.2）：get_stock_list/get_daily_data/get_minute_data
三个稳定键路径增加进程内 in-flight 请求合并（InflightMerger）——同键
并发只放行一个真实取数，其余等待复用其结果，避免重复冲击 TDX/东财。
"""
from __future__ import annotations

import threading
import time

import pandas as pd
from loguru import logger

from ...config import get_settings
from ...utils.calendar import get_market_phase, is_trading_time
from ...utils.market_sanity import (
    DROP_RATIO_SUSPECT,
    is_overview_sane,
    sanitize_quotes,
    validate_dataframe,
)
from ..cache import RedisCache
from ..inflight import InflightMerger
from ..source import CompositeSource
from ..stock_info import get_stock_info_table


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
        # I-02：源健康状态跨重启保留（只在此处 attach 一次，避免每次请求
        # 从 Redis 恢复旧状态覆盖本进程刚刚更新的内存态）
        try:
            from ..source_manager import source_manager
            source_manager.attach_cache(self.cache)
        except Exception as e:  # noqa: BLE001 旁路恢复失败不得影响启动
            logger.debug(f"[I-02] source state attach skipped: {e}")
        # D68：东财「三招」跨进程全局限速。复用同一个已连接的 RedisCache，
        # 不新建连接池；本地无 Redis（is_connected=False）时自动跳过，
        # 完全退化为 D53 的进程内限速，不产生任何 socket 探测开销。
        try:
            from ..sources.eastmoney import attach_global_throttle
            attach_global_throttle(self.cache)
        except Exception as e:  # noqa: BLE001 旁路装配失败不得影响启动
            logger.debug(f"[EM] global throttle attach skipped: {e}")
        self._validation_errors = []
        # 线程锁保护共享缓存
        self._cache_lock = threading.Lock()
        self._all_quotes_cache = None
        self._all_quotes_cache_time = 0
        # in-flight 请求合并（docs/25 §5.2）；惰性创建兼容 __new__ 构造的测试替身
        self._inflight = InflightMerger()

    def _merger(self) -> InflightMerger:
        """in-flight 合并器访问器（__new__ 手工构造的替身无该属性时兜底）"""
        merger = getattr(self, '_inflight', None)
        if merger is None:
            merger = InflightMerger()
            self._inflight = merger
        return merger

    def get_stock_list(self) -> pd.DataFrame:
        cache_key = "stock_list"
        cached = self.cache.get(cache_key)
        if cached is not None:
            df = pd.DataFrame(cached)
            if validate_dataframe(df, "stock_list"):
                return self._filter_markets(df)
        return self._merger().run(cache_key, lambda: self._fetch_stock_list(cache_key))

    @staticmethod
    def _filter_markets(df: pd.DataFrame) -> pd.DataFrame:
        """过滤项目不支持的市场（D44：修复 10-26 日志 920285.BJ 每轮
        扫描浪费一次 TDX 取数 + 一条 ERROR）。

        TDX 网关不支持北交所（tdx.py 实测 920819 两市场均空），且分钟线
        双源（TDX/腾讯）对 BJ 均不可用——扫描池排除 .BJ 是数据源能力
        边界，不是临时规避。缓存命中路径同样过滤：存量缓存（TTL 86400）
        在部署后过期前仍含 BJ。
        """
        if df.empty or 'code' not in df.columns:
            return df
        return df[~df['code'].astype(str).str.endswith('.BJ')]

    def _fetch_stock_list(self, cache_key: str) -> pd.DataFrame:
        # D49：静态信息表 fresh（默认 7 天内）→ 本地构建，不打行情源。
        # 静态表数据来自既有 get_stock_list 链路的 ingest / 盘后 refresh，
        # 与源返回同为 code/name/industry 三列静态信息。
        info = get_stock_info_table()
        if info.is_fresh():
            df = info.to_dataframe()
            if not df.empty:
                self.cache.set(cache_key, df.to_dict(orient='records'), ttl=86400)
                logger.debug(f"[StockInfo] stock_list 本地构建 {len(df)} 只（未打源）")
                return self._filter_markets(df)
        df = self.source.get_stock_list()
        if validate_dataframe(df, "stock_list"):
            self.cache.set(cache_key, df.to_dict(orient='records'), ttl=86400)
            # D49：借势 ingest（不发起新请求，频率由 MIN_INGEST_ROWS 防兜底污染）
            try:
                info.ingest(df)
            except Exception as e:  # noqa: BLE001 兜底：静态表失败不影响股票池
                logger.debug(f"[StockInfo] ingest 跳过: {e}")
        return self._filter_markets(df)

    def get_daily_data(self, code: str, days: int = 120) -> pd.DataFrame:
        # D66：缓存 key 带「期望交易日」——隔夜后自动失效，从根上杜绝
        # 次日读到昨天的日K缓存（TTL 3600 本可缓解，但 worker 长睡眠/
        # 重启后旧 key 仍可能被复用）。历史/回测调用同样安全：它们传的
        # session 由调用方固定，不随墙钟漂移。
        session = self._daily_session()
        cache_key = f"daily:{session}:{code}:{days}"
        cached = self.cache.get(cache_key)
        if cached is not None and validate_dataframe(df := pd.DataFrame(cached), f"daily:{code}"):
            return self._restore_daily_tags(df, code, session)
        return self._merger().run(
            cache_key, lambda: self._fetch_daily_data(code, days, cache_key, session))

    @staticmethod
    def _daily_session() -> str:
        from ...config import get_settings
        from ...utils.kline_provenance import expected_session
        if not bool(get_settings().KLINE_DAY_BOUNDARY_GUARD):
            # 关闭日切护栏时退回旧口径（用于 A/B 对照与应急回退）
            return 'legacy'
        return expected_session()

    def _restore_daily_tags(self, df: pd.DataFrame, code: str,
                            session: str | None = None) -> pd.DataFrame:
        """缓存命中路径也要有标签（缓存里存不了 attrs，需重建）。"""
        from ...utils import kline_frame
        from ...utils import kline_provenance as prov
        from .. import daily_guard

        session = session or self._daily_session()
        prov.tag(df, 'cache')
        # I-04：缓存命中同样做整帧校验，避免异常帧借缓存「洗白」
        try:
            if bool(get_settings().KLINE_FRAME_VALIDATION):
                kline_frame.annotate(df, session, cache=getattr(self, 'cache', None),
                                     code=code)
        except Exception as e:  # noqa: BLE001 标注失败不得影响取数
            logger.debug(f"[I-04] frame annotate on cache hit failed: {e}")
        # 缓存命中也登记封存候选：worker 重启/多进程时保证当日 bar 不漏
        try:
            if bool(get_settings().KLINE_EOD_SEAL):
                daily_guard.collector.collect(df, code)
        except Exception as e:  # noqa: BLE001 封存收集异常不得影响取数
            logger.debug(f"[D66] seal collect on cache hit failed: {e}")
        return df

    def invalidate_daily_cache(self, code: str, session: str | None = None) -> int:
        """I-04：数据修补后统一失效该票日K缓存（含历史 session 残留）。

        外部修复历史涨跌停/复权/缺失 bar 后必须调用，否则新数据要等
        TTL 3600 才生效。返回删除的 key 数。
        """
        pattern = (f'daily:{session}:{code}:*' if session
                   else f'daily:*:{code}:*')
        keys = self.cache.keys(pattern)
        # 二次精确过滤：不依赖缓存后端的 glob 语义（Redis SCAN / 内存替身
        # 行为可能不同），避免误删别的标的缓存。
        keys = [k for k in keys
                if k.startswith('daily:') and f':{code}:' in k
                and (not session or k.startswith(f'daily:{session}:'))]
        n = 0
        for key in keys:
            if self.cache.delete(key):
                n += 1
        if n:
            logger.info(f"[I-04] 失效日K缓存 {code} {n} 个 key（{pattern}）")
        return n

    def _fetch_daily_data(self, code: str, days: int, cache_key: str,
                          session: str | None = None) -> pd.DataFrame:
        from ...utils import kline_provenance as prov
        from .. import daily_guard

        session = session or self._daily_session()
        df = self.source.get_daily_data(code, days)
        if df is None or df.empty or not validate_dataframe(df, f"daily:{code}"):
            return df

        # ① 血缘标签（只写 attrs，不动任何列 → 趋势/指标零影响）
        prov.tag(df, prov.read(df).get('source') or 'composite')

        # ①b I-04 整帧完整性校验：逐根查交易日/单调/价格，结论只写 attrs。
        #     DSA #2389 的关键点是「因子消费整份序列，混入一个节假日 bar 即整帧不可信」，
        #     D66 的 assess() 只判末根，补上这一层。
        try:
            if bool(get_settings().KLINE_FRAME_VALIDATION):
                from ...utils import kline_frame
                kline_frame.annotate(
                    df, session, cache=getattr(self, 'cache', None), code=code)
        except Exception as e:  # noqa: BLE001 标注失败不得影响取数
            logger.debug(f"[I-04] frame annotate skipped for {code}: {e}")

        # ② 针对性跨源复核：仅「疑似降级」时打第二源，正常路径零额外请求
        try:
            if bool(get_settings().KLINE_CROSS_CHECK):
                daily_guard.apply_cross_check(
                    df, code, session, cache=getattr(self, 'cache', None))
        except Exception as e:  # noqa: BLE001 复核失败不得影响取数/配置缺失
            logger.debug(f"[D66] cross-check skipped for {code}: {e}")

        # ③ 收集当日完成 bar，供盘后打包封存（零额外行情请求）
        try:
            if bool(get_settings().KLINE_EOD_SEAL):
                daily_guard.collector.collect(df, code, session)
        except Exception as e:  # noqa: BLE001
            logger.debug(f"[D66] seal collect skipped for {code}: {e}")

        freshness = prov.assess(df, session)
        if freshness != 'current':
            logger.info(f"[D66][新鲜度] {code} {prov.describe(df, session)}")
        # ④ 疑似 T-1 时用封存的「日期锚点」确认，明确标注它不是今天
        if freshness == 'lagging':
            try:
                anchor = daily_guard.verify_against_seal(df, code, session)
                attrs = getattr(df, 'attrs', {})
                attrs['kline_seal_check'] = anchor
                if anchor.get('status') == 'matches':
                    attrs['kline_sealed'] = True
                    logger.info(
                        f"[D66][封存锚点] {code} 末根 {anchor['as_of']} 与封存一致，"
                        f"确认为已完成的上一交易日 bar（非 {session} 数据）")
            except Exception as e:  # noqa: BLE001
                logger.debug(f"[D66] seal verify skipped for {code}: {e}")
        # ④b I-04 拒绝层（默认关闭）：只在开关显式开启时生效。
        #     开启会改变信号，必须先做 D41 双跑 + 半年窗口复验 + 用户裁定；
        #     关闭时是纯 no-op，D41 零漂移。
        try:
            from ...utils.kline_frame import should_reject
            if should_reject(df, session):
                logger.warning(
                    f"[I-04][拒绝层] {code} 整帧异常且非当日数据 → 不出厂 "
                    f"({prov.describe(df, session)})")
                return pd.DataFrame()
        except Exception as e:  # noqa: BLE001 拒绝层异常时保守放行
            logger.debug(f"[I-04] reject check skipped for {code}: {e}")
        self.cache.set(cache_key, df.to_dict(orient='records'), ttl=3600)
        return df

    def get_minute_data(self, code: str, period: str = "5") -> pd.DataFrame:
        cache_key = f"scan:{code}:{period}"
        cached = self.cache.get(cache_key)
        if cached is not None and validate_dataframe(df := pd.DataFrame(cached), f"minute:{code}"):
            return df
        return self._merger().run(
            cache_key, lambda: self._fetch_minute_data(code, period, cache_key))

    def _fetch_minute_data(self, code: str, period: str, cache_key: str) -> pd.DataFrame:
        df = self.source.get_minute_data(code, period)
        if df is not None and not df.empty and validate_dataframe(df, f"minute:{code}"):
            self.cache.set(cache_key, df.to_dict(orient='records'), ttl=300)
        return df

    def get_realtime_quote(self, codes: list[str]) -> pd.DataFrame:
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

    def get_cached_quote(self, code: str) -> dict | None:
        cache_key = f"quote:{code}"
        return self.cache.get(cache_key)

    def set_cached_quote(self, code: str, data: dict):
        cache_key = f"quote:{code}"
        self.cache.set(cache_key, data, ttl=60)

    def get_market_overview(self) -> dict:
        cache_key = "market_overview"
        cached = self.cache.get(cache_key)

        # 非交易时间直接返回缓存数据，不更新（缓存为空则获取一次数据）
        phase = get_market_phase()
        if phase in ["MORNING_CLOSED", "AFTER_HOURS", "WEEKEND", "PRE_MARKET"] and cached is not None:
            return cached

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
            'total': len(df),
            'up': len(df[df['change_pct'] > 0]),
            'down': len(df[df['change_pct'] < 0]),
            'flat': len(df[df['change_pct'] == 0]),
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
