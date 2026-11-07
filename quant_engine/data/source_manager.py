"""数据源管理器 - 多数据源轮询、冷却、随机延迟"""
from __future__ import annotations

import random
import time
from collections.abc import Callable

from loguru import logger

# I-05：网络不可达/超时类失败属于「再试一次大概率还是失败」，盲重试只会
# 把单次 10s 放大成 30s 并拖垮全市场扫描。用异常类名 + 消息特征识别，
# 不硬依赖 requests / urllib3（各源实现可能换库）。
_NETWORK_ERROR_NAMES = frozenset({
    'TimeoutError', 'Timeout', 'ConnectTimeout', 'ReadTimeout',
    'RemoteDisconnected', 'ConnectionError', 'ProxyError',
    'ConnectionAbortedError', 'ConnectionResetError', 'socket.timeout',
})
_NETWORK_ERROR_TEXT = (
    'timed out', 'timeout', 'remote end closed', 'no route to host',
    'connection refused', 'network is unreachable', 'max retries exceeded',
)


def classify_failure(exc: BaseException) -> tuple[str, bool]:
    """返回 ``(kind, non_retriable)``；非网络类异常保持原有重试语义。"""
    name = type(exc).__name__
    text = str(exc).lower()
    if name in _NETWORK_ERROR_NAMES or any(t in text for t in _NETWORK_ERROR_TEXT):
        return 'timeout', True
    return 'error', False


class SourceStatus:
    """数据源状态"""
    def __init__(self, name: str):
        self.name = name
        self.error_count = 0
        self.empty_count = 0  # 空结果次数（与真实错误分开计，见 record_empty）
        self.last_error_time = 0
        self.last_request_time = 0
        self.request_count = 0
        self.success_count = 0
        self.cooldown_until = 0
        self.quarantined_until = 0  # 熔断截止时间（D53）
        self._cooldown_streak = 0  # 连续进入冷却的轮次
        # I-05：失败路径结构化（供能力中心 I-02 与超时抑制使用）
        self.timeout_streak = 0
        self.suppress_until = 0.0
        self.last_error_kind: str | None = None
        self.last_error_message: str | None = None
        self.last_empty_reason: str | None = None
        self.missing_codes: list[str] = []
        self.last_success_time = 0.0
        self.fallback_count = 0
        self.on_state_change = None  # 由 SourceManager 注入（节流持久化）

    def is_available(self, cooldown_seconds: int = 300) -> bool:
        """检查数据源是否可用（冷却中或熔断中均不可用）"""
        now = time.time()
        if now < self.quarantined_until:
            return False
        if now < self.suppress_until:
            return False
        return not now < self.cooldown_until

    def _changed(self) -> None:
        cb = self.on_state_change
        if cb is not None:
            try:
                cb()
            except Exception:  # noqa: S110 状态持久化是旁路，不得影响取数
                pass

    def record_success(self):
        """记录成功请求"""
        had_failure = bool(self.error_count or self.empty_count or self.timeout_streak)
        self.request_count += 1
        self.success_count += 1
        self.last_request_time = time.time()
        self.last_success_time = time.time()
        self.error_count = 0
        self.empty_count = 0
        self._cooldown_streak = 0
        self.timeout_streak = 0
        self.suppress_until = 0.0
        self.last_error_kind = None
        self.last_error_message = None
        if had_failure:
            self._changed()

    def record_empty(self, empty_threshold: int = 10, *,
                     reason: str = 'no_data', missing_codes: list | None = None):
        """记录空结果（D53 / I-05）

        空结果≠源故障：单只票无数据（新股/停牌/腾讯 501）是常态。原实现把
        空 DataFrame 当异常抛出 → 计入 error_count → 连续 3 次即让整个源
        进入 5 分钟冷却，主源（腾讯）被无谓误伤。

        现在空结果单独计数，连续 empty_threshold 次才升级为故障冷却；
        ``reason`` / ``missing_codes`` 让「该票无数据」与「源挂了」可区分。
        """
        self.empty_count += 1
        self.request_count += 1
        self.last_request_time = time.time()
        self.last_empty_reason = reason
        self.missing_codes = list(missing_codes or [])
        if self.empty_count >= empty_threshold:
            self.error_count += 1
            self.empty_count = 0
            self.cooldown_until = time.time() + 300
            self.last_error_kind = 'empty_threshold'
            logger.warning(
                f"Source {self.name} entering cooldown (连续 {empty_threshold} 次空结果)")
            self._changed()

    def record_error(self, cooldown_seconds: int = 300, quarantine_seconds: int = 1800,
                     *, kind: str = 'error', message: str = ''):
        """记录错误，进入冷却；连续多轮冷却未恢复则熔断

        ``kind`` / ``message`` 是 I-05 新增的结构化诊断（keyword-only，
        保持原有位置参数签名不变）。
        """
        self.error_count += 1
        self.empty_count = 0
        self.last_error_time = time.time()
        self.last_request_time = time.time()
        self.last_error_kind = kind
        self.last_error_message = str(message)[:200] or None
        if kind == 'timeout':
            self.timeout_streak += 1
        if self.error_count >= 3:
            self.cooldown_until = time.time() + cooldown_seconds
            self._cooldown_streak += 1
            if self._cooldown_streak >= 3:
                # 连续 3 轮冷却仍未恢复 → 熔断：期间彻底跳过该源，
                # 省掉每次请求的网络超时（TDX 网关不可达 / 东财风控场景）
                self.quarantined_until = time.time() + quarantine_seconds
                self._cooldown_streak = 0
                logger.warning(
                    f"Source {self.name} quarantined for {quarantine_seconds}s"
                    "（连续 3 轮冷却未恢复）")
            else:
                logger.warning(f"Source {self.name} entering cooldown for {cooldown_seconds}s")
        self._changed()

    def reset(self):
        """重置状态"""
        self.error_count = 0
        self.empty_count = 0
        self.cooldown_until = 0
        self.quarantined_until = 0
        self._cooldown_streak = 0
        self.timeout_streak = 0
        self.suppress_until = 0.0
        self.last_error_kind = None
        self.last_error_message = None
        self._changed()


class SourceManager:
    """数据源管理器 - 轮询、冷却、随机延迟"""

    def __init__(self):
        self.sources: dict[str, SourceStatus] = {}
        self.min_delay = 0.5  # 最小延迟（秒）
        self.max_delay = 2.0  # 最大延迟（秒）
        self.cooldown_seconds = 300  # 冷却时间（5分钟）
        self.max_requests_per_minute = 20  # 每分钟最大请求数
        # 快照缓存（最后良好回退）
        self._snapshot_cache: dict[str, dict] = {}
        self.max_snapshot_age = 86400  # 快照最大保留24小时
        # I-02/I-05：熔断状态跨重启保留 + 降级事件留痕（旁路，Redis 不可用时全降级为内存）
        self._state_cache = None
        self._last_persist = 0.0
        self.last_fallback: dict[str, dict] = {}

    def register_source(self, name: str):
        """注册数据源"""
        status = SourceStatus(name)
        status.on_state_change = self._persist_state_throttled
        self.sources[name] = status

    def attach_cache(self, cache) -> None:
        """注入 RedisCache 以持久化熔断状态（I-02）；启动期调用一次。"""
        self._state_cache = cache
        self._restore_state()

    # Redis key 固定，三服务（api/worker/beat）共享同一份源健康视图
    _STATE_KEY = 'capability:source_state'

    def _persist_state_throttled(self) -> None:
        """状态跃迁时写 Redis，最多每 5 秒一次，避免全市场扫描刷 Redis。"""
        now = time.monotonic()
        if now - self._last_persist < 5.0:
            return
        self._last_persist = now
        if self._state_cache is None:
            return
        try:
            payload = {}
            for name, s in self.sources.items():
                payload[name] = {
                    'error_count': s.error_count,
                    'cooldown_until': s.cooldown_until,
                    'quarantined_until': s.quarantined_until,
                    'cooldown_streak': s._cooldown_streak,
                    'last_error_kind': s.last_error_kind,
                    'last_error_message': s.last_error_message,
                }
            self._state_cache.set(self._STATE_KEY, payload, ttl=3600)
        except Exception as e:
            logger.debug(f"[I-02] source state persist failed: {e}")

    def _restore_state(self) -> None:
        """重启后恢复熔断/冷却状态（只恢复未来仍有效的时间窗）。"""
        if self._state_cache is None:
            return
        try:
            saved = self._state_cache.get(self._STATE_KEY)
            if not isinstance(saved, dict):
                return
            now = time.time()
            restored = 0
            for name, row in saved.items():
                s = self.sources.get(name)
                if s is None or not isinstance(row, dict):
                    continue
                if float(row.get('quarantined_until') or 0) > now:
                    s.quarantined_until = float(row['quarantined_until'])
                    restored += 1
                if float(row.get('cooldown_until') or 0) > now:
                    s.cooldown_until = float(row['cooldown_until'])
                s.error_count = int(row.get('error_count') or 0)
                s._cooldown_streak = int(row.get('cooldown_streak') or 0)
                s.last_error_kind = row.get('last_error_kind')
                s.last_error_message = row.get('last_error_message')
            if restored:
                logger.info(f"[I-02] 恢复 {restored} 个数据源的熔断状态（跨重启保留）")
        except Exception as e:
            logger.debug(f"[I-02] source state restore failed: {e}")

    def get_available_source(self, preferred: str | None = None) -> str | None:
        """获取可用的数据源"""
        if preferred and preferred in self.sources:
            source = self.sources[preferred]
            if source.is_available(self.cooldown_seconds):
                return preferred

        # 随机选择可用的数据源
        available = [
            name for name, source in self.sources.items()
            if source.is_available(self.cooldown_seconds)
        ]
        if not available:
            return None
        return random.choice(available)

    def add_random_delay(self):
        """添加随机延迟，模拟人工操作"""
        delay = random.uniform(self.min_delay, self.max_delay)
        time.sleep(delay)

    def should_delay(self, source_name: str) -> bool:
        """检查是否需要延迟"""
        if source_name not in self.sources:
            return False
        source = self.sources[source_name]
        now = time.time()
        # 检查请求频率
        recent_requests = [
            t for t in [source.last_request_time]
            if now - t < 60
        ]
        return len(recent_requests) >= self.max_requests_per_minute

    def execute_with_retry(
        self,
        source_name: str,
        func: Callable,
        *args,
        max_retries: int = 2,
        timeout: float = 30.0,
        skip_delay: bool = False,
        **kwargs
    ) -> any | None:
        """执行数据源请求，带重试机制和超时控制
        
        Args:
            source_name: 数据源名称
            func: 执行函数
            *args: 函数参数
            max_retries: 最大重试次数
            timeout: 超时时间（秒），默认30秒
            skip_delay: 跳过随机延迟（用于批量扫描场景）
            **kwargs: 函数关键字参数
        """
        if source_name not in self.sources:
            return None

        source = self.sources[source_name]
        if not source.is_available(self.cooldown_seconds):
            logger.debug(f"Source {source_name} is in cooldown")
            return None

        # 添加超时参数（仅当方法支持时）
        import inspect
        try:
            sig = inspect.signature(func)
            if ('timeout' in sig.parameters or any(
                p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
            )) and 'timeout' not in kwargs:
                kwargs['timeout'] = timeout
        except (ValueError, TypeError):
            pass

        for attempt in range(max_retries):
            try:
                # 添加随机延迟（可跳过）
                if not skip_delay:
                    self.add_random_delay()
                result = func(*args, **kwargs)
                if result is not None:
                    if hasattr(result, "empty") and result.empty:
                        # D53：空结果不是源故障（单票无数据是常态）——
                        # 不重试、不计错误，只记空结果计数；继续下一个源。
                        # I-05：带 reason / missing_codes，区分「该票无数据」与「源挂了」。
                        code_hint = str(args[0]) if args else None
                        source.record_empty(
                            reason='no_data',
                            missing_codes=[code_hint] if code_hint else None)
                        return None
                    source.record_success()
                    return result
            except Exception as e:
                kind, non_retriable = classify_failure(e)
                source.record_error(self.cooldown_seconds, kind=kind, message=str(e))
                if non_retriable:
                    # I-05：超时/连接类失败**不重试**（原本每次 ~10s × 2 重试，
                    # 全市场扫描被单个不可达源拖垮），立即让下一个源接手。
                    logger.warning(
                        f"Source {source_name} {kind} (non-retriable, "
                        f"attempt {attempt + 1}/{max_retries}): {e}")
                    try:
                        from ..config import get_settings
                        cfg = get_settings()
                        if (bool(cfg.SOURCE_TIMEOUT_SUPPRESSION)
                                and source.timeout_streak
                                >= int(cfg.SOURCE_TIMEOUT_SUPPRESS_STREAK)):
                            source.suppress_until = (
                                time.time() + float(cfg.SOURCE_TIMEOUT_SUPPRESS_SECONDS))
                            logger.warning(
                                f"Source {source_name} suppressed for "
                                f"{cfg.SOURCE_TIMEOUT_SUPPRESS_SECONDS}s after "
                                f"{source.timeout_streak} consecutive {kind} failures")
                    except Exception:  # noqa: S110 配置缺失时退化为「只不重试」
                        pass
                    break
                logger.error(f"Source {source_name} error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)

        return None

    def _record_fallback(self, method_name: str, failed: list[str],
                         landed: str) -> None:
        """I-02：降级事件结构化留痕（内存 + 节流写 Redis）。"""
        event = {
            'dataset': method_name,
            'from': failed,
            'to': landed,
            'fallbackFrom': failed,
            'reason': 'source_unavailable_or_empty',
            'as_of': time.strftime('%Y-%m-%d %H:%M:%S'),
            'count': int((self.last_fallback.get(method_name) or {}).get('count', 0)) + 1,
        }
        self.last_fallback[method_name] = event
        s = self.sources.get(landed)
        if s is not None:
            s.fallback_count += 1
        logger.warning(
            f"[I-02][降级] {method_name}: {' → '.join(failed)} → {landed} "
            f"(本进程第 {event['count']} 次)")
        # 节流：全市场扫描可能高频降级，10s 内只落一次 Redis
        now = time.monotonic()
        if self._state_cache is not None and now - self._last_persist >= 10.0:
            self._last_persist = now
            try:
                self._state_cache.set(
                    f'capability:fallback:{method_name}', event, ttl=3600)
            except Exception as e:
                logger.debug(f"[I-02] fallback persist failed: {e}")

    def execute_with_fallback(
        self,
        source_priority: list[str],
        sources_dict: dict,
        method_name: str,
        *args,
        skip_delay: bool = True,
        return_source: bool = False,
        **kwargs
    ) -> any | None:
        """按优先级尝试多个数据源，使用 execute_with_retry
        
        Args:
            source_priority: 数据源优先级列表，如 ['akshare', 'eastmoney', 'tencent']
            sources_dict: 数据源实例字典
            method_name: 方法名
            *args: 方法参数
            skip_delay: 跳过随机延迟
            **kwargs: 方法关键字参数
        
        Returns:
            ``return_source=True`` 时返回 ``(result, source_name)``（失败为
            ``(None, None)``）；否则返回结果或 None。
        """
        failed: list[str] = []
        for source_name in source_priority:
            if source_name not in self.sources:
                continue
            source_obj = sources_dict.get(source_name)
            if source_obj is None:
                continue
            func = getattr(source_obj, method_name, None)
            if func is None:
                # D66：配置里列了但类未实现该方法 = 死配置，显式告警一次，
                # 避免像 eastmoney 日K那样静默跳过数月无人察觉。
                logger.warning(
                    f"Source {source_name} has no {method_name} — "
                    "priority entry is a no-op")
                continue
            result = self.execute_with_retry(
                source_name, func, *args,
                skip_delay=skip_delay, **kwargs
            )
            if result is not None:
                if failed:
                    self._record_fallback(method_name, failed, source_name)
                # 成功时缓存快照
                self._save_snapshot(method_name, args, result)
                return (result, source_name) if return_source else result
            failed.append(source_name)

        # 所有源失败，尝试回退缓存
        stale = self._load_stale_snapshot(method_name, args)
        if stale is not None:
            if failed:
                self._record_fallback(method_name, failed, 'stale_snapshot')
            return (stale, 'stale_snapshot') if return_source else stale
        return (None, None) if return_source else None

    def get_status_report(self) -> dict:
        """获取数据源状态报告"""
        report = {}
        now = time.time()
        for name, source in self.sources.items():
            report[name] = {
                'available': source.is_available(self.cooldown_seconds),
                'error_count': source.error_count,
                'empty_count': source.empty_count,
                'request_count': source.request_count,
                'success_count': source.success_count,
                'cooldown_until': source.cooldown_until,
                'quarantined_until': source.quarantined_until,
                'quarantined': now < source.quarantined_until,
                # I-02/I-05 结构化诊断
                'suppressed': now < source.suppress_until,
                'suppress_until': source.suppress_until,
                'timeout_streak': source.timeout_streak,
                'last_error_kind': source.last_error_kind,
                'last_error_message': source.last_error_message,
                'last_empty_reason': source.last_empty_reason,
                'missing_codes': list(source.missing_codes),
                'last_success_time': source.last_success_time,
                'fallback_count': source.fallback_count,
            }
        return report

    def _snapshot_key(self, method_name: str, args: tuple) -> str:
        """生成快照缓存 key"""
        args_str = ':'.join(str(a) for a in args)
        return f"{method_name}:{args_str}"

    def _save_snapshot(self, method_name: str, args: tuple, result):
        """缓存成功结果（D66：同时记录末根日期与归属交易日）"""
        import pandas as pd
        key = self._snapshot_key(method_name, args)
        if isinstance(result, pd.DataFrame):
            self._snapshot_cache[key] = {
                'data': result.copy(),
                'timestamp': time.time(),
                # 日K 快照用末根日期判陈旧；其它数据（报价等）用自然日
                'session': self._expected_session(),
                'as_of': (self._last_bar_date(result)
                          if method_name == 'get_daily_data' else None),
            }
        # 只缓存 DataFrame，其他类型不缓存

    @staticmethod
    def _expected_session() -> str:
        try:
            from ..utils.kline_provenance import expected_session
            return expected_session()
        except Exception:  # 日历异常时退回自然日
            from datetime import datetime
            return datetime.now().strftime('%Y-%m-%d')

    @staticmethod
    def _last_bar_date(df) -> str | None:
        try:
            from ..utils.kline_provenance import last_bar_date
            return last_bar_date(df)
        except Exception:  # 日历/格式异常时按无日期处理
            return None

    def _load_stale_snapshot(self, method_name: str, args: tuple):
        """加载过期快照（标记 stale/fallback）

        D66：日K 快照跨交易日一律作废。原实现 max_snapshot_age=86400
        允许「昨天的日K」在次日所有源失败时被原样返回，且下游没有任何
        日期校验 → 昨日收盘被当成今日数据使用（用户点名的第二条误读路径）。
        """
        key = self._snapshot_key(method_name, args)
        entry = self._snapshot_cache.get(key)
        if entry is None:
            return None
        age = time.time() - entry['timestamp']
        if age > self.max_snapshot_age:
            del self._snapshot_cache[key]
            return None
        # 跨交易日作废：不管是日K还是实时快照，隔日数据都不该当今日使用
        try:
            from ..config import get_settings
            guard_on = bool(get_settings().KLINE_DAY_BOUNDARY_GUARD)
        except Exception:  # 配置缺失时默认开启（保守）
            guard_on = True
        if (guard_on and entry.get('session')
                and entry['session'] != self._expected_session()):
            del self._snapshot_cache[key]
            logger.warning(
                f"[D66] stale snapshot rejected: {method_name} {args} "
                f"belongs to {entry['session']}, not today")
            return None
        logger.warning(f"Using stale snapshot for {method_name} (age={age:.0f}s)")
        df = entry['data'].copy()
        df.attrs['_stale'] = True
        df.attrs['_fallback'] = True
        df.attrs['_age_seconds'] = int(age)
        df.attrs['kline_as_of'] = entry.get('as_of')
        if method_name == 'get_daily_data':
            df.attrs['kline_source'] = 'stale_snapshot'
            df.attrs['kline_sealed'] = False
        return df


# 全局实例
source_manager = SourceManager()
