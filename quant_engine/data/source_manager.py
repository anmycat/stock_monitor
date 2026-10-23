"""数据源管理器 - 多数据源轮询、冷却、随机延迟"""
from __future__ import annotations

import random
import time
from collections.abc import Callable

from loguru import logger


class SourceStatus:
    """数据源状态"""
    def __init__(self, name: str):
        self.name = name
        self.error_count = 0
        self.last_error_time = 0
        self.last_request_time = 0
        self.request_count = 0
        self.success_count = 0
        self.cooldown_until = 0

    def is_available(self, cooldown_seconds: int = 300) -> bool:
        """检查数据源是否可用"""
        now = time.time()
        return not now < self.cooldown_until

    def record_success(self):
        """记录成功请求"""
        self.request_count += 1
        self.success_count += 1
        self.last_request_time = time.time()
        self.error_count = 0

    def record_error(self, cooldown_seconds: int = 300):
        """记录错误，进入冷却"""
        self.error_count += 1
        self.last_error_time = time.time()
        self.last_request_time = time.time()
        if self.error_count >= 3:
            self.cooldown_until = time.time() + cooldown_seconds
            logger.warning(f"Source {self.name} entering cooldown for {cooldown_seconds}s")

    def reset(self):
        """重置状态"""
        self.error_count = 0
        self.cooldown_until = 0


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

    def register_source(self, name: str):
        """注册数据源"""
        self.sources[name] = SourceStatus(name)

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
                        raise ValueError(f"{source_name} returned empty dataframe")
                    source.record_success()
                    return result
            except TimeoutError as e:
                logger.warning(f"Source {source_name} timeout (attempt {attempt + 1}/{max_retries}): {e}")
                source.record_error(self.cooldown_seconds)
                if attempt < max_retries - 1:
                    time.sleep(1)
            except Exception as e:
                logger.error(f"Source {source_name} error: {e}")
                source.record_error(self.cooldown_seconds)
                if attempt < max_retries - 1:
                    time.sleep(1)

        return None

    def execute_with_fallback(
        self,
        source_priority: list[str],
        sources_dict: dict,
        method_name: str,
        *args,
        skip_delay: bool = True,
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
            第一个成功的数据源结果，或 None
        """
        for source_name in source_priority:
            if source_name not in self.sources:
                continue
            source_obj = sources_dict.get(source_name)
            if source_obj is None:
                continue
            func = getattr(source_obj, method_name, None)
            if func is None:
                continue
            result = self.execute_with_retry(
                source_name, func, *args,
                skip_delay=skip_delay, **kwargs
            )
            if result is not None:
                # 成功时缓存快照
                self._save_snapshot(method_name, args, result)
                return result

        # 所有源失败，尝试回退缓存
        stale = self._load_stale_snapshot(method_name, args)
        if stale is not None:
            return stale
        return None

    def get_status_report(self) -> dict:
        """获取数据源状态报告"""
        report = {}
        for name, source in self.sources.items():
            report[name] = {
                'available': source.is_available(self.cooldown_seconds),
                'error_count': source.error_count,
                'request_count': source.request_count,
                'success_count': source.success_count,
                'cooldown_until': source.cooldown_until,
            }
        return report

    def _snapshot_key(self, method_name: str, args: tuple) -> str:
        """生成快照缓存 key"""
        args_str = ':'.join(str(a) for a in args)
        return f"{method_name}:{args_str}"

    def _save_snapshot(self, method_name: str, args: tuple, result):
        """缓存成功结果"""
        import pandas as pd
        key = self._snapshot_key(method_name, args)
        if isinstance(result, pd.DataFrame):
            self._snapshot_cache[key] = {
                'data': result.copy(),
                'timestamp': time.time(),
            }
        # 只缓存 DataFrame，其他类型不缓存

    def _load_stale_snapshot(self, method_name: str, args: tuple):
        """加载过期快照（标记 stale/fallback）"""
        key = self._snapshot_key(method_name, args)
        entry = self._snapshot_cache.get(key)
        if entry is None:
            return None
        age = time.time() - entry['timestamp']
        if age > self.max_snapshot_age:
            del self._snapshot_cache[key]
            return None
        logger.warning(f"Using stale snapshot for {method_name} (age={age:.0f}s)")
        df = entry['data'].copy()
        df.attrs['_stale'] = True
        df.attrs['_fallback'] = True
        df.attrs['_age_seconds'] = int(age)
        return df


# 全局实例
source_manager = SourceManager()
