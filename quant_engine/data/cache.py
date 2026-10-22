from __future__ import annotations
import json
import time
from typing import Any, Optional

import redis
from loguru import logger


class RedisCache:
    """Redis缓存管理器，支持连接池、重试、SSL和优雅降级"""

    def __init__(self, host: str = "localhost", port: int = 6379,
                 db: int = 0, password: str = None, ssl: bool = False,
                 max_connections: int = 20, socket_timeout: float = 5.0):
        self._host = host
        self._port = port
        self._db = db
        self._password = password
        self._ssl = ssl
        self._max_connections = max_connections
        self._socket_timeout = socket_timeout
        self._connected = False
        self._pool: redis.ConnectionPool | None = None
        self.client: redis.Redis | None = None
        self._init_pool()

    def _init_pool(self):
        """初始化连接池"""
        try:
            pool_kwargs = dict(
                host=self._host,
                port=self._port,
                db=self._db,
                max_connections=self._max_connections,
                socket_timeout=self._socket_timeout,
                socket_connect_timeout=self._socket_timeout,
                decode_responses=True,
            )
            if self._password:
                pool_kwargs['password'] = self._password
            # redis-py 8.x 不再支持 ConnectionPool 的 ssl 参数
            if self._ssl:
                pool_kwargs['connection_class'] = redis.SSLConnection
            self._pool = redis.ConnectionPool(**pool_kwargs)
            self.client = redis.Redis(connection_pool=self._pool)
            logger.debug(f"Redis连接池已初始化: {self._host}:{self._port}/{self._db}")
        except Exception as e:
            logger.error(f"Redis连接池初始化失败: {e}")
            self._pool = None
            self.client = None

    def connect(self) -> bool:
        """
        连接Redis，含重试机制（最多3次，间隔1秒）
        Returns: 是否连接成功
        """
        max_retries = 3
        retry_interval = 1.0
        last_error = None

        for attempt in range(1, max_retries + 1):
            try:
                if self.client is None:
                    self._init_pool()
                if self.client is None:
                    raise ConnectionError("Redis客户端未初始化")
                self.client.ping()
                self._connected = True
                logger.info(f"Redis连接成功: {self._host}:{self._port}/{self._db}")
                return True
            except Exception as e:
                last_error = e
                logger.warning(
                    f"Redis连接失败 (第{attempt}/{max_retries}次): {type(e).__name__}: {e}"
                )
                self._connected = False
                if attempt < max_retries:
                    time.sleep(retry_interval)

        logger.error(
            f"Redis连接最终失败，已重试{max_retries}次: {self._host}:{self._port} "
            f"(last error: {type(last_error).__name__}: {last_error})"
        )
        self._connected = False
        return False

    def close(self):
        """关闭连接并释放连接池资源"""
        try:
            if self.client:
                self.client.close()
            if self._pool:
                self._pool.disconnect()
            self._connected = False
            logger.info("Redis连接已关闭，连接池资源已释放")
        except Exception as e:
            logger.error(f"Redis关闭连接时出错: {e}")
        finally:
            self._connected = False

    @property
    def is_connected(self) -> bool:
        """检查当前Redis连接状态"""
        if not self._connected or self.client is None:
            return False
        try:
            self.client.ping()
            return True
        except Exception:
            self._connected = False
            return False

    def get(self, key: str) -> Any | None:
        """获取缓存值，失败时返回None"""
        if not self._connected:
            return None
        try:
            data = self.client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Redis get error [{key}]: {e}")
            return None

    def set(self, key: str, value: Any, ttl: int = 3600) -> bool:
        """设置缓存值，失败时返回False"""
        if not self._connected:
            return False
        try:
            self.client.setex(key, ttl, json.dumps(value, default=str))
            return True
        except Exception as e:
            logger.error(f"Redis set error [{key}]: {e}")
            return False

    def delete(self, key: str) -> bool:
        """删除缓存键，失败时返回False"""
        if not self._connected:
            return False
        try:
            self.client.delete(key)
            return True
        except Exception as e:
            logger.error(f"Redis delete error [{key}]: {e}")
            return False

    def exists(self, key: str) -> bool:
        """检查键是否存在"""
        if not self._connected:
            return False
        try:
            return self.client.exists(key) > 0
        except Exception as e:
            logger.error(f"Redis exists error [{key}]: {e}")
            return False

    def keys(self, pattern: str) -> list:
        """按 pattern 扫描键（SCAN，非阻塞；失败返回空列表）"""
        if not self._connected:
            return []
        try:
            return list(self.client.scan_iter(match=pattern, count=100))
        except Exception as e:
            logger.error(f"Redis keys error [{pattern}]: {e}")
            return []

    def __del__(self):
        """析构时确保资源释放"""
        try:
            self.close()
        except Exception:
            pass
