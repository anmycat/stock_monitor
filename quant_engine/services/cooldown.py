import hashlib
from datetime import datetime
from loguru import logger
from ..data.cache import RedisCache


class CooldownManager:
    def __init__(self, cache: RedisCache):
        self.cache = cache
        self.cooldown_hours = 4

    def is_cooled_down(self, code: str) -> bool:
        date_str = datetime.now().strftime("%Y%m%d")
        key = f"signal_cooldown:{code}:{date_str}"
        return not self.cache.exists(key)

    def set_cooldown(self, code: str):
        date_str = datetime.now().strftime("%Y%m%d")
        key = f"signal_cooldown:{code}:{date_str}"
        ttl = self.cooldown_hours * 3600
        self.cache.set(key, {"timestamp": datetime.now().isoformat()}, ttl=ttl)
        logger.info(f"Cooldown set for {code}")

    def is_idempotent(self, signal_hash: str) -> bool:
        key = f"idempotent:{signal_hash}"
        return not self.cache.exists(key)

    def set_idempotent(self, signal_hash: str):
        key = f"idempotent:{signal_hash}"
        self.cache.set(key, {"timestamp": datetime.now().isoformat()}, ttl=3600)

    @staticmethod
    def generate_signal_hash(code: str, action: str, date: str) -> str:
        raw = f"{code}:{action}:{date}"
        return hashlib.md5(raw.encode()).hexdigest()