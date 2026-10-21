"""数据仓储层（P1 重构新增，prd/15 §2.2）

取数编排 + 缓存门面：MarketDataRepository（原 DataCenter 拆分）。
源注册/容错在 data/source.py + source_manager.py；缓存在 data/cache.py。
"""
from .market_data import MarketDataRepository

__all__ = ["MarketDataRepository"]
