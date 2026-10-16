"""数据源配置 - 通过 SourceManager 统一调度"""
import pandas as pd
from typing import List, Dict
from loguru import logger
from .source_manager import source_manager
from .sources.eastmoney import EastMoneySource
from .sources.tencent import TencentSource
from .sources.sina import SinaSource
from .akshare_source import AKShareSource


# 数据源优先级配置
_PRIORITY = {
    'stock_list': ['akshare'],
    'realtime': ['eastmoney', 'tencent', 'akshare'],
    'etf': ['akshare'],
    'daily': ['akshare', 'eastmoney', 'tencent'],
    'minute': ['akshare', 'tencent'],
}


class CompositeSource:
    """组合数据源 - 通过 SourceManager 统一调度"""

    def __init__(self, datacenter=None):
        # 注册数据源
        source_manager.register_source('eastmoney')
        source_manager.register_source('tencent')
        source_manager.register_source('sina')
        source_manager.register_source('akshare')

        # 初始化数据源
        self.sources = {
            'eastmoney': EastMoneySource(),
            'tencent': TencentSource(),
            'sina': SinaSource(),
            'akshare': AKShareSource(),
        }
        self.datacenter = datacenter
        logger.info("CompositeSource initialized (via SourceManager)")

    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        result = source_manager.execute_with_fallback(
            _PRIORITY['stock_list'], self.sources, 'get_stock_list',
            skip_delay=True,
        )
        if result is not None and not result.empty:
            return result
        # 兜底数据
        return pd.DataFrame({
            'code': ['000001.SZ', '000002.SZ', '600000.SH', '600519.SH', '000858.SZ'],
            'name': ['平安银行', '万科A', '浦发银行', '贵州茅台', '五粮液'],
            'industry': ['银行', '房地产', '银行', '白酒', '白酒'],
        })

    def get_realtime_quote(self, codes: List[str]) -> pd.DataFrame:
        """获取实时行情 - 复用全量行情缓存"""
        all_quotes = self.get_all_realtime_quotes()
        if not all_quotes.empty and codes:
            return all_quotes[all_quotes['code'].isin(codes)]
        return all_quotes

    def get_all_realtime_quotes(self) -> pd.DataFrame:
        """获取所有实时行情 - 多数据源轮询 + 缓存"""
        cache_key = "all_realtime_quotes"
        cached = self.datacenter.cache.get(cache_key) if hasattr(self, 'datacenter') and self.datacenter else None
        if cached:
            return pd.DataFrame(cached)

        result = source_manager.execute_with_fallback(
            _PRIORITY['realtime'], self.sources, 'get_all_stocks',
            skip_delay=True,
        )
        if result is not None and not result.empty:
            if hasattr(self, 'datacenter') and self.datacenter:
                self.datacenter.cache.set(cache_key, result.to_dict(orient='records'), ttl=300)
            return result
        return pd.DataFrame()

    def get_etf_quotes(self) -> pd.DataFrame:
        """获取ETF行情"""
        result = source_manager.execute_with_fallback(
            _PRIORITY['etf'], self.sources, 'get_etf_quotes',
            skip_delay=True,
        )
        if result is not None and not result.empty:
            return result
        return pd.DataFrame()

    def get_daily_data(self, code: str, days: int = 120) -> pd.DataFrame:
        """获取日线数据"""
        result = source_manager.execute_with_fallback(
            _PRIORITY['daily'], self.sources, 'get_daily_data',
            code, days, skip_delay=True,
        )
        if result is not None and not result.empty:
            return result
        return pd.DataFrame()

    def get_minute_data(self, code: str, period: str = "5") -> pd.DataFrame:
        """获取分钟线数据"""
        result = source_manager.execute_with_fallback(
            _PRIORITY['minute'], self.sources, 'get_minute_data',
            code, period, skip_delay=True,
        )
        if result is not None and not result.empty:
            return result
        return pd.DataFrame()

    def get_source_status(self) -> Dict:
        """获取数据源状态"""
        return source_manager.get_status_report()
