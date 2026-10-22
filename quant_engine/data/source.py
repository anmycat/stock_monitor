"""数据源配置 - 通过 SourceManager 统一调度"""
from typing import Dict, List

import pandas as pd
from loguru import logger

from .source_manager import source_manager
from .sources.eastmoney import EastMoneySource
from .sources.sina import SinaSource
from .sources.tdx import TdxSource
from .sources.tencent import TencentSource

# 数据源优先级配置（2026-10-20 按用户决策调整：东财出口 IP 经常被风控，
# ECS 实测 22 连败，降为备路；腾讯第一、新浪第二互为配合）
#
# 依据：research/docs/17_ECS性能与架构调研_2026-10-20.md §3.1——东财降级后，腾讯/新浪
# 双线覆盖：
#   realtime  腾讯批量（qt.gtimg，M2' 起代码池动态化：新浪列表主路/东财 clist 备路/
#              静态号段 emergency-only，实测 5575 只全覆盖）→ 新浪批量（hq.sinajs）→ 东财备路
#   stock_list 新浪 hs_a 全市场（实测 5574 只含北交所）→ 东财备路
#   daily     腾讯 fqkline（前复权）→ 新浪 getKLineData → 东财备路
#   minute    通达信 pytdx（TCP 7709，实测 5/15/30/60 分钟K 完整，
#             2026-10-20 落地补 gates 分钟断链；akshare 兜底 2026-10-22 切除，
#             依赖未安装恒返 None，见 prd/15 §P0）
_PRIORITY = {
    'stock_list': ['sina', 'eastmoney'],
    'realtime': ['tencent', 'sina', 'eastmoney'],
    'daily': ['tencent', 'sina', 'eastmoney'],
    'minute': ['tdx'],
}


class CompositeSource:
    """组合数据源 - 通过 SourceManager 统一调度"""

    def __init__(self, datacenter=None):
        # 注册数据源
        source_manager.register_source('eastmoney')
        source_manager.register_source('tencent')
        source_manager.register_source('sina')
        source_manager.register_source('tdx')

        # 初始化数据源
        self.sources = {
            'eastmoney': EastMoneySource(),
            'tencent': TencentSource(),
            'sina': SinaSource(),
            'tdx': TdxSource(),
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

    def get_daily_data(self, code: str, days: int = 120) -> pd.DataFrame:
        """获取日线数据（腾讯 fqkline → 新浪 getKLineData → 东财）"""
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
