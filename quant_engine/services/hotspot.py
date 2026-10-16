"""热点发现系统 - 板块热度排名 + 龙头股关联（借鉴 AlphaSift Hotspot）"""
import time
from typing import List, Dict, Optional
from loguru import logger
from .sector_monitor import SectorMonitor
from ..data.datacenter import DataCenter


class Hotspot:
    """单个热点板块"""
    def __init__(self, name: str, score: float, avg_change: float,
                 leader_stocks: List[Dict], up_count: int, total: int):
        self.name = name
        self.score = score
        self.avg_change = avg_change
        self.leader_stocks = leader_stocks
        self.up_count = up_count
        self.total = total

    def to_dict(self) -> Dict:
        return {
            'name': self.name,
            'score': round(self.score, 1),
            'avg_change': round(self.avg_change, 2),
            'leader_stocks': self.leader_stocks,
            'up_count': self.up_count,
            'total': self.total,
            'up_ratio': round(self.up_count / self.total * 100, 0) if self.total > 0 else 0,
        }


class HotspotService:
    """
    热点发现服务
    
    热度评分 = 板块涨幅归一化 * 0.4 + 上涨比例归一化 * 0.3 + 龙头强度 * 0.3
    """

    def __init__(self, datacenter: DataCenter):
        self.datacenter = datacenter
        self.sector_monitor = SectorMonitor(datacenter)
        self._cache: Optional[List[Dict]] = None
        self._cache_time: float = 0
        self._cache_ttl: float = 300  # 5分钟缓存

    def get_hotspots(self, top_n: int = 5, force_refresh: bool = False) -> List[Dict]:
        """获取热点板块排名"""
        now = time.time()
        if not force_refresh and self._cache and (now - self._cache_time) < self._cache_ttl:
            return self._cache[:top_n] if top_n < len(self._cache) else self._cache

        sectors = self.sector_monitor.get_sector_data()
        if not sectors:
            return self._cache or []

        hotspots = []
        for sector in sectors:
            score = self._calculate_hotspot_score(sector)
            # 热门板块只保留涨幅为正的板块
            if sector.get('avg_change', 0) > 0:
                hotspots.append({
                    'name': sector['name'],
                    'score': score,
                    'avg_change': sector['avg_change'],
                    'leader_stocks': sector.get('stocks', [])[:15],
                    'up_count': sector['up_count'],
                    'total': sector['total'],
                    'up_ratio': sector['up_ratio'],
                })

        # 按涨幅降序排列（涨幅最大的排最前）
        hotspots.sort(key=lambda x: x['avg_change'], reverse=True)
        self._cache = hotspots
        self._cache_time = now
        logger.info(f"Hotspot: {len(self._cache)} sectors identified")
        return self._cache[:top_n]

    def get_hot_sector_codes(self) -> List[str]:
        """获取热点板块内的股票代码列表（用于信号加分）"""
        hotspots = self.get_hotspots(top_n=3)
        codes = set()
        for h in hotspots:
            for stock in h.get('leader_stocks', []):
                code = stock.get('code', '')
                if code:
                    codes.add(code)
        return list(codes)

    def _calculate_hotspot_score(self, sector: Dict) -> float:
        """
        计算板块热度分数 (0-100)
        
        热度 = 涨幅因子 * 40 + 上涨比例因子 * 30 + 龙头强度 * 30
        """
        # 涨幅因子: 涨幅越大越好，归一化到 0-1
        avg_change = sector.get('avg_change', 0)
        change_factor = min(max(avg_change / 5.0, 0), 1)  # 5% 涨幅归一化

        # 上涨比例因子: 上涨股票越多越好
        up_ratio = sector.get('up_ratio', 0) / 100.0
        up_factor = min(max(up_ratio, 0), 1)

        # 龙头强度: 板块内涨幅最大的股票
        stocks = sector.get('stocks', [])
        if stocks:
            max_change = max(abs(s.get('change_pct', 0)) for s in stocks)
            leader_factor = min(max_change / 8.0, 1)  # 8% 涨幅归一化
        else:
            leader_factor = 0

        score = change_factor * 40 + up_factor * 30 + leader_factor * 30
        return round(score, 1)

    def is_hot_stock(self, code: str) -> bool:
        """检查股票是否在热点板块中"""
        hot_codes = self.get_hot_sector_codes()
        return code in hot_codes
