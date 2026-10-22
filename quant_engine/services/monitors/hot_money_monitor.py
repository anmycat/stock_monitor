"""游资监控模块 - 涨停/跌停/炸板池活跃度

数据源现状（2026-10-22，prd/15 P0 重构记录）：
原实现依赖 AKShareSource 专用接口（stock_zt_pool_em 等），akshare 包
在生产与本地均未安装，恒返 None——监控实际输出一直是降级空数据
（counts=0 / activity_score=50）。akshare 遗留链已随 P0 切除；
待替换数据源（东财 push2 涨停板接口）落地前，保持降级输出，
响应结构与前端消费契约不变（行为零变更）。
"""
import time
from typing import Dict

from loguru import logger

from ...data.repositories import MarketDataRepository

# 降级提示只打一次，避免盘中 120s 循环刷屏
_WARNED = False


class HotMoneyMonitor:
    """游资监控 - 涨停/跌停/炸板池活跃度（降级模式，见模块 docstring）"""

    def __init__(self, datacenter: MarketDataRepository):
        self.datacenter = datacenter
        self._cache: Dict = {}
        self._cache_time = 0.0

    def get_hot_money_activity(self) -> Dict:
        """获取游资活跃度数据

        降级路径：数据源不可用时返回空结构（score=50），
        与 akshare 未安装时的历史输出完全一致。
        """
        global _WARNED
        if not _WARNED:
            logger.warning(
                "游资池数据源不可用（akshare 遗留链已切除，待替换数据源），"
                "返回降级空数据；见 prd/15 §P0"
            )
            _WARNED = True

        current_time = time.time()
        # 缓存5分钟（与历史实现一致）
        if self._cache and current_time - self._cache_time < 300:
            return self._cache
        self._cache = self._empty_activity()
        self._cache_time = current_time
        return self._cache

    def _calculate_activity_score(self, limit_up: int, limit_down: int) -> int:
        """计算游资活跃度评分 (0-100)"""
        score = 50

        # 涨停加分
        if limit_up >= 50:
            score += 30
        elif limit_up >= 30:
            score += 20
        elif limit_up >= 10:
            score += 10
        elif limit_up >= 5:
            score += 5

        # 跌停减分
        if limit_down >= 50:
            score -= 30
        elif limit_down >= 30:
            score -= 20
        elif limit_down >= 10:
            score -= 10
        elif limit_down >= 5:
            score -= 5

        return max(0, min(100, score))

    def _empty_activity(self) -> Dict:
        """空的活跃度数据"""
        return {
            'limit_up': {'count': 0, 'stocks': []},
            'limit_down': {'count': 0, 'stocks': []},
            'near_limit': {'count': 0, 'stocks': []},
            'stats': {
                'limit_up': 0,
                'limit_down': 0,
                'near_limit': 0,
                'activity_score': 50,
            },
        }

    def format_activity_summary(self, activity: Dict) -> str:
        """格式化活跃度摘要"""
        stats = activity.get('stats', {})
        limit_up = activity.get('limit_up', {})
        limit_down = activity.get('limit_down', {})
        near_limit = activity.get('near_limit', {})

        lines = [
            f"游资活跃度: {stats.get('activity_score', 50)}/100",
            f"涨停: {limit_up.get('count', 0)}家 | 跌停: {limit_down.get('count', 0)}家 | 炸板: {near_limit.get('count', 0)}家",
        ]

        if limit_up.get('stocks'):
            lines.append("\n涨停股:")
            for stock in limit_up['stocks'][:5]:
                reason = stock.get('reason', '')
                lines.append(f"  {stock['name']}({stock['code']}) +{stock['change_pct']:.1f}% {reason}")

        return "\n".join(lines)
