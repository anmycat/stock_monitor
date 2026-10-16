"""游资监控模块 - 使用akshare专用接口"""
import time
from datetime import datetime
from typing import List, Dict
from loguru import logger
from ..data.datacenter import DataCenter
from ..data.akshare_source import AKShareSource


class HotMoneyMonitor:
    """游资监控 - 使用涨停板接口"""

    def __init__(self, datacenter: DataCenter):
        self.datacenter = datacenter
        self.akshare = AKShareSource()
        self._cache = {}
        self._cache_time = 0

    def get_hot_money_activity(self) -> Dict:
        """获取游资活跃度数据"""
        current_time = time.time()
        
        # 缓存5分钟
        if self._cache and current_time - self._cache_time < 300:
            return self._cache

        try:
            # 使用akshare涨停板接口
            zt_df = self.akshare.get_zt_pool()
            dt_df = self.akshare.get_dt_pool()
            zb_df = self.akshare.get_zb_pool()

            # 涨停股
            limit_up_list = []
            if zt_df is not None and not zt_df.empty:
                for _, row in zt_df.head(10).iterrows():
                    limit_up_list.append({
                        'code': row.get('代码', ''),
                        'name': row.get('名称', ''),
                        'price': float(row.get('最新价', 0)),
                        'change_pct': float(row.get('涨跌幅', 0)),
                        'reason': row.get('涨停原因', ''),
                    })

            # 跌停股
            limit_down_list = []
            if dt_df is not None and not dt_df.empty:
                for _, row in dt_df.head(10).iterrows():
                    limit_down_list.append({
                        'code': row.get('代码', ''),
                        'name': row.get('名称', ''),
                        'price': float(row.get('最新价', 0)),
                        'change_pct': float(row.get('涨跌幅', 0)),
                    })

            # 炸板股
            near_limit_list = []
            if zb_df is not None and not zb_df.empty:
                for _, row in zb_df.head(10).iterrows():
                    near_limit_list.append({
                        'code': row.get('代码', ''),
                        'name': row.get('名称', ''),
                        'price': float(row.get('最新价', 0)),
                        'change_pct': float(row.get('涨跌幅', 0)),
                    })

            # 统计
            limit_up_count = len(zt_df) if zt_df is not None else 0
            limit_down_count = len(dt_df) if dt_df is not None else 0
            near_limit_count = len(zb_df) if zb_df is not None else 0

            # 活跃度评分
            activity_score = self._calculate_activity_score(
                limit_up_count, limit_down_count
            )

            result = {
                'limit_up': {
                    'count': limit_up_count,
                    'stocks': limit_up_list,
                },
                'limit_down': {
                    'count': limit_down_count,
                    'stocks': limit_down_list,
                },
                'near_limit': {
                    'count': near_limit_count,
                    'stocks': near_limit_list,
                },
                'stats': {
                    'limit_up': limit_up_count,
                    'limit_down': limit_down_count,
                    'near_limit': near_limit_count,
                    'activity_score': activity_score,
                },
            }

            self._cache = result
            self._cache_time = current_time
            return result

        except Exception as e:
            logger.error(f"Get hot money activity error: {e}")
            return self._empty_activity()

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
                'limit_up': 0, 'limit_down': 0, 'near_limit': 0,
                'activity_score': 50
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
