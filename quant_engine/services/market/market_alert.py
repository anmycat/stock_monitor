import time
from datetime import datetime

import pandas as pd
from loguru import logger

from ...data.repositories import MarketDataRepository
from ...models.market import MarketState


class MarketAlert:
    """大盘异动检测模块 - 类似同花顺异动监控"""

    def __init__(self, datacenter: MarketDataRepository):
        self.datacenter = datacenter
        self.last_alert_time = {}
        self.alert_cooldown = 300
        self.history = []

    def detect_alerts(self, market: MarketState) -> list[dict]:
        alerts = []
        alerts.extend(self._check_limit_alerts())
        alerts.extend(self._check_volume_alerts())
        alerts.extend(self._check_index_alerts(market))
        alerts.extend(self._check_breadth_alerts(market))
        alerts.extend(self._check_sector_alerts())
        alerts = self._filter_cooldown(alerts)
        self.history.extend(alerts)
        if len(self.history) > 100:
            self.history = self.history[-100:]
        return alerts

    def _check_limit_alerts(self) -> list[dict]:
        alerts = []
        try:
            stocks = self.datacenter.get_stock_list()
            quotes = self.datacenter.get_realtime_quote(stocks['code'].tolist())
            if quotes.empty:
                return alerts
            limit_up = len(quotes[quotes['change_pct'] >= 9.9])
            limit_down = len(quotes[quotes['change_pct'] <= -9.9])
            if limit_up >= 10:
                alerts.append({
                    'type': 'LIMIT_UP',
                    'level': 'HIGH',
                    'title': f'涨停潮：{limit_up}只股票涨停',
                    'detail': f'当前{limit_up}只股票涨停，市场情绪极度亢奋',
                    'icon': '🔥',
                    'timestamp': datetime.now().isoformat(),
                })
            elif limit_up >= 5:
                alerts.append({
                    'type': 'LIMIT_UP',
                    'level': 'MEDIUM',
                    'title': f'涨停家数增加：{limit_up}只',
                    'detail': f'当前{limit_up}只股票涨停',
                    'icon': '📈',
                    'timestamp': datetime.now().isoformat(),
                })
            if limit_down >= 10:
                alerts.append({
                    'type': 'LIMIT_DOWN',
                    'level': 'HIGH',
                    'title': f'跌停潮：{limit_down}只股票跌停',
                    'detail': f'当前{limit_down}只股票跌停，市场恐慌',
                    'icon': '💀',
                    'timestamp': datetime.now().isoformat(),
                })
            elif limit_down >= 5:
                alerts.append({
                    'type': 'LIMIT_DOWN',
                    'level': 'MEDIUM',
                    'title': f'跌停家数增加：{limit_down}只',
                    'detail': f'当前{limit_down}只股票跌停',
                    'icon': '📉',
                    'timestamp': datetime.now().isoformat(),
                })
        except Exception as e:  # noqa: BLE001 外部调用
            logger.error(f"Check limit alerts error: {e}")
        return alerts

    def _check_volume_alerts(self) -> list[dict]:
        alerts = []
        try:
            stocks = self.datacenter.get_stock_list()
            quotes = self.datacenter.get_realtime_quote(stocks['code'].head(50).tolist())
            if quotes.empty:
                return alerts
            if 'volume' in quotes.columns:
                quotes['volume'] = pd.to_numeric(quotes['volume'], errors='coerce').fillna(0)
                total_volume = float(quotes['volume'].sum())
            else:
                total_volume = 0
            if total_volume == 0:
                return alerts
            cache_key = 'last_volume'
            cached = self.datacenter.cache.get(cache_key)
            if cached:
                last_volume = float(cached.get('volume', 0))
                if last_volume > 0:
                    change_ratio = (total_volume - last_volume) / last_volume
                    if change_ratio > 0.5:
                        alerts.append({
                            'type': 'VOLUME_SURGE',
                            'level': 'HIGH',
                            'title': '成交量暴增：+' + str(int(change_ratio * 100)) + '%',
                            'detail': '成交量较上一统计周期增加' + str(int(change_ratio * 100)) + '%，资金活跃度大幅提升',
                            'icon': '📊',
                            'timestamp': datetime.now().isoformat(),
                        })
                    elif change_ratio < -0.3:
                        alerts.append({
                            'type': 'VOLUME_SHRINK',
                            'level': 'MEDIUM',
                            'title': '成交量萎缩：' + str(int(change_ratio * 100)) + '%',
                            'detail': '成交量较上一统计周期减少' + str(int(abs(change_ratio) * 100)) + '%，市场观望情绪浓厚',
                            'icon': '📉',
                            'timestamp': datetime.now().isoformat(),
                        })
            self.datacenter.cache.set(cache_key, {'volume': total_volume}, ttl=300)
        except Exception as e:  # noqa: BLE001 外部调用
            logger.error(f"Check volume alerts error: {e}")
        return alerts

    def _check_index_alerts(self, market: MarketState) -> list[dict]:
        alerts = []
        try:
            cache_key = 'last_market_state'
            cached = self.datacenter.cache.get(cache_key)
            if cached:
                last_type = cached.get('type', '')
                last_score = cached.get('score', 50)
                if market.type.value != last_type:
                    type_names = {
                        'BULL': '牛市',
                        'BULL_WEAK': '弱牛',
                        'SIDEWAYS': '震荡',
                        'BEAR': '熊市',
                    }
                    alerts.append({
                        'type': 'MARKET_CHANGE',
                        'level': 'HIGH',
                        'title': f'市场状态变更：{type_names.get(last_type, "未知")} → {type_names.get(market.type.value, "未知")}',
                        'detail': f'市场评分从{last_score:.0f}变为{market.score:.0f}',
                        'icon': '🔄',
                        'timestamp': datetime.now().isoformat(),
                    })
                score_change = abs(market.score - last_score)
                if score_change >= 15:
                    direction = "上涨" if market.score > last_score else "下跌"
                    alerts.append({
                        'type': 'SCORE_CHANGE',
                        'level': 'MEDIUM',
                        'title': f'市场情绪{direction}：{score_change:.0f}分',
                        'detail': f'市场评分从{last_score:.0f}变为{market.score:.0f}',
                        'icon': '⚡',
                        'timestamp': datetime.now().isoformat(),
                    })
            self.datacenter.cache.set(cache_key, {
                'type': market.type.value,
                'score': market.score,
            }, ttl=120)
        except Exception as e:  # noqa: BLE001 外部调用
            logger.error(f"Check index alerts error: {e}")
        return alerts

    def _check_breadth_alerts(self, market: MarketState) -> list[dict]:
        alerts = []
        try:
            if market.breadth >= 0.8:
                alerts.append({
                    'type': 'BREADTH_STRONG',
                    'level': 'MEDIUM',
                    'title': f'普涨行情：上涨比例{market.breadth:.0%}',
                    'detail': f'超过{market.breadth:.0%}的股票上涨，市场普涨',
                    'icon': '🎉',
                    'timestamp': datetime.now().isoformat(),
                })
            elif market.breadth <= 0.2:
                alerts.append({
                    'type': 'BREADTH_WEAK',
                    'level': 'MEDIUM',
                    'title': f'普跌行情：下跌比例{1 - market.breadth:.0%}',
                    'detail': f'超过{1 - market.breadth:.0%}的股票下跌，市场普跌',
                    'icon': '😱',
                    'timestamp': datetime.now().isoformat(),
                })
        except Exception as e:  # noqa: BLE001 外部调用
            logger.error(f"Check breadth alerts error: {e}")
        return alerts

    def _check_sector_alerts(self) -> list[dict]:
        alerts = []
        try:
            from .sector_monitor import SectorMonitor
            sector_monitor = SectorMonitor(self.datacenter)
            sectors = sector_monitor.get_sector_data()
            for sector in sectors[:5]:
                if sector['avg_change'] >= 3:
                    alerts.append({
                        'type': 'SECTOR_SURGE',
                        'level': 'MEDIUM',
                        'title': f"板块异动：{sector['name']} +{sector['avg_change']:.1f}%",
                        'detail': f"{sector['name']}板块平均涨幅{sector['avg_change']:.1f}%，上涨{sector['up_count']}家",
                        'icon': '🚀',
                        'timestamp': datetime.now().isoformat(),
                    })
                elif sector['avg_change'] <= -3:
                    alerts.append({
                        'type': 'SECTOR_DROP',
                        'level': 'MEDIUM',
                        'title': f"板块异动：{sector['name']} {sector['avg_change']:.1f}%",
                        'detail': f"{sector['name']}板块平均跌幅{abs(sector['avg_change']):.1f}%，下跌{sector['down_count']}家",
                        'icon': '💥',
                        'timestamp': datetime.now().isoformat(),
                    })
        except Exception as e:  # noqa: BLE001 外部调用
            logger.error(f"Check sector alerts error: {e}")
        return alerts

    def _filter_cooldown(self, alerts: list[dict]) -> list[dict]:
        filtered = []
        now = time.time()
        for alert in alerts:
            alert_type = alert['type']
            last_time = self.last_alert_time.get(alert_type, 0)
            if now - last_time > self.alert_cooldown:
                filtered.append(alert)
                self.last_alert_time[alert_type] = now
        return filtered

    def get_history(self, limit: int = 20) -> list[dict]:
        return self.history[-limit:]

    def format_alert_message(self, alert: dict) -> str:
        return f"{alert['icon']} **{alert['title']}**\n\n{alert['detail']}"
