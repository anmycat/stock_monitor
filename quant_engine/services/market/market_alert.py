from __future__ import annotations

import time
from datetime import datetime

import pandas as pd
from loguru import logger

from ...data.repositories import MarketDataRepository
from ...models.market import MarketState


class MarketAlert:
    """大盘异动检测模块 - 类似同花顺异动监控

    板块数据由外部经 sector_data_provider 注入（依赖倒置）：
    market 为 services 叶族子包，按 DAG 宪法不得 import monitors 的
    SectorMonitor；消费者（worker 入口 / api 依赖注入）传入
    ``lambda: SectorMonitor(dc).get_sector_data()``。
    未注入时板块异动检测自动跳过（功能降级而非报错）。
    """

    def __init__(self, datacenter: MarketDataRepository,
                 sector_data_provider=None, ths_context_provider=None,
                 eltdx_context_provider=None):
        self.datacenter = datacenter
        self.sector_data_provider = sector_data_provider
        # D71：同花顺官方数据只做证据注入（默认 None → 零请求零行为变化）
        self.ths_context_provider = ths_context_provider
        self._ths_context = None
        self._ths_context_at = 0.0
        self._ths_context_ttl = 120.0
        # D74：eltdx（TDX 7709）盘面证据注入——连板梯队/资金流/榜单/封单，
        # 只挂告警展示字段，不改任何阈值判定（零信号影响）
        self.eltdx_context_provider = eltdx_context_provider
        self._eltdx_context = None
        self._eltdx_context_at = 0.0
        self._eltdx_context_ttl = 120.0
        self.last_alert_time = {}
        self.alert_cooldown = 300
        self.history = []

    def _get_ths_context(self) -> dict | None:
        """取 THS 证据包（缓存 120s，失败不缓存以便下一轮重试）。"""
        if self.ths_context_provider is None:
            return None
        now = time.time()
        if self._ths_context is not None and now - self._ths_context_at < self._ths_context_ttl:
            return self._ths_context
        try:
            ctx = self.ths_context_provider()
        except Exception as e:  # noqa: BLE001 外部数据源边界
            logger.warning(f"THS market alert context failed: {type(e).__name__}: {e}")
            return None
        if isinstance(ctx, dict) and ctx:
            self._ths_context = ctx
            self._ths_context_at = now
            return ctx
        return None

    def _get_eltdx_context(self) -> dict | None:
        """取 eltdx 盘面证据包（缓存 120s，失败不缓存以便下一轮重试）。"""
        if self.eltdx_context_provider is None:
            return None
        now = time.time()
        if (self._eltdx_context is not None and
                now - self._eltdx_context_at < self._eltdx_context_ttl):
            return self._eltdx_context
        try:
            ctx = self.eltdx_context_provider()
        except Exception as e:  # noqa: BLE001 外部数据源边界
            logger.warning(f"eltdx market alert context failed: {type(e).__name__}: {e}")
            return None
        if isinstance(ctx, dict) and ctx:
            self._eltdx_context = ctx
            self._eltdx_context_at = now
            return ctx
        return None

    def detect_alerts(self, market: MarketState) -> list[dict]:
        ths_ctx = self._get_ths_context()
        eltdx_ctx = self._get_eltdx_context()
        alerts = []
        alerts.extend(self._check_limit_alerts(market, ths_ctx))
        alerts.extend(self._check_volume_alerts())
        alerts.extend(self._check_index_alerts(market))
        alerts.extend(self._check_breadth_alerts(market))
        alerts.extend(self._check_sector_alerts())
        if ths_ctx:
            alerts.extend(self._check_ths_seal_alerts(ths_ctx))
        # D74：eltdx 证据挂到所有已产生告警的展示字段（不改判定、不新增告警维度）
        if eltdx_ctx:
            for alert in alerts:
                evidence = alert.setdefault('evidence', {})
                if isinstance(evidence, dict):
                    evidence['eltdx_context'] = eltdx_ctx
        alerts = self._filter_cooldown(alerts)
        self.history.extend(alerts)
        if len(self.history) > 100:
            self.history = self.history[-100:]
        return alerts

    def _check_ths_seal_alerts(self, ctx: dict) -> list[dict]:
        """封板转弱告警：THS 官方统计才有的维度，默认开关关闭时不发生。"""
        from ...config import get_settings
        cfg = get_settings()
        if not cfg.THS_MARKET_ALERT_ENRICH:
            return []
        seal_rate = ctx.get('seal_rate')
        broken = ctx.get('open_stock_count')
        if not isinstance(seal_rate, (int, float)) or not isinstance(broken, int):
            return []
        if seal_rate >= cfg.THS_SEAL_RATE_WEAK or broken < cfg.THS_LIMIT_BREAK_MIN:
            return []
        return [{
            'type': 'SEAL_RATE_WEAK',
            'level': 'MEDIUM',
            'title': f'封板转弱：封板率{seal_rate:.0%}，炸板{broken}家',
            'detail': (f"同花顺统计：涨停{ctx.get('limit_up_stock_count', '—')}家、"
                       f"触板{ctx.get('touched_limit_up_stock_count', '—')}家、"
                       f"炸板{broken}家；证据时间 {ctx.get('trade_date', '—')}"),
            'icon': '📊',
            'timestamp': datetime.now().isoformat(),
            'evidence': ctx,
        }]

    def _check_limit_alerts(self, market: MarketState | None = None,
                            ths_ctx: dict | None = None) -> list[dict]:
        """涨跌停家数异动（D63 口径修正；D71 追加 THS 证据字段）

        D63 修两个缺陷：
        ① **口径**：原用统一 `>= 9.9` 判涨停 → 创业板/科创板（名义 20%）的
           10~19% 涨幅被误算为涨停，北交所（名义 30%）同理。改为按
           `utils/limit_rules` 的**逐股名义幅度**判定。
        ② **措辞**：原逻辑只看涨停家数就给「市场情绪极度亢奋」，不校验市场宽度，
           与同屏的「市场普跌」自相矛盾（2026-11-06 实测：83% 下跌 + 32 只涨停
           同时出现在一条消息里）。改为**用市场宽度交叉判定**：
           宽度不支持时不给出情绪结论，改为陈述「结构性分化」这一事实。
        """
        alerts = []
        try:
            stocks = self.datacenter.get_stock_list()
            quotes = self.datacenter.get_realtime_quote(stocks['code'].tolist())
            if quotes.empty:
                return alerts
            # D63：与 breadth 同一数据卫生口径（sanitize），否则两个数字样本不同
            from ...utils.market_sanity import sanitize_quotes
            quotes, _stats = sanitize_quotes(quotes)
            if quotes.empty:
                return alerts
            limit_up = self._count_at_limit(quotes, up=True)
            limit_down = self._count_at_limit(quotes, up=False)
            breadth = market.breadth if market is not None else None
            if limit_up >= 10:
                alerts.append({
                    'type': 'LIMIT_UP',
                    'level': 'HIGH',
                    'title': self._limit_up_title(limit_up, breadth),
                    'detail': self._limit_up_detail(limit_up, breadth),
                    'icon': '🔥',
                    'timestamp': datetime.now().isoformat(),
                })
            elif limit_up >= 5:
                alerts.append({
                    'type': 'LIMIT_UP',
                    'level': 'MEDIUM',
                    'title': f'涨停家数增加：{limit_up}只',
                    'detail': self._limit_up_detail(limit_up, breadth),
                    'icon': '📈',
                    'timestamp': datetime.now().isoformat(),
                })
            if limit_down >= 10:
                alerts.append({
                    'type': 'LIMIT_DOWN',
                    'level': 'HIGH',
                    'title': f'跌停潮：{limit_down}只股票跌停',
                    'detail': self._limit_down_detail(limit_down, breadth),
                    'icon': '💀',
                    'timestamp': datetime.now().isoformat(),
                })
            elif limit_down >= 5:
                alerts.append({
                    'type': 'LIMIT_DOWN',
                    'level': 'MEDIUM',
                    'title': f'跌停家数增加：{limit_down}只',
                    'detail': self._limit_down_detail(limit_down, breadth),
                    'icon': '📉',
                    'timestamp': datetime.now().isoformat(),
                })
        except Exception as e:  # noqa: BLE001 外部调用
            logger.error(f"Check limit alerts error: {e}")
        # D71：证据只挂在展示字段上，不参与任何阈值判断/标题口径（零信号影响）
        if ths_ctx:
            for alert in alerts:
                alert['evidence'] = ths_ctx
        return alerts

    @staticmethod
    def _count_at_limit(quotes: pd.DataFrame, up: bool = True) -> int:
        """按逐股名义涨跌停幅度统计涨停/跌停家数（D63）

        全市场 5563 只 → 用 tolist() 一次性取列后纯 Python 判定，避免 pandas
        逐行 .iloc 的索引开销（告警每 5 分钟跑一次，须控制耗时）。
        """
        from ...utils.limit_rules import is_at_limit_down, is_at_limit_up

        if quotes is None or quotes.empty or 'change_pct' not in quotes.columns:
            return 0
        pcts = quotes['change_pct'].tolist()
        codes = (quotes['code'].tolist() if 'code' in quotes.columns
                 else [''] * len(quotes))
        names = (quotes['name'].tolist() if 'name' in quotes.columns
                 else [None] * len(quotes))
        prices = (quotes['price'].tolist() if 'price' in quotes.columns
                  else [None] * len(quotes))
        checker = is_at_limit_up if up else is_at_limit_down
        return sum(1 for i in range(len(pcts))
                   if checker(pcts[i], codes[i], names[i], prices[i]))

    @staticmethod
    def _limit_up_title(limit_up: int, breadth: float | None) -> str:
        """涨停标题（D63：标题同样用宽度交叉判定，不在普跌日称「潮」）

        实测（2026-11-06 收盘）：全市场 5213 只、86% 下跌、当日 35 只涨停。
        旧标题「涨停潮」在这种结构下属夸大 → 改称「结构性涨停」。
        """
        if breadth is not None and breadth < 0.4:
            return f'结构性涨停：{limit_up}只股票涨停'
        return f'涨停潮：{limit_up}只股票涨停'

    @staticmethod
    def _limit_up_detail(limit_up: int, breadth: float | None) -> str:
        """涨停文案（D63：用市场宽度交叉判定，不在宽度不支持时断言「亢奋」）

        宽度口径与「普跌行情」告警**同一个** `market.breadth` —— 同屏两条消息
        引用同一宽度，语义自洽。
        """
        if breadth is None:
            return f'当前{limit_up}只股票涨停'
        up_ratio = breadth
        down_ratio = 1 - breadth
        if limit_up >= 10:
            if up_ratio >= 0.5:
                return (f'当前{limit_up}只股票涨停，全市场上涨占比{up_ratio:.0%}，'
                        f'市场情绪极度亢奋')
            if up_ratio < 0.4:
                return (f'当前{limit_up}只股票涨停，但全市场{down_ratio:.0%}下跌——'
                        f'赚钱效应集中在极少数标的，属结构性分化')
            return (f'当前{limit_up}只股票涨停，全市场上涨占比{up_ratio:.0%}，'
                    f'局部情绪偏热')
        return f'当前{limit_up}只股票涨停，全市场上涨占比{up_ratio:.0%}'

    @staticmethod
    def _limit_down_detail(limit_down: int, breadth: float | None) -> str:
        """跌停文案（D63：对称交叉判定——普涨中的少数跌停不是「恐慌」）"""
        if breadth is None:
            return f'当前{limit_down}只股票跌停'
        up_ratio = breadth
        down_ratio = 1 - breadth
        if limit_down >= 10:
            if down_ratio >= 0.6:
                return (f'当前{limit_down}只股票跌停，全市场下跌占比{down_ratio:.0%}，'
                        f'市场恐慌情绪蔓延')
            if up_ratio > 0.6:
                return (f'当前{limit_down}只股票跌停，但全市场{up_ratio:.0%}上涨——'
                        f'属结构性分化而非全面恐慌')
            return f'当前{limit_down}只股票跌停，全市场下跌占比{down_ratio:.0%}'
        return f'当前{limit_down}只股票跌停，全市场下跌占比{down_ratio:.0%}'

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
        if self.sector_data_provider is None:
            return alerts  # 未注入板块数据源，功能降级跳过
        try:
            sectors = self.sector_data_provider()
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
        msg = f"{alert['icon']} **{alert['title']}**\n\n{alert['detail']}"
        evidence = alert.get('evidence') or {}
        if evidence:
            seal_rate = evidence.get('seal_rate')
            broken = evidence.get('open_stock_count')
            if isinstance(seal_rate, (int, float)):
                msg += f"\n\n数据来源：同花顺官方（封板率{seal_rate:.0%}"
                if isinstance(broken, int):
                    msg += f" / 炸板{broken}家"
                msg += f" / {evidence.get('trade_date', '—')}）"
        # D74：eltdx 盘面证据来源行（纯展示，不参与任何阈值判定）
        eltdx = (evidence.get('eltdx_context') if isinstance(evidence, dict) else None)
        if isinstance(eltdx, dict):
            hot = eltdx.get('hot_rank') or []
            flow = eltdx.get('money_flow') or {}
            bits = [eltdx.get('source', 'eltdx')]
            if hot:
                bits.append(f"热榜{len(hot)}条")
            if flow:
                bits.append(f"资金流{len(flow)}只")
            if eltdx.get('observed_at'):
                bits.append(str(eltdx['observed_at'])[:16])
            msg += "\n\n盘面证据：" + " / ".join(bits)
        return msg
