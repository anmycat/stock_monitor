from __future__ import annotations
from datetime import datetime
from typing import Optional

from loguru import logger

from ...data.repositories import MarketDataRepository
from ...models.market import MarketState
from ...models.signal import Signal, SignalAction
from ...strategies.registry import StrategyRegistry
from ..market.market_analyzer import MarketAnalyzer
from ..notifications.manager import NotificationManager
from ..risk.cooldown import CooldownManager
from ..risk.position import PositionManager
from ..risk.position_sizer import get_position_sizer
from ..risk.risk import RiskManager


class SignalGenerator:
    def __init__(self, datacenter: MarketDataRepository):
        self.datacenter = datacenter
        self.registry = StrategyRegistry(datacenter)
        self.dispatcher = self.registry.dispatcher  # 兼容旧代码直接引用
        self.market_analyzer = MarketAnalyzer(datacenter)
        self.position_manager = PositionManager()
        self.risk_manager = RiskManager()
        self.cooldown_manager = CooldownManager(datacenter.cache)
        self.notifier = NotificationManager()
        self.position_sizer = get_position_sizer()  # Kelly深度融合
        self.current_positions: dict[str, float] = {}
        self.equity_curve: list[float] = []
        self.trades: list[dict] = []

    def scan_all(self, max_signals: int = 3) -> list[Signal]:
        market = self.market_analyzer.analyze_market()
        logger.info(f"Market state: {market.type.value}, score: {market.score}")
        if market.type.value == 'BEAR':
            logger.info("Bear market, skip scanning")
            return []
        stocks = self.datacenter.get_stock_list()

        # 预排序: 用实时行情的涨幅排序，优先分析活跃股
        # 避免数据源顺序导致错过优质标的
        try:
            quotes = self.datacenter.get_realtime_quote(stocks['code'].tolist())
            if not quotes.empty and 'change_pct' in quotes.columns:
                quotes['abs_change'] = quotes['change_pct'].abs()
                top_codes = quotes.nlargest(80, 'abs_change')['code'].tolist()  # 取前80只活跃股
                stock_dict = dict(zip(stocks['code'], stocks['name']))
                scan_list = [(c, stock_dict.get(c, '')) for c in top_codes if c in stock_dict]
            else:
                scan_list = list(stocks.itertuples(index=False, name=None))
        except Exception as e:  # noqa: BLE001 降级默认
            logger.warning(f"Pre-ranking failed, using default order: {e}")
            scan_list = list(stocks.itertuples(index=False, name=None))

        signals = []
        for stock in scan_list:
            try:
                signal = self._analyze_stock(stock[0], stock[1], market)
                if signal is not None:
                    signals.append(signal)
                    if len(signals) >= max_signals * 2:  # 多取一些用于后续排序
                        break
            except Exception as e:  # noqa: BLE001 单项跳过
                logger.error(f"Analyze {stock[0]} error: {e}")

        # 热点板块加分
        try:
            from .hotspot import HotspotService
            hotspot = HotspotService(self.datacenter)
            hot_codes = set(hotspot.get_hot_sector_codes())
            for s in signals:
                if s.code in hot_codes:
                    s.score *= 1.05  # 热点板块 +5%
        except Exception:  # noqa: BLE001 外部调用
            pass

        signals.sort(key=lambda s: s.score, reverse=True)
        signals = signals[:max_signals]

        # 为 BUY 信号添加上下文（新闻/舆情）
        self._enrich_signals(signals)
        return signals

    def scan_stock(self, code: str, name: str = "") -> Signal | None:
        market = self.market_analyzer.analyze_market()
        return self._analyze_stock(code, name, market)

    def _analyze_stock(self, code: str, name: str, market: MarketState) -> Signal | None:
        if not self.cooldown_manager.is_cooled_down(code):
            logger.debug(f"Signal cooldown active for {code}")
            return None
        df = self.datacenter.get_daily_data(code, 120)
        if df is None or df.empty:
            return None
        df['code'] = code
        df['name'] = name
        signal = self.dispatcher.dispatch(df, market, self.trades, self.equity_curve)
        if signal is None:
            return None
        risk_check = self.risk_manager.check_signal(signal, self.current_positions)
        if not risk_check['approved']:
            logger.info(f"Signal rejected: {risk_check['reason']}")
            return None
        
        # Kelly公式深度融合: 用PositionSizer替代简单仓位计算
        current_total = sum(self.current_positions.values())
        num_holdings = len(self.current_positions) + 1  # 包括本次
        kelly_result = self.position_sizer.calculate_position(
            signal_score=signal.score,
            market_state=market,
            volatility=self._estimate_volatility(df),
            current_positions=current_total,
            strategy_name=signal.strategy,
            num_holdings=num_holdings
        )
        
        # 应用Kelly优化后的仓位(取风控仓位和Kelly仓位的较小值)
        risk_position = risk_check['adjusted_position']
        kelly_position = kelly_result.recommended_position
        signal.position = min(risk_position, kelly_position)
        
        # 填充Kelly字段
        signal.kelly_position = kelly_result.kelly_position
        signal.kelly_confidence = kelly_result.kelly_confidence
        signal.kelly_source = kelly_result.kelly_source
        
        date_str = datetime.now().strftime("%Y%m%d")
        signal_hash = CooldownManager.generate_signal_hash(code, signal.action.value, date_str)
        if not self.cooldown_manager.is_idempotent(signal_hash):
            logger.debug(f"Idempotent check failed for {code}")
            return None
        self.cooldown_manager.set_idempotent(signal_hash)
        if signal.action == SignalAction.BUY:
            self.cooldown_manager.set_cooldown(code)
            self.notifier.send_signal(signal)
        return signal
    
    def _estimate_volatility(self, df) -> float:
        """估算年化波动率"""
        try:
            if len(df) < 20:
                return 0.25  # 默认25%
            returns = df['close'].pct_change().dropna()
            daily_vol = returns.std()
            # 年化: * sqrt(252)
            annual_vol = daily_vol * (252 ** 0.5)
            return max(0.1, min(1.0, annual_vol))
        except (KeyError, TypeError, ValueError):
            return 0.25

    def update_after_trade(self, code: str, pnl: float):
        self.position_manager.update_capital(pnl)
        if code in self.current_positions:
            del self.current_positions[code]
        self.trades.append({'code': code, 'pnl': pnl})
        capital_info = self.position_manager.get_capital_info()
        self.equity_curve.append(capital_info['current'])

    def _enrich_signals(self, signals: list[Signal]):
        """为 BUY 信号添加候选股上下文（新闻/舆情）"""
        buy_signals = [s for s in signals if s.action == SignalAction.BUY]
        if not buy_signals:
            return
        try:
            from .candidate_context import CandidateContext
            ctx = CandidateContext()
            candidates = [
                {'code': s.code, 'name': s.name, 'score': s.score}
                for s in buy_signals
            ]
            enriched = ctx.enrich_candidates(candidates, max_candidates=3)
            # 将上下文写回信号
            enriched_map = {c['code']: c.get('context', {}) for c in enriched}
            for s in buy_signals:
                context = enriched_map.get(s.code)
                if context:
                    s.context = context
        except Exception as e:  # noqa: BLE001 外部调用
            logger.warning(f"Signal context enrichment failed: {e}")
