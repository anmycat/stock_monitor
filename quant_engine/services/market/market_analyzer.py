
import pandas as pd
from loguru import logger

from ...data.repositories import MarketDataRepository
from ...models.hmm_detector import get_regime_detector
from ...models.market import MarketState, MarketType
from ...utils.indicators import calculate_indicators


class MarketAnalyzer:
    def __init__(self, datacenter: MarketDataRepository):
        self.datacenter = datacenter
        self.hmm = get_regime_detector()

    def analyze_market(self, index_code: str = "000001.SH") -> MarketState:
        df = self.datacenter.get_daily_data(index_code, 120)
        if df is None or df.empty:
            return self._default_market_state()
        indicators = calculate_indicators(df)
        breadth = self._calculate_breadth()
        trend = self._calculate_trend(indicators)
        volume_ratio = indicators.get('volume_ratio', 1.0)
        market_type = self._determine_market_type(trend, breadth, volume_ratio, df)
        score = self._calculate_score(trend, breadth, volume_ratio, market_type)
        return MarketState(
            type=market_type,
            score=score,
            trend=trend,
            volume_ratio=volume_ratio,
            breadth=breadth,
            timestamp=pd.Timestamp.now(),
            details=indicators,
        )

    def _calculate_breadth(self) -> float:
        try:
            stocks = self.datacenter.get_stock_list()
            quotes = self.datacenter.get_realtime_quote(stocks['code'].tolist())
            if quotes.empty:
                return 0.5
            # B5 数据卫生断言（2026-10-21）：剔除超物理涨跌幅/非正价脏行后再算比例，
            # 结果夹紧 [0,1]（与 utils/market_sanity 同一事实源）
            from ...utils.market_sanity import sanitize_quotes
            quotes, _stats = sanitize_quotes(quotes)
            if quotes.empty:
                logger.warning("[B5] breadth: all quotes dropped by sanity filter, fallback 0.5")
                return 0.5
            up = len(quotes[quotes['change_pct'] > 0])
            total = len(quotes)
            breadth = up / total if total > 0 else 0.5
            return min(1.0, max(0.0, breadth))
        except Exception as e:  # noqa: BLE001 降级默认
            logger.error(f"Calculate breadth error: {e}")
            return 0.5

    def _calculate_trend(self, indicators: dict) -> float:
        score = 0
        ma5 = indicators.get('ma5', 0)
        ma10 = indicators.get('ma10', 0)
        ma20 = indicators.get('ma20', 0)
        if ma5 > ma10 > ma20:
            score += 0.4
        elif ma5 > ma10:
            score += 0.2
        elif ma5 > ma20:
            score += 0.1
        rsi = indicators.get('rsi', 50)
        if 40 < rsi < 70:
            score += 0.3
        elif 30 < rsi < 80:
            score += 0.15
        macd = indicators.get('macd', 0)
        if macd > 0:
            score += 0.3
        return score

    def _determine_market_type(self, trend: float, breadth: float, volume_ratio: float, df: pd.DataFrame = None) -> MarketType:
        # HMM优先检测
        if df is not None and len(df) >= 60:
            try:
                trained = self.hmm.fit(df, n_days=min(60, len(df)))
                if trained:
                    state_idx, label, confidence = self.hmm.predict(df)
                    if confidence > 0.5:
                        state_map = {
                            0: MarketType.BEAR,
                            1: MarketType.BEAR,
                            2: MarketType.SIDEWAYS,
                            3: MarketType.BULL_WEAK,
                            4: MarketType.BULL,
                        }
                        logger.info(f"HMM detected: {label} (confidence={confidence:.2f})")
                        return state_map.get(state_idx, MarketType.SIDEWAYS)
            except Exception as e:  # noqa: BLE001 降级默认
                logger.warning(f"HMM detection failed, using rules: {e}")
        
        # 规则回退
        if trend > 0.7 and breadth > 0.6 and volume_ratio > 1.2:
            return MarketType.BULL
        elif trend > 0.5 and breadth > 0.5:
            return MarketType.BULL_WEAK
        elif trend < 0.3 and breadth < 0.4:
            return MarketType.BEAR
        else:
            return MarketType.SIDEWAYS

    def _calculate_score(self, trend: float, breadth: float, volume_ratio: float, market_type: MarketType) -> float:
        base_scores = {
            MarketType.BULL: 80,
            MarketType.BULL_WEAK: 65,
            MarketType.SIDEWAYS: 50,
            MarketType.BEAR: 30,
        }
        base = base_scores[market_type]
        trend_bonus = trend * 10
        breadth_bonus = breadth * 10
        return min(100, base + trend_bonus + breadth_bonus)

    def _default_market_state(self) -> MarketState:
        return MarketState(
            type=MarketType.SIDEWAYS,
            score=50,
            trend=0.5,
            volume_ratio=1.0,
            breadth=0.5,
            timestamp=pd.Timestamp.now(),
        )
