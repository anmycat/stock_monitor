from typing import List, Optional

import pandas as pd

from ..models.market import MarketState
from ..models.signal import Signal, SignalAction, SignalLevel


class TrendStrategy:
    def __init__(self):
        self.name = "趋势主升"
        self.buy_score = 70
        self.watch_score = 50

    def analyze(self, df: pd.DataFrame, indicators: dict, market: MarketState) -> Optional[Signal]:
        if market.type.value in ['BEAR']:
            return None
        score = self._calculate_score(df, indicators, market)
        if score < self.watch_score:
            return None
        if score >= self.buy_score:
            action = SignalAction.BUY
            level = self._determine_level(score)
            position = self._calculate_position(score, market)
        else:
            action = SignalAction.WATCH
            level = None
            position = 0.0
        reasons = self._get_reasons(df, indicators)
        code = df['code'].iloc[0] if 'code' in df.columns else 'unknown'
        name = df['name'].iloc[0] if 'name' in df.columns else 'unknown'
        return Signal(
            code=code,
            name=name,
            action=action,
            level=level,
            score=score,
            position=position,
            reason=reasons,
            timestamp=pd.Timestamp.now(),
            strategy=self.name,
            price=df['close'].iloc[-1],
            target_price=self._calculate_target(df),
            stop_loss=self._calculate_stop_loss(df),
        )

    def _calculate_score(self, df: pd.DataFrame, indicators: dict, market: MarketState) -> float:
        score = 0
        ma5 = indicators.get('ma5', 0)
        ma10 = indicators.get('ma10', 0)
        ma20 = indicators.get('ma20', 0)
        current = df['close'].iloc[-1]
        if current > ma5 > ma10 > ma20:
            score += 30
        elif current > ma5 > ma10:
            score += 20
        elif current > ma5:
            score += 10
        rsi = indicators.get('rsi', 50)
        if 40 < rsi < 70:
            score += 20
        elif 30 < rsi < 80:
            score += 10
        macd = indicators.get('macd', 0)
        macd_signal = indicators.get('macd_signal', 0)
        if macd > macd_signal:
            score += 20
        vol_ratio = indicators.get('volume_ratio', 1)
        if 1.2 < vol_ratio < 3:
            score += 15
        elif 1 < vol_ratio < 3:
            score += 10
        # 市场因子不再直接加分(避免三重计算)，仅通过dispatcher的仓位因子间接影响
        # 改为小幅加分(最多10分)，主要由策略自身技术指标决定
        score += market.factor * 10
        return min(score, 100)

    def _determine_level(self, score: float) -> SignalLevel:
        if score >= 85:
            return SignalLevel.S
        elif score >= 75:
            return SignalLevel.A
        else:
            return SignalLevel.B

    def _calculate_position(self, score: float, market: MarketState) -> float:
        base = 0.3
        market_factor = market.factor
        score_factor = score / 100
        return base * market_factor * score_factor

    def _get_reasons(self, df: pd.DataFrame, indicators: dict) -> List[str]:
        reasons = []
        ma5 = indicators.get('ma5', 0)
        ma10 = indicators.get('ma10', 0)
        ma20 = indicators.get('ma20', 0)
        current = df['close'].iloc[-1]
        if current > ma5 > ma10 > ma20:
            reasons.append("均线多头排列")
        rsi = indicators.get('rsi', 50)
        if 40 < rsi < 70:
            reasons.append("RSI健康区间")
        macd = indicators.get('macd', 0)
        macd_signal = indicators.get('macd_signal', 0)
        if macd > macd_signal:
            reasons.append("MACD金叉")
        vol_ratio = indicators.get('volume_ratio', 1)
        if vol_ratio > 1.2:
            reasons.append("成交量放大")
        return reasons

    def _estimate_volatility(self, df: pd.DataFrame) -> float:
        """估算日波动率"""
        if len(df) < 20:
            return 0.03  # 默认3%日波动
        returns = df['close'].pct_change().dropna()
        vol = returns.std()
        return max(0.01, min(0.10, vol))  # 限制在 1%-10%

    def _calculate_target(self, df: pd.DataFrame) -> float:
        """波动率自适应目标价: 2倍ATR等效"""
        current = df['close'].iloc[-1]
        vol = self._estimate_volatility(df)
        # 目标涨幅 = 2 * 日波动率 * sqrt(10) (10日持有期)
        upside = 2 * vol * (10 ** 0.5)
        upside = max(0.05, min(0.30, upside))  # 限制 5%-30%
        return current * (1 + upside)

    def _calculate_stop_loss(self, df: pd.DataFrame) -> float:
        """波动率自适应止损: 1.5倍ATR等效"""
        current = df['close'].iloc[-1]
        vol = self._estimate_volatility(df)
        # 止损 = 1.5 * 日波动率 * sqrt(5) (5日缓冲)
        downside = 1.5 * vol * (5 ** 0.5)
        downside = max(0.03, min(0.12, downside))  # 限制 3%-12%
        return current * (1 - downside)