import pandas as pd
from typing import Optional, List
from loguru import logger
from ..models.signal import Signal, SignalAction, SignalLevel
from ..models.market import MarketState


class DipStrategy:
    def __init__(self):
        self.name = "低吸反转"
        self.buy_score = 65
        self.watch_score = 45

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
        if len(df) < 20:
            return 0
        current = df['close'].iloc[-1]
        low_20 = df['low'].rolling(20).min().iloc[-1]
        high_20 = df['high'].rolling(20).max().iloc[-1]
        if high_20 != low_20:
            position = (current - low_20) / (high_20 - low_20)
            if position < 0.3:
                score += 30
            elif position < 0.5:
                score += 20
        rsi = indicators.get('rsi', 50)
        if rsi < 30:
            score += 30
        elif rsi < 40:
            score += 20
        elif rsi < 50:
            score += 10
        stoch_k = indicators.get('stoch_k', 50)
        if stoch_k < 20:
            score += 20
        elif stoch_k < 30:
            score += 10
        # 止跌企稳判断: 最近3天收盘均高于前一日，且收盘价高于MA5
        if len(df) >= 5:
            last_5 = df.tail(5)
            diffs = last_5['close'].diff().iloc[1:]  # 4天的涨跌
            rising_days = sum(1 for d in diffs if d > 0)
            if rising_days >= 3:  # 3天以上上涨 = 企稳
                score += 15
            elif rising_days >= 2:
                score += 8
            # 额外: 收盘价高于MA5加分
            ma5 = df['close'].rolling(5).mean().iloc[-1]
            if last_5['close'].iloc[-1] > ma5:
                score += 5
        # 市场因子小幅加分(避免三重计算)
        score += market.factor * 8
        return min(score, 100)

    def _determine_level(self, score: float) -> SignalLevel:
        if score >= 80:
            return SignalLevel.S
        elif score >= 70:
            return SignalLevel.A
        else:
            return SignalLevel.B

    def _calculate_position(self, score: float, market: MarketState) -> float:
        base = 0.25
        market_factor = market.factor
        score_factor = score / 100
        return base * market_factor * score_factor

    def _get_reasons(self, df: pd.DataFrame, indicators: dict) -> List[str]:
        reasons = []
        current = df['close'].iloc[-1]
        low_20 = df['low'].rolling(20).min().iloc[-1]
        high_20 = df['high'].rolling(20).max().iloc[-1]
        if high_20 != low_20:
            position = (current - low_20) / (high_20 - low_20)
            if position < 0.3:
                reasons.append("处于20日低位")
        rsi = indicators.get('rsi', 50)
        if rsi < 30:
            reasons.append("RSI超卖")
        elif rsi < 40:
            reasons.append("RSI偏低")
        stoch_k = indicators.get('stoch_k', 50)
        if stoch_k < 20:
            reasons.append("KDJ超卖")
        if len(df) >= 5:
            last_5 = df.tail(5)
            diffs = last_5['close'].diff().iloc[1:]
            rising_days = sum(1 for d in diffs if d > 0)
            if rising_days >= 3:
                reasons.append("连续企稳反弹")
            elif rising_days >= 2:
                reasons.append("止跌迹象")
        return reasons

    def _estimate_volatility(self, df: pd.DataFrame) -> float:
        """估算日波动率"""
        if len(df) < 20:
            return 0.03
        returns = df['close'].pct_change().dropna()
        vol = returns.std()
        return max(0.01, min(0.10, vol))

    def _calculate_target(self, df: pd.DataFrame) -> float:
        """波动率自适应目标价"""
        current = df['close'].iloc[-1]
        vol = self._estimate_volatility(df)
        upside = 2 * vol * (10 ** 0.5)
        upside = max(0.05, min(0.25, upside))
        return current * (1 + upside)

    def _calculate_stop_loss(self, df: pd.DataFrame) -> float:
        """波动率自适应止损"""
        current = df['close'].iloc[-1]
        vol = self._estimate_volatility(df)
        downside = 1.5 * vol * (5 ** 0.5)
        downside = max(0.03, min(0.10, downside))
        return current * (1 - downside)