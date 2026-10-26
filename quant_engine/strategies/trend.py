from __future__ import annotations

import pandas as pd
from loguru import logger

from ..models.market import MarketState
from ..models.signal import Signal, SignalAction, SignalLevel


class TrendStrategy:
    def __init__(self):
        self.name = "趋势主升"
        self.buy_score = 70
        self.watch_score = 50
        # ── D44 风控阈值（docs/26 M3-1/M3-5，参考值见 research/docs/26 §1-2）──
        # 乖离率警戒线（5日线，%）：口诀"股价离5日线超9%大概率回踩"；
        # 参考 A股 BIAS 阈值体系（6日±6%/12日±5%，5日线更敏感故容忍度更高）
        self.BIAS_OVERBOUGHT = 9.0
        # 20日价格位置分界（复用 dip.py 算法：低位<0.3 / 高位>0.8）
        self.POSITION_LOW = 0.3
        self.POSITION_HIGH = 0.8
        # 量比分段（标准量比分段判读，项目日K口径见 indicators.volume_ratio）
        self.VR_MODERATE = 1.5    # 温和放量
        self.VR_HIGH = 2.5        # 明显放量
        self.VR_EXTREME = 5.0     # 剧烈放量

    def analyze(self, df: pd.DataFrame, indicators: dict, market: MarketState) -> Signal | None:
        if market.type.value in ['BEAR']:
            return None
        score = self._calculate_score(df, indicators, market)
        # 追高/派发风控闸门（D44）：分数达标但触发硬条件 → 强制降级 WATCH，
        # 不推 BUY。区分"降级"与"过滤"——信号仍可见（WATCH 也返回），
        # 只是不构成买入建议（docs/26 M3-1/M3-5）
        veto, veto_reason = self._check_veto(df, indicators)
        if score >= self.watch_score:
            if score >= self.buy_score and not veto:
                action = SignalAction.BUY
                level = self._determine_level(score)
                position = self._calculate_position(score, market)
            else:
                action = SignalAction.WATCH
                level = None
                position = 0.0
        else:
            return None
        reasons = self._get_reasons(df, indicators)
        if veto_reason:
            reasons.insert(0, veto_reason)
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

    def _check_veto(self, df: pd.DataFrame, indicators: dict) -> tuple[bool, str]:
        """追高/派发风控闸门（D44，docs/26 M3-1 乖离率 + M3-5 位置×量比）

        Returns:
            (是否禁BUY, 原因说明)

        两条硬规则（阈值参考 research/docs/26 §1-2 检索结论）：
        1. 5日乖离率 > 9%：口诀"股价离5日线超9%大概率回踩"，
           超买状态下不追高。参考 BIAS 体系（6日±6%/12日±5%，
           5日线更敏感容忍度更高）与"强势股长期高乖离"警告——
           此处只禁 BUY 不卖（WATCH 仍保留观察）。
        2. 20日位置 > 0.8（高位）且量比 >= 5（剧烈放量）：视频二
           "位置决定性质"——高位剧烈放量是派发信号（"量比过十先跑
           为敬"），百度百科/私募参考一致。
        高位+温和/明显放量(2.5-5)不硬禁，在 _calculate_score 降分
        处理（主升浪加速也呈此形态，一刀切会误杀）。
        """
        current = df['close'].iloc[-1]
        ma5 = indicators.get('ma5') or 0
        if ma5 > 0:
            bias = (current / ma5 - 1) * 100
            if bias > self.BIAS_OVERBOUGHT:
                return True, f"5日乖离{bias:.1f}%超警戒线{self.BIAS_OVERBOUGHT:.0f}%，不追高"
        if len(df) >= 20:
            low_20 = df['low'].rolling(20).min().iloc[-1]
            high_20 = df['high'].rolling(20).max().iloc[-1]
            if high_20 > low_20:
                pos = (current - low_20) / (high_20 - low_20)
                vol_ratio = indicators.get('volume_ratio', 1)
                if pos > self.POSITION_HIGH and vol_ratio >= self.VR_EXTREME:
                    return True, "高位剧烈放量（量比≥5），防派发不追高"
        return False, ""

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
        score += self._volume_position_score(df, indicators)
        # 市场因子不再直接加分(避免三重计算)，仅通过dispatcher的仓位因子间接影响
        # 改为小幅加分(最多10分)，主要由策略自身技术指标决定
        score += market.factor * 10
        return min(score, 100)

    def _volume_position_score(self, df: pd.DataFrame, indicators: dict) -> float:
        """量比分段 + 位置×量比组合 + 量增价平识别（D44，docs/26 M3-5/M3-6）

        参考（research/docs/26 §2）：量比分段判读（<0.5 无人问津 /
        1.5-2.5 温和 / 2.5-5 明显 / 5-10 剧烈高位=出货）+
        "位置决定性质"（低位放量=吸筹加分，高位放量=派发降分）+
        "量增价平要警惕"（放量但涨幅微弱 = 抛压沉重）。
        """
        score = 0.0
        vol_ratio = indicators.get('volume_ratio', 1)
        # ── 量比分段（日K口径：当日量/20日均量）──
        if vol_ratio >= self.VR_EXTREME:
            score += 5   # 剧烈放量本身不加分（性质由位置决定）
        elif vol_ratio >= self.VR_HIGH:
            score += 15  # 明显放量
        elif vol_ratio >= self.VR_MODERATE:
            score += 10  # 温和放量
        elif vol_ratio >= 1.0:
            score += 5   # 略放量（过渡档：标准口径 1.0-1.5 为正常波动，
                         # 直接归零会与旧版 1<vr<3 一律+10 形成断崖误伤）
        elif vol_ratio < 0.5:
            score -= 5   # 无人问津
        # ── 量增价平：放量但涨幅微弱 = 抛压沉重（视频二口诀）──
        if vol_ratio >= self.VR_HIGH and len(df) >= 2:
            pct_chg = (df['close'].iloc[-1] / df['close'].iloc[-2] - 1) * 100
            if abs(pct_chg) < 1.0:
                score -= 5
                logger.debug("Volume up but price flat, -5")
        # ── 位置×量比组合（20日位置，复用 dip.py 算法）──
        if len(df) >= 20:
            current = df['close'].iloc[-1]
            low_20 = df['low'].rolling(20).min().iloc[-1]
            high_20 = df['high'].rolling(20).max().iloc[-1]
            if high_20 > low_20:
                pos = (current - low_20) / (high_20 - low_20)
                if pos < self.POSITION_LOW and vol_ratio >= self.VR_MODERATE:
                    score += 10  # 低位温和放量 = 资金吸筹（视频二最稳健形态）
                elif pos > self.POSITION_HIGH and self.VR_HIGH <= vol_ratio < self.VR_EXTREME:
                    score -= 10  # 高位明显放量 = 派发风险（保留信号不推BUY）
        return score

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

    def _get_reasons(self, df: pd.DataFrame, indicators: dict) -> list[str]:
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
        if vol_ratio >= self.VR_EXTREME:
            reasons.append("剧烈放量")
        elif vol_ratio >= self.VR_HIGH:
            reasons.append("明显放量")
        elif vol_ratio >= self.VR_MODERATE:
            reasons.append("温和放量")
        # 乖离率与位置说明（D44）
        if ma5 > 0:
            bias = (current / ma5 - 1) * 100
            if abs(bias) <= 3 and current > ma5:
                reasons.append("贴近5日线（回踩不破）")
            elif bias > self.BIAS_OVERBOUGHT:
                reasons.append(f"乖离率{bias:.1f}%偏高")
        if len(df) >= 20:
            low_20 = df['low'].rolling(20).min().iloc[-1]
            high_20 = df['high'].rolling(20).max().iloc[-1]
            if high_20 > low_20:
                pos = (current - low_20) / (high_20 - low_20)
                if pos < self.POSITION_LOW:
                    reasons.append("20日低位")
                elif pos > self.POSITION_HIGH:
                    reasons.append("20日高位")
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
