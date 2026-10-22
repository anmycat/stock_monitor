"""多因子评分器 - 计算多维度因子分数

增强版新增因子:
- 资金流向因子(主力净流入)
- 技术指标增强(MACD背离、RSI超买超卖)
- 市场情绪因子(量价配合)
"""
from typing import Dict, Optional

import pandas as pd
from loguru import logger


class FactorScorer:
    """多因子评分器"""

    def __init__(self):
        self.base_weights = {
            'value_score': 0.12,
            'momentum_score': 0.15,
            'liquidity_score': 0.10,
            'stability_score': 0.10,
            'volume_score': 0.10,
            'trend_score': 0.15,
            'fund_flow_score': 0.13,
            'technical_score': 0.15,
        }
        self.weights = dict(self.base_weights)

    def adjust_weights_for_market(self, market_state=None):
        """动态因子权重 — 对标幻方"市场状态调整因子权重"
        
        HMM状态 → 动态因子权重映射:
        - 深熊(0): 防御为主 — 增估值/稳定性，减动量/趋势
        - 弱熊(1): 偏防御 — 微调
        - 震荡(2): 均衡 — 恢复基准权重
        - 弱牛(3): 偏进攻 — 增趋势/资金流
        - 强牛(4): 进攻为主 — 增动量/技术指标
        """
        try:
            from ..models.hmm_detector import get_regime_detector
            hmm = get_regime_detector()
            state = hmm._last_state
        except Exception:  # noqa: BLE001 降级默认
            state = 2  # 默认震荡

        adjustments = {
            0: {  # 深熊
                'value_score': +0.06, 'stability_score': +0.05,
                'momentum_score': -0.05, 'trend_score': -0.06,
                'fund_flow_score': -0.03, 'technical_score': -0.03,
            },
            1: {  # 弱熊
                'value_score': +0.03, 'stability_score': +0.03,
                'momentum_score': -0.03, 'trend_score': -0.02,
            },
            2: {},  # 震荡 — 基准权重
            3: {  # 弱牛
                'trend_score': +0.03, 'fund_flow_score': +0.02,
                'stability_score': -0.02, 'value_score': -0.02,
            },
            4: {  # 强牛
                'momentum_score': +0.05, 'technical_score': +0.05,
                'trend_score': +0.03, 'fund_flow_score': +0.03,
                'value_score': -0.04, 'stability_score': -0.04,
            },
        }

        adj = adjustments.get(state, {})
        self.weights = {}
        for k, base_w in self.base_weights.items():
            delta = adj.get(k, 0)
            self.weights[k] = max(0.05, min(0.25, base_w + delta))

        # 归一化
        total = sum(self.weights.values())
        self.weights = {k: round(v / total, 4) for k, v in self.weights.items()}

        if state != 2:
            logger.info(f"Factor weights adjusted for market state={state}: "
                        f"momentum={self.weights['momentum_score']:.2f} "
                        f"value={self.weights['value_score']:.2f}")

    def calculate_factors(self, df: pd.DataFrame, indicators: Dict, fund_flow: Optional[Dict] = None) -> Dict:
        """计算多维度因子分数
        
        Args:
            df: 历史价格数据
            indicators: 技术指标字典
            fund_flow: 资金流向数据(可选)
        """
        factors = {
            'value_score': self._calc_value_score(df, indicators),
            'momentum_score': self._calc_momentum_score(df, indicators),
            'liquidity_score': self._calc_liquidity_score(df, indicators),
            'stability_score': self._calc_stability_score(df, indicators),
            'volume_score': self._calc_volume_score(df, indicators),
            'trend_score': self._calc_trend_score(df, indicators),
            'fund_flow_score': self._calc_fund_flow_score(df, indicators, fund_flow),  # 新增
            'technical_score': self._calc_technical_score(df, indicators),  # 新增
        }

        # 计算综合分数
        factors['composite_score'] = self.calculate_composite_score(factors)

        return factors

    def calculate_composite_score(self, factors: Dict) -> float:
        """计算综合分数"""
        composite = 0
        for factor, weight in self.weights.items():
            composite += factors.get(factor, 0) * weight
        return round(composite, 1)

    def _calc_value_score(self, df: pd.DataFrame, indicators: Dict) -> float:
        """
        估值因子（0-100）
        使用价格位置作为估值代理
        """
        if len(df) < 60:
            return 50

        current = df['close'].iloc[-1]
        high_60 = df['high'].rolling(60).max().iloc[-1]
        low_60 = df['low'].rolling(60).min().iloc[-1]

        if high_60 == low_60:
            return 50

        position = (current - low_60) / (high_60 - low_60)
        # 位置越低，估值分数越高
        score = max(0, min(100, (1 - position) * 100))

        return score

    def _calc_momentum_score(self, df: pd.DataFrame, indicators: Dict) -> float:
        """
        动量因子（0-100）
        计算近期涨幅，适度上涨得分最高
        """
        if len(df) < 20:
            return 50

        # 计算5日和20日涨幅
        returns_5d = (df['close'].iloc[-1] / df['close'].iloc[-6] - 1) * 100
        returns_20d = (df['close'].iloc[-1] / df['close'].iloc[-21] - 1) * 100

        # 5日动量评分
        if 0 < returns_5d < 10:
            score_5d = 80
        elif 10 <= returns_5d < 20:
            score_5d = 60
        elif returns_5d >= 20:
            score_5d = 40  # 涨太多扣分
        elif -5 < returns_5d <= 0:
            score_5d = 50
        else:
            score_5d = 30

        # 20日动量评分
        if 0 < returns_20d < 20:
            score_20d = 80
        elif 20 <= returns_20d < 40:
            score_20d = 60
        elif returns_20d >= 40:
            score_20d = 40
        elif -10 < returns_20d <= 0:
            score_20d = 50
        else:
            score_20d = 30

        return (score_5d + score_20d) / 2

    def _calc_liquidity_score(self, df: pd.DataFrame, indicators: Dict) -> float:
        """
        流动性因子（0-100）
        使用成交量作为流动性指标
        """
        if len(df) < 20:
            return 50

        vol_avg = df['volume'].rolling(20).mean().iloc[-1]
        if vol_avg == 0:
            return 50

        vol_ratio = df['volume'].iloc[-1] / vol_avg

        if 1.0 < vol_ratio < 2.0:
            return 80  # 适度放量
        elif 2.0 <= vol_ratio < 3.0:
            return 60  # 较大放量
        elif vol_ratio >= 3.0:
            return 40  # 过度放量
        elif 0.5 < vol_ratio <= 1.0:
            return 50  # 略微缩量
        else:
            return 30  # 严重缩量

    def _calc_stability_score(self, df: pd.DataFrame, indicators: Dict) -> float:
        """
        稳定性因子（0-100）
        使用波动率作为稳定性指标
        """
        if len(df) < 20:
            return 50

        returns = df['close'].pct_change().dropna()
        volatility = returns.std() * 100

        if volatility < 1.5:
            return 90  # 非常稳定
        elif volatility < 2.5:
            return 75  # 稳定
        elif volatility < 3.5:
            return 60  # 中等
        elif volatility < 5.0:
            return 45  # 较高波动
        else:
            return 30  # 高波动

    def _calc_volume_score(self, df: pd.DataFrame, indicators: Dict) -> float:
        """
        量能因子（0-100）
        温和放量得分最高
        """
        vol_ratio = indicators.get('volume_ratio', 1)

        if 1.0 < vol_ratio < 1.5:
            return 85  # 温和放量
        elif 1.5 <= vol_ratio < 2.5:
            return 70  # 放量
        elif vol_ratio >= 2.5:
            return 50  # 过度放量
        elif 0.7 < vol_ratio <= 1.0:
            return 55  # 平量
        else:
            return 35  # 缩量

    def _calc_trend_score(self, df: pd.DataFrame, indicators: Dict) -> float:
        """
        趋势因子（0-100）
        均线多头排列得分最高
        """
        ma5 = indicators.get('ma5', 0)
        ma10 = indicators.get('ma10', 0)
        ma20 = indicators.get('ma20', 0)
        current = df['close'].iloc[-1] if len(df) > 0 else 0

        score = 0

        # 价格在均线上方
        if current > ma5:
            score += 20
        if current > ma10:
            score += 20
        if current > ma20:
            score += 20

        # 均线多头排列
        if ma5 > ma10 > ma20:
            score += 30
        elif ma5 > ma10:
            score += 15
        elif ma5 > ma20:
            score += 10

        # RSI补充
        rsi = indicators.get('rsi', 50)
        if 40 < rsi < 70:
            score += 10

        return min(100, score)
    
    def _calc_fund_flow_score(self, df: pd.DataFrame, indicators: Dict, fund_flow: Optional[Dict] = None) -> float:
        """
        资金流向因子（0-100）(新增)
        
        根据主力净流入、超大单净流入等判断资金动向
        """
        if fund_flow is None:
            # 如果没有提供资金流向数据,使用量价配合作为代理
            return self._calc_volume_price_correlation(df)
        
        score = 50  # 基础分
        
        # 主力净流入(万元)
        main_net_inflow = fund_flow.get('main_net_inflow', 0)
        if main_net_inflow > 1000:  # 1000万以上
            score += 30
        elif main_net_inflow > 500:  # 500万以上
            score += 20
        elif main_net_inflow > 100:  # 100万以上
            score += 10
        elif main_net_inflow < -1000:
            score -= 30
        elif main_net_inflow < -500:
            score -= 20
        
        # 超大单净流入
        super_large = fund_flow.get('super_large_net_inflow', 0)
        if super_large > 500:
            score += 20
        elif super_large > 200:
            score += 10
        elif super_large < -500:
            score -= 20
        
        return max(0, min(100, score))
    
    def _calc_volume_price_correlation(self, df: pd.DataFrame) -> float:
        """
        量价配合度评分
        
        上涨放量、下跌缩量为正相关
        """
        if len(df) < 20:
            return 50
        
        # 计算最近20天的量价相关性
        recent = df.tail(20)
        price_changes = recent['close'].pct_change().dropna()
        volume_changes = recent['volume'].pct_change().dropna()
        
        if len(price_changes) < 2 or len(volume_changes) < 2:
            return 50
        
        # 对齐索引
        common_idx = price_changes.index.intersection(volume_changes.index)
        if len(common_idx) < 2:
            return 50
        
        correlation = price_changes[common_idx].corr(volume_changes[common_idx])
        
        if pd.isna(correlation):
            return 50
        
        # 正相关得分高
        score = 50 + correlation * 50
        return max(0, min(100, score))
    
    def _calc_technical_score(self, df: pd.DataFrame, indicators: Dict) -> float:
        """
        技术指标因子（0-100）(新增)
        
        综合MACD、RSI、布林带等技术指标
        """
        score = 50  # 基础分
        
        # MACD信号
        macd = indicators.get('macd', 0)
        macd_signal = indicators.get('macd_signal', 0)
        macd_hist = indicators.get('macd_hist', 0)
        
        if macd > macd_signal:  # MACD金叉
            score += 15
            if macd_hist > 0:  # 红柱放大
                score += 10
        elif macd < macd_signal:  # MACD死叉
            score -= 15
            if macd_hist < 0:  # 绿柱放大
                score -= 10
        
        # MACD背离检测
        if len(df) >= 30:
            # 价格创新低但MACD未创新低 = 底背离(看涨)
            price_low_20 = df['close'].iloc[-20:].min()
            macd_low_20 = indicators.get('macd_min_20d', macd)
            
            if price_low_20 == df['close'].iloc[-1] and macd > macd_low_20:
                score += 20  # 底背离
        
        # RSI超买超卖
        rsi = indicators.get('rsi', 50)
        if rsi < 30:  # 超卖区
            score += 15
        elif rsi < 40:
            score += 10
        elif rsi > 70:  # 超买区
            score -= 15
        elif rsi > 60:
            score -= 10
        
        # 布林带位置
        bb_upper = indicators.get('bb_upper', 0)
        bb_lower = indicators.get('bb_lower', 0)
        bb_middle = indicators.get('bb_middle', 0)
        current = df['close'].iloc[-1] if len(df) > 0 else 0
        
        if bb_lower > 0 and bb_upper > bb_lower:
            # 价格在布林带下轨附近(可能反弹)
            if current <= bb_lower * 1.02:
                score += 15
            # 价格在布林带上轨附近(可能回调)
            elif current >= bb_upper * 0.98:
                score -= 10
            # 价格在中轨附近(中性)
            elif abs(current - bb_middle) / bb_middle < 0.01:
                score += 5
        
        return max(0, min(100, score))

    def get_factor_explanation(self, factors: Dict) -> Dict:
        """获取因子解释"""
        explanations = {}

        for factor, score in factors.items():
            if factor == 'composite_score':
                continue

            if score >= 80:
                level = '优秀'
            elif score >= 60:
                level = '良好'
            elif score >= 40:
                level = '一般'
            else:
                level = '较差'

            explanations[factor] = {
                'score': score,
                'level': level,
            }

        return explanations
