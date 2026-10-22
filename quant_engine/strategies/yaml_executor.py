"""YAML策略执行器 - 执行YAML定义的策略逻辑"""

import pandas as pd
from loguru import logger


class YamlStrategyExecutor:
    """YAML策略执行器"""

    def __init__(self, strategy_config: dict):
        self.config = strategy_config
        self.entry_config = strategy_config.get('entry', {})
        self.filters_config = strategy_config.get('filters', [])
        self.exit_config = strategy_config.get('exit', {})
        self.scoring_config = strategy_config.get('scoring', {})

    def execute(self, df: pd.DataFrame, indicators: dict) -> tuple[bool, float, list[str]]:
        """
        执行YAML策略
        
        Returns:
            (是否通过, 分数, 原因列表)
        """
        # 1. 检查过滤器
        filter_pass, filter_reasons = self._check_filters(df, indicators)
        if not filter_pass:
            return False, 0, filter_reasons

        # 2. 检查入场条件
        entry_pass, entry_score, entry_reasons = self._check_entry(df, indicators)
        if not entry_pass:
            return False, 0, entry_reasons

        # 3. 计算综合分数
        score = self._calculate_score(df, indicators, entry_score)

        # 4. 检查最低分数要求
        min_score = self.scoring_config.get('min_score', 60)
        if score < min_score:
            return False, score, [f"分数不足: {score:.1f} < {min_score}"]

        return True, score, entry_reasons

    def _check_filters(self, df: pd.DataFrame, indicators: dict) -> tuple[bool, list[str]]:
        """检查过滤器"""
        reasons = []

        for filter_config in self.filters_config:
            filter_type = filter_config.get('type')
            passed = True

            if filter_type == 'high_position':
                passed = not self._is_high_position(df, filter_config.get('threshold', 0.85))
                if not passed:
                    reasons.append("高位过滤")

            elif filter_type == 'consecutive_up':
                passed = not self._is_consecutive_up(df, filter_config.get('limit', 5))
                if not passed:
                    reasons.append("连涨过滤")

            elif filter_type == 'weak_market':
                passed = not self._is_weak_market(df)
                if not passed:
                    reasons.append("弱势市场过滤")

            elif filter_type == 'bear_market':
                passed = not self._is_bear_market(indicators)
                if not passed:
                    reasons.append("熊市过滤")

            elif filter_type == 'high_volatility':
                passed = not self._is_high_volatility(df, filter_config.get('threshold', 0.05))
                if not passed:
                    reasons.append("高波动过滤")

            elif filter_type == 'low_liquidity':
                passed = not self._is_low_liquidity(df, filter_config.get('min_volume', 100000))
                if not passed:
                    reasons.append("低流动性过滤")

            if not passed:
                return False, reasons

        return True, []

    def _check_entry(self, df: pd.DataFrame, indicators: dict) -> tuple[bool, float, list[str]]:
        """检查入场条件"""
        conditions = self.entry_config.get('conditions', [])
        logic = self.entry_config.get('logic', 'and')

        if not conditions:
            return True, 100, ["无条件限制"]

        passed_conditions = []
        failed_conditions = []
        total_weight = 0
        passed_weight = 0

        for condition in conditions:
            indicator = condition.get('indicator')
            op = condition.get('op')
            value = condition.get('value')
            weight = condition.get('weight', 1.0)

            total_weight += weight
            result = self._evaluate_condition(indicator, op, value, df, indicators)

            if result:
                passed_conditions.append(condition)
                passed_weight += weight
            else:
                failed_conditions.append(condition)

        # 计算通过比例
        if total_weight > 0:
            score = (passed_weight / total_weight) * 100
        else:
            score = 0

        # 根据逻辑判断是否通过
        if logic == 'and':
            passed = len(failed_conditions) == 0
        else:  # or
            passed = len(passed_conditions) > 0

        reasons = [c.get('description', '') for c in passed_conditions]

        return passed, score, reasons

    def _evaluate_condition(self, indicator: str, op: str, value, df: pd.DataFrame, indicators: dict) -> bool:
        """评估单个条件"""
        # 获取指标值
        indicator_value = self._get_indicator_value(indicator, df, indicators)

        if indicator_value is None:
            return False

        # 执行比较
        try:
            if op == '>':
                return indicator_value > value
            elif op == '<':
                return indicator_value < value
            elif op == '>=':
                return indicator_value >= value
            elif op == '<=':
                return indicator_value <= value
            elif op == '==':
                return indicator_value == value
            elif op == '!=':
                return indicator_value != value
            else:
                logger.warning(f"Unknown operator: {op}")
                return False
        except (TypeError, ValueError) as e:
            logger.error(f"Condition evaluation error: {e}")
            return False

    def _get_indicator_value(self, indicator: str, df: pd.DataFrame, indicators: dict):
        """获取指标值"""
        # 从indicators字典获取
        if indicator in indicators:
            return indicators[indicator]

        # 计算特定指标
        if indicator == 'ma5_above_ma20':
            ma5 = indicators.get('ma5', 0)
            ma20 = indicators.get('ma20', 0)
            return ma5 > ma20

        elif indicator == 'ma5_above_ma10':
            ma5 = indicators.get('ma5', 0)
            ma10 = indicators.get('ma10', 0)
            return ma5 > ma10

        elif indicator == 'position_20d':
            if len(df) < 20:
                return None
            current = df['close'].iloc[-1]
            high_20 = df['high'].rolling(20).max().iloc[-1]
            low_20 = df['low'].rolling(20).min().iloc[-1]
            if high_20 == low_20:
                return 0.5
            return (current - low_20) / (high_20 - low_20)

        elif indicator == 'stop_stabilize':
            if len(df) < 5:
                return False
            last_5 = df.tail(5)
            return last_5['close'].iloc[-1] > last_5['close'].iloc[-2]

        elif indicator == 'trend_score':
            return indicators.get('trend_score', 50)

        elif indicator == 'momentum_score':
            return indicators.get('momentum_score', 50)

        elif indicator == 'volume_score':
            return indicators.get('volume_score', 50)

        elif indicator == 'stability_score':
            return indicators.get('stability_score', 50)

        # 默认从indicators获取
        return indicators.get(indicator)

    def _calculate_score(self, df: pd.DataFrame, indicators: dict, entry_score: float) -> float:
        """计算综合分数"""
        weights = self.scoring_config.get('weights', {})

        if not weights:
            return entry_score

        score = 0
        total_weight = 0

        for factor, weight in weights.items():
            factor_score = indicators.get(f'{factor}_score', 50)
            score += factor_score * weight
            total_weight += weight

        if total_weight > 0:
            score = score / total_weight

        # 结合入场分数
        score = (score * 0.6 + entry_score * 0.4)

        return min(100, max(0, score))

    def _is_high_position(self, df: pd.DataFrame, threshold: float) -> bool:
        """检查是否处于高位"""
        if len(df) < 120:
            return False
        current = df['close'].iloc[-1]
        high_120 = df['high'].rolling(120).max().iloc[-1]
        low_120 = df['low'].rolling(120).min().iloc[-1]
        if high_120 == low_120:
            return False
        position = (current - low_120) / (high_120 - low_120)
        return position > threshold

    def _is_consecutive_up(self, df: pd.DataFrame, limit: int) -> bool:
        """检查是否连涨"""
        if len(df) < limit:
            return False
        recent = df.tail(limit)
        return all(recent['close'].diff().iloc[1:] > 0)

    def _is_weak_market(self, df: pd.DataFrame) -> bool:
        """检查是否弱势市场"""
        if len(df) < 20:
            return False
        ma5 = df['close'].rolling(5).mean().iloc[-1]
        ma10 = df['close'].rolling(10).mean().iloc[-1]
        ma20 = df['close'].rolling(20).mean().iloc[-1]
        return ma5 < ma10 < ma20

    def _is_bear_market(self, indicators: dict) -> bool:
        """检查是否熊市"""
        # 简化判断：RSI低于30
        rsi = indicators.get('rsi', 50)
        return rsi < 30

    def _is_high_volatility(self, df: pd.DataFrame, threshold: float) -> bool:
        """检查是否高波动"""
        if len(df) < 20:
            return False
        returns = df['close'].pct_change().dropna()
        volatility = returns.std()
        return volatility > threshold

    def _is_low_liquidity(self, df: pd.DataFrame, min_volume: float) -> bool:
        """检查是否低流动性"""
        if len(df) < 5:
            return False
        avg_volume = df['volume'].tail(5).mean()
        return avg_volume < min_volume

    def get_exit_conditions(self) -> dict:
        """获取出场条件"""
        return self.exit_config
