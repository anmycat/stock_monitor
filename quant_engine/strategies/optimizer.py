"""
策略优化器 - 根据市场状态动态调整策略权重

功能:
- 根据市场类型(牛市/震荡/熊市)动态分配策略权重
- 基于历史表现调整策略优先级
- 自动选择最优策略组合
"""
from typing import Dict, List, Tuple
from dataclasses import dataclass
from ..models.market import MarketState, MarketType


@dataclass
class StrategyWeight:
    """策略权重"""
    strategy_name: str
    weight: float  # 0-1
    enabled: bool = True


class StrategyOptimizer:
    """策略权重优化器"""
    
    # 默认权重配置
    DEFAULT_WEIGHTS = {
        'trend': 0.5,
        'bottom_fishing': 0.3,
        'momentum': 0.1,
        'mean_reversion': 0.1,
    }
    
    # 不同市场状态下的最优权重配置
    MARKET_WEIGHTS = {
        MarketType.BULL: {
            'trend': 0.7,
            'bottom_fishing': 0.2,
            'momentum': 0.05,
            'mean_reversion': 0.05,
        },
        MarketType.BEAR: {
            'trend': 0.1,
            'bottom_fishing': 0.1,
            'momentum': 0.0,
            'mean_reversion': 0.0,
            'defensive': 0.8,  # 防守模式
        },
        MarketType.SIDEWAYS: {
            'trend': 0.3,
            'bottom_fishing': 0.4,
            'momentum': 0.15,
            'mean_reversion': 0.15,
        },
    }
    
    def __init__(self):
        self.current_weights = self.DEFAULT_WEIGHTS.copy()
        self.performance_history: Dict[str, List[float]] = {}  # strategy_name -> [returns]
        self.min_weight = 0.05  # 最小权重5%
        self.max_weight = 0.8   # 最大权重80%
    
    def get_weights_for_market(self, market_state: MarketState) -> Dict[str, float]:
        """
        根据市场状态获取策略权重
        
        Args:
            market_state: 市场状态
            
        Returns:
            策略名称 -> 权重映射
        """
        # 获取基础权重配置
        base_weights = self.MARKET_WEIGHTS.get(market_state.type, self.DEFAULT_WEIGHTS)
        
        # 根据市场评分微调权重
        adjustment = self._calculate_adjustment(market_state)
        
        # 应用调整
        adjusted_weights = {}
        for strategy, base_weight in base_weights.items():
            adj = adjustment.get(strategy, 0.0)
            new_weight = base_weight + adj
            # 限制在合理范围内
            new_weight = max(self.min_weight, min(self.max_weight, new_weight))
            adjusted_weights[strategy] = new_weight
        
        # 归一化(确保总和为1)
        total = sum(adjusted_weights.values())
        if total > 0:
            adjusted_weights = {k: v / total for k, v in adjusted_weights.items()}
        
        self.current_weights = adjusted_weights
        return adjusted_weights
    
    def update_from_performance(self, strategy_returns: Dict[str, float]):
        """
        根据策略历史表现更新权重
        
        Args:
            strategy_returns: 策略名称 -> 收益率映射
        """
        for strategy, ret in strategy_returns.items():
            if strategy not in self.performance_history:
                self.performance_history[strategy] = []
            self.performance_history[strategy].append(ret)
            
            # 保留最近30条记录
            if len(self.performance_history[strategy]) > 30:
                self.performance_history[strategy] = self.performance_history[strategy][-30:]
    
    def _calculate_adjustment(self, market_state: MarketState) -> Dict[str, float]:
        """
        根据市场状态计算权重调整
        
        Returns:
            策略名称 -> 调整值映射
        """
        adjustment = {}
        score = market_state.score  # -100 to 100
        
        # 根据市场评分调整
        if market_state.type == MarketType.BULL:
            # 牛市: 强化趋势策略
            if score > 50:
                adjustment['trend'] = 0.1
                adjustment['bottom_fishing'] = -0.05
            else:
                adjustment['trend'] = 0.05
        elif market_state.type == MarketType.BEAR:
            # 熊市: 降低所有策略权重(防守)
            adjustment['trend'] = -0.1
            adjustment['bottom_fishing'] = -0.1
        elif market_state.type == MarketType.SIDEWAYS:
            # 震荡市: 提升抄底和均值回归
            if abs(score) < 20:
                adjustment['bottom_fishing'] = 0.1
                adjustment['mean_reversion'] = 0.05
        
        return adjustment
    
    def get_top_strategies(self, market_state: MarketState, top_n: int = 3) -> List[Tuple[str, float]]:
        """
        获取最优策略
        
        Returns:
            [(策略名称, 权重), ...] 按权重降序
        """
        weights = self.get_weights_for_market(market_state)
        sorted_strategies = sorted(weights.items(), key=lambda x: x[1], reverse=True)
        return sorted_strategies[:top_n]
    
    def should_enable_strategy(self, strategy_name: str, market_state: MarketState) -> bool:
        """判断是否应该启用某个策略"""
        weights = self.get_weights_for_market(market_state)
        weight = weights.get(strategy_name, 0.0)
        return weight >= self.min_weight
    
    def get_strategy_config(self, market_state: MarketState) -> Dict:
        """
        获取策略配置建议
        
        Returns:
            完整的策略配置信息
        """
        weights = self.get_weights_for_market(market_state)
        
        return {
            'market_type': market_state.type.value,
            'market_score': market_state.score,
            'strategy_weights': weights,
            'enabled_strategies': [name for name, w in weights.items() if w >= self.min_weight],
            'disabled_strategies': [name for name, w in weights.items() if w < self.min_weight],
            'recommendation': self._generate_recommendation(market_state, weights)
        }
    
    def _generate_recommendation(self, market_state: MarketState, weights: Dict) -> str:
        """生成策略建议"""
        if market_state.type == MarketType.BULL:
            return "牛市行情,建议加大趋势策略权重,减少抄底操作"
        elif market_state.type == MarketType.BEAR:
            return "熊市行情,建议进入防守模式,严格控制仓位"
        elif market_state.type == MarketType.SIDEWAYS:
            return "震荡行情,建议平衡趋势和抄底策略,关注板块轮动"
        else:
            return "市场状态不明,建议使用默认策略配置"


# 全局策略优化器
_strategy_optimizer = None

def get_strategy_optimizer() -> StrategyOptimizer:
    """获取全局策略优化器"""
    global _strategy_optimizer
    if _strategy_optimizer is None:
        _strategy_optimizer = StrategyOptimizer()
    return _strategy_optimizer
