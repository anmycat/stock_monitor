"""动态仓位管理器 - 根据信号强度和市场风险动态调整仓位

功能:
- 根据信号评分计算建议仓位
- 考虑市场波动率调整仓位
- 应用风险限额控制
- Kelly公式优化仓位(支持 Full / Half / Quarter / 自定义分数)
- 动态Kelly参数: 从SignalTracker读取策略真实胜率/盈亏比
- 组合级Kelly调整: 根据持仓数量和相关性控制总风险
- Kelly信心指数: 0-100, 量化策略在该标的上的历史优势
"""
from typing import Dict, Optional, List
from dataclasses import dataclass, field
from loguru import logger
from ..models.market import MarketState, MarketType


@dataclass
class KellyConfig:
    """Kelly公式配置"""
    kelly_fraction: float = 0.5       # Kelly分数: 0.25=Quarter, 0.5=Half, 1.0=Full
    max_kelly_position: float = 0.40  # 单只Kelly仓位上限
    min_trades_required: int = 10     # 使用动态参数所需的最少交易数
    use_dynamic_params: bool = True   # 是否从SignalTracker读取真实参数
    default_win_rate: float = 0.55    # 无历史数据时的默认胜率
    default_profit_loss_ratio: float = 1.5  # 无历史数据时的默认盈亏比
    portfolio_kelly_enabled: bool = True    # 是否启用组合级Kelly
    max_portfolio_kelly: float = 0.80       # 组合Kelly总仓位上限


@dataclass
class PositionSizingResult:
    """仓位计算结果"""
    recommended_position: float   # 建议仓位比例(0-1)
    max_position: float           # 最大仓位限制
    kelly_position: float         # Kelly公式计算的仓位(Fractional)
    kelly_full_position: float    # Full Kelly仓位(参考)
    kelly_confidence: float       # Kelly信心指数(0-100)
    kelly_source: str             # Kelly参数来源: 'dynamic' / 'default'
    risk_adjusted_position: float # 风险调整后的仓位
    reasoning: str                # 仓位调整原因


class PositionSizer:
    """动态仓位管理器(含Kelly公式深度融合)"""
    
    def __init__(self, config: KellyConfig = None):
        self.config = config or KellyConfig()
        
        # 仓位限制
        self.min_position = 0.05   # 最小仓位5%
        self.max_position = 0.40   # 单只股票最大仓位40%
        self.max_total_position = self.config.max_portfolio_kelly
        
        # 风险参数
        self.risk_free_rate = 0.03  # 无风险利率3%
        self.target_sharpe = 1.5    # 目标夏普比率
        self.max_drawdown_limit = 0.08  # 最大回撤限制8%
    
    def get_strategy_kelly_params(self, strategy_name: str) -> Dict:
        """
        从SignalTracker获取策略的真实Kelly参数
        
        Args:
            strategy_name: 策略名称
            
        Returns:
            {'win_rate': float, 'profit_loss_ratio': float, 'source': str, 'trade_count': int}
        """
        if not self.config.use_dynamic_params:
            return {
                'win_rate': self.config.default_win_rate,
                'profit_loss_ratio': self.config.default_profit_loss_ratio,
                'source': 'default',
                'trade_count': 0
            }
        
        try:
            from .signal_tracker import get_signal_tracker
            tracker = get_signal_tracker()
            perf = tracker.get_strategy_performance()
            
            if strategy_name in perf:
                stats = perf[strategy_name]
                count = stats.get('count', 0)
                if count >= self.config.min_trades_required:
                    # 从真实数据计算
                    win_rate = stats.get('win_rate', self.config.default_win_rate)
                    avg_return = stats.get('avg_return', 0)
                    
                    # 估算盈亏比: 用avg_return和win_rate反推
                    # avg_return = win_rate * avg_win + (1-win_rate) * avg_loss
                    # profit_loss_ratio = avg_win / abs(avg_loss)
                    if win_rate > 0 and win_rate < 1:
                        # 基于平均收益和胜率估算
                        avg_win = avg_return / win_rate if win_rate > 0 else 0.05
                        avg_loss = (avg_return - win_rate * avg_win) / (1 - win_rate) if (1 - win_rate) > 0 else -0.03
                        if avg_loss != 0:
                            pl_ratio = abs(avg_win / avg_loss)
                        else:
                            pl_ratio = self.config.default_profit_loss_ratio
                    else:
                        pl_ratio = self.config.default_profit_loss_ratio
                    
                    # 合理性约束
                    win_rate = max(0.2, min(0.9, win_rate))
                    pl_ratio = max(0.5, min(5.0, pl_ratio))
                    
                    return {
                        'win_rate': round(win_rate, 4),
                        'profit_loss_ratio': round(pl_ratio, 2),
                        'source': 'dynamic',
                        'trade_count': count
                    }
        except Exception as e:
            logger.debug(f"获取策略 {strategy_name} Kelly参数失败: {e}")
        
        return {
            'win_rate': self.config.default_win_rate,
            'profit_loss_ratio': self.config.default_profit_loss_ratio,
            'source': 'default',
            'trade_count': 0
        }
    
    def calculate_kelly_position(
        self,
        win_rate: float,
        profit_loss_ratio: float,
        num_positions: int = 1
    ) -> Dict:
        """
        计算Kelly仓位(支持Fractional Kelly + 组合调整)
        
        Kelly公式: f* = (p * b - q) / b
            p = 胜率
            q = 败率 = 1 - p
            b = 盈亏比
        
        Args:
            win_rate: 胜率
            profit_loss_ratio: 盈亏比
            num_positions: 当前持仓数量(用于组合Kelly调整)
            
        Returns:
            {
                'full_kelly': Full Kelly仓位,
                'fractional_kelly': 分数Kelly仓位,
                'portfolio_kelly': 组合调整后Kelly,
                'kelly_confidence': 信心指数(0-100),
                'has_edge': 是否有正期望
            }
        """
        p = win_rate
        q = 1 - p
        b = profit_loss_ratio
        
        # Full Kelly公式
        if b > 0:
            full_kelly = (p * b - q) / b
        else:
            full_kelly = 0
        
        # 判断是否有正期望(edge)
        has_edge = full_kelly > 0
        
        # Fractional Kelly
        fractional_kelly = full_kelly * self.config.kelly_fraction
        
        # 组合级Kelly调整
        # 当持有N个标的时, 每个标的Kelly仓位按比例缩减, 避免总风险叠加
        portfolio_kelly = fractional_kelly
        if self.config.portfolio_kelly_enabled and num_positions > 1:
            # 使用平方根法则: 分散化允许稍微提高单标的仓位
            # 比简单除以N更合理: portfolio_factor = 1/sqrt(N)
            import math
            portfolio_factor = 1.0 / math.sqrt(num_positions)
            portfolio_kelly = fractional_kelly * portfolio_factor
        
        # 限制在合理范围内
        full_kelly = max(0, min(self.max_position, full_kelly))
        fractional_kelly = max(0, min(self.config.max_kelly_position, fractional_kelly))
        portfolio_kelly = max(0, min(self.config.max_kelly_position, portfolio_kelly))
        
        # Kelly信心指数(0-100)
        # 基于Kelly%的绝对值 + 胜率 + 盈亏比综合评估
        kelly_confidence = self._calculate_kelly_confidence(full_kelly, p, b)
        
        return {
            'full_kelly': round(full_kelly, 4),
            'fractional_kelly': round(fractional_kelly, 4),
            'portfolio_kelly': round(portfolio_kelly, 4),
            'kelly_confidence': round(kelly_confidence, 1),
            'has_edge': has_edge
        }
    
    def _calculate_kelly_confidence(self, full_kelly: float, win_rate: float, pl_ratio: float) -> float:
        """
        计算Kelly信心指数(0-100)
        
        综合Kelly%绝对值、胜率、盈亏比三个维度:
        - Kelly% > 0 且较大 → 历史优势明显
        - 胜率 > 0.5 → 方向正确率高
        - 盈亏比 > 1.5 → 赚得比亏得多
        """
        score = 0.0
        
        # Kelly%贡献(0-40分)
        if full_kelly > 0:
            score += min(40, full_kelly * 200)  # 20% Kelly → 40分
        else:
            return 0  # 负Kelly = 无优势, 信心为0
        
        # 胜率贡献(0-30分)
        if win_rate >= 0.6:
            score += 30
        elif win_rate >= 0.5:
            score += 20
        elif win_rate >= 0.4:
            score += 10
        
        # 盈亏比贡献(0-30分)
        if pl_ratio >= 2.0:
            score += 30
        elif pl_ratio >= 1.5:
            score += 20
        elif pl_ratio >= 1.0:
            score += 10
        
        return min(100, max(0, score))
    
    def calculate_position(
        self,
        signal_score: float,
        market_state: MarketState,
        volatility: float = 0.25,
        account_value: float = 0,
        current_positions: float = 0,
        win_rate: float = 0,
        profit_loss_ratio: float = 0,
        strategy_name: str = '',
        num_holdings: int = 1
    ) -> PositionSizingResult:
        """
        计算建议仓位(Kelly深度融合版)
        
        Args:
            signal_score: 信号评分(0-100)
            market_state: 市场状态
            volatility: 波动率(年化)
            account_value: 账户总值
            current_positions: 当前仓位比例
            win_rate: 胜率(传0则自动从SignalTracker获取)
            profit_loss_ratio: 盈亏比(传0则自动从SignalTracker获取)
            strategy_name: 策略名称(用于动态Kelly参数)
            num_holdings: 当前持仓数量
            
        Returns:
            仓位计算结果
        """
        # 0. 获取Kelly参数(动态或静态)
        kelly_source = 'manual'
        if win_rate <= 0 or profit_loss_ratio <= 0:
            if strategy_name:
                params = self.get_strategy_kelly_params(strategy_name)
                win_rate = params['win_rate']
                profit_loss_ratio = params['profit_loss_ratio']
                kelly_source = params['source']
            else:
                win_rate = self.config.default_win_rate
                profit_loss_ratio = self.config.default_profit_loss_ratio
                kelly_source = 'default'
        
        # 1. 基础仓位(基于信号强度)
        base_position = self._calculate_base_position(signal_score)
        
        # 2. 市场风险调整
        market_adjustment = self._adjust_for_market_risk(market_state)
        
        # 3. 波动率调整
        volatility_adjustment = self._adjust_for_volatility(volatility)
        
        # 4. Kelly公式(含组合调整)
        kelly_result = self.calculate_kelly_position(
            win_rate, profit_loss_ratio, num_holdings
        )
        kelly_position = kelly_result['portfolio_kelly']
        kelly_full = kelly_result['full_kelly']
        kelly_confidence = kelly_result['kelly_confidence']
        
        # 5. 综合计算
        recommended = base_position * market_adjustment * volatility_adjustment
        
        # 6. 应用Kelly公式限制
        if kelly_result['has_edge']:
            recommended = min(recommended, kelly_position)
        else:
            # 无正期望 → 降至最小仓位
            recommended = self.min_position
        
        # 7. 总仓位限制
        if current_positions + recommended > self.max_total_position:
            recommended = max(0, self.max_total_position - current_positions)
        
        # 8. 最终限制
        recommended = max(self.min_position, min(self.max_position, recommended))
        
        # 生成调整原因
        reasoning = self._generate_reasoning(
            signal_score, market_state, volatility, 
            base_position, recommended, kelly_position,
            kelly_source, kelly_confidence
        )
        
        return PositionSizingResult(
            recommended_position=round(recommended, 4),
            max_position=self.max_position,
            kelly_position=round(kelly_position, 4),
            kelly_full_position=round(kelly_full, 4),
            kelly_confidence=kelly_confidence,
            kelly_source=kelly_source,
            risk_adjusted_position=round(recommended * volatility_adjustment, 4),
            reasoning=reasoning
        )
    
    def _calculate_base_position(self, signal_score: float) -> float:
        """
        基于信号强度计算基础仓位
        
        信号强度与仓位的关系:
        - 90-100: 40% (最强)
        - 80-90: 35%
        - 70-80: 30%
        - 60-70: 25%
        - 50-60: 20%
        - <50: 10% (弱信号)
        """
        if signal_score >= 90:
            return 0.40
        elif signal_score >= 80:
            return 0.35
        elif signal_score >= 70:
            return 0.30
        elif signal_score >= 60:
            return 0.25
        elif signal_score >= 50:
            return 0.20
        else:
            return 0.10
    
    def _adjust_for_market_risk(self, market_state: MarketState) -> float:
        """
        根据市场风险调整仓位
        
        Returns:
            调整系数(0.2-1.2)
        """
        score = market_state.score
        
        if market_state.type == MarketType.BULL:
            if score > 50:
                return 1.2
            else:
                return 1.0
        elif market_state.type == MarketType.BEAR:
            return 0.2
        elif market_state.type == MarketType.SIDEWAYS:
            if abs(score) < 20:
                return 0.8
            else:
                return 0.6
        else:
            return 0.5
    
    def _adjust_for_volatility(self, volatility: float) -> float:
        """
        根据波动率调整仓位
        波动率越高,仓位越低
        """
        target_vol = 0.20
        if volatility <= target_vol:
            return 1.0
        elif volatility <= 0.30:
            return 0.7
        elif volatility <= 0.50:
            return 0.5
        else:
            return 0.3
    
    def _generate_reasoning(
        self,
        signal_score: float,
        market_state: MarketState,
        volatility: float,
        base_position: float,
        final_position: float,
        kelly_position: float,
        kelly_source: str = '',
        kelly_confidence: float = 0
    ) -> str:
        """生成仓位调整原因"""
        reasons = []
        
        # 信号强度
        if signal_score >= 80:
            reasons.append(f"强信号(评分{signal_score:.0f})")
        elif signal_score >= 60:
            reasons.append(f"中等信号(评分{signal_score:.0f})")
        else:
            reasons.append(f"弱信号(评分{signal_score:.0f})")
        
        # 市场状态
        if market_state.type == MarketType.BULL:
            reasons.append("牛市环境")
        elif market_state.type == MarketType.BEAR:
            reasons.append("熊市防守")
        elif market_state.type == MarketType.SIDEWAYS:
            reasons.append("震荡市")
        
        # 波动率
        if volatility > 0.40:
            reasons.append("高波动降仓")
        elif volatility < 0.20:
            reasons.append("低波动可加仓")
        
        # Kelly信息
        if kelly_source == 'dynamic':
            reasons.append(f"Kelly动态(信心{kelly_confidence:.0f})")
        elif final_position >= kelly_position * 0.9:
            reasons.append("Kelly公式限制")
        
        return " | ".join(reasons)
    
    def get_position_limits(self, current_positions: float) -> Dict:
        """
        获取仓位限制信息
        
        Args:
            current_positions: 当前总仓位
            
        Returns:
            仓位限制字典
        """
        available = self.max_total_position - current_positions
        
        return {
            'min_position': self.min_position,
            'max_single_position': self.max_position,
            'max_total_position': self.max_total_position,
            'current_position': current_positions,
            'available_for_new': max(0, available),
            'risk_level': 'high' if current_positions > 0.6 else 'medium' if current_positions > 0.3 else 'low',
            'kelly_config': {
                'fraction': self.config.kelly_fraction,
                'max_kelly': self.config.max_kelly_position,
                'portfolio_enabled': self.config.portfolio_kelly_enabled
            }
        }


# 全局仓位管理器
_position_sizer = None

def get_position_sizer(config: KellyConfig = None) -> PositionSizer:
    """获取全局仓位管理器"""
    global _position_sizer
    if _position_sizer is None:
        _position_sizer = PositionSizer(config)
    return _position_sizer

def reset_position_sizer(config: KellyConfig = None):
    """重置仓位管理器(用于配置变更)"""
    global _position_sizer
    _position_sizer = PositionSizer(config)
