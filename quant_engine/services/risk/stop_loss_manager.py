"""
动态止盈止损管理器 - 根据市场波动自动调整止盈止损位

功能:
- 追踪止损(Trailing Stop)
- 移动止盈(Take Profit Trail)
- 波动率自适应(ATR倍数)
- 动态调整止盈止损位
"""
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class StopLossLevel:
    """止损止盈级别"""
    stop_loss: float      # 止损价
    take_profit: float    # 止盈价
    trailing_stop: float  # 追踪止损价
    stop_type: str        # fixed/trailing/volatility


@dataclass
class PositionStatus:
    """持仓状态"""
    code: str
    entry_price: float
    current_price: float
    highest_price: float  # 持仓期间最高价
    lowest_price: float   # 持仓期间最低价
    stop_loss_level: StopLossLevel
    profit_pct: float
    is_stopped: bool = False
    stop_reason: str = ""


class StopLossManager:
    """动态止盈止损管理器"""
    
    def __init__(self):
        # 默认止损止盈参数
        self.default_stop_loss_pct = 0.08    # 默认止损8%
        self.default_take_profit_pct = 0.20  # 默认止盈20%
        
        # 追踪止损参数
        self.trailing_stop_pct = 0.05        # 追踪止损5%
        self.trailing_activation_pct = 0.10  # 追踪止损激活阈值10%
        
        # ATR参数
        self.atr_stop_multiplier = 2.0       # ATR止损倍数
        self.atr_take_profit_multiplier = 3.0  # ATR止盈倍数
        
        # 移动止盈参数
        self.take_profit_trail_pct = 0.03    # 止盈回撤3%触发
        self.take_profit_trail_activation = 0.15  # 止盈追踪激活阈值15%
    
    def calculate_stop_levels(
        self,
        entry_price: float,
        current_price: float,
        highest_price: float,
        atr: Optional[float] = None,
        volatility: Optional[float] = None
    ) -> StopLossLevel:
        """
        计算止损止盈位
        
        Args:
            entry_price: 入场价
            current_price: 当前价
            highest_price: 持仓期间最高价
            atr: 平均真实波幅(ATR)
            volatility: 波动率
            
        Returns:
            止损止盈级别
        """
        profit_pct = (current_price - entry_price) / entry_price
        
        # 1. 固定止损
        fixed_stop = entry_price * (1 - self.default_stop_loss_pct)
        
        # 2. 追踪止损
        trailing_stop = self._calculate_trailing_stop(entry_price, current_price, highest_price)
        
        # 3. ATR止损(如果提供ATR)
        atr_stop = None
        if atr:
            atr_stop = current_price - atr * self.atr_stop_multiplier
        
        # 4. 波动率自适应止损
        vol_stop = None
        if volatility:
            vol_stop = current_price * (1 - volatility * 2)
        
        # 选择最严格的止损
        stops = [fixed_stop]
        if trailing_stop:
            stops.append(trailing_stop)
        if atr_stop:
            stops.append(atr_stop)
        if vol_stop:
            stops.append(vol_stop)
        
        # 止损取最大值(最严格)
        final_stop = max(stops)
        
        # 止盈计算
        take_profit = self._calculate_take_profit(
            entry_price, current_price, highest_price, atr, profit_pct
        )
        
        # 确定止损类型
        if trailing_stop and final_stop == trailing_stop:
            stop_type = "trailing"
        elif atr_stop and final_stop == atr_stop:
            stop_type = "volatility"
        else:
            stop_type = "fixed"
        
        return StopLossLevel(
            stop_loss=round(final_stop, 2),
            take_profit=round(take_profit, 2),
            trailing_stop=round(trailing_stop, 2) if trailing_stop else 0,
            stop_type=stop_type
        )
    
    def _calculate_trailing_stop(
        self,
        entry_price: float,
        current_price: float,
        highest_price: float
    ) -> Optional[float]:
        """
        计算追踪止损位
        
        规则:
        - 盈利达到激活阈值后,追踪止损激活
        - 止损位 = 最高价 * (1 - 追踪止损比例)
        """
        profit_pct = (current_price - entry_price) / entry_price
        
        # 未达到激活阈值
        if profit_pct < self.trailing_activation_pct:
            return None
        
        # 计算追踪止损
        trailing = highest_price * (1 - self.trailing_stop_pct)
        
        # 追踪止损不应低于成本价
        trailing = max(trailing, entry_price)
        
        return trailing
    
    def _calculate_take_profit(
        self,
        entry_price: float,
        current_price: float,
        highest_price: float,
        atr: Optional[float] = None,
        profit_pct: float = 0.0
    ) -> float:
        """
        计算止盈位
        
        使用移动止盈策略:
        - 盈利达到激活阈值后,启用移动止盈
        - 止盈位 = 最高价 * (1 - 止盈回撤比例)
        """
        # ATR止盈
        if atr:
            atr_tp = current_price + atr * self.atr_take_profit_multiplier
        else:
            # 固定止盈
            atr_tp = entry_price * (1 + self.default_take_profit_pct)
        
        # 移动止盈
        if profit_pct >= self.take_profit_trail_activation:
            trailing_tp = highest_price * (1 - self.take_profit_trail_pct)
            # 取较高者
            return max(atr_tp, trailing_tp)
        else:
            return atr_tp
    
    def check_stop_triggered(
        self,
        position: PositionStatus,
        current_price: float
    ) -> Dict:
        """
        检查是否触发止损/止盈
        
        Returns:
            {triggered: bool, type: str, price: float, reason: str}
        """
        position.current_price = current_price
        position.highest_price = max(position.highest_price, current_price)
        position.lowest_price = min(position.lowest_price, current_price)
        position.profit_pct = (current_price - position.entry_price) / position.entry_price
        
        # 更新止损位
        position.stop_loss_level = self.calculate_stop_levels(
            position.entry_price,
            position.current_price,
            position.highest_price
        )
        
        # 检查止损触发
        if current_price <= position.stop_loss_level.stop_loss:
            return {
                'triggered': True,
                'type': 'stop_loss',
                'price': current_price,
                'reason': f"止损触发, 止损价: {position.stop_loss_level.stop_loss:.2f}"
            }
        
        # 检查止盈触发
        if current_price >= position.stop_loss_level.take_profit:
            return {
                'triggered': True,
                'type': 'take_profit',
                'price': current_price,
                'reason': f"止盈触发, 止盈价: {position.stop_loss_level.take_profit:.2f}"
            }
        
        # 检查追踪止损触发
        if (position.stop_loss_level.trailing_stop > 0 and 
            current_price <= position.stop_loss_level.trailing_stop):
            return {
                'triggered': True,
                'type': 'trailing_stop',
                'price': current_price,
                'reason': f"追踪止损触发, 止损价: {position.stop_loss_level.trailing_stop:.2f}"
            }
        
        return {
            'triggered': False,
            'type': None,
            'price': current_price,
            'reason': ""
        }
    
    def get_position_status(
        self,
        code: str,
        entry_price: float,
        current_price: float,
        highest_price: float,
        lowest_price: float,
        atr: Optional[float] = None
    ) -> PositionStatus:
        """获取持仓状态"""
        stop_levels = self.calculate_stop_levels(
            entry_price, current_price, highest_price, atr
        )
        
        profit_pct = (current_price - entry_price) / entry_price
        
        return PositionStatus(
            code=code,
            entry_price=entry_price,
            current_price=current_price,
            highest_price=highest_price,
            lowest_price=lowest_price,
            stop_loss_level=stop_levels,
            profit_pct=profit_pct
        )
    
    def batch_check_positions(
        self,
        positions: List[Dict],
        current_prices: Dict[str, float]
    ) -> List[Dict]:
        """
        批量检查持仓止损状态
        
        Args:
            positions: 持仓列表 [{code, entry_price, highest_price, lowest_price}, ...]
            current_prices: 当前价格 {code: price}
            
        Returns:
            触发止损的持仓列表
        """
        triggered_positions = []
        
        for position in positions:
            code = position['code']
            current_price = current_prices.get(code)
            
            if current_price is None:
                continue
            
            status = self.get_position_status(
                code=code,
                entry_price=position['entry_price'],
                current_price=current_price,
                highest_price=position.get('highest_price', current_price),
                lowest_price=position.get('lowest_price', current_price),
                atr=position.get('atr')
            )
            
            # 检查触发
            result = self.check_stop_triggered(status, current_price)
            
            if result['triggered']:
                triggered_positions.append({
                    'code': code,
                    'type': result['type'],
                    'price': result['price'],
                    'reason': result['reason'],
                    'profit_pct': status.profit_pct,
                    'entry_price': status.entry_price
                })
        
        return triggered_positions
    
    def get_dynamic_stop_params(self, volatility: float) -> Dict:
        """
        根据波动率获取动态止损参数
        
        Args:
            volatility: 波动率
            
        Returns:
            止损参数配置
        """
        # 波动率高时,放宽止损;波动率低时,收紧止损
        if volatility > 0.50:  # 高波动
            return {
                'stop_loss_pct': 0.12,
                'take_profit_pct': 0.30,
                'trailing_stop_pct': 0.08,
                'trailing_activation_pct': 0.15
            }
        elif volatility > 0.30:  # 中波动
            return {
                'stop_loss_pct': 0.08,
                'take_profit_pct': 0.20,
                'trailing_stop_pct': 0.05,
                'trailing_activation_pct': 0.10
            }
        else:  # 低波动
            return {
                'stop_loss_pct': 0.05,
                'take_profit_pct': 0.15,
                'trailing_stop_pct': 0.03,
                'trailing_activation_pct': 0.08
            }


# 全局止盈止损管理器
_stop_loss_manager = None

def get_stop_loss_manager() -> StopLossManager:
    """获取全局止盈止损管理器"""
    global _stop_loss_manager
    if _stop_loss_manager is None:
        _stop_loss_manager = StopLossManager()
    return _stop_loss_manager
