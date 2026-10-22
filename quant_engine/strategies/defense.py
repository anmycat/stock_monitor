from typing import Dict, List

from ..models.market import MarketState, MarketType


class DefenseStrategy:
    def __init__(self):
        self.name = "防守策略"
        self.consecutive_loss_limit = 3
        self.max_drawdown = 0.10

    def check_market(self, market: MarketState) -> Dict:
        if market.type == MarketType.BEAR:
            return {
                'action': 'STOP',
                'reason': '熊市停止交易',
                'position_factor': 0.0,
            }
        elif market.type == MarketType.SIDEWAYS:
            return {
                'action': 'REDUCE',
                'reason': '震荡市降低仓位',
                'position_factor': 0.5,
            }
        elif market.type == MarketType.BULL_WEAK:
            return {
                'action': 'WATCH',
                'reason': '牛市减弱，谨慎操作',
                'position_factor': 0.7,
            }
        else:
            return {
                'action': 'NORMAL',
                'reason': '正常交易',
                'position_factor': 1.0,
            }

    def check_consecutive_losses(self, trades: List[Dict]) -> Dict:
        if not trades:
            return {
                'action': 'NORMAL',
                'reason': '无交易记录',
                'position_factor': 1.0,
            }
        consecutive_losses = 0
        for trade in reversed(trades):
            if trade.get('pnl', 0) < 0:
                consecutive_losses += 1
            else:
                break
        if consecutive_losses >= self.consecutive_loss_limit:
            return {
                'action': 'STOP',
                'reason': f'连续亏损{consecutive_losses}次，停止交易',
                'position_factor': 0.0,
            }
        elif consecutive_losses >= 2:
            return {
                'action': 'REDUCE',
                'reason': f'连续亏损{consecutive_losses}次，降低仓位',
                'position_factor': 0.5,
            }
        else:
            return {
                'action': 'NORMAL',
                'reason': '正常交易',
                'position_factor': 1.0,
            }

    def check_drawdown(self, equity_curve: List[float]) -> Dict:
        if not equity_curve or len(equity_curve) < 2:
            return {
                'action': 'NORMAL',
                'reason': '无足够数据',
                'position_factor': 1.0,
            }
        peak = equity_curve[0]
        max_dd = 0
        for value in equity_curve:
            if value > peak:
                peak = value
            dd = (peak - value) / peak
            if dd > max_dd:
                max_dd = dd
        if max_dd > self.max_drawdown:
            return {
                'action': 'STOP',
                'reason': f'最大回撤{max_dd:.1%}超过限制',
                'position_factor': 0.0,
            }
        elif max_dd > self.max_drawdown * 0.7:
            return {
                'action': 'REDUCE',
                'reason': f'回撤较大{max_dd:.1%}',
                'position_factor': 0.5,
            }
        else:
            return {
                'action': 'NORMAL',
                'reason': '回撤正常',
                'position_factor': 1.0,
            }

    def get_position_factor(self, market: MarketState, trades: List[Dict], equity_curve: List[float]) -> float:
        market_check = self.check_market(market)
        loss_check = self.check_consecutive_losses(trades)
        dd_check = self.check_drawdown(equity_curve)
        factors = [market_check['position_factor'], loss_check['position_factor'], dd_check['position_factor']]
        return min(factors)