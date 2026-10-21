from typing import List, Dict
from ...models.signal import Signal
from ...utils.helpers import calculate_max_drawdown


class RiskManager:
    def __init__(self):
        self.stop_loss_pct = 0.05
        self.max_drawdown_pct = 0.10
        self.max_consecutive_losses = 3
        self.max_position_per_stock = 0.25

    def check_signal(self, signal: Signal, current_positions: Dict[str, float]) -> Dict:
        if signal.position > self.max_position_per_stock:
            return {
                'approved': False,
                'reason': f'单只股票仓位{signal.position:.1%}超过限制{self.max_position_per_stock:.1%}',
                'adjusted_position': self.max_position_per_stock,
            }
        total_position = sum(current_positions.values()) + signal.position
        if total_position > 1.0:
            return {
                'approved': False,
                'reason': f'总仓位{total_position:.1%}超过100%',
                'adjusted_position': max(0, 1.0 - sum(current_positions.values())),
            }
        return {
            'approved': True,
            'reason': '通过风控检查',
            'adjusted_position': signal.position,
        }

    def check_stop_loss(self, entry_price: float, current_price: float) -> Dict:
        if entry_price == 0:
            return {'approved': True, 'reason': '入场价格为0', 'pnl_pct': 0}
        pnl_pct = (current_price - entry_price) / entry_price
        if pnl_pct < -self.stop_loss_pct:
            return {
                'approved': False,
                'reason': f'亏损{pnl_pct:.1%}超过止损线{-self.stop_loss_pct:.1%}',
                'pnl_pct': pnl_pct,
            }
        return {
            'approved': True,
            'reason': '未触发止损',
            'pnl_pct': pnl_pct,
        }

    def check_drawdown(self, equity_curve: List[float]) -> Dict:
        if not equity_curve or len(equity_curve) < 2:
            return {'approved': True, 'reason': '无足够数据', 'max_drawdown': 0}
        max_dd = calculate_max_drawdown(equity_curve)
        if max_dd > self.max_drawdown_pct:
            return {
                'approved': False,
                'reason': f'最大回撤{max_dd:.1%}超过限制{self.max_drawdown_pct:.1%}',
                'max_drawdown': max_dd,
            }
        return {
            'approved': True,
            'reason': '回撤正常',
            'max_drawdown': max_dd,
        }

    def check_consecutive_losses(self, trades: List[Dict]) -> Dict:
        if not trades:
            return {'approved': True, 'reason': '无交易记录', 'consecutive_losses': 0}
        consecutive_losses = 0
        for trade in reversed(trades):
            if trade.get('pnl', 0) < 0:
                consecutive_losses += 1
            else:
                break
        if consecutive_losses >= self.max_consecutive_losses:
            return {
                'approved': False,
                'reason': f'连续亏损{consecutive_losses}次超过限制{self.max_consecutive_losses}',
                'consecutive_losses': consecutive_losses,
            }
        return {
            'approved': True,
            'reason': '正常',
            'consecutive_losses': consecutive_losses,
        }