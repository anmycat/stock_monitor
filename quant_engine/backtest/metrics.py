
import numpy as np
import pandas as pd

from ..utils.helpers import calculate_max_drawdown


def calculate_metrics(trades: list[dict], equity_curve: list[float]) -> dict:
    if not trades:
        return {
            'total_trades': 0,
            'win_rate': 0,
            'profit_loss_ratio': 0,
            'max_drawdown': 0,
            'total_return': 0,
            'sharpe_ratio': 0,
        }
    wins = [t for t in trades if t.get('pnl', 0) > 0]
    losses = [t for t in trades if t.get('pnl', 0) <= 0]
    win_rate = len(wins) / len(trades) if trades else 0
    avg_win = np.mean([t['pnl'] for t in wins]) if wins else 0
    avg_loss = abs(np.mean([t['pnl'] for t in losses])) if losses else 0
    profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else 0
    max_drawdown = _calculate_max_drawdown(equity_curve) if equity_curve else 0
    total_return = _calculate_total_return(equity_curve) if equity_curve else 0
    sharpe_ratio = _calculate_sharpe(equity_curve) if equity_curve else 0
    return {
        'total_trades': len(trades),
        'win_rate': win_rate,
        'profit_loss_ratio': profit_loss_ratio,
        'max_drawdown': max_drawdown,
        'total_return': total_return,
        'sharpe_ratio': sharpe_ratio,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'wins': len(wins),
        'losses': len(losses),
    }


# _calculate_max_drawdown 已废弃，使用 calculate_max_drawdown (from utils.helpers)
_calculate_max_drawdown = calculate_max_drawdown


def _calculate_total_return(equity_curve: list[float]) -> float:
    if not equity_curve or len(equity_curve) < 2:
        return 0
    return (equity_curve[-1] - equity_curve[0]) / equity_curve[0]


def _calculate_sharpe(equity_curve: list[float], risk_free_rate: float = 0.03) -> float:
    if not equity_curve or len(equity_curve) < 2:
        return 0
    returns = pd.Series(equity_curve).pct_change().dropna()
    if returns.std() == 0:
        return 0
    excess_returns = returns.mean() - risk_free_rate / 252
    return excess_returns / returns.std() * np.sqrt(252)