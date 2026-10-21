from typing import List, Dict
from ...models.market import MarketState
from ...models.signal import Signal


class PositionManager:
    def __init__(self, initial_capital: float = 100000):
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.base_position = 0.3
        self.max_position = 0.8
        self.min_position = 0.1

    def calculate_position(self, signal: Signal, market: MarketState, equity_curve: List[float] = None) -> float:
        base = self.base_position
        market_factor = market.factor
        signal_factor = signal.level_factor
        equity_factor = self._calculate_equity_factor(equity_curve)
        position = base * market_factor * signal_factor * equity_factor
        position = max(self.min_position, min(self.max_position, position))
        return position

    def _calculate_equity_factor(self, equity_curve: List[float] = None) -> float:
        if not equity_curve or len(equity_curve) < 2:
            return 1.0
        current = equity_curve[-1]
        initial = equity_curve[0]
        if initial == 0:
            return 1.0
        pnl_pct = (current - initial) / initial
        if pnl_pct > 0.2:
            return 1.2
        elif pnl_pct > 0.1:
            return 1.1
        elif pnl_pct > 0:
            return 1.0
        elif pnl_pct > -0.05:
            return 0.9
        elif pnl_pct > -0.1:
            return 0.8
        else:
            return 0.7

    def calculate_position_size(self, signal: Signal, market: MarketState, equity_curve: List[float] = None) -> float:
        position_pct = self.calculate_position(signal, market, equity_curve)
        position_size = self.current_capital * position_pct
        return position_size

    def update_capital(self, pnl: float):
        self.current_capital += pnl

    def get_capital_info(self) -> Dict:
        return {
            'initial': self.initial_capital,
            'current': self.current_capital,
            'pnl': self.current_capital - self.initial_capital,
            'pnl_pct': (self.current_capital - self.initial_capital) / self.initial_capital if self.initial_capital > 0 else 0,
        }