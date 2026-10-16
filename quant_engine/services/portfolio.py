"""持仓管理器 - 记录持仓、盈亏跟踪"""
import json
from datetime import datetime
from typing import Dict, List, Optional
from loguru import logger
from ..data.cache import RedisCache


class Position:
    """持仓记录"""
    def __init__(self, code: str, name: str, entry_price: float, quantity: float, 
                 entry_date: str, strategy: str):
        self.code = code
        self.name = name
        self.entry_price = entry_price
        self.quantity = quantity
        self.entry_date = entry_date
        self.strategy = strategy
        self.current_price = entry_price
        self.pnl = 0
        self.pnl_pct = 0

    def update_price(self, current_price: float):
        """更新当前价格"""
        self.current_price = current_price
        self.pnl = self.quantity * (current_price - self.entry_price)
        self.pnl_pct = (current_price - self.entry_price) / self.entry_price if self.entry_price > 0 else 0

    def to_dict(self) -> Dict:
        return {
            'code': self.code,
            'name': self.name,
            'entry_price': self.entry_price,
            'quantity': self.quantity,
            'entry_date': self.entry_date,
            'strategy': self.strategy,
            'current_price': self.current_price,
            'pnl': round(self.pnl, 2),
            'pnl_pct': round(self.pnl_pct, 4),
        }


class PortfolioManager:
    """持仓管理器"""

    def __init__(self, cache: RedisCache, initial_capital: float = 100000):
        self.cache = cache
        self.initial_capital = initial_capital
        self.positions: Dict[str, Position] = {}
        self.trades: List[Dict] = []
        self._load_from_cache()

    def _load_from_cache(self):
        """从缓存加载持仓"""
        try:
            positions_data = self.cache.get("portfolio:positions")
            if positions_data:
                for code, data in positions_data.items():
                    self.positions[code] = Position(**data)
            
            trades_data = self.cache.get("portfolio:trades")
            if trades_data:
                self.trades = trades_data
        except Exception as e:
            logger.error(f"Load portfolio error: {e}")

    def _save_to_cache(self):
        """保存到缓存"""
        try:
            positions_data = {code: pos.to_dict() for code, pos in self.positions.items()}
            self.cache.set("portfolio:positions", positions_data, ttl=86400)
            self.cache.set("portfolio:trades", self.trades[-100:], ttl=86400)
        except Exception as e:
            logger.error(f"Save portfolio error: {e}")

    def add_position(self, code: str, name: str, price: float, quantity: float, strategy: str) -> bool:
        """添加持仓"""
        if code in self.positions:
            logger.warning(f"Position already exists for {code}")
            return False
        
        position = Position(
            code=code,
            name=name,
            entry_price=price,
            quantity=quantity,
            entry_date=datetime.now().strftime("%Y-%m-%d"),
            strategy=strategy,
        )
        self.positions[code] = position
        self._save_to_cache()
        logger.info(f"Added position: {code} {name} {quantity}@{price}")
        return True

    def close_position(self, code: str, price: float, reason: str = "manual") -> Optional[Dict]:
        """平仓"""
        if code not in self.positions:
            logger.warning(f"No position found for {code}")
            return None
        
        position = self.positions[code]
        position.update_price(price)
        
        trade = {
            'code': code,
            'name': position.name,
            'entry_price': position.entry_price,
            'exit_price': price,
            'quantity': position.quantity,
            'pnl': position.pnl,
            'pnl_pct': position.pnl_pct,
            'entry_date': position.entry_date,
            'exit_date': datetime.now().strftime("%Y-%m-%d"),
            'strategy': position.strategy,
            'reason': reason,
        }
        
        self.trades.append(trade)
        del self.positions[code]
        self._save_to_cache()
        
        logger.info(f"Closed position: {code} PnL: {position.pnl:.2f} ({position.pnl_pct:.2%})")
        return trade

    def update_prices(self, prices: Dict[str, float]):
        """更新所有持仓价格"""
        for code, position in self.positions.items():
            if code in prices:
                position.update_price(prices[code])
        self._save_to_cache()

    def get_positions(self) -> List[Dict]:
        """获取所有持仓"""
        return [pos.to_dict() for pos in self.positions.values()]

    def get_position(self, code: str) -> Optional[Dict]:
        """获取单个持仓"""
        if code in self.positions:
            return self.positions[code].to_dict()
        return None

    def get_trades(self, limit: int = 50) -> List[Dict]:
        """获取交易记录"""
        return self.trades[-limit:]

    def get_portfolio_summary(self) -> Dict:
        """获取持仓摘要"""
        total_value = sum(pos.current_price * pos.quantity for pos in self.positions.values())
        total_pnl = sum(pos.pnl for pos in self.positions.values())
        total_cost = sum(pos.entry_price * pos.quantity for pos in self.positions.values())
        
        return {
            'total_positions': len(self.positions),
            'total_value': round(total_value, 2),
            'total_cost': round(total_cost, 2),
            'total_pnl': round(total_pnl, 2),
            'total_pnl_pct': round(total_pnl / total_cost if total_cost > 0 else 0, 4),
            'positions': self.get_positions(),
        }

    def get_performance_stats(self) -> Dict:
        """获取绩效统计"""
        if not self.trades:
            return {
                'total_trades': 0,
                'win_rate': 0,
                'avg_pnl': 0,
                'max_win': 0,
                'max_loss': 0,
            }
        
        wins = [t for t in self.trades if t.get('pnl', 0) > 0]
        losses = [t for t in self.trades if t.get('pnl', 0) <= 0]
        
        return {
            'total_trades': len(self.trades),
            'win_rate': len(wins) / len(self.trades) if self.trades else 0,
            'avg_pnl': sum(t.get('pnl', 0) for t in self.trades) / len(self.trades),
            'max_win': max((t.get('pnl', 0) for t in wins), default=0),
            'max_loss': min((t.get('pnl', 0) for t in losses), default=0),
            'avg_win': sum(t.get('pnl', 0) for t in wins) / len(wins) if wins else 0,
            'avg_loss': sum(t.get('pnl', 0) for t in losses) / len(losses) if losses else 0,
        }