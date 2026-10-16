"""
信号后验评估系统 - 量化评估信号质量,持续优化策略

功能:
- 信号准确率跟踪(7日/30日/90日)
- 平均收益率统计
- 最大回撤分析
- 胜率/盈亏比计算
- 策略表现排名
"""
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from loguru import logger
from ..models.signal import Signal


@dataclass
class SignalRecord:
    """信号记录"""
    signal: Signal
    created_at: datetime
    entry_price: Optional[float] = None
    exit_price: Optional[float] = None
    current_price: Optional[float] = None
    is_closed: bool = False
    profit_pct: float = 0.0
    max_profit: float = 0.0
    max_drawdown: float = 0.0
    holding_days: int = 0


class SignalTracker:
    """信号跟踪与评估系统"""
    
    def __init__(self, max_history_days: int = 90):
        self.max_history_days = max_history_days
        self.signal_records: Dict[str, SignalRecord] = {}  # signal_id -> SignalRecord
        self.history: List[SignalRecord] = []
    
    def record_signal(self, signal: Signal):
        """记录新信号"""
        record = SignalRecord(
            signal=signal,
            created_at=datetime.now(),
            entry_price=signal.price if hasattr(signal, 'price') else None
        )
        self.signal_records[signal.id] = record
        logger.info(f"记录信号: {signal.code} - {signal.action.value}")
    
    def update_signal_price(self, signal_id: str, current_price: float, high_price: Optional[float] = None):
        """更新信号价格"""
        if signal_id not in self.signal_records:
            logger.warning(f"信号 {signal_id} 未找到")
            return
        
        record = self.signal_records[signal_id]
        record.current_price = current_price
        
        # 更新最高价和最大回撤
        if high_price:
            if record.entry_price:
                profit = (high_price - record.entry_price) / record.entry_price
                record.max_profit = max(record.max_profit, profit)
                
                drawdown = (current_price - high_price) / high_price
                record.max_drawdown = min(record.max_drawdown, drawdown)
        
        # 计算持有天数
        record.holding_days = (datetime.now() - record.created_at).days
    
    def close_signal(self, signal_id: str, exit_price: float):
        """平仓信号"""
        if signal_id not in self.signal_records:
            logger.warning(f"信号 {signal_id} 未找到")
            return
        
        record = self.signal_records[signal_id]
        record.exit_price = exit_price
        record.is_closed = True
        
        if record.entry_price:
            record.profit_pct = (exit_price - record.entry_price) / record.entry_price
        
        # 移动到历史记录
        self.history.append(record)
        del self.signal_records[signal_id]
        
        logger.info(f"信号平仓: {record.signal.code} - 收益 {record.profit_pct:.2%}")
    
    def get_signal_metrics(self, days: int = 30) -> Dict:
        """
        获取信号评估指标
        
        Returns:
            包含准确率、胜率、盈亏比等指标
        """
        # 获取指定时间范围内的已平仓信号
        cutoff_date = datetime.now() - timedelta(days=days)
        recent_signals = [
            r for r in self.history
            if r.created_at >= cutoff_date and r.is_closed
        ]
        
        if not recent_signals:
            return {
                'count': 0,
                'accuracy': 0.0,
                'win_rate': 0.0,
                'avg_return': 0.0,
                'max_drawdown': 0.0,
                'profit_loss_ratio': 0.0,
                'sharpe_ratio': 0.0
            }
        
        # 计算胜率
        wins = [r for r in recent_signals if r.profit_pct > 0]
        losses = [r for r in recent_signals if r.profit_pct <= 0]
        win_rate = len(wins) / len(recent_signals)
        
        # 计算平均收益
        avg_return = sum(r.profit_pct for r in recent_signals) / len(recent_signals)
        
        # 计算最大回撤
        max_drawdown = min(r.max_drawdown for r in recent_signals) if recent_signals else 0.0
        
        # 计算盈亏比
        avg_win = sum(r.profit_pct for r in wins) / len(wins) if wins else 0.0
        avg_loss = sum(r.profit_pct for r in losses) / len(losses) if losses else -0.001
        profit_loss_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0.0
        
        # 计算夏普比率(简化版)
        if len(recent_signals) > 1:
            returns = [r.profit_pct for r in recent_signals]
            std_dev = pd.Series(returns).std()
            sharpe_ratio = (avg_return / std_dev) * (252 ** 0.5) if std_dev > 0 else 0.0
        else:
            sharpe_ratio = 0.0
        
        # 计算准确率(信号方向正确的比例)
        correct_signals = sum(1 for r in recent_signals if (
            (r.signal.action.value == 'BUY' and r.profit_pct > 0) or
            (r.signal.action.value == 'SELL' and r.profit_pct < 0)
        ))
        accuracy = correct_signals / len(recent_signals)
        
        return {
            'count': len(recent_signals),
            'accuracy': round(accuracy, 4),
            'win_rate': round(win_rate, 4),
            'avg_return': round(avg_return, 4),
            'max_drawdown': round(max_drawdown, 4),
            'profit_loss_ratio': round(profit_loss_ratio, 2),
            'sharpe_ratio': round(sharpe_ratio, 2)
        }
    
    def get_strategy_performance(self) -> Dict[str, Dict]:
        """获取各策略的表现"""
        strategy_stats = {}
        
        for record in self.history:
            strategy_name = record.signal.strategy
            if strategy_name not in strategy_stats:
                strategy_stats[strategy_name] = {
                    'count': 0,
                    'wins': 0,
                    'total_return': 0.0,
                    'max_drawdown': 0.0
                }
            
            stats = strategy_stats[strategy_name]
            stats['count'] += 1
            stats['total_return'] += record.profit_pct
            stats['max_drawdown'] = min(stats['max_drawdown'], record.max_drawdown)
            
            if record.profit_pct > 0:
                stats['wins'] += 1
        
        # 计算胜率
        for stats in strategy_stats.values():
            stats['win_rate'] = stats['wins'] / stats['count'] if stats['count'] > 0 else 0.0
            stats['avg_return'] = stats['total_return'] / stats['count'] if stats['count'] > 0 else 0.0
        
        return strategy_stats
    
    def get_active_signals(self) -> List[SignalRecord]:
        """获取活跃信号"""
        return list(self.signal_records.values())
    
    def cleanup_old_signals(self):
        """清理过期信号"""
        cutoff_date = datetime.now() - timedelta(days=self.max_history_days)
        
        # 清理历史记录
        self.history = [
            r for r in self.history
            if r.created_at >= cutoff_date
        ]
        
        logger.info(f"清理过期信号, 剩余 {len(self.history)} 条历史记录")


# 全局信号跟踪器
_signal_tracker = None

def get_signal_tracker() -> SignalTracker:
    """获取全局信号跟踪器"""
    global _signal_tracker
    if _signal_tracker is None:
        _signal_tracker = SignalTracker()
    return _signal_tracker
