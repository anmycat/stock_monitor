"""抗过拟合检查器 - 增强回测评估的可靠性"""
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
from loguru import logger
from ..utils.helpers import calculate_max_drawdown


class OverfitChecker:
    """抗过拟合检查器"""

    def __init__(self):
        self.min_signals = 30
        self.max_train_val_gap = 0.15
        self.max_val_test_gap = 0.12
        self.max_complexity_score = 10

    def full_check(self, trades: List[Dict], equity_curve: List[float], 
                   strategy_complexity: int = 0) -> Dict:
        """
        全面检查过拟合
        
        Args:
            trades: 交易记录
            equity_curve: 资金曲线
            strategy_complexity: 策略复杂度（条件数量）
        
        Returns:
            检查结果
        """
        checks = {
            'signal_count': self._check_signal_count(trades),
            'train_val_split': self._check_train_val_split(trades),
            'walk_forward': self._check_walk_forward(trades),
            'complexity': self._check_complexity(strategy_complexity),
            'stability': self._check_stability(equity_curve),
            'parameter_sensitivity': self._check_parameter_sensitivity(trades),
        }
        
        # 计算综合分数
        score = self._calculate_overfit_score(checks)
        
        return {
            'checks': checks,
            'score': score,
            'is_overfit': score < 50,
            'recommendation': self._get_recommendation(checks, score),
        }

    def _check_signal_count(self, trades: List[Dict]) -> Dict:
        """检查信号数量"""
        count = len(trades)
        if count < self.min_signals:
            return {
                'status': 'warning',
                'score': 30,
                'message': f'信号数量不足({count}<{self.min_signals})，结果可能不可靠',
            }
        elif count < 50:
            return {
                'status': 'ok',
                'score': 60,
                'message': f'信号数量较少({count})，建议增加样本',
            }
        else:
            return {
                'status': 'pass',
                'score': 100,
                'message': f'信号数量充足({count})',
            }

    def _check_train_val_split(self, trades: List[Dict]) -> Dict:
        """检查训练/验证集分割"""
        if len(trades) < 10:
            return {
                'status': 'warning',
                'score': 30,
                'message': '交易数量不足，无法进行分割检查',
            }
        
        # 分割为训练集和验证集
        split_idx = len(trades) * 2 // 3
        train_trades = trades[:split_idx]
        val_trades = trades[split_idx:]
        
        train_win_rate = len([t for t in train_trades if t.get('pnl', 0) > 0]) / len(train_trades)
        val_win_rate = len([t for t in val_trades if t.get('pnl', 0) > 0]) / len(val_trades)
        
        gap = abs(train_win_rate - val_win_rate)
        
        if gap > self.max_train_val_gap:
            return {
                'status': 'fail',
                'score': 20,
                'message': f'训练/验证差距过大({gap:.1%})，过拟合风险高',
                'train_win_rate': train_win_rate,
                'val_win_rate': val_win_rate,
                'gap': gap,
            }
        elif gap > 0.08:
            return {
                'status': 'warning',
                'score': 50,
                'message': f'训练/验证差距较大({gap:.1%})，需关注',
                'train_win_rate': train_win_rate,
                'val_win_rate': val_win_rate,
                'gap': gap,
            }
        else:
            return {
                'status': 'pass',
                'score': 90,
                'message': f'训练/验证差距可控({gap:.1%})',
                'train_win_rate': train_win_rate,
                'val_win_rate': val_win_rate,
                'gap': gap,
            }

    def _check_walk_forward(self, trades: List[Dict]) -> Dict:
        """步进验证"""
        if len(trades) < 20:
            return {
                'status': 'warning',
                'score': 30,
                'message': '交易数量不足，无法进行步进验证',
            }
        
        # 滚动窗口验证
        window_size = max(10, len(trades) // 5)
        step = max(5, window_size // 2)
        
        win_rates = []
        for i in range(0, len(trades) - window_size, step):
            window = trades[i:i + window_size]
            win_rate = len([t for t in window if t.get('pnl', 0) > 0]) / len(window)
            win_rates.append(win_rate)
        
        if len(win_rates) < 2:
            return {
                'status': 'warning',
                'score': 40,
                'message': '步进验证窗口不足',
            }
        
        # 计算稳定性
        std = np.std(win_rates)
        mean = np.mean(win_rates)
        
        if std > 0.2:
            return {
                'status': 'fail',
                'score': 20,
                'message': f'步进验证波动大(std={std:.2f})，策略不稳定',
                'win_rates': win_rates,
                'std': std,
            }
        elif std > 0.1:
            return {
                'status': 'warning',
                'score': 50,
                'message': f'步进验证波动较大(std={std:.2f})',
                'win_rates': win_rates,
                'std': std,
            }
        else:
            return {
                'status': 'pass',
                'score': 90,
                'message': f'步进验证稳定(std={std:.2f})',
                'win_rates': win_rates,
                'std': std,
            }

    def _check_complexity(self, complexity: int) -> Dict:
        """检查策略复杂度"""
        if complexity > self.max_complexity_score:
            return {
                'status': 'fail',
                'score': 20,
                'message': f'策略过于复杂({complexity}>{self.max_complexity_score})，过拟合风险高',
            }
        elif complexity > 6:
            return {
                'status': 'warning',
                'score': 50,
                'message': f'策略较复杂({complexity})，需关注',
            }
        else:
            return {
                'status': 'pass',
                'score': 90,
                'message': f'策略复杂度适中({complexity})',
            }

    def _check_stability(self, equity_curve: List[float]) -> Dict:
        """检查稳定性"""
        if len(equity_curve) < 2:
            return {
                'status': 'warning',
                'score': 30,
                'message': '数据不足',
            }
        
        # 计算最大回撤（使用公共实现）
        max_dd = calculate_max_drawdown(equity_curve)
        
        # 计算收益稳定性
        returns = pd.Series(equity_curve).pct_change().dropna()
        return_std = returns.std() if len(returns) > 0 else 0
        
        if max_dd > 0.20:
            return {
                'status': 'fail',
                'score': 20,
                'message': f'最大回撤过大({max_dd:.1%})',
                'max_drawdown': max_dd,
                'return_std': return_std,
            }
        elif max_dd > 0.10:
            return {
                'status': 'warning',
                'score': 50,
                'message': f'回撤较大({max_dd:.1%})',
                'max_drawdown': max_dd,
                'return_std': return_std,
            }
        else:
            return {
                'status': 'pass',
                'score': 90,
                'message': f'回撤可控({max_dd:.1%})',
                'max_drawdown': max_dd,
                'return_std': return_std,
            }

    def _check_parameter_sensitivity(self, trades: List[Dict]) -> Dict:
        """参数敏感性检查"""
        # 简化版本：检查交易分布是否均匀
        if len(trades) < 10:
            return {
                'status': 'warning',
                'score': 30,
                'message': '交易数量不足',
            }
        
        # 按月分组
        monthly_trades = {}
        for trade in trades:
            month = trade.get('entry_date', '')[:7]  # YYYY-MM
            if month not in monthly_trades:
                monthly_trades[month] = []
            monthly_trades[month].append(trade)
        
        if len(monthly_trades) < 2:
            return {
                'status': 'warning',
                'score': 40,
                'message': '交易时间跨度不足',
            }
        
        # 检查每月交易数量
        monthly_counts = [len(v) for v in monthly_trades.values()]
        avg_count = np.mean(monthly_counts)
        std_count = np.std(monthly_counts)
        
        if std_count > avg_count * 0.5:
            return {
                'status': 'warning',
                'score': 50,
                'message': f'交易分布不均匀(std/avg={std_count/avg_count:.2f})',
            }
        else:
            return {
                'status': 'pass',
                'score': 80,
                'message': f'交易分布较均匀',
            }

    def _calculate_overfit_score(self, checks: Dict) -> float:
        """计算过拟合综合分数"""
        weights = {
            'signal_count': 0.15,
            'train_val_split': 0.25,
            'walk_forward': 0.25,
            'complexity': 0.10,
            'stability': 0.15,
            'parameter_sensitivity': 0.10,
        }
        
        total_score = 0
        total_weight = 0
        
        for check_name, weight in weights.items():
            if check_name in checks:
                score = checks[check_name].get('score', 0)
                total_score += score * weight
                total_weight += weight
        
        return total_score / total_weight if total_weight > 0 else 0

    def _get_recommendation(self, checks: Dict, score: float) -> str:
        """获取建议"""
        if score >= 80:
            return "策略过拟合风险低，可以信任"
        elif score >= 60:
            return "策略过拟合风险中等，建议进一步验证"
        elif score >= 40:
            return "策略过拟合风险较高，建议简化策略或增加样本"
        else:
            return "策略过拟合风险高，建议重新设计"