"""增强回测评估器 - 多维度评估回测结果"""
from typing import Dict, List

import pandas as pd

from ..utils.helpers import calculate_max_drawdown
from .metrics import calculate_metrics


class BacktestEvaluator:
    """增强回测评估器"""

    def __init__(self):
        self.min_signals = 30

    def evaluate(self, trades: List[Dict], equity_curve: List[float]) -> Dict:
        """
        全面评估回测结果
        
        Returns:
            包含metrics、overfit_check、stability_check、confidence_score
        """
        if not trades:
            return {
                'metrics': self._empty_metrics(),
                'overfit_check': {'status': 'unknown', 'message': '无交易数据'},
                'stability_check': {'status': 'unknown', 'message': '无交易数据'},
                'confidence_score': 0,
            }

        metrics = calculate_metrics(trades, equity_curve)

        # 添加抗过拟合检查
        overfit_check = self._check_overfitting(trades, equity_curve)

        # 添加稳定性检查
        stability_check = self._check_stability(trades, equity_curve)

        # 计算置信度
        confidence_score = self._calculate_confidence(metrics, overfit_check, stability_check)

        return {
            'metrics': metrics,
            'overfit_check': overfit_check,
            'stability_check': stability_check,
            'confidence_score': confidence_score,
        }

    def _check_overfitting(self, trades: List[Dict], equity_curve: List[float]) -> Dict:
        """检查过拟合"""
        if len(trades) < self.min_signals:
            return {
                'status': 'warning',
                'message': f'信号数量不足({len(trades)}<{self.min_signals})',
                'train_win_rate': 0,
                'val_win_rate': 0,
                'gap': 0,
            }

        # 将数据分为训练集和验证集
        split_idx = len(trades) * 2 // 3
        train_trades = trades[:split_idx]
        val_trades = trades[split_idx:]

        train_win_rate = len([t for t in train_trades if t.get('pnl', 0) > 0]) / len(train_trades) if train_trades else 0
        val_win_rate = len([t for t in val_trades if t.get('pnl', 0) > 0]) / len(val_trades) if val_trades else 0

        gap = abs(train_win_rate - val_win_rate)

        if gap > 0.15:
            return {
                'status': 'fail',
                'message': f'过拟合风险高(训练/验证胜率差距: {gap:.1%})',
                'train_win_rate': round(train_win_rate, 3),
                'val_win_rate': round(val_win_rate, 3),
                'gap': round(gap, 3),
            }
        elif gap > 0.10:
            return {
                'status': 'warning',
                'message': f'过拟合风险中等(差距: {gap:.1%})',
                'train_win_rate': round(train_win_rate, 3),
                'val_win_rate': round(val_win_rate, 3),
                'gap': round(gap, 3),
            }
        else:
            return {
                'status': 'pass',
                'message': f'过拟合风险低(差距: {gap:.1%})',
                'train_win_rate': round(train_win_rate, 3),
                'val_win_rate': round(val_win_rate, 3),
                'gap': round(gap, 3),
            }

    def _check_stability(self, trades: List[Dict], equity_curve: List[float]) -> Dict:
        """检查稳定性"""
        if not equity_curve or len(equity_curve) < 2:
            return {'status': 'unknown', 'message': '数据不足', 'max_drawdown': 0}

        # 计算最大回撤
        max_drawdown = self._calc_max_drawdown(equity_curve)

        # 计算收益稳定性
        returns = pd.Series(equity_curve).pct_change().dropna()
        return_std = returns.std() if len(returns) > 0 else 0

        # 计算月度胜率
        monthly_wins = 0
        monthly_total = 0
        for trade in trades:
            monthly_total += 1
            if trade.get('pnl', 0) > 0:
                monthly_wins += 1

        monthly_win_rate = monthly_wins / monthly_total if monthly_total > 0 else 0

        if max_drawdown > 0.20:
            return {
                'status': 'fail',
                'message': f'最大回撤过大({max_drawdown:.1%})',
                'max_drawdown': round(max_drawdown, 3),
                'return_std': round(return_std, 4),
                'win_rate': round(monthly_win_rate, 3),
            }
        elif max_drawdown > 0.10:
            return {
                'status': 'warning',
                'message': f'回撤较大({max_drawdown:.1%})',
                'max_drawdown': round(max_drawdown, 3),
                'return_std': round(return_std, 4),
                'win_rate': round(monthly_win_rate, 3),
            }
        else:
            return {
                'status': 'pass',
                'message': f'回撤可控({max_drawdown:.1%})',
                'max_drawdown': round(max_drawdown, 3),
                'return_std': round(return_std, 4),
                'win_rate': round(monthly_win_rate, 3),
            }

    def _calc_max_drawdown(self, equity_curve: List[float]) -> float:
        """计算最大回撤（使用公共实现）"""
        return calculate_max_drawdown(equity_curve)

    def _calculate_confidence(self, metrics: Dict, overfit_check: Dict, stability_check: Dict) -> float:
        """计算置信度分数"""
        score = 50  # 基础分

        # 胜率加分
        win_rate = metrics.get('win_rate', 0)
        if win_rate > 0.6:
            score += 20
        elif win_rate > 0.5:
            score += 10
        elif win_rate < 0.4:
            score -= 10

        # 盈亏比加分
        profit_loss_ratio = metrics.get('profit_loss_ratio', 0)
        if profit_loss_ratio > 2.0:
            score += 15
        elif profit_loss_ratio > 1.5:
            score += 10
        elif profit_loss_ratio < 1.0:
            score -= 10

        # 过拟合检查
        if overfit_check['status'] == 'pass':
            score += 15
        elif overfit_check['status'] == 'fail':
            score -= 20

        # 稳定性检查
        if stability_check['status'] == 'pass':
            score += 15
        elif stability_check['status'] == 'fail':
            score -= 20

        # 最大回撤扣分
        max_drawdown = stability_check.get('max_drawdown', 0)
        if max_drawdown > 0.15:
            score -= 15
        elif max_drawdown > 0.10:
            score -= 10

        return max(0, min(100, score))

    def _empty_metrics(self) -> Dict:
        """空指标"""
        return {
            'total_trades': 0,
            'win_rate': 0,
            'profit_loss_ratio': 0,
            'max_drawdown': 0,
            'total_return': 0,
            'sharpe_ratio': 0,
        }

    def generate_report(self, evaluation: Dict, strategy_name: str = "") -> str:
        """生成评估报告"""
        metrics = evaluation.get('metrics', {})
        overfit = evaluation.get('overfit_check', {})
        stability = evaluation.get('stability_check', {})
        confidence = evaluation.get('confidence_score', 0)

        report = f"""
# 回测评估报告 {f'- {strategy_name}' if strategy_name else ''}

## 核心指标
- 总交易次数: {metrics.get('total_trades', 0)}
- 胜率: {metrics.get('win_rate', 0):.1%}
- 盈亏比: {metrics.get('profit_loss_ratio', 0):.2f}
- 最大回撤: {metrics.get('max_drawdown', 0):.1%}
- 总收益: {metrics.get('total_return', 0):.1%}
- 夏普比率: {metrics.get('sharpe_ratio', 0):.2f}

## 过拟合检查
- 状态: {overfit.get('status', 'unknown')}
- 说明: {overfit.get('message', '')}
- 训练集胜率: {overfit.get('train_win_rate', 0):.1%}
- 验证集胜率: {overfit.get('val_win_rate', 0):.1%}

## 稳定性检查
- 状态: {stability.get('status', 'unknown')}
- 说明: {stability.get('message', '')}
- 最大回撤: {stability.get('max_drawdown', 0):.1%}

## 置信度评分
- 分数: {confidence:.0f}/100
- 评级: {'优秀' if confidence >= 80 else '良好' if confidence >= 60 else '一般' if confidence >= 40 else '较差'}
"""
        return report