"""T+N后验评估 - 保存运行结果，用后续数据验证"""
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from loguru import logger
from ..data.cache import RedisCache
from ..data.datacenter import DataCenter


class PostEvaluation:
    """T+N后验评估"""

    def __init__(self, cache: RedisCache, datacenter: DataCenter):
        self.cache = cache
        self.datacenter = datacenter

    def save_run(self, run_id: str, signals: List[Dict], market_state: Dict) -> bool:
        """保存运行结果"""
        try:
            run_data = {
                'run_id': run_id,
                'timestamp': datetime.now().isoformat(),
                'signals': signals,
                'market_state': market_state,
            }
            self.cache.set(f"run:{run_id}", run_data, ttl=86400 * 30)  # 保存30天
            
            # 添加到运行列表
            runs = self.cache.get("runs:list") or []
            runs.append(run_id)
            if len(runs) > 100:
                runs = runs[-100:]
            self.cache.set("runs:list", runs, ttl=86400 * 30)
            
            logger.info(f"Saved run: {run_id}")
            return True
        except Exception as e:
            logger.error(f"Save run error: {e}")
            return False

    def evaluate_run(self, run_id: str) -> Dict:
        """评估运行结果"""
        try:
            run_data = self.cache.get(f"run:{run_id}")
            if not run_data:
                return {'error': f'Run not found: {run_id}'}
            
            signals = run_data.get('signals', [])
            if not signals:
                return {'run_id': run_id, 'message': 'No signals to evaluate'}
            
            # 获取后续数据
            results = []
            for signal in signals:
                code = signal.get('code')
                entry_price = signal.get('price', 0)
                entry_date = signal.get('timestamp', '')[:10]
                
                if not code or not entry_price:
                    continue
                
                # 获取后续数据
                df = self.datacenter.get_daily_data(code, 30)
                if df is None or df.empty:
                    continue
                
                # 找到入场日期后的数据
                entry_idx = None
                for i, row in df.iterrows():
                    if row.get('trade_date', '') >= entry_date:
                        entry_idx = i
                        break
                
                if entry_idx is None:
                    continue
                
                # 计算T+N收益
                current_price = df['close'].iloc[-1]
                pnl_pct = (current_price - entry_price) / entry_price
                
                # 计算最大浮盈和最大回撤
                prices_after_entry = df['close'].iloc[entry_idx:]
                max_price = prices_after_entry.max()
                min_price = prices_after_entry.min()
                max_profit = (max_price - entry_price) / entry_price
                max_drawdown = (entry_price - min_price) / entry_price
                
                results.append({
                    'code': code,
                    'name': signal.get('name', ''),
                    'action': signal.get('action', ''),
                    'entry_price': entry_price,
                    'current_price': current_price,
                    'pnl_pct': pnl_pct,
                    'max_profit': max_profit,
                    'max_drawdown': max_drawdown,
                    'days_held': len(prices_after_entry),
                })
            
            # 计算汇总
            if results:
                win_count = len([r for r in results if r['pnl_pct'] > 0])
                total_count = len(results)
                avg_pnl = sum(r['pnl_pct'] for r in results) / total_count
                avg_max_profit = sum(r['max_profit'] for r in results) / total_count
                avg_max_drawdown = sum(r['max_drawdown'] for r in results) / total_count
            else:
                win_count = 0
                total_count = 0
                avg_pnl = 0
                avg_max_profit = 0
                avg_max_drawdown = 0
            
            return {
                'run_id': run_id,
                'run_date': run_data.get('timestamp', ''),
                'total_signals': total_count,
                'win_count': win_count,
                'win_rate': win_count / total_count if total_count > 0 else 0,
                'avg_pnl': avg_pnl,
                'avg_max_profit': avg_max_profit,
                'avg_max_drawdown': avg_max_drawdown,
                'details': results,
            }
        except Exception as e:
            logger.error(f"Evaluate run error: {e}")
            return {'error': str(e)}

    def evaluate_batch(self, limit: int = 10) -> Dict:
        """批量评估"""
        try:
            runs = self.cache.get("runs:list") or []
            if not runs:
                return {'message': 'No runs to evaluate'}
            
            # 评估最近的运行
            recent_runs = runs[-limit:]
            evaluations = []
            
            for run_id in recent_runs:
                eval_result = self.evaluate_run(run_id)
                if 'error' not in eval_result:
                    evaluations.append(eval_result)
            
            # 计算汇总统计
            if evaluations:
                avg_win_rate = sum(e['win_rate'] for e in evaluations) / len(evaluations)
                avg_pnl = sum(e['avg_pnl'] for e in evaluations) / len(evaluations)
                total_signals = sum(e['total_signals'] for e in evaluations)
            else:
                avg_win_rate = 0
                avg_pnl = 0
                total_signals = 0
            
            return {
                'total_runs': len(evaluations),
                'avg_win_rate': avg_win_rate,
                'avg_pnl': avg_pnl,
                'total_signals': total_signals,
                'evaluations': evaluations,
            }
        except Exception as e:
            logger.error(f"Evaluate batch error: {e}")
            return {'error': str(e)}

    def get_run_list(self, limit: int = 20) -> List[Dict]:
        """获取运行列表"""
        try:
            runs = self.cache.get("runs:list") or []
            run_list = []
            
            for run_id in runs[-limit:]:
                run_data = self.cache.get(f"run:{run_id}")
                if run_data:
                    run_list.append({
                        'run_id': run_id,
                        'run_date': run_data.get('timestamp', ''),
                        'total_signals': len(run_data.get('signals', [])),
                        'market_state': run_data.get('market_state', {}).get('type', 'unknown'),
                    })
            
            return run_list
        except Exception as e:
            logger.error(f"Get run list error: {e}")
            return []