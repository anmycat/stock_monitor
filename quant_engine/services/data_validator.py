import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from loguru import logger
from ..data.cache import RedisCache


class DataValidator:
    """数据验证模块 - 确保数据来源和计算正确性"""

    def __init__(self, cache: RedisCache):
        self.cache = cache
        self.validation_history = []

    def validate_stock_data(self, df: pd.DataFrame, code: str) -> Tuple[bool, List[str]]:
        errors = []
        if df is None or df.empty:
            errors.append(f"{code}: 数据为空")
            return False, errors
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            errors.append(f"{code}: 缺少列 {missing_cols}")
        if df.isnull().any().any():
            null_cols = df.columns[df.isnull().any()].tolist()
            errors.append(f"{code}: 存在空值 {null_cols}")
        if 'close' in df.columns:
            if (df['close'] <= 0).any():
                errors.append(f"{code}: 存在非正数收盘价")
        if all(col in df.columns for col in ['high', 'low', 'close']):
            invalid_hl = df[df['high'] < df['low']]
            if not invalid_hl.empty:
                errors.append(f"{code}: 最高价低于最低价")
            invalid_close = df[(df['close'] > df['high']) | (df['close'] < df['low'])]
            if not invalid_close.empty:
                errors.append(f"{code}: 收盘价超出最高最低价范围")
        if 'close' in df.columns and len(df) > 1:
            returns = df['close'].pct_change().dropna()
            extreme_returns = returns[abs(returns) > 0.2]
            if not extreme_returns.empty:
                errors.append(f"{code}: 存在异常涨跌幅 {len(extreme_returns)} 条")
        is_valid = len(errors) == 0
        return is_valid, errors

    def validate_market_data(self, market_data: Dict) -> Tuple[bool, List[str]]:
        errors = []
        required_fields = ['total', 'up', 'down', 'flat', 'avg_change']
        missing_fields = [f for f in required_fields if f not in market_data]
        if missing_fields:
            errors.append(f"市场数据缺少字段: {missing_fields}")
        if 'total' in market_data and 'up' in market_data and 'down' in market_data:
            total = market_data['total']
            up = market_data['up']
            down = market_data['down']
            flat = market_data.get('flat', 0)
            if up + down + flat != total:
                errors.append(f"涨跌平之和不等于总数: {up}+{down}+{flat} != {total}")
        if 'avg_change' in market_data:
            avg_change = market_data['avg_change']
            if abs(avg_change) > 10:
                errors.append(f"平均涨跌幅异常: {avg_change}")
        is_valid = len(errors) == 0
        return is_valid, errors

    def validate_signal(self, signal_dict: Dict) -> Tuple[bool, List[str]]:
        errors = []
        required_fields = ['code', 'name', 'action', 'score', 'position']
        missing_fields = [f for f in required_fields if f not in signal_dict]
        if missing_fields:
            errors.append(f"信号缺少字段: {missing_fields}")
        if 'action' in signal_dict:
            valid_actions = ['BUY', 'WATCH', 'SKIP']
            if signal_dict['action'] not in valid_actions:
                errors.append(f"无效的信号类型: {signal_dict['action']}")
        if 'score' in signal_dict:
            score = signal_dict['score']
            if not (0 <= score <= 100):
                errors.append(f"评分超出范围: {score}")
        if 'position' in signal_dict:
            position = signal_dict['position']
            if not (0 <= position <= 1):
                errors.append(f"仓位超出范围: {position}")
        is_valid = len(errors) == 0
        return is_valid, errors

    def validate_calculation(self, calc_name: str, inputs: Dict, output: float, expected_range: Tuple[float, float] = None) -> Tuple[bool, str]:
        if expected_range:
            min_val, max_val = expected_range
            if not (min_val <= output <= max_val):
                return False, f"{calc_name} 结果 {output} 超出预期范围 [{min_val}, {max_val}]"
        if np.isnan(output) or np.isinf(output):
            return False, f"{calc_name} 结果无效: {output}"
        return True, f"{calc_name} 验证通过"

    def log_validation(self, module: str, is_valid: bool, errors: List[str]):
        record = {
            'module': module,
            'is_valid': is_valid,
            'errors': errors,
            'timestamp': datetime.now().isoformat(),
        }
        self.validation_history.append(record)
        if len(self.validation_history) > 1000:
            self.validation_history = self.validation_history[-1000:]
        if not is_valid:
            for error in errors:
                logger.warning(f"[DataValidator] {module}: {error}")
        cache_key = f"validation:{module}"
        self.cache.set(cache_key, record, ttl=3600)

    def get_validation_report(self) -> Dict:
        total = len(self.validation_history)
        failed = len([r for r in self.validation_history if not r['is_valid']])
        by_module = {}
        for record in self.validation_history:
            module = record['module']
            if module not in by_module:
                by_module[module] = {'total': 0, 'failed': 0, 'errors': []}
            by_module[module]['total'] += 1
            if not record['is_valid']:
                by_module[module]['failed'] += 1
                by_module[module]['errors'].extend(record['errors'])
        return {
            'total_checks': total,
            'failed_checks': failed,
            'success_rate': (total - failed) / total * 100 if total > 0 else 100,
            'by_module': by_module,
            'last_check': self.validation_history[-1]['timestamp'] if self.validation_history else None,
        }