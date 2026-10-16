import pandas as pd
from typing import List, Tuple
from loguru import logger


class AntiSignalFilter:
    def __init__(self):
        self.high_position_threshold = 0.92  # 高位阈值
        self.consecutive_up_limit = 8       # 连涨天数
        self.fake_breakout_days = 3

    def filter(self, df: pd.DataFrame, indicators: dict) -> Tuple[bool, List[str]]:
        reasons = []
        if self._is_high_position(df):
            reasons.append("高位过滤")
        if self._is_weak_market(df):
            reasons.append("弱势市场过滤")
        if self._is_fake_breakout(df):
            reasons.append("假突破过滤")
        if self._is_consecutive_up(df):
            reasons.append("连涨过滤")
        # 严格模式: 任何一个过滤条件触发即拒绝
        return len(reasons) == 0, reasons

    def _is_high_position(self, df: pd.DataFrame) -> bool:
        if len(df) < 60:  # 减少数据要求
            return False
        current = df['close'].iloc[-1]
        high_60 = df['high'].rolling(60).max().iloc[-1]
        low_60 = df['low'].rolling(60).min().iloc[-1]
        if high_60 == low_60:
            return False
        position = (current - low_60) / (high_60 - low_60)
        return position > self.high_position_threshold

    def _is_weak_market(self, df: pd.DataFrame) -> bool:
        if len(df) < 20:
            return False
        ma5 = df['close'].rolling(5).mean().iloc[-1]
        ma10 = df['close'].rolling(10).mean().iloc[-1]
        ma20 = df['close'].rolling(20).mean().iloc[-1]
        # 只有完全空头排列且价格低于均线才算弱势
        return ma5 < ma10 < ma20 and df['close'].iloc[-1] < ma5

    def _is_fake_breakout(self, df: pd.DataFrame) -> bool:
        if len(df) < 5:
            return False
        recent = df.tail(self.fake_breakout_days)
        high_max = recent['high'].max()
        close_last = df['close'].iloc[-1]
        # 假突破判断: 收盘低于近期高点的93%
        return close_last < high_max * 0.93

    def _is_consecutive_up(self, df: pd.DataFrame) -> bool:
        if len(df) < self.consecutive_up_limit:
            return False
        recent = df.tail(self.consecutive_up_limit)
        return all(recent['close'].diff().iloc[1:] > 0)
