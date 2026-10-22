
import pandas as pd
from loguru import logger


class GateModel:
    def __init__(self, datacenter=None):
        self.trend_threshold = 0.5
        self.fund_flow_threshold = 0.3
        self.datacenter = datacenter

    def check(self, df: pd.DataFrame, indicators: dict, code: str = None) -> tuple[bool, list[str]]:
        reasons = []
        trend_pass, trend_reason = self._check_trend(df, indicators)
        if not trend_pass:
            reasons.append(f"趋势不通过: {trend_reason}")
        fund_pass, fund_reason = self._check_fund_flow(df, indicators)
        if not fund_pass:
            reasons.append(f"资金不通过: {fund_reason}")
        trigger_pass, trigger_reason = self._check_trigger(df, indicators)
        if not trigger_pass:
            reasons.append(f"触发不通过: {trigger_reason}")
        if self.datacenter and code:
            minute_pass, minute_reason = self._check_minute_trigger(code, indicators)
            if not minute_pass:
                reasons.append(f"分钟触发不通过: {minute_reason}")
        # 严格模式: 任何一个门控不通过即拒绝
        return len(reasons) == 0, reasons

    def _check_trend(self, df: pd.DataFrame, indicators: dict) -> tuple[bool, str]:
        if len(df) < 20:
            return False, "数据不足"
        ma5 = df['close'].rolling(5).mean().iloc[-1]
        ma10 = df['close'].rolling(10).mean().iloc[-1]
        ma20 = df['close'].rolling(20).mean().iloc[-1]
        current = df['close'].iloc[-1]
        # 放宽趋势条件：价格高于MA5或MA5>MA10即可
        if current > ma5 or ma5 > ma10:
            return True, ""
        if len(df) >= 60:
            ma60 = df['close'].rolling(60).mean().iloc[-1]
            if ma20 < ma60:
                return False, "MA20低于MA60"
        return True, ""

    def _check_fund_flow(self, df: pd.DataFrame, indicators: dict) -> tuple[bool, str]:
        if 'fund_flow' in indicators:
            flow = indicators['fund_flow']
            if flow < self.fund_flow_threshold:
                return False, f"资金流入不足: {flow:.2f}"
        # 成交量条件: 5日均量不低于20日均量的40%
        if 'volume' in df.columns:
            vol_ma5 = df['volume'].rolling(5).mean().iloc[-1]
            vol_ma20 = df['volume'].rolling(20).mean().iloc[-1]
            if vol_ma5 < vol_ma20 * 0.4:
                return False, "成交量萎缩"
        return True, ""

    def _check_trigger(self, df: pd.DataFrame, indicators: dict) -> tuple[bool, str]:
        if len(df) < 5:
            return False, "数据不足"
        last_5 = df.tail(5)
        if last_5['close'].is_monotonic_decreasing:
            return False, "连续下跌"
        rsi = indicators.get('rsi', 50)
        if rsi > 88:  # RSI超买阈值
            return False, f"RSI超买: {rsi:.1f}"
        return True, ""

    def _check_minute_trigger(self, code: str, indicators: dict) -> tuple[bool, str]:
        if not self.datacenter:
            return True, "无数据源"
        try:
            minute_df = self.datacenter.get_minute_data(code, "5")
            if minute_df is None or minute_df.empty or len(minute_df) < 10:
                return True, "分钟数据不足"
            last_close = minute_df['close'].iloc[-1]
            prev_close = minute_df['close'].iloc[-2]
            change_pct = (last_close - prev_close) / prev_close * 100
            if change_pct > 2:
                return True, "分钟突破"
            vol_last = minute_df['volume'].iloc[-1]
            vol_avg = minute_df['volume'].rolling(10).mean().iloc[-1]
            if vol_avg > 0 and vol_last / vol_avg > 1.5:
                return True, "分钟放量"
            last_3 = minute_df.tail(3)
            if last_3['close'].is_monotonic_increasing:
                return True, "分钟连续上涨"
            return True, "分钟触发通过"
        except Exception as e:  # noqa: BLE001 降级默认
            logger.warning(f"Minute trigger check failed for {code}: {e}")
            return True, "分钟检查异常"
