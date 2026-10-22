
import pandas as pd

from ..models.market import MarketState
from ..utils.indicators import calculate_indicators
from .filters import AntiSignalFilter
from .gates import GateModel


class StrategyEngine:
    def __init__(self, datacenter=None):
        self.anti_filter = AntiSignalFilter()
        self.gate_model = GateModel(datacenter)
        self.datacenter = datacenter

    def process(self, df: pd.DataFrame, market: MarketState, code: str = None) -> dict:
        indicators = calculate_indicators(df)
        filter_pass, filter_reasons = self.anti_filter.filter(df, indicators)
        if not filter_pass:
            return {
                'passed': False,
                'stage': 'filter',
                'reasons': filter_reasons,
                'indicators': indicators,
            }
        gate_pass, gate_reasons = self.gate_model.check(df, indicators, code)
        if not gate_pass:
            return {
                'passed': False,
                'stage': 'gate',
                'reasons': gate_reasons,
                'indicators': indicators,
            }
        return {
            'passed': True,
            'stage': 'all',
            'reasons': [],
            'indicators': indicators,
        }