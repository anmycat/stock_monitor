from typing import Dict, List

import pandas as pd
from loguru import logger

from ..data.repositories import MarketDataRepository
from ..models.market import MarketState, MarketType
from ..strategies.dispatcher import StrategyDispatcher
from .metrics import calculate_metrics


class BacktestEngine:
    def __init__(self, datacenter: MarketDataRepository, initial_capital: float = 100000,
                 commission_rate: float = 0.0003, stop_loss: float = 0.05,
                 take_profit: float = 0.15):
        self.datacenter = datacenter
        self.dispatcher = StrategyDispatcher(datacenter)
        # P4 净化：原构造的 PositionManager/RiskManager 实例全文从未使用（engine 用
        # stop_loss/take_profit 参数直算），且构造无 IO 副作用——直接切除而非注入
        # （注入永不使用的实例即屎山）。依赖宪法见 prd/15 §2.5。
        self.initial_capital = initial_capital
        self.commission_rate = commission_rate
        self.stop_loss = stop_loss
        self.take_profit = take_profit

    def run(self, codes: List[str], start_date: str, end_date: str, initial_capital: float = None) -> Dict:
        starting_capital = self.initial_capital if initial_capital is None else initial_capital
        trades = []
        equity_curve = [starting_capital]
        current_capital = starting_capital
        for code in codes:
            try:
                df = self.datacenter.get_daily_data(code, 250)
                if df is None or df.empty:
                    continue
                df['code'] = code
                df['name'] = code
                code_trades, code_equity = self._backtest_stock(df, start_date, end_date, current_capital)
                trades.extend(code_trades)
                if code_equity:
                    current_capital = code_equity[-1]
                    equity_curve.extend(code_equity[1:])
            except Exception as e:  # noqa: BLE001 单项跳过
                logger.error(f"Backtest {code} error: {e}")
        metrics = calculate_metrics(trades, equity_curve)
        return {
            'trades': trades,
            'equity_curve': equity_curve,
            'metrics': metrics,
            'start_date': start_date,
            'end_date': end_date,
            'codes': codes,
        }

    def _backtest_stock(self, df: pd.DataFrame, start_date: str, end_date: str, capital: float) -> tuple:
        trades = []
        equity = []
        position = 0
        entry_price = 0
        entry_date = ""
        window = 60
        for i in range(window, len(df)):
            current_date = df['trade_date'].iloc[i]
            if current_date < start_date or current_date > end_date:
                continue
            historical = df.iloc[max(0, i-window):i+1]
            market = self._create_mock_market(historical)
            signal = self.dispatcher.dispatch(historical, market)
            if position > 0:
                current_price = df['close'].iloc[i]
                pnl_pct = (current_price - entry_price) / entry_price
                if pnl_pct < -self.stop_loss:
                    sell_value = position * current_price
                    commission = sell_value * self.commission_rate
                    pnl = position * (current_price - entry_price) - commission
                    capital += sell_value - commission
                    trades.append({
                        'code': df['code'].iloc[0],
                        'entry_date': entry_date,
                        'exit_date': current_date,
                        'entry_price': entry_price,
                        'exit_price': current_price,
                        'pnl': pnl,
                        'pnl_pct': pnl_pct,
                        'reason': 'stop_loss',
                    })
                    position = 0
                    entry_price = 0
                elif pnl_pct > self.take_profit:
                    sell_value = position * current_price
                    commission = sell_value * self.commission_rate
                    pnl = position * (current_price - entry_price) - commission
                    capital += sell_value - commission
                    trades.append({
                        'code': df['code'].iloc[0],
                        'entry_date': entry_date,
                        'exit_date': current_date,
                        'entry_price': entry_price,
                        'exit_price': current_price,
                        'pnl': pnl,
                        'pnl_pct': pnl_pct,
                        'reason': 'take_profit',
                    })
                    position = 0
                    entry_price = 0
            if position == 0 and signal and signal.action.value == 'BUY':
                price = df['close'].iloc[i]
                size = capital * signal.position / price
                buy_value = size * price
                commission = buy_value * self.commission_rate
                if buy_value + commission <= capital:
                    position = size
                    entry_price = price
                    entry_date = current_date
                    capital -= buy_value + commission
            equity.append(capital + position * df['close'].iloc[i])
        if position > 0:
            final_price = df['close'].iloc[-1]
            sell_value = position * final_price
            commission = sell_value * self.commission_rate
            pnl = position * (final_price - entry_price) - commission
            capital += sell_value - commission
            trades.append({
                'code': df['code'].iloc[0],
                'entry_date': entry_date,
                'exit_date': df['trade_date'].iloc[-1],
                'entry_price': entry_price,
                'exit_price': final_price,
                'pnl': pnl,
                'pnl_pct': (final_price - entry_price) / entry_price,
                'reason': 'end_of_period',
            })
            position = 0
        return trades, equity

    def _create_mock_market(self, df: pd.DataFrame) -> MarketState:
        if len(df) < 20:
            return MarketState(
                type=MarketType.SIDEWAYS,
                score=50,
                trend=0.5,
                volume_ratio=1.0,
                breadth=0.5,
                timestamp=pd.Timestamp.now(),
            )
        ma5 = df['close'].rolling(5).mean().iloc[-1]
        ma10 = df['close'].rolling(10).mean().iloc[-1]
        ma20 = df['close'].rolling(20).mean().iloc[-1]
        trend = 0
        if ma5 > ma10 > ma20:
            trend = 0.8
            market_type = MarketType.BULL
        elif ma5 > ma10:
            trend = 0.6
            market_type = MarketType.BULL_WEAK
        elif ma5 < ma10 < ma20:
            trend = 0.2
            market_type = MarketType.BEAR
        else:
            trend = 0.4
            market_type = MarketType.SIDEWAYS
        return MarketState(
            type=market_type,
            score=trend * 100,
            trend=trend,
            volume_ratio=1.0,
            breadth=0.5,
            timestamp=pd.Timestamp.now(),
        )
