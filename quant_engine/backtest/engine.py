
from __future__ import annotations

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

    def run(self, codes: list[str], start_date: str, end_date: str, initial_capital: float | None = None) -> dict:
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
        """单票回测。

        S1/I-12（2026-11-21 用户裁定）两处修正：

        1. **日期列归一**：真实日K（腾讯/新浪/东财/eltdx）输出列是 ``date``，
           原实现硬用 ``df['trade_date']``，对真实数据直接 KeyError → 每票
           静默失败、回测端点实际返回空。此处 ``date`` / ``trade_date`` 兼容。
        2. **未来函数修复**：原实现用「截至今日收盘」的数据出信号，又用
           **同一根 bar 的当日收盘价**当场成交——决策价与成交价重合＝系统性
           乐观偏差。现改为「收盘决策 → 次日开盘成交」（挂单次日成交）。开关
           ``BACKTEST_FILL_NEXT_OPEN`` 置 False 时保留旧的同日收盘成交，仅用于
           A/B 量化乐观偏差量，不影响任何实盘信号（回测是纯分析工具）。
        """
        from ..config import get_settings
        next_open_fill = bool(getattr(get_settings(), 'BACKTEST_FILL_NEXT_OPEN', True))

        date_col = 'trade_date' if 'trade_date' in df.columns else 'date'
        dates = df[date_col]
        has_open = 'open' in df.columns
        code = df['code'].iloc[0] if 'code' in df.columns else ''
        window = 60

        trades: list[dict] = []
        equity: list[float] = []
        # 用 state dict + 闭包统一买卖，避免多头 hand-off 逻辑在分支里重复
        state = {
            'position': 0.0,
            'entry_price': 0.0,
            'entry_date': '',
            'capital': float(capital),
            # next_open 模式的挂单：上一交易日收盘挂、今日开盘成交
            'pending_entry': False,
            'pending_frac': 0.0,
            'pending_exit': None,  # 'stop_loss' / 'take_profit'
            'last_close': 0.0,
            'last_date': '',
        }

        def _sell(price: float, date, reason: str) -> None:
            pos = state['position']
            if pos <= 0 or state['entry_price'] <= 0:
                return
            sell_value = pos * price
            commission = sell_value * self.commission_rate
            pnl = pos * (price - state['entry_price']) - commission
            state['capital'] += sell_value - commission
            trades.append({
                'code': code,
                'entry_date': state['entry_date'],
                'exit_date': date,
                'entry_price': state['entry_price'],
                'exit_price': price,
                'pnl': pnl,
                'pnl_pct': (price - state['entry_price']) / state['entry_price'],
                'reason': reason,
            })
            state['position'] = 0.0
            state['entry_price'] = 0.0
            state['entry_date'] = ''

        def _buy(price: float, date, frac: float) -> None:
            if price <= 0 or frac <= 0:
                return
            size = state['capital'] * frac / price
            buy_value = size * price
            commission = buy_value * self.commission_rate
            if buy_value + commission <= state['capital']:
                state['position'] = size
                state['entry_price'] = price
                state['entry_date'] = date
                state['capital'] -= buy_value + commission

        for i in range(window, len(df)):
            current_date = dates.iloc[i]
            if current_date > end_date:
                break  # 越过回测区间即停（遗留持仓由期末清算处理）
            close_i = float(df['close'].iloc[i])
            open_i = float(df['open'].iloc[i]) if has_open else close_i

            # ① next_open 模式：先执行「上一交易日收盘」挂出的委托，成交于今日开盘
            if next_open_fill:
                if state['pending_exit'] is not None:
                    _sell(open_i, current_date, state['pending_exit'])
                    state['pending_exit'] = None
                if state['pending_entry']:
                    _buy(open_i, current_date, state['pending_frac'])
                    state['pending_entry'] = False
                    state['pending_frac'] = 0.0

            if current_date < start_date:
                continue  # 区间前不决策、不计权益（也不会有挂单）
            state['last_close'] = close_i
            state['last_date'] = current_date

            historical = df.iloc[max(0, i - window):i + 1]
            market = self._create_mock_market(historical)
            signal = self.dispatcher.dispatch(historical, market)

            if next_open_fill:
                # ② 收盘评估止损/止盈 → 挂次日开盘卖出
                if state['position'] > 0 and state['pending_exit'] is None:
                    pnl_pct = (close_i - state['entry_price']) / state['entry_price']
                    if pnl_pct < -self.stop_loss:
                        state['pending_exit'] = 'stop_loss'
                    elif pnl_pct > self.take_profit:
                        state['pending_exit'] = 'take_profit'
                # ③ 空仓且有 BUY 信号 → 挂次日开盘买入
                if (state['position'] == 0 and state['pending_exit'] is None
                        and signal and signal.action.value == 'BUY'):
                    state['pending_entry'] = True
                    state['pending_frac'] = float(signal.position)
            else:
                # legacy：当日收盘即时成交（仅用于 A/B 对比乐观偏差量）
                if state['position'] > 0:
                    pnl_pct = (close_i - state['entry_price']) / state['entry_price']
                    if pnl_pct < -self.stop_loss:
                        _sell(close_i, current_date, 'stop_loss')
                    elif pnl_pct > self.take_profit:
                        _sell(close_i, current_date, 'take_profit')
                if (state['position'] == 0 and signal
                        and signal.action.value == 'BUY'):
                    _buy(close_i, current_date, float(signal.position))

            equity.append(state['capital'] + state['position'] * close_i)

        # 期末清算：用区间内最后一日值（而非全序列末根，避免越过 end_date 的错位）
        if state['position'] > 0:
            final_price = state['last_close'] or float(df['close'].iloc[-1])
            _sell(final_price, state['last_date'] or dates.iloc[-1], 'end_of_period')
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
