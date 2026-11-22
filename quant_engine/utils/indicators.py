"""技术指标计算 - 包含基础指标和高级特征

D78：三处 Python 级逐元素循环（Wilder 平滑 / 平台天数 / 连续涨跌）已下沉到
本项目自有 Rust 扩展 `qerust`（`rust_accel/`），由 `utils.rust_accel` 统一调度；
扩展不可用时自动回退到本文件内的纯 Python 实现，两者逐位等价（专项测试 + D41 双跑）。
"""

import numpy as np
import pandas as pd

from . import rust_accel


def _wilder_smooth(series: pd.Series, period: int) -> float:
    """Wilder 平滑（RSI/ATR 标准口径）：首值为前 period 期简单均值，
    其后 a=1/period 指数平滑。返回最新值（标量）。

    与 pandas ``ewm(alpha=1/period, adjust=False)`` 数值等价，但显式用
    前 period 期 SMMA 作种子，避免 EWM 起始值偏差；数据不足 period+1 时
    退回整体均值（与旧行为兼容的兜底，仅在极短序列触发）。

    D78：实现下沉到 Rust（`qerust.wild_smooth`），回退为纯 Python，逐位一致。
    """
    return rust_accel.wild_smooth(series, period)



def calculate_indicators(df: pd.DataFrame) -> dict:
    """计算全部技术指标（基础 + 高级特征）"""
    if len(df) < 20:
        return {}
    indicators = {}

    # ── 基础均线 ──
    indicators['ma5'] = df['close'].rolling(5).mean().iloc[-1]
    indicators['ma10'] = df['close'].rolling(10).mean().iloc[-1]
    indicators['ma20'] = df['close'].rolling(20).mean().iloc[-1]
    indicators['ma60'] = df['close'].rolling(60).mean().iloc[-1] if len(df) >= 60 else None

    # ── RSI（S3/N-07：统一为标准 Wilder RSI，EMA 型 alpha=1/14）──
    # 原实现用简单移动平均 (rolling.mean) 平滑涨跌幅 → 与主流软件（通达信/文华）
    # 的 Wilder 口径不一致。2026-11-21 用户裁定 S3 统一为 Wilder：
    # 首值用前 14 期简单均值，其后按 alpha=1/14 指数平滑（等价 Wilder 递归）。
    # D78：diff → gain/loss → 平滑 → RSI 整链下沉到 Rust（回退纯 Python，逐位一致）。
    indicators['rsi'] = rust_accel.wilder_rsi(df['close'], 14)

    # ── MACD ──
    ema12 = df['close'].ewm(span=12).mean()
    ema26 = df['close'].ewm(span=26).mean()
    macd_line = ema12 - ema26
    indicators['macd'] = macd_line.iloc[-1]
    indicators['macd_signal'] = macd_line.ewm(span=9).mean().iloc[-1]
    indicators['macd_hist'] = (macd_line - macd_line.ewm(span=9).mean()).iloc[-1]

    # ── KDJ ──
    high14 = df['high'].rolling(14).max()
    low14 = df['low'].rolling(14).min()
    denom = high14 - low14
    stoch_k = ((df['close'] - low14) / denom * 100).iloc[-1] if denom.iloc[-1] != 0 else 50
    indicators['stoch_k'] = stoch_k

    # ── 量比 ──
    vol_ma20 = df['volume'].rolling(20).mean().iloc[-1]
    indicators['volume_ratio'] = df['volume'].iloc[-1] / vol_ma20 if vol_ma20 > 0 else 1.0

    # ── 高级特征（借鉴 AlphaSift） ──
    close = df['close'].iloc[-1]
    indicators.update(_advanced_features(df, close))

    return indicators


def _advanced_features(df: pd.DataFrame, close: float) -> dict:
    """高级K线特征"""
    feat = {}
    n = len(df)

    # 突破强度: 当前价相对N日最高价的距离 + 成交量放大倍数
    for period in [20, 60]:
        if n >= period:
            high_n = df['high'].rolling(period).max().iloc[-1]
            feat[f'breakout_strength_{period}d'] = (close / high_n - 1) * 100 if high_n > 0 else 0
            vol_n = df['volume'].rolling(period).mean().iloc[-1]
            feat[f'vol_amplify_{period}d'] = df['volume'].iloc[-1] / vol_n if vol_n > 0 else 1.0

    # 回踩距离: 当前价距离最近均线的百分比
    for ma_name, ma_val in [('ma5', 'ma5'), ('ma10', 'ma10'), ('ma20', 'ma20')]:
        ma = df['close'].rolling({'ma5': 5, 'ma10': 10, 'ma20': 20}[ma_name]).mean().iloc[-1]
        feat[f'pullback_{ma_name}'] = (close / ma - 1) * 100 if ma > 0 else 0

    # 平台天数: 最近N日中价格在X%范围内波动的连续天数
    feat['consolidation_days'] = _consolidation_days(df, pct_range=0.03, lookback=30)

    # ATR (Average True Range)
    if n >= 14:
        high = df['high']
        low = df['low']
        prev_close = df['close'].shift(1)
        tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
        feat['atr'] = tr.rolling(14).mean().iloc[-1]
        feat['atr_pct'] = feat['atr'] / close * 100 if close > 0 else 0

    # 布林带位置: 当前价格在布林带中的相对位置 (0=下轨, 1=上轨)
    if n >= 20:
        ma20 = df['close'].rolling(20).mean().iloc[-1]
        std20 = df['close'].rolling(20).std().iloc[-1]
        upper = ma20 + 2 * std20
        lower = ma20 - 2 * std20
        band_width = upper - lower
        feat['bollinger_position'] = (close - lower) / band_width if band_width > 0 else 0.5
        feat['bollinger_upper'] = upper
        feat['bollinger_lower'] = lower

    # OBV (On Balance Volume) — 归一化
    if n >= 10:
        obv = _obv(df)
        feat['obv_slope'] = obv.iloc[-5:].mean() - obv.iloc[-10:-5].mean() if n >= 10 else 0

    # 涨跌幅统计
    pct = df['close'].pct_change()
    feat['volatility_5d'] = pct.tail(5).std() * 100 if n >= 5 else 0
    feat['volatility_20d'] = pct.tail(20).std() * 100 if n >= 20 else 0

    # 连续涨跌天数
    feat['consecutive_up'] = _consecutive_direction(df)

    return feat


def _consolidation_days(df: pd.DataFrame, pct_range: float = 0.03, lookback: int = 30) -> int:
    """计算平台天数：价格在 pct_range 范围内波动的连续天数（D78：Rust 优先）"""
    n = min(lookback, len(df))
    if n < 2:
        return 0
    return rust_accel.consolidation_days(
        df['close'].iloc[-n:].to_numpy(dtype='float64'), pct_range, lookback
    )


def _obv(df: pd.DataFrame) -> pd.Series:
    """On Balance Volume"""
    direction = np.sign(df['close'].diff())
    direction.iloc[0] = 0
    return (direction * df['volume']).cumsum()


def _consecutive_direction(df: pd.DataFrame) -> int:
    """计算连续涨跌天数（正=连涨，负=连跌；D78：Rust 优先）"""
    pct = df['close'].pct_change().dropna()
    if pct.empty:
        return 0
    return rust_accel.consecutive_direction(pct.to_numpy(dtype='float64'))
