"""盘中日K 实时修正（D65，2026-11-06）

**为什么需要**：日K 各源对「当日未完成 bar」的处理不一致——
- 腾讯 `fqkline`（第一线）**盘中含**当日未完成 bar → `close.iloc[-1]` = 盘中实时价
- 新浪 `getKLineData` scale=240（第二线）**盘中不含**（当日 bar 收盘后才出现）
  → `close.iloc[-1]` = **昨收**

降级到新浪时，**均线/RSI/MACD 全部基于 T-1**，当日跳空完全不在评分视野。
实测（2026-11-06）：当日 12 个 BUY 中 **4 个（33%）是「跳空 ≥3% 下行的票」**——
大晟文化 600892 跳空 -7.00% 仍给出 82 分 S 级 BUY，当日收盘跌停 -9.92%。

**本模块做什么**：用实时报价把日K 的最后一根对齐到当下——
- 末根已是**当日** → 用最新报价刷新 close/high/low/volume（腾讯路径的常规情形）
- 末根是**昨日**（降级源） → **追加**一根当日 bar（open/high/low/close/volume 取自实时报价）
  → 指标首次反映当日行情（跳空会直接反映在均线排列与 MACD 上）

**边界（诚实声明）**：
1. 本模块**只在实时报价可得时**生效；取不到则原样返回（不猜、不造数）
2. 修正的是**指标输入**，因此**会改变信号**（属信号逻辑变更）——已配 A/B 影子对比取证
3. 历史完成日上该修正是 **no-op**（当日 bar 已是收盘价）→ 半年窗口无法直接验证，
   真实证据只能来自实盘交易日的 A/B 记录（Redis `intraday_kline:ab`）
4. 仅用于**实时扫描链路**（`SignalGenerator._analyze_stock`）；回测/研究脚本不经过此处，
   历史口径保持不变
"""
from __future__ import annotations

import math

import pandas as pd
from loguru import logger

# 需要对齐的列（与日K schema 一致）
_KLINE_COLS = ('date', 'open', 'high', 'low', 'close', 'volume')


def _num(value, default: float = 0.0) -> float:
    try:
        f = float(value)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return default


def _is_adjacent_session(last_date: str, today: str) -> bool:
    """末根日期是否与今天相邻（今天或上一交易日）

    用交易日历判定；日历不可用时降级为「工作日相邻」近似（偏保守：
    拿不准就不修正）。

    为什么必须有这道护栏：研究/回归脚本用**历史日期**的数据跑信号，
    若按墙钟今天追加 bar，历史序列会凭空多一根 → 指标被污染 → 回归结论失真。
    """
    from datetime import datetime as _dt

    from .calendar import get_trading_days_between
    try:
        start = _dt.strptime(last_date, '%Y-%m-%d')
        end = _dt.strptime(today, '%Y-%m-%d')
    except (TypeError, ValueError):
        return False
    if start > end:
        return False
    try:
        sessions = get_trading_days_between(start, end)
    except Exception as e:  # noqa: BLE001 日历异常 → 保守不修正
        logger.debug(f"[D65] calendar check failed: {e}")
        return False
    # sessions 含首尾：末根==今天 → 1 个；末根==上一交易日 → 2 个；更旧 → ≥3
    return len(sessions) <= 2


def apply_intraday_correction(df: pd.DataFrame, quote: dict | None,
                               today: str | None = None) -> tuple[pd.DataFrame, dict]:
    """用实时报价修正日K 最后一根

    Args:
        df: 日K DataFrame（须含 date/open/high/low/close/volume）
        quote: 实时报价 dict（code/name/price/change_pct/volume + open/high/low/prev_close）
        today: 今日日期 YYYY-MM-DD；缺省用本机当天

    Returns:
        (修正后的 df, 修正信息 dict)
        修正信息：{applied: bool, mode: 'update'|'append'|'none', price, base_close,
                   gap_pct, reason}
    """
    info = {'applied': False, 'mode': 'none', 'reason': ''}
    if df is None or df.empty or 'close' not in df.columns:
        info['reason'] = 'empty_df'
        return df, info
    if not quote:
        info['reason'] = 'no_quote'
        return df, info

    price = _num(quote.get('price'))
    if price <= 0:
        info['reason'] = 'invalid_price'
        return df, info

    if today is None:
        from datetime import datetime
        today = datetime.now().strftime('%Y-%m-%d')

    try:
        last_date = str(df['date'].iloc[-1])
    except (IndexError, KeyError, TypeError):
        info['reason'] = 'no_date_col'
        return df, info

    base_close = _num(df['close'].iloc[-1])
    info['price'] = price
    info['base_close'] = base_close
    if base_close > 0:
        info['gap_pct'] = round(price / base_close - 1, 6)

    # ---- 情形 1：末根已是当日 → 刷新 ----
    if last_date == today:
        try:
            out = df.copy()
            idx = out.index[-1]
            out.loc[idx, 'close'] = price
            if 'high' in out.columns:
                out.loc[idx, 'high'] = max(_num(out['high'].iloc[-1]), price)
            if 'low' in out.columns:
                out.loc[idx, 'low'] = min(_num(out['low'].iloc[-1]), price) \
                    if _num(out['low'].iloc[-1]) > 0 else price
            qv = _num(quote.get('volume'))
            if qv > 0 and 'volume' in out.columns:
                out.loc[idx, 'volume'] = max(_num(out['volume'].iloc[-1]), qv)
            info['applied'] = True
            info['mode'] = 'update'
            return out, info
        except Exception as e:  # noqa: BLE001 修正失败不得影响信号主流程
            info['reason'] = f'update_failed:{type(e).__name__}'
            return df, info

    # ---- 情形 2：末根是上一交易日 → 追加当日 bar ----
    if last_date < today:
        # 陈旧数据护栏：末根必须与今天相邻（今天或上一交易日）。
        # 否则不追加——否则历史/陈旧数据集会凭空多出一根「今天」的 bar，
        # 指标被污染（研究回归脚本跑历史日期时尤其危险）。
        if not _is_adjacent_session(last_date, today):
            info['reason'] = f'stale_data:{last_date}'
            logger.debug(f"[D65] intraday correction skipped: {info['reason']}")
            return df, info
        try:
            out = df.copy()
            q_open = _num(quote.get('open')) or _num(quote.get('prev_close'))
            q_high = _num(quote.get('high'))
            q_low = _num(quote.get('low'))
            q_vol = _num(quote.get('volume'))
            if q_open <= 0:
                q_open = price
            row = {
                'date': today,
                'open': q_open,
                'high': max(q_high, price, q_open) if q_high > 0 else max(price, q_open),
                'low': min(q_low, price, q_open) if q_low > 0 else min(price, q_open),
                'close': price,
                'volume': q_vol,
            }
            for col in out.columns:
                if col not in row:
                    row[col] = None
            row = {c: row.get(c) for c in out.columns}
            out = pd.concat([out, pd.DataFrame([row])], ignore_index=True)
            info['applied'] = True
            info['mode'] = 'append'
            return out, info
        except Exception as e:  # noqa: BLE001
            info['reason'] = f'append_failed:{type(e).__name__}'
            return df, info

    # ---- 情形 3：末根日期在未来（异常）→ 不动 ----
    info['reason'] = f'last_date_ahead:{last_date}'
    logger.debug(f"[D65] intraday correction skipped: {info['reason']}")
    return df, info


def describe(info: dict) -> str:
    """把修正信息渲染成一行日志（便于 grep）"""
    if not info.get('applied'):
        return f"未修正({info.get('reason', '')})"
    gap = info.get('gap_pct')
    gap_s = f'{gap:+.2%}' if isinstance(gap, float) else '—'
    return (f"{info.get('mode')} 现价 {info.get('price')} "
            f"基准 {info.get('base_close')} 偏离 {gap_s}")
