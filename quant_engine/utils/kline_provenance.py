"""日K 数据血缘与新鲜度标签（D66，2026-11-06）

**要解决的问题**：ECS 日志与 D64/D65 共同暴露一类隐性错误——
日K 各源对「当日未完成 bar」处理不一致（腾讯盘中含、新浪盘中不含），
下游一律用 ``df.iloc[-1]`` 隐式假设「末根就是今天」，于是降级源的
**昨收被当成今天的收盘价**用于信号价、涨跌幅、跳空判断。

本模块不改变任何 DataFrame 的列与数值，只把「末根到底是哪一天」显式
写进 ``df.attrs``（pandas 官方元数据位）。这是刻意设计：

- 指标/趋势计算（MA/RSI/MACD/回测）读的是列，attrs 完全不参与
  → **历史序列原样可用，不受封存/标签影响**（用户口径：趋势类除外）
- 只有需要判断「这是不是今天的数据」的出口才读标签
  → 从根上杜绝「昨日数据被当作今日数据使用」

**标签字段**（统一前缀 ``kline_``，避免与 source_manager 的 ``_stale``
等历史字段冲突）：

================== ====================================================
字段               含义
================== ====================================================
kline_as_of        末根 bar 的日期 YYYY-MM-DD（数据实际截止日）
kline_source       产出该 DataFrame 的数据源名
kline_bar_count    行数
kline_sealed       bool，末根是否为「收盘后已封存的完成 bar」
kline_verified_by  跨源复核来源名（None=未复核）
kline_cross_check  复核结论 dict（见 daily_cross_check）
kline_fetched_at   打标签时刻 ISO8601
================== ====================================================

**新鲜度判定**：``assess()`` 用交易日历而非自然日，避免节假日后首日
把「上一交易日」误判为陈旧。判定结果供缓存日切、复核触发、告警使用。
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd

_ATTR_PREFIX = 'kline_'


def last_bar_date(df: pd.DataFrame) -> str | None:
    """取末根 bar 的日期（统一 YYYY-MM-DD 字符串），取不到返回 None。"""
    if df is None or df.empty or 'date' not in df.columns:
        return None
    try:
        raw = str(df['date'].iloc[-1])
    except (IndexError, KeyError, TypeError):
        return None
    # 兼容 '2026-11-06' / '2026-11-06 15:00:00' / '20261106' 三种源格式
    raw = raw.strip().split(' ')[0]
    if len(raw) == 8 and raw.isdigit():
        return f'{raw[:4]}-{raw[4:6]}-{raw[6:]}'
    return raw[:10] if len(raw) >= 10 else None


def tag(df: pd.DataFrame, source: str, *, sealed: bool = False,
        verified_by: str | None = None,
        cross_check: dict | None = None) -> pd.DataFrame:
    """给日K 打血缘标签（就地写 attrs，返回同一个对象便于链式调用）。

    刻意不 ``copy()``：调用方持有的是刚取到的 DataFrame，就地打标零成本；
    任何需要保留原对象的场景由调用方自行 copy。
    """
    if df is None:
        return df
    try:
        df.attrs['kline_as_of'] = last_bar_date(df)
        df.attrs['kline_source'] = source
        df.attrs['kline_bar_count'] = len(df)
        df.attrs['kline_sealed'] = bool(sealed)
        df.attrs['kline_verified_by'] = verified_by
        df.attrs['kline_cross_check'] = cross_check
        df.attrs['kline_fetched_at'] = datetime.now().isoformat(timespec='seconds')
    except Exception:  # noqa: S110, BLE001 attrs 不可写（极少数替身对象）不影响取数
        pass
    return df


def read(df: pd.DataFrame) -> dict:
    """读标签；未打标返回全默认（下游必须容忍无标签的历史路径）。"""
    attrs = getattr(df, 'attrs', {}) or {}
    return {
        'as_of': attrs.get('kline_as_of'),
        'source': attrs.get('kline_source'),
        'bar_count': attrs.get('kline_bar_count'),
        'sealed': bool(attrs.get('kline_sealed', False)),
        'verified_by': attrs.get('kline_verified_by'),
        'cross_check': attrs.get('kline_cross_check'),
        'fetched_at': attrs.get('kline_fetched_at'),
        # I-04：整帧健康结论（None=未启用校验或历史路径）
        'frame_health': attrs.get('kline_frame_health'),
        'frame_status': attrs.get('kline_frame_status'),
        # source_manager 的旧快照标记，一并暴露方便统一判陈旧
        'stale_snapshot': bool(attrs.get('_stale', False)),
    }


def evidence(df: pd.DataFrame, session: str | None = None) -> dict:
    """I-06/I-09：把血缘标签收敛成给 API/前端/日报用的只读证据块。

    刻意不包含任何评分字段——下游不得据此改变信号，只用于展示与审计。
    """
    meta = read(df)
    cc = meta['cross_check'] or {}
    return {
        'data_as_of': meta['as_of'],
        'data_source': meta['source'],
        'freshness': assess(df, session),
        'cross_check': cc.get('status'),
        'verified_by': meta['verified_by'],
        'sealed': meta['sealed'],
        'frame_status': meta['frame_status'],
        'fetched_at': meta['fetched_at'],
    }


def expected_session(now: datetime | None = None) -> str:
    """当前应有的数据截止交易日。

    - 交易日 09:30 之后（含盘后）：期望「今天」（当日 bar 应当可得）
    - 交易日开盘前：期望上一交易日
    - 非交易日：期望最近一个已结束交易日
    """
    from .calendar import is_trading_day

    now = now or datetime.now()
    day = now
    if is_trading_day(day) and (now.hour * 60 + now.minute) >= 9 * 60 + 30:
        return day.strftime('%Y-%m-%d')
    # 往前找最近一个交易日（最多回看 15 个自然日）
    from datetime import timedelta
    for _ in range(15):
        day = day - timedelta(days=1)
        if is_trading_day(day):
            return day.strftime('%Y-%m-%d')
    return now.strftime('%Y-%m-%d')


def is_adjacent_session(last_date: str, session: str) -> bool:
    """末根日期是否等于 session 或恰好是它的上一交易日。

    用交易日历枚举而非自然日差，节假日/周末自动正确。
    """
    from .calendar import get_trading_days_between

    if not last_date or not session:
        return False
    if last_date == session:
        return True
    if last_date > session:
        return False
    try:
        s = datetime.strptime(last_date, '%Y-%m-%d')
        e = datetime.strptime(session, '%Y-%m-%d')
        return len(get_trading_days_between(s, e)) <= 2
    except (TypeError, ValueError):
        return False


def assess(df: pd.DataFrame, session: str | None = None) -> str:
    """判定日K 新鲜度。

    Returns:
        ``current``   末根 == 期望交易日（正常）
        ``lagging``   末根 == 上一交易日（**降级源典型症状，需复核**）
        ``stale``     末根更早（陈旧/历史回归数据，禁止当今日使用）
        ``unknown``   无日期列或日期不可解析
    """
    session = session or expected_session()
    as_of = last_bar_date(df)
    if not as_of:
        return 'unknown'
    if as_of == session:
        return 'current'
    if is_adjacent_session(as_of, session):
        return 'lagging'
    return 'stale'


def should_cross_check(df: pd.DataFrame, session: str | None = None,
                       *, phase: str | None = None) -> bool:
    """是否触发针对性跨源复核。

    只在「lagging + 交易时段或盘后」触发：盘前本来就该停在上一交易日，
    复核没有意义；stale 是历史数据，复核只会污染历史序列。
    """
    if assess(df, session) != 'lagging':
        return False
    if phase is None:
        from .calendar import get_market_phase
        phase = get_market_phase()
    return phase in ('MORNING_SESSION', 'AFTERNOON_SESSION',
                     'NOON_BREAK', 'AFTER_HOURS')


def describe(df: pd.DataFrame, session: str | None = None) -> str:
    """一行日志用的可读摘要。"""
    meta = read(df)
    frame = meta['frame_status'] or 'unchecked'
    return (f"as_of={meta['as_of']} session={session or expected_session()} "
            f"source={meta['source']} freshness={assess(df, session)} "
            f"frame={frame} sealed={meta['sealed']} "
            f"verified_by={meta['verified_by']}")
