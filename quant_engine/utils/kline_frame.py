"""日K 整帧完整性校验（I-04，2026-11-07）

承接 ``kline_provenance.assess()`` 只判末根的不足：DSA #2389 的做法是
**校验整帧所有 bar 都是有效交易日**，因为因子/指标消费的是整份序列，
只要历史帧里混入一个周末或节假日 bar，整份数据就不可信。

设计铁律（与 D66 一致）：

- **只写 ``df.attrs``，不改任何列、不改任何值** → MA/RSI/MACD/回测读列，零影响
- 历史/回测数据同样适用：``session`` 由调用方传入，不随墙钟漂移
- 拒绝层是可选项（``KLINE_REJECT_STALE_FRAME``，默认 **False**）——
  开启后会让「整帧异常且非当日」的数据不再出厂，会改变信号，
  按项目纪律必须先过 D41 双跑 + 半年窗口复验 + 用户裁定

异常状态另存 Redis（``kline_frame_issue:{session}:{code}``，TTL 7 天），
重启/多进程不绕过；只有显式 ``clear_issue()`` 才清除。这是旁路索引，
不替代连续历史序列。
"""
from __future__ import annotations

from datetime import datetime
from functools import lru_cache

import pandas as pd
from loguru import logger

from .kline_provenance import last_bar_date

MAX_REPORTED_ISSUES = 8
ISSUE_TTL = 7 * 86400
_ISSUE_KEY = 'kline_frame_issue:{session}:{code}'


@lru_cache(maxsize=4096)
def _is_session_day(day: str) -> bool:
    """带缓存的交易日判定。

    日历不可用时返回 True（无法判定则不误报），避免历史回测数据被整批标脏。
    """
    from .calendar import is_trading_day

    try:
        return bool(is_trading_day(datetime.strptime(day, '%Y-%m-%d')))
    except Exception:  # noqa: BLE001 日历异常不得让取数失败
        return True


def _dates(df: pd.DataFrame) -> list[str]:
    """统一解析日期列为 YYYY-MM-DD 字符串列表（不修改 df）。"""
    if df is None or df.empty or 'date' not in df.columns:
        return []
    out: list[str] = []
    for raw in df['date'].astype(str).tolist():
        s = raw.strip().split(' ')[0]
        if len(s) == 8 and s.isdigit():
            s = f'{s[:4]}-{s[4:6]}-{s[6:]}'
        out.append(s[:10])
    return out


def validate_frame(df: pd.DataFrame, session: str | None = None,
                   *, max_issues: int = MAX_REPORTED_ISSUES) -> dict:
    """逐根校验日K 整帧，返回结构化健康结论（纯函数，不写 df）。

    Returns:
        ``{status, checked_bars, issues[], first_bad_index, as_of, session}``
        status ∈ ``ok`` / ``invalid`` / ``unknown``
    """
    out = {
        'status': 'unknown', 'checked_bars': 0, 'issues': [],
        'first_bad_index': None, 'as_of': last_bar_date(df),
        'session': session,
    }
    if df is None or df.empty:
        return out

    dates = _dates(df)
    out['checked_bars'] = len(dates)
    issues: list[str] = []
    first_bad: int | None = None

    def add(idx: int, msg: str) -> None:
        nonlocal first_bad
        if first_bad is None:
            first_bad = idx
        if len(issues) < max_issues:
            issues.append(f'bar[{idx}] {msg}')

    # ① 日期可解析 + 严格单调 + 无重复 + 无未来日
    prev = ''
    for i, d in enumerate(dates):
        try:
            datetime.strptime(d, '%Y-%m-%d')
        except ValueError:
            add(i, f'unparsable date {d!r}')
            prev = ''
            continue
        if prev and d <= prev:
            add(i, f'non-monotonic date {d} (prev={prev})')
        if session and d > session:
            add(i, f'future bar {d} > session {session}')
        # ② 每根都必须是交易日（DSA #2389 的核心：整帧而非只判末根）
        if not _is_session_day(d):
            add(i, f'non-trading-day bar {d}')
        prev = d

    # ③ 价格合法性（只校验存在的列，缺失列由 validate_dataframe 负责）
    has_ohlc = all(c in df.columns for c in ('open', 'high', 'low', 'close'))
    if has_ohlc:
        try:
            o = pd.to_numeric(df['open'], errors='coerce')
            h = pd.to_numeric(df['high'], errors='coerce')
            low = pd.to_numeric(df['low'], errors='coerce')
            c = pd.to_numeric(df['close'], errors='coerce')
            bad = (~(o > 0) | ~(h > 0) | ~(low > 0) | ~(c > 0)
                   | (h < low) | (c > h * 1.0001) | (c < low * 0.9999)
                   | (o > h * 1.0001) | (o < low * 0.9999))
            for i in bad[bad].index.tolist()[:max_issues]:
                pos = df.index.get_loc(i) if i in df.index else 0
                add(int(pos), 'invalid OHLC')
        except (TypeError, ValueError, KeyError):
            add(0, 'OHLC numeric conversion failed')
    elif 'close' in df.columns:
        try:
            c = pd.to_numeric(df['close'], errors='coerce')
            for i in c[~(c > 0)].index.tolist()[:max_issues]:
                pos = df.index.get_loc(i) if i in df.index else 0
                add(int(pos), 'invalid close')
        except (TypeError, ValueError, KeyError):
            add(0, 'close numeric conversion failed')

    out['issues'] = issues
    out['first_bad_index'] = first_bad
    out['status'] = 'ok' if not issues else 'invalid'
    return out


def annotate(df: pd.DataFrame, session: str | None = None,
             *, cache=None, code: str | None = None) -> pd.DataFrame:
    """校验并把结论写进 attrs；异常状态旁路持久化（重启不绕过）。"""
    if df is None or df.empty:
        return df
    health = validate_frame(df, session)
    try:
        df.attrs['kline_frame_health'] = health
        df.attrs['kline_frame_status'] = health['status']
    except Exception:  # noqa: BLE001 attrs 不可写（测试替身）不影响取数
        return df

    if health['status'] == 'invalid':
        logger.warning(
            f"[I-04][整帧异常] {code or '-'} {health['checked_bars']} 根中首错于 "
            f"bar[{health['first_bad_index']}]: {'; '.join(health['issues'][:3])}")
        if cache is not None and code and session:
            try:
                cache.set(
                    _ISSUE_KEY.format(session=session, code=code),
                    {**health, 'code': code,
                     'detected_at': datetime.now().isoformat(timespec='seconds')},
                    ttl=ISSUE_TTL)
            except Exception as e:  # noqa: BLE001 旁路持久化失败不影响取数
                logger.debug(f"[I-04] issue persist failed: {e}")
    return df


def clear_issue(cache, session: str, code: str) -> None:
    """复核 + 依赖重算完成后才调用，清除异常状态。"""
    if cache is None:
        return
    try:
        cache.delete(_ISSUE_KEY.format(session=session, code=code))
    except Exception as e:  # noqa: BLE001 旁路清理失败不影响取数
        logger.debug(f"[I-04] issue clear failed: {e}")


def list_issues(cache, session: str, *, limit: int = 50) -> list[dict]:
    """列出某交易日仍待处理的整帧异常（供能力中心/日报使用）。"""
    if cache is None:
        return []
    try:
        keys = cache.keys(_ISSUE_KEY.format(session=session, code='*'))
        out = []
        for k in keys[:limit]:
            v = cache.get(k)
            if isinstance(v, dict):
                out.append(v)
        return out
    except Exception as e:  # noqa: BLE001 旁路读取失败返回空
        logger.debug(f"[I-04] issue list failed: {e}")
        return []


def should_reject(df: pd.DataFrame, session: str | None = None) -> bool:
    """拒绝层判定（**默认不启用**，启用会改变信号，需用户裁定 + D41 双跑）。

    只在「整帧异常 + 数据本身已不是当日」时拒绝，避免把可修复的盘中
    未完成 bar 误杀。开关在 ``Settings.KLINE_REJECT_STALE_FRAME``。
    """
    from ..config import get_settings

    try:
        if not bool(get_settings().KLINE_REJECT_STALE_FRAME):
            return False
    except Exception:  # noqa: BLE001 配置缺失时保守放行
        return False
    health = (getattr(df, 'attrs', {}) or {}).get('kline_frame_health')
    if health is None:
        health = validate_frame(df, session)
    if health.get('status') != 'invalid':
        return False
    from .kline_provenance import assess
    return assess(df, session) != 'current'
