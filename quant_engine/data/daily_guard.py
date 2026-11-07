"""日K 数据防误用编排（D66，2026-11-06）

针对用户提出的两条治理路径，本模块同时落地：

**① 其他数据源「针对性复核」**
不是全市场每轮都多打一遍源（那会放大东财风控），而是只在
``kline_provenance.should_cross_check()`` 判为「疑似降级」时触发：
主源日K末根停在上一交易日、但当下本应有当日 bar → 换一条**独立数据源**
（东财 push2his / 同花顺）重新取同一只票的日K，比对末根日期与收盘价：

- 复核源有当日 bar  → 主源确实漏了当日，记 ``confirmed_current``，
  可用复核源的当日 bar 补齐（由 D65 的盘中修正消费）
- 复核源也停在 T-1  → 记 ``confirmed_lagging``，说明是市场/停牌原因，
  不是源故障，允许 D65 按实时报价追加当日 bar
- 两者 T-1 收盘价差异 > 1%  → 记 ``price_mismatch``，数据质量告警
- 复核源取不到      → ``unavailable``，绝不凭空猜数

复核结论写进 ``df.attrs['kline_cross_check']`` + Redis 审计 key，
**不改变任何 DataFrame 列**（趋势/指标计算零影响，D41 回归 no-op）。

**② 每天结束后「打包打标签」封存**
收盘后把当日**已确认完成**的日K末根打包写入 SQLite
``daily_bar_seals``，每条带 trade_date / source / verified_by /
price_ref / sealed_at 标签。次日：

- 缓存 key 带交易日（见 market_data.py），跨日自动失效，读不到昨天的缓存
- 进程内 24h stale 快照按 as_of 校验（见 source_manager.py），隔夜快照作废
- 已封存的 bar 只作**日期与价格基准**，不替换连续历史序列
  —— 用户口径「用于计算趋势之类需要上下行情的除外」由设计保证：
  封存表是旁路索引，趋势计算读的仍是完整 df，一列都不动

**零额外行情请求**：封存数据来自扫描链路本轮已经取到的日K
（``SessionBarCollector`` 顺手收集末根），不重新拉全市场。
"""
from __future__ import annotations

import threading
from datetime import datetime

import pandas as pd
from loguru import logger

from ..utils import kline_provenance as prov

# 复核结论常量
CC_CONFIRMED_CURRENT = 'confirmed_current'   # 复核源有当日 → 主源滞后
CC_CONFIRMED_LAGGING = 'confirmed_lagging'   # 复核源同样 T-1 → 正常停更
CC_PRICE_MISMATCH = 'price_mismatch'         # T-1 收盘价对不上
CC_UNAVAILABLE = 'unavailable'               # 复核源不可用
CC_NOT_NEEDED = 'not_needed'                 # 数据本来就新鲜

_PRICE_TOLERANCE = 0.01   # T-1 收盘价允许偏差 1%（复权口径差异留余量）


class SessionBarCollector:
    """进程内收集本交易日扫描过的日K末根（供盘后封存）

    线程安全；只保留最近一个交易日，切日自动清空，避免跨日数据混装。
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._session: str | None = None
        self._bars: dict[str, dict] = {}

    def collect(self, df: pd.DataFrame, code: str, session: str | None = None) -> None:
        """登记一根日K末根（只在数据新鲜时登记，陈旧/历史数据拒绝封存）。"""
        if df is None or df.empty or not code:
            return
        session = session or prov.expected_session()
        if prov.assess(df, session) != 'current':
            return
        meta = prov.read(df)
        try:
            last = df.iloc[-1]
            bar = {
                'trade_date': session,
                'code': code,
                'open': float(last.get('open') or 0),
                'high': float(last.get('high') or 0),
                'low': float(last.get('low') or 0),
                'close': float(last.get('close') or 0),
                'volume': float(last.get('volume') or 0),
                'source': meta.get('source'),
                'verified_by': meta.get('verified_by'),
                'price_ref': (meta.get('cross_check') or {}).get('ref_close'),
                'payload': {
                    'as_of': meta.get('as_of'),
                    'source': meta.get('source'),
                    'verified_by': meta.get('verified_by'),
                    'cross_check': meta.get('cross_check'),
                    'freshness': 'current',
                    'fetched_at': meta.get('fetched_at'),
                },
            }
        except (ValueError, TypeError, IndexError, AttributeError):
            return
        if bar['close'] <= 0:
            return
        with self._lock:
            if self._session != session:
                self._session = session
                self._bars = {}
            self._bars[code] = bar

    def drain(self, session: str | None = None) -> list[dict]:
        """取出并清空当前交易日的封存候选。"""
        with self._lock:
            if session and self._session and self._session != session:
                return []
            rows = list(self._bars.values())
            self._bars = {}
            return rows

    def size(self) -> int:
        with self._lock:
            return len(self._bars)


# 进程级单例：扫描链路与盘后封存共享同一收集器
collector = SessionBarCollector()


def cross_check(df: pd.DataFrame, code: str,
                session: str | None = None,
                *, verifier=None, phase: str | None = None) -> dict:
    """针对性跨源复核：仅在主源疑似降级时调用。

    Args:
        df: 主源日K
        code: 标准代码
        session: 期望交易日
        verifier: 复核源实例（需实现 ``get_daily_data(code, days)``）。
            缺省用 ``EastMoneySource``；测试可注入替身。
        phase: 市场阶段（测试可注入，避免依赖墙钟）

    Returns:
        结论 dict：{status, verified_by, ref_date, ref_close,
                    main_date, main_close, gap_pct, checked_at}
    """
    session = session or prov.expected_session()
    result = {
        'status': CC_NOT_NEEDED, 'verified_by': None,
        'ref_date': None, 'ref_close': None,
        'main_date': prov.last_bar_date(df),
        'main_close': None, 'gap_pct': None,
        'checked_at': datetime.now().isoformat(timespec='seconds'),
    }
    if not prov.should_cross_check(df, session, phase=phase):
        return result
    try:
        result['main_close'] = float(df['close'].iloc[-1])
    except (KeyError, IndexError, TypeError, ValueError):
        return result

    if verifier is None:
        try:
            from .sources.eastmoney import EastMoneySource
            verifier = EastMoneySource
        except ImportError:
            result['status'] = CC_UNAVAILABLE
            return result

    try:
        ref = verifier.get_daily_data(code, 10)
    except Exception as e:  # noqa: BLE001 复核失败不得影响主链
        logger.debug(f"[D66] cross-check source failed for {code}: {e}")
        ref = None

    if ref is None or ref.empty or 'close' not in ref.columns:
        result['status'] = CC_UNAVAILABLE
        return result

    result['verified_by'] = getattr(verifier, '__name__', type(verifier).__name__)
    result['ref_date'] = prov.last_bar_date(ref)
    try:
        result['ref_close'] = float(ref['close'].iloc[-1])
    except (KeyError, IndexError, TypeError, ValueError):
        result['status'] = CC_UNAVAILABLE
        return result

    if result['ref_date'] == session:
        result['status'] = CC_CONFIRMED_CURRENT
        logger.info(
            f"[D66][复核] {code} 主源末根 {result['main_date']} 滞后，"
            f"复核源 {result['verified_by']} 已有当日 {session} bar "
            f"(收 {result['ref_close']})")
    else:
        main_close = result['main_close']
        ref_close = result['ref_close']
        if main_close > 0 and ref_close > 0:
            result['gap_pct'] = round(ref_close / main_close - 1, 6)
            if abs(result['gap_pct']) > _PRICE_TOLERANCE:
                result['status'] = CC_PRICE_MISMATCH
                logger.warning(
                    f"[D66][复核] {code} 两源 {result['main_date']} 收盘价不一致: "
                    f"主源 {main_close} vs 复核源 {ref_close} "
                    f"({result['gap_pct']:+.2%})，数据质量存疑")
            else:
                result['status'] = CC_CONFIRMED_LAGGING
        else:
            result['status'] = CC_CONFIRMED_LAGGING
    return result


def apply_cross_check(df: pd.DataFrame, code: str,
                      session: str | None = None, *,
                      verifier=None, phase: str | None = None,
                      cache=None) -> pd.DataFrame:
    """复核并把结论写回标签（同一交易日每票只复核一次，避免刷源）。

    Args:
        cache: 可选 RedisCache，用于跨轮去重 + 审计留痕；None 时仅进程内生效。
    """
    if df is None or df.empty:
        return df
    session = session or prov.expected_session()
    dedup_key = f'kline_cross_check:{session}:{code}'
    if cache is not None:
        try:
            cached = cache.get(dedup_key)
            if isinstance(cached, dict) and cached.get('status'):
                prov.tag(df, prov.read(df).get('source') or 'unknown',
                         verified_by=cached.get('verified_by'),
                         cross_check=cached)
                return df
        except Exception as e:  # noqa: BLE001 旁路缓存异常不得影响主链
            logger.debug(f"[D66] cross-check cache read failed: {e}")

    conclusion = cross_check(df, code, session, verifier=verifier, phase=phase)
    source = prov.read(df).get('source') or 'unknown'
    if conclusion['status'] != CC_NOT_NEEDED:
        prov.tag(df, source,
                 verified_by=conclusion.get('verified_by'),
                 cross_check=conclusion)
        if cache is not None:
            try:
                cache.set(dedup_key, conclusion, ttl=86400)
            except Exception as e:  # noqa: BLE001 旁路缓存异常不得影响主链
                logger.debug(f"[D66] cross-check cache write failed: {e}")
    return df


def verify_against_seal(df: pd.DataFrame, code: str,
                        session: str | None = None, *, store=None) -> dict:
    """次日读取校验：拿已封存的上一交易日 bar 当「日期锚点」。

    用户口径「避免次日再次错误读取使用」的最后一道闸：
    若某源次日返回的日K末根仍是 T-1，且封存表里 T-1 的收盘价与之一致，
    就能明确判定「这是昨天的完成 bar，不是今天的」→ 标 ``sealed=True``
    并给出 ``freshness='lagging'``，下游据此拒绝把它当当日数据用。
    趋势计算读的是完整序列，不受该标签影响。

    Returns:
        {status: 'no_seal'|'matches'|'mismatch'|'fresh', ...}
    """
    from ..utils.calendar import get_trading_days_between

    session = session or prov.expected_session()
    as_of = prov.last_bar_date(df)
    out = {'status': 'fresh', 'as_of': as_of, 'session': session,
           'sealed_close': None, 'current_close': None, 'gap_pct': None}
    if not as_of or as_of == session:
        return out
    # 只在「末根恰好是上一交易日」时查封存（更早的属历史数据，不判）
    try:
        from datetime import datetime as _dt
        s = _dt.strptime(as_of, '%Y-%m-%d')
        e = _dt.strptime(session, '%Y-%m-%d')
        if s > e or len(get_trading_days_between(s, e)) > 2:
            out['status'] = 'no_seal'
            return out
    except (TypeError, ValueError):
        out['status'] = 'no_seal'
        return out

    if store is None:
        from .store import get_history_store
        store = get_history_store()
    seal = store.get_daily_seal(as_of, code)
    if not seal:
        out['status'] = 'no_seal'
        return out
    try:
        cur_close = float(df['close'].iloc[-1])
        sealed_close = float(seal.get('close') or 0)
    except (KeyError, IndexError, TypeError, ValueError):
        out['status'] = 'no_seal'
        return out
    out['sealed_close'] = sealed_close
    out['current_close'] = cur_close
    if sealed_close > 0 and cur_close > 0:
        out['gap_pct'] = round(cur_close / sealed_close - 1, 6)
        out['status'] = ('matches' if abs(out['gap_pct']) <= _PRICE_TOLERANCE
                         else 'mismatch')
    else:
        out['status'] = 'no_seal'
    if out['status'] == 'mismatch':
        logger.warning(
            f"[D66][封存校验] {code} 末根 {as_of} 收盘 {cur_close} 与封存的"
            f" {sealed_close} 不符 ({out['gap_pct']:+.2%})，疑似源端口径变化")
    return out


def seal_session(session: str | None = None, *, store=None,
                 cache=None) -> dict:
    """盘后打包封存：把本轮扫描收集到的当日完成 bar 落 SQLite。

    Returns:
        {sealed, skipped, session, reason}
    """
    session = session or prov.expected_session()
    rows = collector.drain(session)
    if not rows:
        return {'sealed': 0, 'skipped': 0, 'session': session,
                'reason': 'no_confirmed_bars'}
    if store is None:
        from .store import get_history_store
        store = get_history_store()
    n = store.save_daily_seals(rows)
    summary = {
        'sealed': int(n or 0), 'skipped': len(rows) - int(n or 0),
        'session': session, 'reason': 'ok' if n else 'store_unavailable',
    }
    logger.info(f"[D66][封存] {session} 当日完成 bar {summary['sealed']} 条"
                f"（跳过 {summary['skipped']}）")
    if cache is not None and n:
        try:
            cache.set(f'kline_seal:{session}', summary, ttl=604800)
        except Exception as e:  # noqa: BLE001 封存汇总写缓存失败不影响封存
            logger.debug(f"[D66] seal summary cache write failed: {e}")
    return summary
