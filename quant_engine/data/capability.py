"""数据源能力中心（I-02，2026-11-07）

把原来只存在 ``SourceManager`` **进程内存**里的冷却/熔断/空结果/降级状态，
收敛成一份只读、可跨服务观测的契约。schema 直接采用 DSA #2304
``dataCapability.ts`` 的四段结构（``providers / datasets / priorities /
as_of + warnings``），便于 future 对照与外部工具复用。

设计约束：

- **纯只读**：不修改任何源状态、不触发取数、不参与信号评分
- Redis 不可用时自动退化为「当前进程内存快照」，接口仍可用
- ``datasets.stale`` / ``degraded`` 只表示观测结论，不改变取数行为
"""
from __future__ import annotations

import time

from loguru import logger

# 能力中心对外暴露的数据集（与 _PRIORITY 的 scenario 对齐，另加派生数据集）
_DATASET_LABELS = {
    'stock_list': '股票池',
    'realtime': '实时行情',
    'daily': '日K',
    'minute': '分钟K',
    'capital_flow': '资金流',
    'daily_seal': '日K收盘封存',
    'frame_health': '日K整帧校验',
}


def _iso(ts: float | None) -> str | None:
    if not ts:
        return None
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))


def _provider_status(row: dict) -> str:
    if row.get('quarantined'):
        return 'unavailable'
    if row.get('suppressed'):
        return 'partial'
    if not row.get('available'):
        return 'partial'
    if row.get('error_count'):
        return 'degraded'
    return 'ok'


def build_capability_report(cache=None, *, session: str | None = None,
                            source_manager=None, priorities: dict | None = None) -> dict:
    """生成能力中心快照（只读）。

    Args:
        cache: 可选 RedisCache（读取降级事件 / 整帧异常 / 跨重启熔断状态）
        session: 期望交易日，用于筛整帧异常；缺省由 kline_provenance 推导
        source_manager: 可注入（测试用），缺省全局单例
        priorities: 可注入优先级表，缺省 ``source._PRIORITY``
    """
    sm = source_manager
    if sm is None:
        from .source_manager import source_manager as _global_sm
        sm = _global_sm
    if priorities is None:
        from .source import _PRIORITY
        priorities = _PRIORITY
    if session is None:
        from ..utils.kline_provenance import expected_session
        session = expected_session()

    status = sm.get_status_report()
    warnings: list[str] = []

    # ---------- providers ----------
    providers = []
    for name, row in sorted(status.items()):
        w: list[str] = []
        if row.get('quarantined'):
            w.append(f'熔断至 {_iso(row.get("quarantined_until"))}')
        if row.get('suppressed'):
            w.append(f'超时抑制至 {_iso(row.get("suppress_until"))}')
        if row.get('last_error_kind'):
            w.append(f'最近异常类型 {row["last_error_kind"]}')
        datasets = [scenario for scenario, seq in priorities.items()
                    if name in seq]
        providers.append({
            'name': name,
            'enabled': True,
            'configured': True,
            'status': _provider_status(row),
            'priority': {k: v.index(name) + 1 for k, v in priorities.items()
                         if name in v},
            'markets': ['CN'],
            'datasets': datasets,
            'warnings': w,
            'lastError': row.get('last_error_message'),
            'lastErrorKind': row.get('last_error_kind'),
            'lastSuccess': _iso(row.get('last_success_time')),
            'cooldownUntil': _iso(row.get('cooldown_until')),
            'quarantinedUntil': _iso(row.get('quarantined_until')),
            'suppressedUntil': _iso(row.get('suppress_until')),
            'emptyCount': row.get('empty_count', 0),
            'errorCount': row.get('error_count', 0),
            'requestCount': row.get('request_count', 0),
            'successCount': row.get('success_count', 0),
            'fallbackCount': row.get('fallback_count', 0),
            'missingCodes': row.get('missing_codes', []),
        })
        if row.get('quarantined'):
            warnings.append(f'源 {name} 处于熔断（{_iso(row.get("quarantined_until"))}）')
        if row.get('suppressed'):
            warnings.append(f'源 {name} 处于超时抑制（{_iso(row.get("suppress_until"))}）')

    # ---------- priorities ----------
    prio_rows = []
    for scenario, seq in priorities.items():
        dead = [n for n in seq if n not in status]
        prio_rows.append({
            'scenario': scenario,
            'label': _DATASET_LABELS.get(scenario, scenario),
            'providers': list(seq),
            'source': 'quant_engine/data/source.py::_PRIORITY',
            'warnings': ([f'未注册源: {",".join(dead)}'] if dead else []),
        })
        if dead:
            warnings.append(f'{scenario} 优先级含未注册源 {",".join(dead)}')

    # ---------- datasets ----------
    fallback_events: dict[str, dict] = {}
    if cache is not None:
        for scenario in priorities:
            try:
                ev = cache.get(f'capability:fallback:{scenario}')
                if isinstance(ev, dict):
                    fallback_events[scenario] = ev
            except Exception as e:  # noqa: BLE001 只读观测失败降级为空
                logger.debug(f'[I-02] fallback event read failed: {e}')
    # 内存态优先（本进程刚发生的降级比 Redis 更及时）
    for scenario, ev in (getattr(sm, 'last_fallback', {}) or {}).items():
        fallback_events[scenario] = ev

    datasets = []
    for scenario, seq in priorities.items():
        ev = fallback_events.get(scenario) or {}
        src_states = [status.get(n, {}) for n in seq]
        healthy = [n for n in seq if status.get(n, {}).get('available')]
        stale = not healthy
        ds_warnings: list[str] = []
        if stale:
            ds_warnings.append('优先级内所有源均不可用')
            warnings.append(f'{scenario} 全部源不可用（将依赖 stale snapshot）')
        if ev:
            ds_warnings.append(
                f"最近降级 {' → '.join(ev.get('fallbackFrom') or [])} → {ev.get('to')} "
                f"(累计 {ev.get('count', 0)} 次)")
        datasets.append({
            'name': scenario,
            'label': _DATASET_LABELS.get(scenario, scenario),
            'status': 'unavailable' if stale else ('degraded' if ev else 'ok'),
            'source': ev.get('to') or (healthy[0] if healthy else None),
            'stale': stale,
            'lastSuccess': max((_iso(s.get('last_success_time')) or ''
                                for s in src_states), default=None),
            'lastError': next((s.get('last_error_message') for s in src_states
                               if s.get('last_error_message')), None),
            'fallbackFrom': ev.get('fallbackFrom') or [],
            'coverage': {'providers_total': len(seq),
                         'providers_available': len(healthy)},
            'warnings': ds_warnings,
        })

    # ---------- 派生数据集：资金流 / 封存 / 整帧异常 ----------
    issues = []
    try:
        from ..utils.kline_frame import list_issues
        issues = list_issues(cache, session)
    except Exception as e:  # noqa: BLE001 只读观测失败降级为空
        logger.debug(f'[I-02] frame issues read failed: {e}')
    if issues:
        warnings.append(f'本交易日 {len(issues)} 只标的日K整帧异常（详见 frame_issues）')
    datasets.append({
        'name': 'frame_health', 'label': _DATASET_LABELS['frame_health'],
        'status': 'degraded' if issues else 'ok',
        'source': 'kline_frame.validate_frame',
        'stale': False,
        'lastSuccess': None, 'lastError': None, 'fallbackFrom': [],
        'coverage': {'session': session, 'issues': len(issues)},
        'warnings': [f'{len(issues)} 条待复核'] if issues else [],
    })

    sealed_dates = []
    try:
        from .store import get_history_store
        sealed_dates = get_history_store().list_sealed_dates(limit=5)
    except Exception as e:  # noqa: BLE001 SQLite 不可用时该行标 unknown
        logger.debug(f'[I-02] sealed dates read failed: {e}')
    datasets.append({
        'name': 'daily_seal', 'label': _DATASET_LABELS['daily_seal'],
        'status': 'ok' if sealed_dates else 'unknown',
        'source': 'SQLite daily_bar_seals',
        'stale': False,
        'lastSuccess': sealed_dates[0] if sealed_dates else None,
        'lastError': None, 'fallbackFrom': [],
        'coverage': {'recent_dates': sealed_dates},
        'warnings': (['暂无封存记录（非交易日或尚未收盘）'] if not sealed_dates else []),
    })

    return {
        'as_of': time.strftime('%Y-%m-%d %H:%M:%S'),
        'session': session,
        'providers': providers,
        'datasets': datasets,
        'priorities': prio_rows,
        'frame_issues': issues[:20],
        'fallback_events': fallback_events,
        'warnings': warnings,
    }
