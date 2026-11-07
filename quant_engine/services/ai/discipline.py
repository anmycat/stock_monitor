"""日报「过程纪律」客观指标（I-08，2026-11-07）

严格遵循 D61 设计铁律 **客观与主观分离**：本节所有数字都由代码从真实
信号对象与持久化记录直接统计，模型不参与、不得编造分值；它**不进入**
门控 5 维综合分，也不改变仓位建议，只是一个可选的展示分区。

口径先按「可无争议客观定义」的指标落地：

- 信号数 / BUY 数
- 数据当日覆盖率（``freshness == current``）
- 隔夜或历史数据信号数（降级可见性）
- 已跨源复核数
- 现价与日K基准偏离 > 3% 的票数（D64 跳空提示命中）
- 近 5 个交易日日报封存覆盖（流程是否按日执行）

``DAILY_REPORT_DISCIPLINE_DIMENSION`` 默认 **False**：口径是否纳入正式
第六维、是否参与任何评分，仍需用户裁定（docs/34 I-08）。默认关闭时本模块
完全不被调用，日报输出与上一版逐字段一致。
"""
from __future__ import annotations

from datetime import datetime

_GAP_THRESHOLD = 0.03


def build_discipline_metrics(signals, *, sealed_dates: list | None = None) -> dict:
    """从真实信号对象统计客观过程指标（纯函数，无 IO、无模型调用）。"""
    def f(obj, key, default=None):
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)

    items = list(signals or [])
    total = len(items)
    buys = 0
    current = 0
    lagging = 0
    stale = 0
    cross_checked = 0
    gap_warned = 0
    frame_invalid = 0
    for s in items:
        if f(s, 'action') is not None and str(
                getattr(f(s, 'action'), 'value', f(s, 'action'))) == 'BUY':
            buys += 1
        fresh = f(s, 'freshness')
        if fresh == 'current':
            current += 1
        elif fresh == 'lagging':
            lagging += 1
        elif fresh == 'stale':
            stale += 1
        if f(s, 'cross_check'):
            cross_checked += 1
        if f(s, 'frame_status') == 'invalid':
            frame_invalid += 1
        cur, ref = f(s, 'current_price'), f(s, 'price_ref')
        try:
            if cur and ref and ref > 0 and abs(cur / ref - 1) > _GAP_THRESHOLD:
                gap_warned += 1
        except (TypeError, ZeroDivisionError):
            pass

    dates = list(sealed_dates or [])
    return {
        'signal_count': total,
        'buy_count': buys,
        'fresh_current_count': current,
        'fresh_current_ratio': round(current / total, 4) if total else None,
        'lagging_count': lagging,
        'stale_count': stale,
        'cross_checked_count': cross_checked,
        'frame_invalid_count': frame_invalid,
        'gap_warned_count': gap_warned,
        'recent_sealed_sessions': dates[:5],
        'computed_by': 'code',
        'computed_at': datetime.now().isoformat(timespec='seconds'),
        'note': '客观统计，不参与门控 5 维综合分与仓位建议；口径是否升为正式维度待用户裁定',
    }


def render_discipline_fields(metrics: dict) -> list[tuple[str, str]]:
    """转成日报字段（label, value），保持与 UI 的 ReportField 契约一致。"""
    ratio = metrics['fresh_current_ratio']
    return [
        ('信号总数', f"{metrics['signal_count']} 个（BUY {metrics['buy_count']} 个）"),
        ('数据当日覆盖', ('--' if ratio is None else f'{ratio:.0%}')
         + f"（隔夜 {metrics['lagging_count']} / 历史 {metrics['stale_count']}）"),
        ('跨源复核', f"{metrics['cross_checked_count']} 个信号带复核结论"),
        ('跳空提示', f"{metrics['gap_warned_count']} 个信号现价偏离日K基准 > 3%"),
        ('整帧异常', f"{metrics['frame_invalid_count']} 个信号日K整帧校验未通过"),
        ('流程执行', f"近 {len(metrics['recent_sealed_sessions'])} 个交易日有收盘封存"),
        ('口径说明', metrics['note']),
    ]
