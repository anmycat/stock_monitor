"""交易时段闸门与唤醒策略（D53，2026-11-01）

用户要求："非交易时间内不要访问行情服务器，按照原来设计保持静默，
在交易日交易发生前唤起即可"。

本模块提供三个事实源：
1. `is_quote_window()` —— 行情访问窗口（允许访问行情服务器的时间段）
2. `next_wakeup()`   —— 按当前阶段计算下一次唤醒时刻（整块睡眠，不再每分钟轮询）
3. `seconds_until()` —— 距目标时刻的秒数

设计要点：
- 行情访问只发生在交易日的 9:00-11:30 / 13:00-15:05；其余时段一律静默，
  总结推送（12:10 早盘 / 18:10 全日）改用交易时段内采集的内存快照。
- 唤醒点与阶段边界严格对齐，保证任何阶段都不会被跳过
  （原实现每分钟醒一次"轮询式"推进，代价是整夜 720 次空唤醒）。
"""
from __future__ import annotations

from datetime import datetime, timedelta

from .calendar import get_next_trading_day, is_trading_day

# 行情访问窗口（分钟数）：盘前校验 + 竞价 + 上午 + 午后 + 收盘
QUOTE_WINDOWS = ((9 * 60, 11 * 60 + 30), (13 * 60, 15 * 60 + 5))

# 关键阶段边界（分钟数）
T_PRE_MARKET_START = 9 * 60        # 盘前静态校验窗口起点
T_SELF_CHECK = 9 * 60 + 15         # 系统自检 + 集合竞价校验
T_CALL_AUCTION = 9 * 60 + 25       # 集合竞价播报
T_OPEN = 9 * 60 + 30               # 开盘
T_AM_END = 11 * 60 + 30            # 上午收盘
T_MORNING_REVIEW = 12 * 60 + 10    # 早盘总结
T_AFTERNOON = 13 * 60              # 午后开盘
T_CLOSE = 15 * 60                  # 收盘
T_DAILY_REVIEW = 18 * 60 + 10      # 全日总结

# 快照采集窗口（会话内，提前把总结所需数据备好）
T_MORNING_SNAPSHOT = (11 * 60 + 25, 11 * 60 + 30)
T_DAILY_SNAPSHOT = (14 * 60 + 55, 15 * 60 + 5)


def _now(now: datetime | None = None) -> datetime:
    return now or datetime.now()


def is_quote_window(now: datetime | None = None) -> bool:
    """当前是否允许访问行情服务器

    仅交易日 9:00-11:30 / 13:00-15:05 放行；其余时段一律禁止
    （盘后总结、盘前静默、周末休眠）。
    """
    now = _now(now)
    if not is_trading_day(now):
        return False
    t = now.hour * 60 + now.minute
    return any(lo <= t <= hi for lo, hi in QUOTE_WINDOWS)


def _next_trading_day_at(hour: int, minute: int, now: datetime | None = None) -> datetime:
    """下一个交易日的指定时刻（跨周末/节假日）"""
    now = _now(now)
    nxt = get_next_trading_day(now)
    if hasattr(nxt, "to_pydatetime"):
        nxt = nxt.to_pydatetime()  # pandas.Timestamp → datetime
    return nxt.replace(hour=hour, minute=minute, second=0, microsecond=0)


def next_wakeup(phase: str, now: datetime | None = None) -> datetime:
    """按当前阶段计算下一次唤醒时刻

    与 `worker.get_current_phase()` 的阶段划分严格对齐：
      PRE_MARKET   → 9:15（自检+竞价）
      SELF_CHECK   → 9:25（集合竞价播报）
      CALL_AUCTION → 9:30（开盘）
      MORNING_SESSION / AFTERNOON_SESSION → 5 分钟后（扫描节奏）
      NOON_BREAK   → 12:10（早盘总结）或 13:00（午后开盘）
      MORNING_REVIEW → 13:00（午后开盘）
      AFTER_HOURS  → 18:10（全日总结）或 次交易日 9:00
      DAILY_REVIEW → 次交易日 9:00（盘前静态校验）
      WEEKEND      → 次交易日 9:00
    """
    now = _now(now)
    t = now.hour * 60 + now.minute

    if phase == "PRE_MARKET":
        return now.replace(hour=9, minute=15, second=0, microsecond=0)
    if phase == "SELF_CHECK":
        return now.replace(hour=9, minute=25, second=0, microsecond=0)
    if phase == "CALL_AUCTION":
        return now.replace(hour=9, minute=30, second=0, microsecond=0)
    if phase in ("MORNING_SESSION", "AFTERNOON_SESSION"):
        return now + timedelta(seconds=300)
    if phase == "NOON_BREAK":
        if t < T_MORNING_REVIEW:
            return now.replace(hour=12, minute=10, second=0, microsecond=0)
        return now.replace(hour=13, minute=0, second=0, microsecond=0)
    if phase == "MORNING_REVIEW":
        return now.replace(hour=13, minute=0, second=0, microsecond=0)
    if phase == "AFTER_HOURS":
        if t < T_DAILY_REVIEW:
            return now.replace(hour=18, minute=10, second=0, microsecond=0)
        return _next_trading_day_at(9, 0, now)
    if phase == "DAILY_REVIEW":
        return _next_trading_day_at(9, 0, now)
    # WEEKEND / 未知阶段 → 次交易日 9:00
    return _next_trading_day_at(9, 0, now)


def seconds_until(target: datetime, now: datetime | None = None) -> int:
    """距目标时刻的秒数（至少 1 秒，避免 0 秒空转）"""
    now = _now(now)
    return max(1, int((target - now).total_seconds()))


def in_snapshot_window(kind: str, now: datetime | None = None) -> bool:
    """是否处于总结快照采集窗口（morning/daily）"""
    now = _now(now)
    t = now.hour * 60 + now.minute
    window = T_MORNING_SNAPSHOT if kind == "morning" else T_DAILY_SNAPSHOT
    return window[0] <= t <= window[1]
