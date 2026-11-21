"""数值清洗工具（D70 / N-08，2026-11-21）

背景：DSA 在**数据入口**用 sanitize_non_finite 拦截 NaN/Inf，避免非有限数
穿透到 JSON 序列化层变成 ``NaN``/``Infinity``（非法 JSON，前端 JSON.parse
直接抛错，或 pandas→dict 后污染缓存）。本项目此前只在个别渲染点兜底，
缺少统一 choke point。

本模块提供 ``finite_or_none``：把 NaN / ±Inf 归一为 None，正常数值原样返回，
非数值类型（str/bool/None）原样返回。放在 utils 层供 API / 日报 / WS 复用。

**零信号影响**：本工具只作用于「序列化前」的展示 dict，不触碰 Signal 决策
字段本身，不参与评分/风控/Kelly/回测。NaN/Inf 本身是异常数据，转 None 只
改变「非法 JSON」为「字段缺省」，不改变任何 BUY/WATCH/SKIP 判断。
"""
from __future__ import annotations

import math


def finite_or_none(value):
    """NaN / ±Inf → None；其余原样返回。

    bool 是 int 子类但不会是 NaN，直接放行；Decimal/Fraction 等照原样返回
    （由各自序列化逻辑处理），只拦截 float 与 numpy 浮点。
    """
    if isinstance(value, float):
        return None if not math.isfinite(value) else value
    # numpy.float32/64 不是 Python float，用 math.isfinite 兜底（可转 float）
    type_name = type(value).__name__
    if type_name.startswith('float') and hasattr(value, '__float__'):
        try:
            return None if not math.isfinite(float(value)) else value
        except (TypeError, ValueError, OverflowError):
            return None
    return value


def sanitize_floats(d: dict, keys: tuple[str, ...] | None = None) -> dict:
    """清洗 dict 中的浮点字段（原地不改，返回新 dict）。

    keys 指定时只清洗这些键；否则清洗全部值。
    """
    if keys is None:
        return {k: finite_or_none(v) for k, v in d.items()}
    return {k: (finite_or_none(v) if k in keys else v) for k, v in d.items()}
