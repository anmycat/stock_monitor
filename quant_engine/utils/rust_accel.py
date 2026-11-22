"""本项目自有 Rust 扩展（`qerust`）的加载层与纯 Python 回退（D78）。

设计要点
--------
1. **分层**：只把「Python 级逐元素循环」下沉到 Rust（Wilder 平滑 / RSI /
   平台天数 / 连续涨跌），pandas/numpy 已经向量化的路径保持不动，业务语义
   全部留在 Python——与 eltdx（Rust 协议核 + Python 编排）同一思路，但这次
   扩展是**本项目自己的代码**（`rust_accel/`）。
2. **精确等价**：Rust 侧复刻 numpy `pairwise_sum` 求和顺序与 f64 运算次序，
   与改造前逐位一致；由 `test/test_rust_accel.py` 的逐位断言 + D41 双跑双保险。
3. **优雅降级**：扩展不可用（未构建 / 平台不匹配 / 已安装但导入失败）时静默
   回退纯 Python，行为与改造前完全相同；`RUST_ACCEL_ENABLED=false` 可强制回退
   用于 A/B 对比。
4. **不阻塞部署**：`deploy/update_ecs.sh` 只在有 cargo 时构建，构建失败仅告警
   （服务继续用 Python 回退跑）。

加载顺序：`import qerust`（pip 安装） → `quant_engine/utils/_qerust/qerust*.so`
（本地/部署脚本构建产物） → 纯 Python 回退。
"""

from __future__ import annotations

import importlib.util
import logging
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_EXT_DIR = Path(__file__).resolve().parent / "_qerust"

_module: Any = None
_load_attempted = False
_load_error: str | None = None


def _enabled() -> bool:
    """读取开关；配置不可用时按「启用」处理（与代码默认值一致）。"""
    try:
        from ..config import get_settings

        return bool(getattr(get_settings(), "RUST_ACCEL_ENABLED", True))
    except Exception:  # noqa: BLE001 - 配置层异常不应阻断指标计算
        return True


def _load() -> Any:
    global _module, _load_attempted, _load_error
    if _load_attempted:
        return _module
    _load_attempted = True
    if not _enabled():
        logger.info("Rust 加速扩展已按 RUST_ACCEL_ENABLED=false 关闭，使用纯 Python")
        return None
    # ① 已安装（pip）
    try:
        import qerust as mod  # type: ignore[import-not-found]

        _module = mod
        logger.info("Rust 加速扩展已加载（已安装 qerust %s）", getattr(mod, "__version__", "?"))
        return _module
    except Exception as exc:  # noqa: BLE001 - 未安装是预期分支
        _load_error = str(exc)
    # ② 构建产物（deploy/build_rust_accel.sh 输出）
    for pattern in ("qerust*.so", "qerust*.dylib", "qerust*.pyd"):
        for so in sorted(_EXT_DIR.glob(pattern)):
            try:
                spec = importlib.util.spec_from_file_location("qerust", so)
                if spec is None or spec.loader is None:
                    continue
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                sys.modules.setdefault("qerust", mod)
                _module = mod
                logger.info("Rust 加速扩展已加载：%s", so.name)
                return _module
            except Exception as exc:  # noqa: BLE001 - 逐个候选尝试
                _load_error = str(exc)
    logger.warning(
        "Rust 加速扩展不可用，指标计算走纯 Python 回退（构建：bash deploy/build_rust_accel.sh）"
        "；原因：%s",
        _load_error or "未找到 qerust 产物",
    )
    return None


def available() -> bool:
    """Rust 扩展是否可用（首次调用会触发加载）。"""
    return _load() is not None


def backend() -> str:
    """当前实际后端，便于健康面板/日志展示：'rust' 或 'python'。"""
    return "rust" if available() else "python"


def reset_for_test() -> None:
    """清空加载缓存（仅测试用：模拟「扩展不可用」或切换开关）。"""
    global _module, _load_attempted, _load_error
    _module = None
    _load_attempted = False
    _load_error = None


# ---------------------------------------------------------------------------
# 数值内核：Rust 优先，否则回退到与 indicators.py 完全一致的纯 Python 实现
# ---------------------------------------------------------------------------


def wild_smooth(series: pd.Series, period: int) -> float:
    """Wilder 平滑的最新值（等价 `indicators._wilder_smooth`）。"""
    mod = _load()
    if mod is not None:
        vals = series.to_numpy(dtype="float64").tolist()
        return float(mod.wild_smooth(vals, period))
    return _wild_smooth_py(series, period)


def wilder_rsi(series: pd.Series, period: int = 14) -> float:
    """端到端 Wilder RSI（等价 `calculate_indicators` 中的 RSI 计算）。"""
    mod = _load()
    if mod is not None:
        vals = series.to_numpy(dtype="float64").tolist()
        return float(mod.wilder_rsi(vals, period))
    return _wilder_rsi_py(series, period)


def wilder_rsi_py_fallback(series: pd.Series, period: int = 14) -> float:
    """强制纯 Python 版本的 RSI（测试用于逐位比对，生产路径请用 `wilder_rsi`）。"""
    return _wilder_rsi_py(series, period)



def consolidation_days(closes: np.ndarray, pct_range: float = 0.03, lookback: int = 30) -> int:
    """平台天数（等价 `indicators._consolidation_days`）。"""
    mod = _load()
    arr = np.asarray(closes, dtype="float64")
    if mod is not None:
        return int(mod.consolidation_days(arr.tolist(), float(pct_range), int(lookback)))
    return _consolidation_days_py(arr, pct_range, lookback)


def consecutive_direction(pct_values: np.ndarray) -> int:
    """连续涨跌天数（等价 `indicators._consecutive_direction` 的循环部分）。"""
    mod = _load()
    arr = np.asarray(pct_values, dtype="float64")
    if mod is not None:
        return int(mod.consecutive_direction(arr.tolist()))
    return _consecutive_direction_py(arr)


# --- 纯 Python 回退（与改造前实现逐字一致，勿在此处优化语义）---------------


def _wild_smooth_py(series: pd.Series, period: int) -> float:
    vals = series.to_numpy(dtype="float64")
    valid = vals[~np.isnan(vals)]
    n = len(valid)
    if n == 0:
        return 0.0
    if n < period:
        return float(valid.mean())
    smoothed = float(valid[:period].mean())
    alpha = 1.0 / period
    for v in valid[period:]:
        smoothed = smoothed + alpha * (v - smoothed)
    return float(smoothed)


def _wilder_rsi_py(series: pd.Series, period: int) -> float:
    """纯 Python 端到端 RSI（与改造前 `calculate_indicators` 内实现逐字一致）"""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -(delta.where(delta < 0, 0.0))
    avg_gain = _wild_smooth_py(gain, period)
    avg_loss = _wild_smooth_py(loss, period)
    rs = avg_gain / avg_loss if avg_loss != 0 else 0.0
    return 100 - (100 / (1 + rs))


def _consolidation_days_py(closes: np.ndarray, pct_range: float, lookback: int) -> int:
    n = min(lookback, len(closes))
    if n < 2:
        return 0
    tail = closes[-n:]
    days = 0
    ref = tail[-1]
    for c in reversed(tail):
        if abs(c / ref - 1) <= pct_range:
            days += 1
        else:
            break
    return days


def _consecutive_direction_py(pct: np.ndarray) -> int:
    if len(pct) == 0:
        return 0
    count = 0
    direction = 1 if pct[-1] >= 0 else -1
    for p in reversed(pct):
        if (direction > 0 and p >= 0) or (direction < 0 and p < 0):
            count += 1
        else:
            break
    return count * direction
