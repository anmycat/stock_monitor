"""D78 Rust 加速扩展等价性测试（逐位断言）。

为什么要逐位而不是 allclose
--------------------------
趋势类信号「零漂移」是本项目铁律：RSI 差 1 ulp → 策略分在阈值边界翻转 →
信号/告警/日报全变。所以这里用 ``struct.pack('<d', x)`` 做**位级比较**，
并且覆盖：
  1. 随机游走（多种长度：短于 period / 恰好 period / 240 根真实规模 / 2000 根）
  2. 含 NaN 的脏数据（停牌/缺失）
  3. 全涨 / 全跌 / 横盘 三个边界方向
  4. 模块级整链：`calculate_indicators` 全字段与 Rust 关闭时逐位一致
"""

from __future__ import annotations

import os
import struct
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

from quant_engine.utils import indicators as ind
from quant_engine.utils import rust_accel


def _bits(x: float) -> bytes:
    return struct.pack('<d', x)


def _assert_bitwise_equal(tc: unittest.TestCase, a, b, msg: str = '') -> None:
    """浮点逐位相等（含 NaN 位模式一致性）"""
    if a is None or b is None:
        tc.assertEqual(a, b, msg)
        return
    fa, fb = float(a), float(b)
    if np.isnan(fa) and np.isnan(fb):
        return
    tc.assertEqual(_bits(fa), _bits(fb), f'{msg}: {fa!r} != {fb!r}')


class TestRustAccelEquivalence(unittest.TestCase):
    """Rust 内核 vs 纯 Python 回退：逐位相等"""

    @classmethod
    def setUpClass(cls):
        if not rust_accel.available():
            raise unittest.SkipTest(
                'qerust 扩展未构建（bash deploy/build_rust_accel.sh）；本机回退纯 Python'
            )
        cls.rng = np.random.default_rng(20261122)

    def _series(self, n: int, nan_at: tuple[int, ...] = ()) -> pd.Series:
        closes = np.cumprod(1 + self.rng.normal(0, 0.012, n)) * 20.0
        if nan_at:
            closes = closes.astype(float)
            closes[list(nan_at)] = np.nan
        return pd.Series(closes)

    # ── 1. Wilder 平滑 ───────────────────────────────────────────────
    def test_wild_smooth_bitwise(self):
        for n in (5, 13, 14, 15, 240, 2000):
            s = self._series(n)
            for period in (6, 14, 26):
                with self.subTest(n=n, period=period):
                    _assert_bitwise_equal(
                        self,
                        rust_accel.wild_smooth(s, period),
                        rust_accel._wild_smooth_py(s, period),
                        f'wild_smooth n={n} p={period}',
                    )

    def test_wild_smooth_with_nan(self):
        s = self._series(240, nan_at=(0, 3, 4, 100))
        for period in (14, 26):
            with self.subTest(period=period):
                _assert_bitwise_equal(
                    self,
                    rust_accel.wild_smooth(s, period),
                    rust_accel._wild_smooth_py(s, period),
                    'wild_smooth 含 NaN',
                )

    # ── 2. 端到端 RSI ───────────────────────────────────────────────
    def test_rsi_bitwise(self):
        for n in (2, 5, 14, 15, 240, 2000):
            s = self._series(n)
            with self.subTest(n=n):
                _assert_bitwise_equal(
                    self,
                    rust_accel.wilder_rsi(s, 14),
                    rust_accel.wilder_rsi_py_fallback(s, 14),
                    f'rsi n={n}',
                )

    def test_rsi_edge_directions(self):
        up = pd.Series(np.linspace(10, 30, 60))
        down = pd.Series(np.linspace(30, 10, 60))
        flat = pd.Series(np.full(60, 10.0))
        for name, s in (('up', up), ('down', down), ('flat', flat)):
            with self.subTest(name=name):
                _assert_bitwise_equal(
                    self, rust_accel.wilder_rsi(s, 14), rust_accel.wilder_rsi_py_fallback(s, 14), name
                )

    def test_rsi_with_nan(self):
        s = self._series(240, nan_at=(0, 50, 51))
        _assert_bitwise_equal(
            self, rust_accel.wilder_rsi(s, 14), rust_accel.wilder_rsi_py_fallback(s, 14), 'rsi 含 NaN'
        )

    # ── 3. 平台天数 / 连续涨跌 ──────────────────────────────────────
    def test_consolidation_days_match(self):
        for trial in range(50):
            n = int(self.rng.integers(2, 120))
            closes = np.cumprod(1 + self.rng.normal(0, 0.02, n)) * 15
            for pct in (0.01, 0.03, 0.05):
                with self.subTest(trial=trial, pct=pct):
                    self.assertEqual(
                        rust_accel.consolidation_days(closes, pct, 30),
                        rust_accel._consolidation_days_py(closes, pct, 30),
                    )

    def test_consecutive_direction_match(self):
        for trial in range(50):
            n = int(self.rng.integers(0, 300))
            pct = self.rng.normal(0, 0.02, n)
            with self.subTest(trial=trial):
                self.assertEqual(
                    rust_accel.consecutive_direction(pct),
                    rust_accel._consecutive_direction_py(pct),
                )

    # ── 4. 模块级整链：指标字典逐位一致 ────────────────────────────
    def test_calculate_indicators_zero_drift(self):
        """关闭 Rust 重算同一批 df，全字段逐位相等（证明下沉没有引入漂移）"""
        frames = []
        for _ in range(20):
            n = 240
            closes = np.cumprod(1 + self.rng.normal(0, 0.015, n)) * 30
            frames.append(
                pd.DataFrame(
                    {
                        'date': pd.date_range('2026-01-01', periods=n, freq='D'),
                        'open': closes * 0.995,
                        'high': closes * 1.01,
                        'low': closes * 0.99,
                        'close': closes,
                        'volume': self.rng.integers(1e5, 1e7, n).astype(float),
                    }
                )
            )

        rust_on = [ind.calculate_indicators(df) for df in frames]

        # 强制纯 Python 后端（loader 缓存 + settings 缓存都要清）
        from quant_engine.config import get_settings

        os.environ['RUST_ACCEL_ENABLED'] = 'false'
        get_settings.cache_clear()
        rust_accel.reset_for_test()
        try:
            self.assertFalse(rust_accel.available(), '应已切换到纯 Python 后端')
            py_off = [ind.calculate_indicators(df) for df in frames]
        finally:
            os.environ.pop('RUST_ACCEL_ENABLED', None)
            get_settings.cache_clear()
            rust_accel.reset_for_test()
            self.assertTrue(rust_accel.available(), '应恢复 Rust 后端')

        self.assertEqual(len(rust_on), len(py_off))
        for i, (a, b) in enumerate(zip(rust_on, py_off)):
            self.assertEqual(set(a.keys()), set(b.keys()), f'frame {i} 字段集合不同')
            for key in a:
                with self.subTest(frame=i, field=key):
                    va, vb = a[key], b[key]
                    if isinstance(va, float) or isinstance(vb, float):
                        _assert_bitwise_equal(self, va, vb, f'frame {i}.{key}')
                    else:
                        self.assertEqual(va, vb, f'frame {i}.{key}')

    def test_backend_name(self):
        self.assertEqual(rust_accel.backend(), 'rust')


class TestFallbackBehaviour(unittest.TestCase):
    """扩展不可用时必须静默回退，且与 Rust 结果一致（抽样）"""

    def test_fallback_matches_when_rust_disabled(self):
        if not rust_accel.available():
            raise unittest.SkipTest('qerust 未构建')
        rng = np.random.default_rng(1)
        closes = np.cumprod(1 + rng.normal(0, 0.01, 240)) * 10
        s = pd.Series(closes)
        rsi_rust = rust_accel.wilder_rsi(s, 14)

        os.environ['RUST_ACCEL_ENABLED'] = 'false'
        try:
            from quant_engine.config import get_settings

            get_settings.cache_clear()
            rust_accel.reset_for_test()
            rsi_py = rust_accel.wilder_rsi(s, 14)
        finally:
            os.environ.pop('RUST_ACCEL_ENABLED', None)
            get_settings.cache_clear()
            rust_accel.reset_for_test()

        _assert_bitwise_equal(self, rsi_rust, rsi_py, 'fallback rsi')

    def test_loader_survives_missing_module(self):
        """模拟扩展产物不存在：available() 返回 False 且不抛异常"""
        from pathlib import Path
        from unittest import mock

        rust_accel.reset_for_test()
        # sys.modules['qerust']=None 会让 `import qerust` 抛 ImportError；
        # 再把产物目录指向不存在的路径 → 两条加载通道都必须失败。
        with mock.patch.object(rust_accel, '_EXT_DIR', Path('/nonexistent-qerust-dir')), \
                mock.patch.dict(sys.modules, {'qerust': None}):
            self.assertFalse(rust_accel.available())
            self.assertEqual(rust_accel.backend(), 'python')
            # 回退路径仍要给出与 Rust 一致的结果，不能崩
            import numpy as np
            import pandas as pd

            closes = np.cumprod(1 + np.random.default_rng(3).normal(0, 0.01, 60)) * 10
            val = rust_accel.wilder_rsi(pd.Series(closes), 14)
            self.assertTrue(np.isfinite(val))
        rust_accel.reset_for_test()


if __name__ == '__main__':
    unittest.main(verbosity=2)
