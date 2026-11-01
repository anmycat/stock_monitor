"""HMM 检测器健壮性测试（D53，2026-11-01）

覆盖 D53 修复的四个根因：
1. covariance_type='full' 在生产栈（numpy 2.x + hmmlearn 0.3.3）必抛
   "'covars' must be symmetric, positive-definite" → 自动降级 diag
2. transmat_ 零行（不可达状态）→ 修复为自吸收，行和恢复为 1
3. 训练缓存：同一交易日不重复 fit
4. 限频告警：同一错误只 WARNING N 次，之后降 DEBUG
"""
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.models import hmm_detector as hd
from quant_engine.models.hmm_detector import HMM_AVAILABLE, MarketRegimeDetector


def _df(n=60, seed=42) -> pd.DataFrame:
    """构造日K fixture：振幅必须随机（range 恒定 → 方差 0 → HMM 按设计跳过）"""
    rng = np.random.default_rng(seed)
    closes = list(10 * np.exp(np.cumsum(rng.normal(0, 0.015, n))))
    span = rng.uniform(0.004, 0.03, n)
    highs = [c * (1 + s) for c, s in zip(closes, span)]
    lows = [c * (1 - s) for c, s in zip(closes, span)]
    return pd.DataFrame({
        'date': pd.date_range('2026-05-01', periods=n).strftime('%Y-%m-%d'),
        'open': closes, 'high': highs, 'low': lows,
        'close': closes, 'volume': list(rng.uniform(1e6, 2e6, n)),
    })


_REAL_AVAILABLE = hd.HMM_AVAILABLE


class _patch_hmm:
    """临时把 hmmlearn 替身注入模块（hmmlearn 未安装时模块无 hmm 属性）"""

    def __init__(self, hmm_cls):
        self.hmm_cls = hmm_cls
        self.had_attr = hasattr(hd, 'hmm')
        self.original = getattr(hd, 'hmm', None)
        self.real_available = hd.HMM_AVAILABLE

    def __enter__(self):
        hd.hmm = type('H', (), {'GaussianHMM': self.hmm_cls})
        hd.HMM_AVAILABLE = True
        return self

    def __exit__(self, *args):
        hd.HMM_AVAILABLE = self.real_available
        if self.had_attr:
            hd.hmm = self.original
        else:
            del hd.hmm
        return False


class _FakeModel:
    """最小 hmmlearn 模型替身：可控 transmat_"""

    def __init__(self, transmat):
        self.transmat_ = transmat


class TestTransmatRepair(unittest.TestCase):
    def test_zero_row_repaired_to_self_absorbing(self):
        """零行 → 自吸收；行和全部恢复为 1"""
        tm = np.array([
            [0.5, 0.5, 0.0],
            [0.0, 0.0, 0.0],   # 不可达状态
            [0.2, 0.3, 0.5],
        ])
        model = _FakeModel(tm)
        ok = MarketRegimeDetector._repair_transmat(model)
        self.assertTrue(ok)
        self.assertTrue(np.allclose(model.transmat_.sum(axis=1), 1.0, atol=1e-3))
        np.testing.assert_allclose(model.transmat_[1], [0.0, 1.0, 0.0])

    def test_valid_transmat_untouched(self):
        """合法 transmat_ 不被改动"""
        tm = np.array([[0.7, 0.3], [0.4, 0.6]])
        model = _FakeModel(tm.copy())
        self.assertTrue(MarketRegimeDetector._repair_transmat(model))
        np.testing.assert_allclose(model.transmat_, tm)

    def test_renormalizes_slightly_off_rows(self):
        """行和轻微偏离 1 → 归一化"""
        tm = np.array([[0.8, 0.3], [0.4, 0.6]])
        model = _FakeModel(tm)
        self.assertTrue(MarketRegimeDetector._repair_transmat(model))
        self.assertTrue(np.allclose(model.transmat_.sum(axis=1), 1.0, atol=1e-3))

    def test_missing_transmat_returns_false(self):
        model = _FakeModel(None)
        self.assertFalse(MarketRegimeDetector._repair_transmat(model))


class TestCovarianceFallback(unittest.TestCase):
    """协方差类型降级链"""

    def test_falls_back_to_diag_when_full_fails(self):
        """full 抛 covars 异常 → 自动改用 diag 并训练成功"""
        calls = []

        class _FakeHMM:
            def __init__(self, **kw):
                self.covariance_type = kw.get('covariance_type')
                self.n_components = kw.get('n_components', 5)
                calls.append(self.covariance_type)

            def fit(self, X):
                if self.covariance_type == 'full':
                    raise ValueError("'covars' must be symmetric, positive-definite")
                self.transmat_ = np.full((5, 5), 0.2)
                self.startprob_ = np.full(5, 0.2)

        with _patch_hmm(_FakeHMM):
            det = MarketRegimeDetector()
            ok = det.fit(_df(), n_days=60)

        self.assertTrue(ok, 'full 失败后应降级 diag 成功')
        self.assertEqual(calls, ['full', 'diag'])
        self.assertEqual(det.model.covariance_type, 'diag')

    def test_all_covariance_types_fail_returns_false(self):
        """全部协方差类型失败 → 返回 False 且不抛异常"""
        class _FakeHMM:
            def __init__(self, **kw):
                pass

            def fit(self, X):
                raise ValueError("boom")

        with _patch_hmm(_FakeHMM):
            det = MarketRegimeDetector()
            ok = det.fit(_df(), n_days=60)
        self.assertFalse(ok)
        self.assertFalse(det._trained)

    def test_covariance_types_order(self):
        """降级链顺序：full 优先，diag 兜底"""
        self.assertEqual(MarketRegimeDetector.COVARIANCE_TYPES, ('full', 'diag'))


class TestFitCache(unittest.TestCase):
    """训练缓存"""

    def test_same_data_fits_only_once(self):
        """同一份数据连续 fit → 只训练一次"""
        if not HMM_AVAILABLE:
            self.skipTest('hmmlearn 未安装')
        det = MarketRegimeDetector()
        df = _df()
        self.assertTrue(det.fit(df, n_days=60))
        first_model = det.model
        self.assertTrue(det.fit(df, n_days=60))
        self.assertIs(det.model, first_model, '同数据不应重新训练')

    def test_new_bar_triggers_refit(self):
        """新增一根 bar → 重新训练"""
        if not HMM_AVAILABLE:
            self.skipTest('hmmlearn 未安装')
        det = MarketRegimeDetector()
        df = _df()
        self.assertTrue(det.fit(df, n_days=60))
        first_model = det.model
        df2 = pd.concat([df, _df(2, seed=7).assign(date=['2026-07-01', '2026-07-02'])],
                        ignore_index=True)
        self.assertTrue(det.fit(df2, n_days=60))
        self.assertIsNot(det.model, first_model, '新数据应触发重训')


class TestWarnLimiting(unittest.TestCase):
    """限频告警"""

    def test_repeat_warns_limited(self):
        det = MarketRegimeDetector()
        for _ in range(10):
            det._warn('fit', 'boom')
        self.assertEqual(det._warn_counts['fit'], 10)
        self.assertEqual(MarketRegimeDetector.MAX_WARN_REPEAT, 3)

    def test_predict_failure_keeps_last_state(self):
        """预测失败时保留上次状态（而非回落震荡）"""
        det = MarketRegimeDetector()
        det._last_state = 3
        det._trained = True
        hd.HMM_AVAILABLE = True

        class _Boom:
            def predict_proba(self, X):
                raise ValueError('boom')

        det.model = _Boom()
        try:
            state, label, conf = det.predict(_df(5))
        finally:
            hd.HMM_AVAILABLE = _REAL_AVAILABLE
        self.assertEqual(state, 3)
        self.assertEqual(label, '弱牛')
        self.assertEqual(conf, 0.5)

    def test_predict_failure_without_history_falls_back(self):
        """从未成功过 → 回落震荡"""
        det = MarketRegimeDetector()
        det._trained = True
        hd.HMM_AVAILABLE = True

        class _Boom:
            def predict_proba(self, X):
                raise ValueError('boom')

        det.model = _Boom()
        try:
            state, label, conf = det.predict(_df(5))
        finally:
            hd.HMM_AVAILABLE = _REAL_AVAILABLE
        self.assertEqual((state, label, conf), (2, '震荡', 0.5))


if __name__ == '__main__':
    unittest.main()
