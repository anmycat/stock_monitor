"""HMM 数据退化测试（D44，修复 10-26 日志 covars 数值崩溃）

覆盖：一字板（价格无波动→方差0）不训练不抛异常 / 非有限值
（停牌 volume=0 → vol_chg inf）不训练 / 正常数据可训练 /
降级路径 predict 返回"震荡"。fixture：内存 DataFrame。
"""
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.models.hmm_detector import HMM_AVAILABLE, MarketRegimeDetector


def _df(closes, volumes=None, highs=None, lows=None) -> pd.DataFrame:
    n = len(closes)
    if volumes is None:
        volumes = [1_000_000.0] * n
    if highs is None:
        highs = [c * 1.01 for c in closes]
    if lows is None:
        lows = [c * 0.99 for c in closes]
    return pd.DataFrame({
        'date': pd.date_range('2026-08-01', periods=n),
        'open': closes, 'high': highs, 'low': lows,
        'close': closes, 'volume': volumes,
    })


class TestHmmDegenerate(unittest.TestCase):
    def setUp(self):
        self.detector = MarketRegimeDetector()

    @unittest.skipUnless(HMM_AVAILABLE, 'hmmlearn 未安装（降级路径已在别处覆盖）')
    def test_one_way_board_returns_false_not_raise(self):
        """一字板：close/high/low 全恒定 → 特征方差0 → 返回 False 不抛异常"""
        df = _df([10.0] * 60)
        ok = self.detector.fit(df, n_days=60)
        self.assertFalse(ok)
        self.assertFalse(self.detector._trained)

    @unittest.skipUnless(HMM_AVAILABLE, 'hmmlearn 未安装')
    def test_nonfinite_features_returns_false(self):
        """停牌场景：volume 全 0 → vol_chg 出现 inf → isfinite 拦截"""
        df = _df([10.0 + 0.01 * i for i in range(60)], volumes=[0.0] * 60)
        ok = self.detector.fit(df, n_days=60)
        self.assertFalse(ok)

    @unittest.skipUnless(HMM_AVAILABLE, 'hmmlearn 未安装')
    def test_normal_data_trains(self):
        """正常波动数据：应训练成功（修复不误杀）"""
        rng = np.random.default_rng(42)
        closes = list(10 * np.exp(np.cumsum(rng.normal(0, 0.02, 60))))
        df = _df(closes)
        ok = self.detector.fit(df, n_days=60)
        self.assertTrue(ok)
        self.assertTrue(self.detector._trained)

    def test_predict_fallback_when_not_trained(self):
        """未训练时 predict 降级"震荡"（降级链不受 D44 影响）"""
        state, label, conf = self.detector.predict(_df([10.0] * 5))
        self.assertEqual(state, 2)
        self.assertEqual(label, '震荡')
        self.assertEqual(conf, 0.5)

    def test_insufficient_data_returns_false(self):
        """样本不足 20 行仍返回 False"""
        df = _df([10.0 + 0.01 * i for i in range(10)])
        self.assertFalse(self.detector.fit(df, n_days=60))


if __name__ == '__main__':
    unittest.main()
