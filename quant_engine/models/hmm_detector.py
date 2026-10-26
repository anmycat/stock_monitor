"""HMM市场状态检测器 — 替代简单枚举，5种精细状态识别"""
from __future__ import annotations

import numpy as np
import pandas as pd
from loguru import logger

try:
    from hmmlearn import hmm
    HMM_AVAILABLE = True
except ImportError:
    HMM_AVAILABLE = False
    logger.warning("hmmlearn not installed, fallback to simple state detection")


class MarketRegimeDetector:
    """HMM隐马尔可夫市场状态识别
    
    输入特征：日收益率、波动率(ATR)、成交量变化率、换手率、涨跌比
    输出5种状态：强牛/弱牛/震荡/弱熊/深熊
    """
    
    STATE_LABELS = ["深熊", "弱熊", "震荡", "弱牛", "强牛"]  # noqa: RUF012 类级只读常量，运行期无原地修改
    N_STATES = 5
    
    def __init__(self):
        self.model = None
        self._trained = False
        self._last_state = None
        self._state_counts = {}
        
    def fit(self, market_data: pd.DataFrame, n_days: int = 60) -> bool:
        """训练HMM模型
        
        Args:
            market_data: 包含 close, high, low, volume, turnover 列的DataFrame
            n_days: 使用最近N天数据训练
        Returns:
            训练是否成功
        """
        if not HMM_AVAILABLE or len(market_data) < n_days:
            return False
        
        try:
            data = market_data.tail(n_days).copy()
            
            # 特征工程
            data['return'] = data['close'].pct_change()
            data['range'] = (data['high'] - data['low']) / data['close']
            data['vol_chg'] = data['volume'].pct_change()
            
            features = data[['return', 'range', 'vol_chg']].dropna()
            if len(features) < 20:
                return False
            
            X = features.values

            # 数据卫生检查（D44 修复 10-26 日志 'covars must be symmetric,
            # positive-definite'）：一字板/停牌股价格无波动时 return/range
            # 方差精确为 0，协方差矩阵退化，hmmlearn 抛 ValueError。
            # hmmlearn 官方 FAQ 对数据退化的建议即降级不训练——本项目的
            # 降级路径是 predict 返回"震荡"（不影响信号主流程）。
            if not np.isfinite(X).all():
                logger.info("HMM skipped: non-finite features (suspended stock?)")
                return False
            if (X.std(axis=0) < 1e-12).any():
                logger.info("HMM skipped: degenerate feature variance (one-way board?)")
                return False

            self.model = hmm.GaussianHMM(
                n_components=self.N_STATES,
                covariance_type="full",
                n_iter=100,
                random_state=42,
                min_covar=1e-3,  # 协方差对角线下限，官方建议的数值加固
            )
            self.model.fit(X)
            self._trained = True
            logger.info(f"HMM trained on {len(features)} samples")
            return True
            
        except Exception as e:
            logger.warning(f"HMM training failed: {e}")
            return False
    
    def predict(self, market_data: pd.DataFrame) -> tuple[int, str, float]:
        """预测当前市场状态
        
        Args:
            market_data: 最新市场数据
        Returns:
            (state_index, state_label, confidence)
        """
        if not self._trained or not HMM_AVAILABLE:
            return 2, "震荡", 0.5  # fallback: 震荡
        
        try:
            data = market_data.tail(2).copy()
            data['return'] = data['close'].pct_change()
            data['range'] = (data['high'] - data['low']) / data['close']
            data['vol_chg'] = data['volume'].pct_change()
            
            features = data[['return', 'range', 'vol_chg']].dropna()
            if len(features) < 1:
                return 2, "震荡", 0.5
            
            X = features.values[-1:].reshape(1, -1)
            state_probs = self.model.predict_proba(X)[0]
            state = int(np.argmax(state_probs))
            confidence = float(state_probs[state])
            
            self._last_state = state
            self._state_counts[state] = self._state_counts.get(state, 0) + 1
            
            return state, self.STATE_LABELS[state], confidence
            
        except Exception as e:
            logger.warning(f"HMM prediction failed: {e}")
            return 2, "震荡", 0.5
    
    def get_market_factor(self, state: int) -> float:
        """获取市场系数（用于自适应阈值调节）"""
        factors = {
            0: 0.3,   # 深熊：大幅降低阈值
            1: 0.5,   # 弱熊：适度降低
            2: 0.7,   # 震荡：偏保守
            3: 0.85,  # 弱牛：偏进攻
            4: 1.0,   # 强牛：正常阈值
        }
        return factors.get(state, 0.7)
    
    def get_state_distribution(self) -> dict:
        """获取状态分布统计"""
        total = sum(self._state_counts.values()) or 1
        return {
            self.STATE_LABELS[i]: round(self._state_counts.get(i, 0) / total, 3)
            for i in range(self.N_STATES)
        }


# 全局单例
_detector: MarketRegimeDetector | None = None


def get_regime_detector() -> MarketRegimeDetector:
    global _detector
    if _detector is None:
        _detector = MarketRegimeDetector()
    return _detector
