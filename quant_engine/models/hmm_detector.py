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

    # 协方差类型降级链（D53，2026-11-01）
    # ECS 实测（python3.14 + numpy 2.4.6 + hmmlearn 0.3.3）：59×3 特征上
    # covariance_type='full' 必抛 "'covars' must be symmetric, positive-definite"
    # ——某状态分到的样本过少，per-state 协方差矩阵非正定；同一份数据
    # diag/tied/spherical 全部 fit+predict 成功。保留 full 优先（表达力更强），
    # 失败自动降级 diag，避免整个 HMM 通道静默失效。
    COVARIANCE_TYPES = ("full", "diag")

    # 同一错误最多 WARNING 3 次，之后降级 DEBUG
    # （D53：10-26~10-30 生产日志 330 条 HMM 错误，刷屏且淹没真实故障）
    MAX_WARN_REPEAT = 3

    def __init__(self):
        self.model = None
        self._trained = False
        self._last_state = None
        self._state_counts = {}
        self._fit_key: str | None = None  # 训练数据指纹——同交易日不重复 fit
        self._warn_counts: dict[str, int] = {}

    # ---------------------------------------------------------------- 内部工具

    def _warn(self, key: str, msg: str) -> None:
        """限频告警：同 key 前 N 次 WARNING，之后只 DEBUG（防刷屏）"""
        n = self._warn_counts.get(key, 0) + 1
        self._warn_counts[key] = n
        if n <= self.MAX_WARN_REPEAT:
            logger.warning(msg)
        else:
            logger.debug(f"{msg}（重复 {n} 次，已降级为 DEBUG）")

    @staticmethod
    def _fingerprint(market_data: pd.DataFrame) -> str:
        """训练数据指纹：末根 bar 日期 + 行数

        决定是否需要重新训练——同一交易日数据不变则复用已训练模型
        （hmmlearn n_iter=100 是市场分析路径最重的一步）。
        """
        last = ''
        try:
            if 'date' in market_data.columns and len(market_data):
                last = str(market_data['date'].iloc[-1])
        except Exception:  # 指纹失败不阻断训练
            last = ''
        return f'{last}|{len(market_data)}'

    @staticmethod
    def _repair_transmat(model) -> bool:
        """修复不可达状态（transmat_ 零行）

        hmmlearn 0.3.3 在 EM 收敛后可能出现整行全 0 的转移行（某状态从未被
        访问），其 `_check_matrix` 校验行和为 1 时抛
        "transmat_ rows must sum to 1 (got row sums of [1. 1. 1. 0. 1.])"
        ——生产日志 10-26~10-30 记录 42 次 predict 失败，HMM 通道实质失效。

        修复：零行改为自吸收（该状态变成合法但不可达的吸收态），
        其余行按行和归一化。返回是否修复成功。
        """
        tm = getattr(model, 'transmat_', None)
        if tm is None:
            return False
        tm = np.array(tm, dtype=float, copy=True)
        sums = tm.sum(axis=1)
        for i in np.where(sums < 1e-9)[0]:
            tm[i, :] = 0.0
            tm[i, i] = 1.0
        for i in range(tm.shape[0]):
            s = tm[i].sum()
            if s > 0 and not np.isclose(s, 1.0):
                tm[i] /= s
        model.transmat_ = tm
        return bool(np.allclose(tm.sum(axis=1), 1.0, atol=1e-3))

    @staticmethod
    def _validate_model(model) -> bool:
        """校验训练产出的模型参数是否可用

        hmmlearn 在 predict 时会对模型参数做 `_check` 校验：
          - transmat_ 行和必须为 1（零行 → 不可达状态）
          - covars_ 必须对称且正定（full/tied 协方差）
          - startprob_ 必须为有效分布（EM 偶发 NaN）
        fit 阶段不抛异常但参数不合法的模型，到 predict 才会炸——D53 生产
        日志里 "HMM prediction failed: 'covars' must be symmetric,
        positive-definite" 135 次即此类。校验不通过就换下一种协方差类型。
        """
        if not MarketRegimeDetector._repair_transmat(model):
            return False
        if not MarketRegimeDetector._repair_startprob(model):
            return False
        covars = getattr(model, 'covars_', None)
        if covars is None:
            return True
        cov = np.asarray(covars, dtype=float)
        mats = [cov] if cov.ndim == 2 else ([m for m in cov] if cov.ndim == 3 else [])
        for m in mats:
            if m.shape[0] != m.shape[1]:
                continue
            if not np.allclose(m, m.T, atol=1e-6):
                return False
            try:
                if float(np.min(np.linalg.eigvalsh(m))) <= 0:
                    return False
            except np.linalg.LinAlgError:
                return False
        return True

    @staticmethod
    def _repair_startprob(model) -> bool:
        """修复初始状态分布（EM 偶发 NaN → "startprob_ must sum to 1 (got nan)"）"""
        sp = getattr(model, 'startprob_', None)
        n = int(getattr(model, 'n_components', 0) or 0)
        if sp is None:
            if n <= 0:
                return False
            model.startprob_ = np.full(n, 1.0 / n)
            return True
        sp = np.array(sp, dtype=float, copy=True)
        if not np.isfinite(sp).all() or sp.sum() <= 0:
            model.startprob_ = np.full(len(sp), 1.0 / len(sp))
            return True
        if not np.isclose(sp.sum(), 1.0):
            model.startprob_ = sp / sp.sum()
        return True

    @staticmethod
    def _regularize_covars(model, eps: float = 1e-8) -> bool:
        """把非正定的状态协方差正则化（特征值抬升）

        hmmlearn 的 covars_ setter 会校验「对称且正定」——full 协方差下某些
        状态样本过少 → 协方差非正定 → 赋值直接抛 ValueError；predict 时的
        _check 同样会抛（生产日志 135 次 "predict failed: 'covars' must be
        symmetric, positive-definite" 的根因）。
        """
        cov = getattr(model, 'covars_', None)
        if cov is None:
            return False
        if getattr(model, 'covariance_type', 'full') != 'full' or cov.ndim != 3:
            return False
        out = np.array(cov, dtype=float, copy=True)
        for i in range(out.shape[0]):
            m = out[i]
            m = 0.5 * (m + m.T)
            try:
                w, v = np.linalg.eigh(m)
            except np.linalg.LinAlgError:
                continue
            if not np.all(np.isfinite(w)) or float(w.min()) > eps:
                out[i] = m
                continue
            w = np.where(np.isfinite(w), np.maximum(w, eps), eps)
            m2 = v @ np.diag(w) @ v.T
            out[i] = 0.5 * (m2 + m2.T)
        model.covars_ = out
        return True

    @staticmethod
    def _relabel_states(model) -> bool:
        """按状态内平均收益重排状态编号（修正 HMM 标签切换）

        根因（用户 2026-11-01 指令 3：牛熊市阈值自适应机制优化）：
        hmmlearn 的 EM 收敛后，状态编号 0..N-1 与"市场强弱"**无语义关联**
        （经典 label switching）。项目却把 0→深熊、4→强牛 硬编码映射，
        于是"市场类型"实际是随机的 → 仓位因子（BEAR=0.2 / BULL=1.0）随机
        摆动 → 信号被随机过滤。D53 实测：同一份指数数据 full/diag 两种
        协方差给出完全不同的状态序列；不重排时状态与未来 5 日收益**毫无
        单调关系**（深熊 +0.21% / 弱熊 -0.98% / 强牛 +0.10%）。

        修复：训练后按每个状态的平均日收益升序重排（0=最熊 → 4=最强）。
        实测（本地 py3.9 + numpy 1.26 + hmmlearn 0.3.3，129 日窗口）：
          重排前：熊市占比 84.5%，状态↔前瞻收益无单调性
          重排后：熊市占比 33.3%，深熊 -1.13% → 弱熊 -0.79% → 震荡 -0.04%
                  （三级单调，与 2026 年 4-10 月"非持续熊市"的现实相符）
        """
        means = getattr(model, 'means_', None)
        if means is None or len(means) < 2:
            return False
        try:
            MarketRegimeDetector._regularize_covars(model)
            order = np.argsort(means[:, 0])
            if np.array_equal(order, np.arange(len(order))):
                return True
            model.startprob_ = model.startprob_[order]
            model.transmat_ = model.transmat_[np.ix_(order, order)]
            model.means_ = means[order]
            cov = getattr(model, 'covars_', None)
            if cov is not None:
                ctype = getattr(model, 'covariance_type', 'full')
                # full: (n, f, f) / diag: (n, f) / spherical: (n,) —— 需按状态重排
                if (ctype == 'full' and cov.ndim == 3) or \
                        (ctype == 'diag' and cov.ndim == 2) or \
                        (ctype == 'spherical' and cov.ndim == 1):
                    model.covars_ = cov[order]
            return True
        except Exception:  # 重排失败不阻断训练（退回原编号）
            return False

    # ---------------------------------------------------------------- 训练

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

            # 训练缓存（D53）：同一根 bar 不重复 fit——生产每 5 分钟扫描一次，
            # 原实现每轮都重训（hmmlearn n_iter=100），纯浪费 CPU。
            fit_key = self._fingerprint(market_data)
            if self._trained and self._fit_key == fit_key:
                return True

            last_err: Exception | None = None
            for cov_type in self.COVARIANCE_TYPES:
                try:
                    model = hmm.GaussianHMM(
                        n_components=self.N_STATES,
                        covariance_type=cov_type,
                        n_iter=100,
                        random_state=42,
                        min_covar=1e-3,  # 协方差对角线下限，官方建议的数值加固
                    )
                    model.fit(X)
                    if not self._validate_model(model):
                        last_err = ValueError('模型参数校验未通过（covars 非正定/transmat 不可达）')
                        continue
                    # 标签切换修正：按状态平均收益重排编号（0=最熊 → 4=最强）
                    self._relabel_states(model)
                    self.model = model
                    self._trained = True
                    self._fit_key = fit_key
                    self._warn_counts.pop('fit', None)
                    if cov_type == self.COVARIANCE_TYPES[0]:
                        logger.info(f"HMM trained on {len(features)} samples")
                    else:
                        logger.info(
                            f"HMM trained with covariance_type={cov_type} "
                            f"（{self.COVARIANCE_TYPES[0]} 失败自动降级: {last_err}）")
                    return True
                except Exception as e:  # 换下一种协方差类型重试
                    last_err = e
                    continue

            # 全部协方差类型失败：保留旧模型（若有效）供 predict 使用
            self._warn('fit', f"HMM training failed: {last_err}")
            return False

        except Exception as e:  # 兜底
            self._warn('fit', f"HMM training failed: {e}")
            return False

    # ---------------------------------------------------------------- 预测

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

        except Exception as e:  # 降级
            self._warn('predict', f"HMM prediction failed: {e}")
            # 降级：保留上次已知状态（因子权重仍按最后有效状态调整），
            # 置信度 0.5 —— 调用方 confidence>0.5 的门槛下会走规则回退，
            # 与既有降级语义一致
            if self._last_state is not None:
                return self._last_state, self.STATE_LABELS[self._last_state], 0.5
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
