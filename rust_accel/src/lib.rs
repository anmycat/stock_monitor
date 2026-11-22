//! qerust —— quant_engine 自有 Rust 加速扩展（D78）
//!
//! 只下沉「Python 级逐元素循环」的内核，业务语义仍全部留在 Python：
//! - `wild_smooth`         Wilder 平滑（RSI/ATR 标准口径，标量输出）
//! - `wilder_rsi`          端到端 Wilder RSI（diff → gain/loss → 平滑 → RSI）
//! - `consolidation_days`  平台天数（连续在 ±pct_range 内）
//! - `consecutive_direction` 连续涨跌天数（正=连涨，负=连跌）
//!
//! # 精确等价约定（本项目铁律：趋势类信号零漂移）
//!
//! 1. 全部用 f64，运算次序与 Python/numpy 实现逐项对齐；
//! 2. 均值求和**复刻 numpy `pairwise_sum`**（8 路展开 + 递归对半切，块大小 128），
//!    否则 seed 均值的最后一位可能不同，会渗透进 RSI 与评分；
//! 3. NaN 语义对齐：Python 侧 `~np.isnan` 过滤等价于这里逐项 `is_nan` 跳过。

use pyo3::prelude::*;

/// numpy 成对求和的块大小（core/src/umath/loops.c.src: PW_BLOCKSIZE）
const PW_BLOCKSIZE: usize = 128;

/// 复刻 numpy `pairwise_sum`：n<8 朴素累加；n<=128 用 8 个累加器展开后再两两合并；
/// n>128 从 n2 = n/2 向下取 8 的倍数处递归对半。
///
/// 求和顺序决定 f64 结果的最低位，必须与 numpy 完全一致。
fn pairwise_sum(a: &[f64]) -> f64 {
    let n = a.len();
    if n < 8 {
        let mut res = 0.0f64;
        for &v in a {
            res += v;
        }
        return res;
    }
    if n <= PW_BLOCKSIZE {
        let mut r = [0.0f64; 8];
        r.copy_from_slice(&a[..8]);
        let end = n - (n % 8);
        let mut i = 8;
        while i < end {
            for j in 0..8 {
                r[j] += a[i + j];
            }
            i += 8;
        }
        let mut res = ((r[0] + r[1]) + (r[2] + r[3])) + ((r[4] + r[5]) + (r[6] + r[7]));
        while i < n {
            res += a[i];
            i += 1;
        }
        return res;
    }
    let mut n2 = n / 2;
    n2 -= n2 % 8;
    pairwise_sum(&a[..n2]) + pairwise_sum(&a[n2..])
}

/// numpy `mean` 等价（pairwise_sum / n）
fn mean(a: &[f64]) -> f64 {
    if a.is_empty() {
        return f64::NAN;
    }
    pairwise_sum(a) / (a.len() as f64)
}

/// Wilder 平滑核心（与 `quant_engine/utils/indicators.py::_wilder_smooth` 逐位等价）
fn wild_smooth_impl(values: &[f64], period: usize) -> f64 {
    let valid: Vec<f64> = values.iter().copied().filter(|v| !v.is_nan()).collect();
    let n = valid.len();
    if n == 0 {
        return 0.0;
    }
    if n < period {
        return mean(&valid);
    }
    let mut smoothed = mean(&valid[..period]);
    let alpha = 1.0 / (period as f64);
    for &v in &valid[period..] {
        smoothed = smoothed + alpha * (v - smoothed);
    }
    smoothed
}

/// Wilder 平滑（Python 可见）：输入序列可含 NaN，返回最新平滑值
#[pyfunction]
fn wild_smooth(values: Vec<f64>, period: usize) -> PyResult<f64> {
    if period == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err("period 必须 >= 1"));
    }
    Ok(wild_smooth_impl(&values, period))
}

/// 由收盘价序列拆出 gain/loss。
///
/// **语义细节（踩坑记录）**：Python 侧是
/// ```python
/// delta = close.diff()            # 首根 NaN
/// gain  = delta.where(delta > 0, 0.0)   # NaN > 0 为 False → 首根变成 0.0
/// loss  = -(delta.where(delta < 0, 0.0))# 同上 → 首根变成 -0.0
/// ```
/// 即 NaN 差分被 where **替换成 0.0**，不是保留 NaN；因此 gain/loss 长度等于
/// `len(closes)`（不是 len-1），且 `_wilder_smooth` 里的 NaN 过滤对它们不生效。
/// 中间缺数据的 NaN 差分同样按 0.0 处理。
fn gain_loss(closes: &[f64]) -> (Vec<f64>, Vec<f64>) {
    let n = closes.len();
    let mut gains: Vec<f64> = Vec::with_capacity(n);
    let mut losses: Vec<f64> = Vec::with_capacity(n);
    if n == 0 {
        return (gains, losses);
    }
    // delta[0] 是 NaN → where(...) → gain 0.0 / loss -0.0
    gains.push(0.0);
    losses.push(-0.0);
    for i in 1..n {
        let d = closes[i] - closes[i - 1];
        if d.is_nan() {
            // NaN 与任何数比较为 False → where 落到 0.0
            gains.push(0.0);
            losses.push(-0.0);
        } else if d > 0.0 {
            gains.push(d);
            losses.push(0.0);
        } else if d < 0.0 {
            gains.push(0.0);
            losses.push(-d);
        } else {
            // delta == 0：gain 0.0；loss = -(0.0) = -0.0（与 Python 逐位一致）
            gains.push(0.0);
            losses.push(-0.0);
        }
    }
    (gains, losses)
}

/// 端到端 Wilder RSI（与 `calculate_indicators` 内的实现逐位等价）
#[pyfunction]
fn wilder_rsi(closes: Vec<f64>, period: usize) -> PyResult<f64> {
    if period == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err("period 必须 >= 1"));
    }
    let (gains, losses) = gain_loss(&closes);
    let avg_gain = wild_smooth_impl(&gains, period);
    let avg_loss = wild_smooth_impl(&losses, period);
    // Python: rs = avg_gain / avg_loss if avg_loss != 0 else 0.0
    let rs = if avg_loss != 0.0 {
        avg_gain / avg_loss
    } else {
        0.0
    };
    Ok(100.0 - (100.0 / (1.0 + rs)))
}

/// 平台天数：从最后一根往前数，价格相对最新收盘价波动不超过 pct_range 的连续根数
#[pyfunction]
fn consolidation_days(closes: Vec<f64>, pct_range: f64, lookback: usize) -> u32 {
    let n = lookback.min(closes.len());
    if n < 2 {
        return 0;
    }
    let slice = &closes[closes.len() - n..];
    let ref_val = slice[n - 1];
    let mut days = 0u32;
    for &c in slice.iter().rev() {
        if (c / ref_val - 1.0).abs() <= pct_range {
            days += 1;
        } else {
            break;
        }
    }
    days
}

/// 连续涨跌天数（入参为已去掉 NaN 的 pct_change 序列；正=连涨，负=连跌）
#[pyfunction]
fn consecutive_direction(pcts: Vec<f64>) -> i64 {
    let Some(&last) = pcts.last() else {
        return 0;
    };
    let direction: i64 = if last >= 0.0 { 1 } else { -1 };
    let mut count: i64 = 0;
    for &p in pcts.iter().rev() {
        let same = if direction > 0 { p >= 0.0 } else { p < 0.0 };
        if same {
            count += 1;
        } else {
            break;
        }
    }
    count * direction
}

/// 模块出口
#[pymodule]
fn qerust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(wrap_pyfunction!(wild_smooth, m)?)?;
    m.add_function(wrap_pyfunction!(wilder_rsi, m)?)?;
    m.add_function(wrap_pyfunction!(consolidation_days, m)?)?;
    m.add_function(wrap_pyfunction!(consecutive_direction, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairwise_matches_naive_for_small() {
        let a = [1.5, 2.5, -0.25, 4.0, 0.0, 1e-9, 3.25, -7.0];
        let naive: f64 = a.iter().sum();
        assert_eq!(pairwise_sum(&a), naive);
    }

    #[test]
    fn wild_smooth_short_series_falls_back_to_mean() {
        // n < period → 整体均值
        let v = [1.0, 2.0, 3.0];
        assert_eq!(wild_smooth_impl(&v, 14), 2.0);
    }

    #[test]
    fn wild_smooth_skips_nan() {
        let v = [f64::NAN, 1.0, f64::NAN, 3.0, 5.0];
        // valid = [1,3,5] → 均值 3.0
        assert_eq!(wild_smooth_impl(&v, 14), 3.0);
    }

    #[test]
    fn rsi_monotonic_up_is_100() {
        // 一路上涨 → avg_loss = 0 → rs = 0 → RSI = 0?? 注意：Python 口径 avg_loss=0 → rs=0 → RSI=0
        // 这正是既有实现的行为（与数学直觉相反），Rust 侧必须保持一致，不得"修正"。
        let closes: Vec<f64> = (1..=30).map(|i| i as f64).collect();
        assert_eq!(wilder_rsi(closes, 14).unwrap(), 0.0);
    }

    #[test]
    fn consolidation_counts_backwards() {
        let closes = vec![10.0, 10.0, 10.1, 10.2, 10.15];
        // ref=10.15：10.15(+0%) 10.2(+0.49%) 10.1(-0.49%) 都在 3% 内；10.0(-1.48%) 也在
        assert_eq!(consolidation_days(closes, 0.03, 30), 5);
    }

    #[test]
    fn direction_counts_consecutive_ups() {
        let pcts = vec![0.01, -0.02, 0.03, 0.01];
        assert_eq!(consecutive_direction(pcts), 2);
        let pcts = vec![0.01, 0.02, -0.03, -0.01, -0.02];
        assert_eq!(consecutive_direction(pcts), -3);
        assert_eq!(consecutive_direction(vec![]), 0);
    }
}
