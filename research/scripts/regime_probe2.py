#!/usr/bin/env python3
"""D53 诊断：HMM 状态重排有效性验证（生产栈跑）

背景：hmmlearn 的 EM 收敛后，状态编号 0..N-1 与"市场强弱"无语义关联
（经典 label switching）。项目却把 0→深熊、4→强牛 硬编码映射，于是
"市场类型"实际是随机的 → 仓位因子（BEAR=0.2 / BULL=1.0）随机摆动 →
信号被随机过滤（D53 实测：同一份数据 full/diag 两种协方差给出完全不同的
状态序列；本地栈无 hmmlearn 走规则回退出 505 笔，生产栈 HMM 生效只出 16 笔）。

本脚本验证"按状态内平均收益重排编号"能否修复语义：
  1. 重排前后状态分布对比
  2. 状态与未来 5 日指数收益的对应关系（信息量检验）
  3. 重排后状态是否与实际走势方向一致

用法：python3 regime_probe2.py [--relabel] [--out PATH]
"""
import argparse
import json
import os
import sys

import numpy as np
import pandas as pd

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(BASE))

VAL_START, VAL_END = "2026-04-01", "2026-10-21"
INDEX_CODE = "000001.SH"
STATE_LABELS = ["深熊", "弱熊", "震荡", "弱牛", "强牛"]
FWD_DAYS = 5


def load_index():
    with open(os.path.join(BASE, "raw", "index_daily.json"), encoding="utf-8") as f:
        raw = json.load(f)
    inner = raw["daily"][INDEX_CODE]["d"]
    return pd.DataFrame([
        {"date": dt, "open": v["o"], "high": v["h"], "low": v["l"],
         "close": v["c"], "volume": v.get("vol", 0.0)}
        for dt, v in sorted(inner.items())
    ])


def features(df):
    d = df.tail(60).copy()
    d["return"] = d["close"].pct_change()
    d["range"] = (d["high"] - d["low"]) / d["close"]
    d["vol_chg"] = d["volume"].pct_change()
    return d[["return", "range", "vol_chg"]].dropna().values


def repair_transmat(model):
    tm = np.array(model.transmat_, dtype=float, copy=True)
    sums = tm.sum(axis=1)
    for i in np.where(sums < 1e-9)[0]:
        tm[i, :] = 0.0
        tm[i, i] = 1.0
    for i in range(tm.shape[0]):
        s = tm[i].sum()
        if s > 0 and not np.isclose(s, 1.0):
            tm[i] /= s
    model.transmat_ = tm
    return True


def regularize_covars(model, eps=1e-6):
    """把非正定的状态协方差正则化（加特征值抬升）

    hmmlearn 的 covars_ setter 会校验「对称且正定」——full 协方差下某些
    状态样本过少 → 协方差非正定 → 赋值直接抛 ValueError（且 predict 时
    的 _check 同样会抛，这正是生产日志 135 次 predict 失败的根因）。
    """
    cov = getattr(model, "covars_", None)
    if cov is None:
        return False
    ctype = getattr(model, "covariance_type", "full")
    if ctype == "full" and cov.ndim == 3:
        out = np.array(cov, dtype=float, copy=True)
        for i in range(out.shape[0]):
            m = out[i]
            m = 0.5 * (m + m.T)
            w, v = np.linalg.eigh(m)
            if float(w.min()) <= eps:
                w = np.maximum(w, eps)
                m = v @ np.diag(w) @ v.T
                m = 0.5 * (m + m.T)
            out[i] = m
        model.covars_ = out
        return True
    return False


def relabel_by_return(model):
    """按状态内平均收益重排状态编号（0=最熊 → 4=最强）"""
    means = getattr(model, "means_", None)
    if means is None:
        return False
    try:
        regularize_covars(model)
    except Exception:  # noqa: BLE001 非正定且无法正则化 → 放弃重排
        return False
    order = np.argsort(means[:, 0])
    if np.array_equal(order, np.arange(len(order))):
        return True
    model.startprob_ = model.startprob_[order]
    model.transmat_ = model.transmat_[np.ix_(order, order)]
    model.means_ = means[order]
    cov = getattr(model, "covars_", None)
    if cov is not None:
        ctype = getattr(model, "covariance_type", "full")
        if ctype == "full" and cov.ndim == 3:
            model.covars_ = cov[order]
        elif ctype == "diag" and cov.ndim == 2:
            model.covars_ = cov[order]
        elif ctype == "spherical" and cov.ndim == 1:
            model.covars_ = cov[order]
    return True


def fit_model(X, cov_types=("full", "diag")):
    from hmmlearn import hmm
    last_err = None
    for cov in cov_types:
        try:
            m = hmm.GaussianHMM(n_components=5, covariance_type=cov, n_iter=100,
                                random_state=42, min_covar=1e-3)
            m.fit(X)
            repair_transmat(m)
            return m, cov, None
        except Exception as e:  # noqa: BLE001
            last_err = e
    return None, None, last_err


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--relabel", action="store_true")
    ap.add_argument("--cov", default="auto", choices=["full", "diag", "auto"])
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    df = load_index()
    df["fwd"] = df["close"].shift(-FWD_DAYS) / df["close"] - 1.0
    dates = [d for d in df["date"] if VAL_START <= d <= VAL_END]

    rows = []
    cov_used, errs = {}, {}
    for d in dates:
        cut = df[df["date"] <= d]
        if len(cut) < 60:
            continue
        X = features(cut)
        if len(X) < 20:
            continue
        cov_types = {"full": ("full",), "diag": ("diag",), "auto": ("full", "diag")}[args.cov]
        m, cov, err = fit_model(X, cov_types)
        if m is None:
            key = str(err)[:48]
            errs[key] = errs.get(key, 0) + 1
            rows.append({"date": d, "state": None, "label": "FIT_FAIL"})
            continue
        cov_used[cov] = cov_used.get(cov, 0) + 1
        if args.relabel:
            relabel_by_return(m)
        try:
            cut2 = cut.tail(2).copy()
            cut2["return"] = cut2["close"].pct_change()
            cut2["range"] = (cut2["high"] - cut2["low"]) / cut2["close"]
            cut2["vol_chg"] = cut2["volume"].pct_change()
            f2 = cut2[["return", "range", "vol_chg"]].dropna()
            probs = m.predict_proba(f2.values[-1:].reshape(1, -1))[0]
            state = int(np.argmax(probs))
            fwd = float(cut["fwd"].iloc[-1]) if pd.notna(cut["fwd"].iloc[-1]) else None
            rows.append({"date": d, "state": state, "label": STATE_LABELS[state],
                         "conf": round(float(probs[state]), 3),
                         "fwd5": None if fwd is None else round(fwd, 5)})
        except Exception as e:  # noqa: BLE001
            key = str(e)[:48]
            errs[key] = errs.get(key, 0) + 1
            rows.append({"date": d, "state": None, "label": "PREDICT_FAIL"})

    valid = [r for r in rows if r["state"] is not None]
    from collections import Counter
    dist = Counter(r["label"] for r in valid)
    bear = sum(v for k, v in dist.items() if k in ("深熊", "弱熊"))
    print(f"[relabel={args.relabel}] 日期 {len(valid)}  协方差 {cov_used}  错误 {errs}")
    print(f"  状态分布: {dict(dist)}")
    print(f"  熊市占比: {bear}/{len(valid)} = {bear / max(1, len(valid)):.1%}")

    # 状态 → 未来 5 日收益（信息量检验）
    print("  状态 → 未来5日平均收益:")
    per_state = {}
    for st in range(5):
        vals = [r["fwd5"] for r in valid if r["state"] == st and r.get("fwd5") is not None]
        if vals:
            per_state[st] = (STATE_LABELS[st], len(vals), float(np.mean(vals)))
            print(f"    {STATE_LABELS[st]}: n={len(vals):3d} 均值 {np.mean(vals):+.4%}")
    # 单调性检验：状态序号越大，未来收益应越高
    if len(per_state) >= 3:
        seq = [per_state[s][2] for s in sorted(per_state)]
        mono = all(seq[i] < seq[i + 1] for i in range(len(seq) - 1))
        print(f"  单调性（状态序号↑ → 未来收益↑）: {'✅ 成立' if mono else '❌ 不成立'}  {seq}")

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump({"relabel": args.relabel, "rows": rows,
                       "per_state": {str(k): v for k, v in per_state.items()}},
                      f, ensure_ascii=False, indent=1)
        print(f"  已写出: {args.out}")


if __name__ == "__main__":
    main()
