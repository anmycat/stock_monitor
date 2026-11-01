#!/usr/bin/env python3
"""D53 诊断：市场状态（HMM）逐日序列 —— 栈间对比用

同一份指数数据（research/raw/index_daily.json）在不同栈（本地 py3.9/pandas2.2
vs ECS py3.14/pandas3.0/numpy2.4）上跑 HMM 状态判定，输出逐日状态序列，
用于定位"信号数 505 vs 16"的根因（市场状态判定差异 → 弱势市场过滤 → 信号被抹）。

用法：python3 research/scripts/regime_probe.py [--cov full|diag|auto] [--out PATH]
"""
import argparse
import json
import os
import sys

import numpy as np
import pandas as pd

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.path.dirname(BASE)
sys.path.insert(0, ROOT)

VAL_START, VAL_END = "2026-04-01", "2026-10-21"
INDEX_CODE = "000001.SH"
STATE_LABELS = ["深熊", "弱熊", "震荡", "弱牛", "强牛"]


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
    f = d[["return", "range", "vol_chg"]].dropna()
    return f.values


def fit_state(X, cov_types):
    from hmmlearn import hmm
    last_err = None
    for cov in cov_types:
        try:
            m = hmm.GaussianHMM(n_components=5, covariance_type=cov, n_iter=100,
                                random_state=42, min_covar=1e-3)
            m.fit(X)
            tm = np.array(m.transmat_, dtype=float)
            sums = tm.sum(axis=1)
            for i in np.where(sums < 1e-9)[0]:
                tm[i, :] = 0.0
                tm[i, i] = 1.0
            m.transmat_ = tm
            return m, cov, None
        except Exception as e:  # noqa: BLE001
            last_err = e
    return None, None, last_err


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cov", default="auto", choices=["full", "diag", "auto"])
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    cov_types = {"full": ["full"], "diag": ["diag"], "auto": ["full", "diag"]}[args.cov]

    df = load_index()
    dates = [d for d in df["date"] if VAL_START <= d <= VAL_END]

    rows = []
    cov_used = {}
    err_counts = {}
    for d in dates:
        cut = df[df["date"] <= d]
        if len(cut) < 60:
            continue
        X = features(cut)
        if len(X) < 20:
            continue
        m, cov, err = fit_state(X, cov_types)
        if m is None:
            key = str(err)[:50]
            err_counts[key] = err_counts.get(key, 0) + 1
            rows.append({"date": d, "state": None, "label": "FIT_FAIL", "err": key})
            continue
        cov_used[cov] = cov_used.get(cov, 0) + 1
        try:
            cut2 = cut.tail(2).copy()
            cut2["return"] = cut2["close"].pct_change()
            cut2["range"] = (cut2["high"] - cut2["low"]) / cut2["close"]
            cut2["vol_chg"] = cut2["volume"].pct_change()
            f2 = cut2[["return", "range", "vol_chg"]].dropna()
            probs = m.predict_proba(f2.values[-1:].reshape(1, -1))[0]
            state = int(np.argmax(probs))
            rows.append({"date": d, "state": state, "label": STATE_LABELS[state],
                         "conf": round(float(probs[state]), 3)})
        except Exception as e:  # noqa: BLE001
            key = str(e)[:50]
            err_counts[key] = err_counts.get(key, 0) + 1
            rows.append({"date": d, "state": None, "label": "PREDICT_FAIL", "err": key})

    from collections import Counter
    dist = Counter(r["label"] for r in rows)
    print(f"日期数 {len(rows)}  协方差使用 {cov_used}  错误 {err_counts}")
    print("状态分布:", dict(dist))
    bear = sum(v for k, v in dist.items() if k in ("深熊", "弱熊"))
    print(f"熊市占比: {bear}/{len(rows)} = {bear / max(1, len(rows)):.1%}")
    print("前 12 日:", [(r['date'], r['label']) for r in rows[:12]])

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump({"rows": rows, "cov_used": cov_used, "errors": err_counts},
                      f, ensure_ascii=False, indent=1)
        print(f"已写出: {args.out}")


if __name__ == "__main__":
    main()
