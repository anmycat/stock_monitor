#!/usr/bin/env python3
"""
产品线信号重构前后回归验证（重构 P0-P5 vs 重构前 91162c76）。

目的：证明架构重构（data/services/api 分层 + 五层依赖宪法 + 风格现代化）
未改变产品线信号逻辑——同一份研究线已双源校验的实盘数据（27 只票 ×
2026-04-01~10-21 日K），重构前后 SignalGenerator.scan_stock 逐票逐日
输出必须完全一致（action/score/strategy/position/kelly）。

口径：
  - 数据注入：raw/market_full_6m.json + market_full_ext_6m.json（TDX 主源+
    腾讯不复权独立校验，止于 10-21）；指数 000001.SH 用 raw/index_daily.json
  - 防未来函数：T 日扫描只喂 ≤T 的日K（get_daily_data 截断）
  - breadth 市场宽度：mock get_realtime_quote 返回研究池 T 日涨跌统计
    （与 MarketAnalyzer._calculate_breadth 同口径 up/total，免打全市场网络）
  - 增强层中和：sys.modules 注入假 llm_ranker/candidate_context（透传），
    等价"LLM 不可用"的原生降级路径——新旧代码一致，且避免 3483 次 API 调用
  - 通知中和：notifier.send_signal 记录不真发（钉钉 webhook 未配置时本就降级）
  - cooldown：RedisCache 无 Redis 优雅降级，同序列前后一致

输出：product_signal_regression.json（新）/ _pre.json（旧），逐字段比对。

用法（仓库根）：
  coding/.venv/bin/python research/scripts/product_signal_regression.py [--out PATH]
"""
import argparse
import json
import os
import sys

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.path.dirname(BASE)  # stock_monitor 仓库根
sys.path.insert(0, ROOT)

import pandas as pd

VAL_START, VAL_END = "2026-04-01", "2026-10-21"
INDEX_CODE = "000001.SH"


# ---------- 增强层中和（前后一致，避免真实 LLM/新闻 API 调用） ----------
class _FakeLLMRanker:
    def rank(self, candidates, max_candidates=10):
        return candidates


class _FakeCandidateContext:
    def enrich_candidates(self, candidates, max_candidates=5):
        return candidates


def _install_fake_enhancers():
    """中和 LLM 排序/候选上下文增强层（前后一致，避免真实 LLM/新闻 API 调用）。

    键名覆盖新旧两种模块路径：重构后 services.signals.llm_ranker/
    services.signals.candidate_context（P2 六子包），重构前 services.llm_ranker/
    services.candidate_context（扁平结构）。
    """
    import types
    for key, attr, cls in (
        ("quant_engine.services.signals.llm_ranker", "LLMRanker", _FakeLLMRanker),
        ("quant_engine.services.ai.llm_ranker", "LLMRanker", _FakeLLMRanker),
        ("quant_engine.services.signals.candidate_context", "CandidateContext", _FakeCandidateContext),
        ("quant_engine.services.candidate_context", "CandidateContext", _FakeCandidateContext),
    ):
        mod = sys.modules.get(key) or types.ModuleType(key)
        setattr(mod, attr, cls)
        sys.modules[key] = mod


# ---------- 数据加载 ----------
def load_research_data():
    daily = {}
    for fn in ("market_full_6m.json", "market_full_ext_6m.json"):
        with open(os.path.join(BASE, "raw", fn), encoding="utf-8") as f:
            d = json.load(f)
        for code, blk in d["daily"].items():
            rows = blk["d"]
            df = pd.DataFrame([
                {"date": dt, "open": v["o"], "high": v["h"], "low": v["l"],
                 "close": v["c"], "volume": v["vol"], "pct": v.get("pct", 0.0)}
                for dt, v in sorted(rows.items())
            ])
            daily[code] = df
    with open(os.path.join(BASE, "raw", "index_daily.json"), encoding="utf-8") as f:
        idx_raw = json.load(f)
    inner = idx_raw["daily"]["000001.SH"]["d"]
    idx_df = pd.DataFrame([
        {"date": dt, "open": v["o"], "high": v["h"], "low": v["l"],
         "close": v["c"], "volume": v.get("vol", 0.0), "pct": v.get("pct", 0.0)}
        for dt, v in sorted(inner.items())
    ])
    return daily, idx_df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(ROOT, "product_signal_regression.json"))
    args = ap.parse_args()

    _install_fake_enhancers()

    daily, idx_df = load_research_data()
    all_dates = sorted(set().union(*[set(df["date"]) for df in daily.values()]))
    all_dates = [d for d in all_dates if VAL_START <= d <= VAL_END]
    codes = sorted(daily.keys())

    # ---- 构造信号发生器（新旧 API 兼容） ----
    from quant_engine.api import dependencies as deps

    dc = deps.datacenter
    gen = deps.signal_generator

    # ---- 数据注入（防未来函数：按当前 T 日截断） ----
    state = {"date": VAL_START}

    def fake_get_daily_data(code, days=120):
        df = daily.get(code, idx_df if code == INDEX_CODE else None)
        if df is None:
            return None
        cut = df[df["date"] <= state["date"]]
        return cut.tail(days).reset_index(drop=True) if len(cut) else None

    def fake_get_stock_list():
        return pd.DataFrame([{"code": c, "name": c} for c in codes])

    def fake_get_realtime_quote(code_list):
        # T 日研究池快照（休市态=收盘），供 breadth 计算
        rows = []
        for c in code_list:
            df = daily.get(c)
            if df is None:
                continue
            cut = df[df["date"] <= state["date"]]
            if cut.empty:
                continue
            last = cut.iloc[-1]
            rows.append({"code": c, "name": c, "price": last["close"],
                         "change_pct": last["pct"], "volume": last["volume"]})
        return pd.DataFrame(rows)

    dc.get_daily_data = fake_get_daily_data
    dc.get_stock_list = fake_get_stock_list
    dc.get_realtime_quote = fake_get_realtime_quote
    # 分钟线通道中和：gates._check_minute_trigger 会走 source_manager→TDX 全池探活
    # （本地无 TDX 网关，每次 80s+）。mock 为 None 走"分钟线不可用"降级路径——
    # 与生产"网关故障"场景同路径，前后代码同一 mock，对比有效
    # （"分钟线可用"路径依赖 TDX 网关实时数据，本地不可复现，由 ECS 交易时段验证覆盖）
    dc.get_minute_data = lambda code, period="5": None

    sent_signals = []
    gen.notifier.send_signal = lambda s: sent_signals.append(s.code)

    # ---- 逐日逐票扫描 ----
    results = {}
    n_signals = 0
    for d in all_dates:
        state["date"] = d
        day_res = {}
        for code in codes:
            name = code
            try:
                sig = gen.scan_stock(code, name)
            except Exception as e:  # noqa: BLE001  # 回归脚本自身容错：记录异常形态
                day_res[code] = {"error": f"{type(e).__name__}: {e}"}
                continue
            if sig is None:
                day_res[code] = None
            else:
                n_signals += 1
                day_res[code] = {
                    "action": sig.action.value,
                    "score": round(float(sig.score), 4),
                    "strategy": sig.strategy,
                    "reason": (sig.reason or "")[:120],
                    "position": round(float(sig.position), 6),
                    "kelly_position": round(float(getattr(sig, "kelly_position", 0) or 0), 6),
                }
        results[d] = day_res
        if n_signals and len(results) % 20 == 0:
            print(f"  ...{d} 累计信号 {n_signals}", flush=True)

    payload = {
        "meta": {
            "window": f"{VAL_START}~{VAL_END}",
            "dates": len(all_dates),
            "codes": len(codes),
            "signals": n_signals,
            "notifier_sent": len(sent_signals),
            "datacenter_class": type(dc).__name__,
        },
        "results": results,
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)
    print(f"完成: {len(all_dates)} 日 × {len(codes)} 票, 信号 {n_signals} 笔 -> {args.out}")


if __name__ == "__main__":
    main()
