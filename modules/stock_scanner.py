import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

from .decision_support import TradeDecisionCard, build_trade_decision_card
from .fund_flow import get_stock_fund_flow_days
from .market import get_daily_klines, normalize_symbol
from .sector_analysis import get_market_active_top


def _sector_lookup(hot_sectors: List[Dict]) -> Dict[str, Dict]:
    mapping = {}
    for sector in hot_sectors or []:
        sector_name = sector.get("sector") or sector.get("name")
        sector_signal = sector.get("signal")
        sector_pct = sector.get("avg_pct_change", sector.get("pct_change"))
        for stock in sector.get("top_stocks", []) or []:
            code = normalize_symbol(stock.get("code"))
            if not code:
                continue
            mapping[code] = {
                "sector": sector_name,
                "sector_signal": sector_signal,
                "sector_pct": sector_pct,
            }
    return mapping


def _positive_flow_days(flow_rows: List[Dict]) -> int:
    streak = 0
    for row in reversed(flow_rows or []):
        value = row.get("main_net_in")
        if value is None:
            continue
        if float(value) > 0:
            streak += 1
            continue
        break
    return streak


def _candidate_rows(
    active_rows: List[Dict], hot_sectors: List[Dict], universe_size: int
) -> List[Dict]:
    sector_meta = _sector_lookup(hot_sectors)
    merged = {}

    for row in active_rows or []:
        code = normalize_symbol(row.get("code"))
        if not code:
            continue
        merged[code] = {
            "code": code,
            "name": row.get("name") or code,
            "price": row.get("price"),
            "pct_change": row.get("pct_change"),
            "turnover_rate": row.get("turnover_rate"),
            "amount": row.get("amount"),
            **sector_meta.get(code, {}),
        }

    for sector in hot_sectors or []:
        sector_name = sector.get("sector") or sector.get("name")
        sector_signal = sector.get("signal")
        sector_pct = sector.get("avg_pct_change", sector.get("pct_change"))
        for row in sector.get("top_stocks", []) or []:
            code = normalize_symbol(row.get("code"))
            if not code:
                continue
            existing = merged.setdefault(
                code,
                {
                    "code": code,
                    "name": row.get("name") or code,
                    "price": row.get("price"),
                    "pct_change": row.get("pct_change"),
                    "turnover_rate": row.get("turnover_rate"),
                    "amount": row.get("amount"),
                },
            )
            existing["sector"] = sector_name
            existing["sector_signal"] = sector_signal
            existing["sector_pct"] = sector_pct

    rows = list(merged.values())
    rows.sort(key=lambda x: float(x.get("amount") or 0.0), reverse=True)
    return rows[: max(20, int(universe_size))]


def _fetch_stock_data(row: Dict, use_fund_flow: bool) -> Optional[Dict]:
    """并行获取单只股票数据"""
    try:
        code = row["code"]
        klines = get_daily_klines(code, days=30)
        closes = [item.get("close") for item in klines if item.get("close") is not None]
        highs = [item.get("high") for item in klines if item.get("high") is not None]
        lows = [item.get("low") for item in klines if item.get("low") is not None]
        turnover_history = [
            item.get("turnover_rate")
            for item in klines
            if item.get("turnover_rate") is not None
        ]

        flow_rows = []
        flow_total = None
        if use_fund_flow:
            flow = get_stock_fund_flow_days(code, days=5)
            flow_rows = flow.get("data", [])
            flow_total = flow.get("main_total")

        return {
            "row": row,
            "closes": closes,
            "highs": highs,
            "lows": lows,
            "turnover_history": turnover_history,
            "flow_rows": flow_rows,
            "flow_total": flow_total,
        }
    except Exception:
        return None


def scan_market_trade_candidates(
    hot_sectors: List[Dict],
    limit: int = 10,
    universe_size: int = 160,
    market_context: str = "",
) -> List[TradeDecisionCard]:
    active_rows = get_market_active_top(limit=max(30, int(universe_size)))
    candidates = _candidate_rows(active_rows, hot_sectors, universe_size)

    min_pct = float(os.environ.get("SCAN_MIN_PCT_CHANGE", "1.5"))
    max_pct = float(os.environ.get("SCAN_MAX_PCT_CHANGE", "9.5"))
    min_turnover = float(os.environ.get("SCAN_MIN_TURNOVER_RATE", "1.5"))
    min_score = int(os.environ.get("SCAN_MIN_SCORE", "35"))
    use_fund_flow = os.environ.get("INTRADAY_USE_FUND_FLOW", "false").lower() == "true"
    max_workers = int(os.environ.get("SCAN_MAX_WORKERS", "10"))
    max_eval_rows = int(os.environ.get("SCAN_MAX_EVAL_ROWS", "24"))
    adaptive_enabled = (
        os.environ.get("SCAN_ADAPTIVE_ENABLED", "true").lower() == "true"
    )

    def _prefilter_rows(pct_floor: float, turnover_floor: float) -> List[Dict]:
        rows = []
        for row in candidates:
            pct = row.get("pct_change")
            turnover = row.get("turnover_rate")
            if pct is None or turnover is None:
                continue
            if not (pct_floor <= float(pct) <= max_pct):
                continue
            if float(turnover) < turnover_floor:
                continue
            rows.append(row)
        return rows

    def _build_cards(rows: List[Dict], min_score_gate: int, scan_mode: str) -> List[TradeDecisionCard]:
        if not rows:
            return []
        rows = rows[: max(1, max_eval_rows)]
        cards: List[TradeDecisionCard] = []
        worker_n = max(1, min(max_workers, len(rows)))
        with ThreadPoolExecutor(max_workers=worker_n) as executor:
            futures = {
                executor.submit(_fetch_stock_data, row, use_fund_flow): row
                for row in rows
            }
            for future in as_completed(futures):
                result = future.result()
                if result is None:
                    continue

                row = result["row"]
                card = build_trade_decision_card(
                    code=row["code"],
                    name=row.get("name") or row["code"],
                    price=row.get("price"),
                    pct_change=row.get("pct_change"),
                    turnover_rate=row.get("turnover_rate"),
                    amount=row.get("amount"),
                    closes=result["closes"],
                    highs=result["highs"],
                    lows=result["lows"],
                    turnover_history=result["turnover_history"],
                    sector=row.get("sector"),
                    sector_signal=row.get("sector_signal"),
                    sector_pct=row.get("sector_pct"),
                    positive_flow_days=_positive_flow_days(result["flow_rows"]),
                    flow_total=result["flow_total"],
                )
                if card.score < min_score_gate:
                    continue
                card.metrics["scan_mode"] = scan_mode
                cards.append(card)
        return cards

    filtered = _prefilter_rows(min_pct, min_turnover)[: max(1, max_eval_rows)]
    cards = _build_cards(filtered, min_score, "normal")

    if not cards and adaptive_enabled:
        is_hot = len(hot_sectors or []) >= int(
            os.environ.get("SCAN_RELAX_SECTOR_THRESHOLD", "4")
        )
        context = str(market_context or "").upper()
        if is_hot or context in {"BULL", "CONSOLIDATION", "BULL_WEAK"}:
            relax_min_pct = float(
                os.environ.get("SCAN_RELAX_MIN_PCT_CHANGE", str(max(1.2, min_pct - 1.2)))
            )
            relax_min_turnover = float(
                os.environ.get(
                    "SCAN_RELAX_MIN_TURNOVER_RATE",
                    str(max(1.8, min_turnover - 1.0)),
                )
            )
            relax_min_score = int(
                os.environ.get("SCAN_RELAX_MIN_SCORE", str(max(38, min_score - 7)))
            )
            relaxed = _prefilter_rows(relax_min_pct, relax_min_turnover)[
                : max(1, max_eval_rows)
            ]
            cards = _build_cards(relaxed, relax_min_score, "relaxed")

    cards.sort(
        key=lambda x: (
            x.score,
            float(x.amount or 0.0),
            float(x.turnover_rate or 0.0),
            float(x.pct_change or 0.0),
        ),
        reverse=True,
    )
    return cards[: max(1, int(limit))]


def scan_burst_fund_signals(limit: int = 10, universe_size: int = 300) -> List[Dict]:
    """A段：起爆资金扫描（3秒双快照）"""
    topn = max(1, int(limit))
    size = max(100, int(universe_size))
    interval_seconds = max(
        1.0, float(os.getenv("BURST_SCAN_INTERVAL_SECONDS", "3"))
    )
    min_delta_pct = float(os.getenv("BURST_MIN_3S_PCT_CHANGE", "0.4"))
    min_delta_amount = float(os.getenv("BURST_MIN_3S_AMOUNT", "6000000"))
    min_pct = float(os.getenv("BURST_MIN_PCT_CHANGE", "2.0"))
    max_pct = float(os.getenv("BURST_MAX_PCT_CHANGE", "7.5"))

    first = get_market_active_top(limit=size, force_refresh=True)
    if not first:
        return []
    time.sleep(interval_seconds)
    second = get_market_active_top(limit=size, force_refresh=True)
    if not second:
        return []

    first_map = {}
    for row in first:
        code = normalize_symbol(row.get("code"))
        if not code:
            continue
        first_map[code] = row

    out = []
    for row in second:
        code = normalize_symbol(row.get("code"))
        if not code:
            continue
        prev = first_map.get(code)
        if not prev:
            continue
        price1 = prev.get("price")
        price2 = row.get("price")
        pct1 = prev.get("pct_change")
        pct2 = row.get("pct_change")
        amount1 = prev.get("amount")
        amount2 = row.get("amount")
        turnover = row.get("turnover_rate")

        if amount1 is None or amount2 is None:
            continue
        try:
            delta_amount = float(amount2) - float(amount1)
        except (TypeError, ValueError):
            continue
        if delta_amount < min_delta_amount:
            continue

        delta_pct_3s = None
        if price1 not in (None, 0) and price2 is not None:
            try:
                delta_pct_3s = (float(price2) - float(price1)) / float(price1) * 100.0
            except (TypeError, ValueError, ZeroDivisionError):
                delta_pct_3s = None
        if delta_pct_3s is None and pct1 is not None and pct2 is not None:
            try:
                delta_pct_3s = float(pct2) - float(pct1)
            except (TypeError, ValueError):
                delta_pct_3s = None
        if delta_pct_3s is None or delta_pct_3s < min_delta_pct:
            continue

        try:
            pct_now = float(pct2)
        except (TypeError, ValueError):
            continue
        if pct_now < min_pct or pct_now > max_pct:
            continue

        out.append(
            {
                "code": code,
                "name": row.get("name") or code,
                "price": row.get("price"),
                "pct_change": pct_now,
                "turnover_rate": turnover,
                "amount": amount2,
                "delta_pct_3s": delta_pct_3s,
                "delta_amount_3s": delta_amount,
            }
        )

    out.sort(
        key=lambda item: (
            float(item.get("delta_amount_3s") or 0.0),
            float(item.get("delta_pct_3s") or 0.0),
            float(item.get("amount") or 0.0),
        ),
        reverse=True,
    )
    return out[:topn]


def scan_small_cap_monitor(
    hot_sectors: List[Dict], limit: int = 10, universe_size: int = 300
) -> List[Dict]:
    """B段：小盘股监控"""
    topn = max(1, int(limit))
    size = max(100, int(universe_size))
    min_mv_yi = float(os.getenv("SMALL_CAP_MIN_MV_YI", "30"))
    max_mv_yi = float(os.getenv("SMALL_CAP_MAX_MV_YI", "300"))
    min_pct = float(os.getenv("SMALL_CAP_MIN_PCT_CHANGE", "1.5"))
    max_pct = float(os.getenv("SMALL_CAP_MAX_PCT_CHANGE", "9.5"))
    min_turnover = float(os.getenv("SMALL_CAP_MIN_TURNOVER_RATE", "3.0"))
    min_amount_wan = float(os.getenv("SMALL_CAP_MIN_AMOUNT_WAN", "3000"))

    rows = get_market_active_top(limit=size)
    if not rows:
        return []

    sector_meta = _sector_lookup(hot_sectors)
    out = []
    for row in rows:
        code = normalize_symbol(row.get("code"))
        if not code:
            continue
        total_mv = row.get("total_mv")
        pct = row.get("pct_change")
        turnover = row.get("turnover_rate")
        amount = row.get("amount")
        if total_mv is None or pct is None or turnover is None or amount is None:
            continue
        try:
            total_mv_v = float(total_mv)
            pct_v = float(pct)
            turnover_v = float(turnover)
            amount_v = float(amount)
        except (TypeError, ValueError):
            continue

        if total_mv_v < (min_mv_yi * 100000000.0):
            continue
        if total_mv_v > (max_mv_yi * 100000000.0):
            continue
        if pct_v < min_pct or pct_v > max_pct:
            continue
        if turnover_v < min_turnover:
            continue
        if amount_v < (min_amount_wan * 10000.0):
            continue

        action = "观察"
        if turnover_v >= 5.0 and 2.0 <= pct_v <= 7.5:
            action = "关注"
        elif pct_v > 7.5:
            action = "过热"

        out.append(
            {
                "code": code,
                "name": row.get("name") or code,
                "price": row.get("price"),
                "pct_change": pct_v,
                "turnover_rate": turnover_v,
                "amount": amount_v,
                "total_mv": total_mv_v,
                "sector": (sector_meta.get(code) or {}).get("sector"),
                "action": action,
            }
        )

    out.sort(
        key=lambda item: (
            float(item.get("amount") or 0.0),
            float(item.get("turnover_rate") or 0.0),
            float(item.get("pct_change") or 0.0),
        ),
        reverse=True,
    )
    return out[:topn]


def format_stock_alert(analysis: Dict, reason: str = "") -> str:
    """格式化股票告警消息: 名称(代码)—换手率—推送理由"""
    from .market import is_trading_day, now_bj

    name = analysis.get("name", "")
    code = analysis.get("code", "")
    pct = analysis.get("pct_change", 0) or 0
    turnover = analysis.get("turnover_rate", 0) or 0
    rating = analysis.get("rating", "HOLD")
    reasons = analysis.get("reasons", [])

    trading = is_trading_day()
    time_str = now_bj().strftime("%H:%M")
    in_trading_hours = "09:30" <= time_str <= "15:00" and trading

    signal = "HOLD"
    if rating == "STRONG":
        signal = "强烈关注"
    elif rating == "BUY":
        signal = "关注"

    reason_text = reason
    if not reason_text and reasons:
        reason_text = " ".join(reasons[:2])
    elif not reason_text and analysis.get("signals"):
        reason_text = " ".join(analysis["signals"][:2])

    turnover_str = f"{turnover:.2f}" if turnover < 1 else f"{turnover:.1f}"
    pct_str = f"{pct:+.2f}"

    time_note = "" if in_trading_hours else "[盘后数据]"

    return f"{name}({code})—换手{turnover_str}%—涨跌{pct_str}%—{signal} {reason_text} {time_note}"


def analyze_stock_activity(code: str, sector_stocks: List[Dict] = None) -> Dict:
    """股票活跃度分析"""
    from .market import get_quote

    snapshot = get_quote(code)
    if not snapshot:
        return {"code": code, "error": "no data"}

    score = 0
    signals = []
    concerns = []
    reasons = []

    turnover = snapshot.get("turnover_rate", 0) or 0
    pct = snapshot.get("pct_change", 0) or 0
    volume = snapshot.get("volume")

    if turnover >= 10:
        score += 30
        signals.append(f"高换手{turnover:.1f}%")
        reasons.append("换手率激增")
    elif turnover >= 5:
        score += 20
        signals.append(f"活跃{turnover:.1f}%")
        reasons.append("换手率放大")
    elif turnover >= 3:
        score += 10
    elif turnover < 0.5:
        concerns.append(f"换手过低{turnover:.1f}%")

    if 3 <= pct <= 9:
        score += 30
        signals.append(f"涨幅{pct:.1f}%")
        reasons.append("涨幅健康")
    elif pct > 9:
        score += 15
        concerns.append(f"涨幅过大{pct:.1f}%")
        reasons.append("涨幅过大注意风险")
    elif pct > 0:
        score += 10
    elif -3 <= pct < 0:
        score += 10
    elif pct < -5:
        concerns.append(f"跌幅过大{pct:.1f}%")
        reasons.append("跌幅较大")

    if volume and volume > 100000000:
        score += 20
        signals.append("成交活跃")
        reasons.append("成交量放大")
    elif volume and volume > 50000000:
        score += 10

    if score >= 80:
        rating = "STRONG"
    elif score >= 60:
        rating = "BUY"
    elif score >= 40:
        rating = "HOLD"
    else:
        rating = "WEAK"

    return {
        "code": code,
        "name": snapshot.get("name", ""),
        "price": snapshot.get("price"),
        "pct_change": pct,
        "turnover_rate": turnover,
        "volume": volume,
        "score": score,
        "rating": rating,
        "signals": signals,
        "concerns": concerns,
        "reasons": reasons[:3],
    }


def scan_watchlist_stocks(
    watch_stocks: List[Dict], min_score: int = 30, sector_data: Dict = None
) -> List[Dict]:
    """扫描自选股"""
    results = []

    for stock in watch_stocks:
        code = stock.get("code")
        if not code:
            continue

        sector = stock.get("sector", "")
        sector_stocks = sector_data.get(sector, []) if sector_data and sector else None

        analysis = analyze_stock_activity(code, sector_stocks)
        if "error" not in analysis and analysis.get("score", 0) >= min_score:
            analysis["sector"] = sector
            results.append(analysis)

    results.sort(key=lambda x: x.get("score", 0), reverse=True)
    return results


def detect_turnover_alerts(
    watch_stocks: List[Dict], threshold: float = 5.0
) -> List[Dict]:
    """检测换手率异动"""
    alerts = []

    for stock in watch_stocks:
        code = stock.get("code")
        if not code:
            continue

        analysis = analyze_stock_activity(code)
        turnover = analysis.get("turnover_rate", 0) or 0

        if turnover >= threshold:
            alerts.append(analysis)

    alerts.sort(key=lambda x: x.get("turnover_rate", 0), reverse=True)
    return alerts


def detect_fund_flow_proxy(watch_stocks: List[Dict]) -> List[Dict]:
    """资金流向代理检测（基于价格和换手率变化）"""
    flows = []

    for stock in watch_stocks:
        code = stock.get("code")
        if not code:
            continue

        analysis = analyze_stock_activity(code)
        pct = analysis.get("pct_change", 0) or 0
        turnover = analysis.get("turnover_rate", 0) or 0

        if turnover >= 3 and abs(pct) >= 1:
            if pct > 0:
                flow = "净流入"
                strength = "强" if turnover >= 5 else "中"
            else:
                flow = "净流出"
                strength = "强" if turnover >= 5 else "中"

            flows.append(
                {
                    "code": code,
                    "name": analysis.get("name", ""),
                    "flow": flow,
                    "strength": strength,
                    "pct_change": pct,
                    "turnover_rate": turnover,
                    "score": analysis.get("score", 0),
                }
            )

    flows.sort(key=lambda x: x.get("score", 0), reverse=True)
    return flows
