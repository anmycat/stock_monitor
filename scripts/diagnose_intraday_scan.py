#!/usr/bin/env python3
import os
import sys
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

load_dotenv(ROOT / "config" / ".env")

from modules.decision_support import build_trade_decision_card
from modules.fund_flow import get_stock_fund_flow_days
from modules.sector_analysis import auto_discover_hot_sectors, get_market_active_top
from modules.stock_scanner import _candidate_rows, _fetch_stock_data, _positive_flow_days


def _float_env(name: str, default: str) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return float(default)


def _int_env(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return int(default)


def main():
    universe_size = _int_env("INTRADAY_SCAN_UNIVERSE", "180")
    active_limit = max(30, universe_size)
    min_pct = _float_env("SCAN_MIN_PCT_CHANGE", "1.5")
    max_pct = _float_env("SCAN_MAX_PCT_CHANGE", "9.5")
    min_turnover = _float_env("SCAN_MIN_TURNOVER_RATE", "1.5")
    min_score = _int_env("SCAN_MIN_SCORE", "35")
    use_fund_flow = os.getenv("INTRADAY_USE_FUND_FLOW", "false").lower() == "true"
    detail_limit = _int_env("DIAGNOSE_DETAIL_LIMIT", "15")
    max_eval_rows = _int_env("DIAGNOSE_MAX_EVAL_ROWS", "12")

    hot_sectors = auto_discover_hot_sectors(
        min_stocks_rising=_int_env("HOT_SECTOR_MIN_STOCKS", "5"),
        min_avg_pct=_float_env("HOT_SECTOR_MIN_PCT", "2.0"),
    )
    active_rows = get_market_active_top(limit=active_limit)
    candidates = _candidate_rows(active_rows, hot_sectors, universe_size)

    print("=== Intraday Scan Diagnose ===")
    print(
        f"active_rows={len(active_rows)} hot_sectors={len(hot_sectors)} "
        f"candidate_rows={len(candidates)}"
    )
    print(
        f"thresholds: min_pct={min_pct} max_pct={max_pct} "
        f"min_turnover={min_turnover} min_score={min_score} "
        f"use_fund_flow={use_fund_flow}"
    )

    missing_pct = sum(1 for row in candidates if row.get("pct_change") is None)
    missing_turnover = sum(1 for row in candidates if row.get("turnover_rate") is None)
    prefiltered = []
    near_miss = []
    for row in candidates:
        pct = row.get("pct_change")
        turnover = row.get("turnover_rate")
        if pct is None or turnover is None:
            continue
        pct_v = float(pct)
        turnover_v = float(turnover)
        if min_pct <= pct_v <= max_pct and turnover_v >= min_turnover:
            prefiltered.append(row)
        elif (pct_v >= min_pct - 1.0 and pct_v <= max_pct) or (
            turnover_v >= max(0.5, min_turnover - 1.0)
        ):
            near_miss.append(row)

    print(
        f"missing_pct={missing_pct} missing_turnover={missing_turnover} "
        f"prefiltered={len(prefiltered)} near_miss={len(near_miss)}"
    )

    print("\n--- Top Active Sample ---")
    for idx, row in enumerate(candidates[:detail_limit], 1):
        pct = row.get("pct_change")
        turnover = row.get("turnover_rate")
        amount = row.get("amount")
        amount_text = "NA" if amount is None else f"{float(amount) / 100000000.0:.2f}亿"
        pct_text = "NA" if pct is None else f"{float(pct):+.2f}%"
        turnover_text = "NA" if turnover is None else f"{float(turnover):.2f}%"
        print(
            f"{idx}. {row.get('name')}({row.get('code')}) "
            f"{pct_text} 换手{turnover_text} 成交额{amount_text} "
            f"板块={row.get('sector') or 'NA'}"
        )

    print("\n--- Prefilter Sample ---")
    for idx, row in enumerate(prefiltered[:detail_limit], 1):
        print(
            f"{idx}. {row.get('name')}({row.get('code')}) "
            f"{float(row.get('pct_change')):+.2f}% "
            f"换手{float(row.get('turnover_rate')):.2f}%"
        )

    print("\n--- Near Miss Sample ---")
    for idx, row in enumerate(near_miss[:detail_limit], 1):
        pct = row.get("pct_change")
        turnover = row.get("turnover_rate")
        pct_text = "NA" if pct is None else f"{float(pct):+.2f}%"
        turnover_text = "NA" if turnover is None else f"{float(turnover):.2f}%"
        print(
            f"{idx}. {row.get('name')}({row.get('code')}) "
            f"{pct_text} 换手{turnover_text}"
        )

    cards = []
    eval_limit = max(1, min(max_eval_rows, max(detail_limit, 6)))
    for row in prefiltered[:eval_limit]:
        result = _fetch_stock_data(row, use_fund_flow)
        if result is None:
            continue
        flow_rows = result["flow_rows"]
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
            positive_flow_days=_positive_flow_days(flow_rows),
            flow_total=result["flow_total"],
        )
        cards.append(card)

    cards.sort(
        key=lambda item: (
            item.score,
            float(item.amount or 0.0),
            float(item.turnover_rate or 0.0),
        ),
        reverse=True,
    )

    print("\n--- Decision Cards ---")
    if not cards:
        print("no_cards")
        return

    passed = 0
    for idx, card in enumerate(cards[:detail_limit], 1):
        if card.score >= min_score:
            passed += 1
        check_text = " | ".join(item.render() for item in card.checklist)
        print(
            f"{idx}. {card.name}({card.code}) "
            f"score={card.score} action={card.action} "
            f"pct={card.pct_change:+.2f}% turnover={card.turnover_rate:.2f}%"
        )
        print(f"   summary={card.summary}")
        print(f"   checklist={check_text}")

    print(f"\npassed_score_gate={passed}/{len(cards[:detail_limit])}")


if __name__ == "__main__":
    main()
