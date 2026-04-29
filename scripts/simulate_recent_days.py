import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

import pandas as pd
import pandas_market_calendars as mcal
import tushare as ts
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules.decision_support import build_trade_decision_card
from modules.market import get_daily_klines
from modules.sector_analysis import get_market_active_top


def _positive_flow_days(rows: List[Dict]) -> int:
    streak = 0
    for row in reversed(rows):
        if float(row.get("main_net_in", 0.0)) > 0:
            streak += 1
            continue
        break
    return streak


def _pro_api():
    load_dotenv("config/.env")
    token = os.getenv("TUSHARE_TOKEN", "").strip()
    if not token:
        raise RuntimeError("missing TUSHARE_TOKEN")
    ts.set_token(token)
    return ts.pro_api()


def _recent_trade_days(pro, count: int) -> List[str]:
    end = datetime.now().date()
    start = end - timedelta(days=30)
    cal = mcal.get_calendar("SSE")
    schedule = cal.schedule(start_date=start, end_date=end)
    if schedule is None or schedule.empty:
        raise RuntimeError("trading calendar empty")
    return [idx.strftime("%Y%m%d") for idx in schedule.index[-count:]]


def _load_name_map(pro) -> Dict[str, str]:
    try:
        df = pro.stock_basic(exchange="", list_status="L", fields="ts_code,name")
    except Exception:
        return {}
    if df is None or df.empty:
        return {}
    return {str(row["ts_code"]): str(row["name"]) for _, row in df.iterrows()}


def _load_day_universe(pro, trade_date: str) -> pd.DataFrame:
    daily = pro.daily(
        trade_date=trade_date,
        fields="ts_code,trade_date,close,high,low,pct_chg,amount",
    )
    basic = pro.daily_basic(
        trade_date=trade_date,
        fields="ts_code,turnover_rate,turnover_rate_f,volume_ratio,total_mv,circ_mv",
    )
    if daily is None or daily.empty or basic is None or basic.empty:
        return pd.DataFrame()
    frame = daily.merge(basic, on="ts_code", how="left")
    frame = frame[frame["ts_code"].str.endswith((".SH", ".SZ"))]
    frame["turnover_rate"] = frame["turnover_rate_f"].fillna(frame["turnover_rate"])
    frame["amount"] = frame["amount"].fillna(0) * 1000.0
    return frame


def _load_history(pro, ts_code: str, trade_date: str, lookback_days: int) -> Dict:
    start_date = (datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=lookback_days * 3)).strftime("%Y%m%d")
    daily = pro.daily(
        ts_code=ts_code,
        start_date=start_date,
        end_date=trade_date,
        fields="ts_code,trade_date,close,high,low",
    )
    basic = pro.daily_basic(
        ts_code=ts_code,
        start_date=start_date,
        end_date=trade_date,
        fields="ts_code,trade_date,turnover_rate,turnover_rate_f",
    )
    money = pro.moneyflow(
        ts_code=ts_code,
        start_date=(datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=15)).strftime("%Y%m%d"),
        end_date=trade_date,
        fields="ts_code,trade_date,buy_lg_amount,sell_lg_amount,buy_elg_amount,sell_elg_amount,net_mf_amount",
    )
    daily = daily.sort_values("trade_date") if daily is not None and not daily.empty else pd.DataFrame()
    basic = basic.sort_values("trade_date") if basic is not None and not basic.empty else pd.DataFrame()
    money = money.sort_values("trade_date") if money is not None and not money.empty else pd.DataFrame()
    hist = daily.merge(basic, on=["ts_code", "trade_date"], how="left") if not daily.empty else pd.DataFrame()
    flows = []
    if not money.empty:
        for _, row in money.iterrows():
            net = row.get("net_mf_amount")
            if pd.isna(net):
                buy = float(row.get("buy_lg_amount") or 0) + float(row.get("buy_elg_amount") or 0)
                sell = float(row.get("sell_lg_amount") or 0) + float(row.get("sell_elg_amount") or 0)
                net = buy - sell
            flows.append({"trade_date": row["trade_date"], "main_net_in": float(net) * 10000.0})
    return {
        "closes": hist["close"].tolist()[-lookback_days:] if not hist.empty else [],
        "highs": hist["high"].tolist()[-lookback_days:] if not hist.empty else [],
        "lows": hist["low"].tolist()[-lookback_days:] if not hist.empty else [],
        "turnovers": hist["turnover_rate_f"].fillna(hist["turnover_rate"]).tolist()[-lookback_days:] if not hist.empty else [],
        "flows": flows[-5:],
    }


def _to_monitor_code(ts_code: str) -> str:
    text = str(ts_code or "").lower()
    if text.endswith(".sh"):
        return f"sh{text[:-3]}"
    if text.endswith(".sz"):
        return f"sz{text[:-3]}"
    return text


def _build_live_snapshot_cards(topn: int, prefilter: int, lookback_days: int, min_pct: float, max_pct: float, min_turnover: float, min_score: int):
    rows = get_market_active_top(limit=max(topn * 4, prefilter))
    rows = [
        row
        for row in rows
        if row.get("pct_change") is not None
        and row.get("turnover_rate") is not None
        and min_pct <= float(row.get("pct_change")) <= max_pct
        and float(row.get("turnover_rate")) >= min_turnover
    ]
    rows = sorted(rows, key=lambda item: float(item.get("amount") or 0.0), reverse=True)[:prefilter]

    cards = []
    use_kline = os.getenv("SIM_FALLBACK_USE_KLINE", "false").lower() == "true"
    for row in rows:
        closes = []
        highs = []
        lows = []
        turnovers = []
        if use_kline:
            try:
                klines = get_daily_klines(row["code"], days=lookback_days)
            except Exception:
                klines = []
            if klines:
                closes = [
                    item.get("close")
                    for item in klines
                    if item.get("close") is not None
                ]
                highs = [item.get("high") for item in klines if item.get("high") is not None]
                lows = [item.get("low") for item in klines if item.get("low") is not None]
                turnovers = [
                    item.get("turnover_rate")
                    for item in klines
                    if item.get("turnover_rate") is not None
                ]
        card = build_trade_decision_card(
            code=row["code"],
            name=row.get("name", row["code"]),
            price=row.get("price"),
            pct_change=row.get("pct_change"),
            turnover_rate=row.get("turnover_rate"),
            amount=row.get("amount"),
            closes=closes,
            highs=highs,
            lows=lows,
            turnover_history=turnovers,
            positive_flow_days=0,
            flow_total=None,
        )
        if card.score >= min_score:
            cards.append(card)
    cards.sort(
        key=lambda x: (x.score, float(x.amount or 0.0), float(x.turnover_rate or 0.0)),
        reverse=True,
    )
    return cards[:topn]


def _print_cards(title: str, cards):
    print(f"=== {title} ===")
    if not cards:
        print("no_data")
        return
    for i, card in enumerate(cards, 1):
        price_text = "NA" if card.price is None else f"{card.price:.2f}"
        pct_text = "NA" if card.pct_change is None else f"{card.pct_change:+.2f}%"
        turnover_text = "NA" if card.turnover_rate is None else f"{card.turnover_rate:.2f}%"
        print(
            f"{i}. {card.name}({card.code}) {price_text} {pct_text} "
            f"换手{turnover_text} 评分{card.score} [{card.action}]"
        )
        print(f"   结论: {card.summary}")
        print(f"   风险: {card.risk_warning}")


def simulate_recent_days(days: int, topn: int, prefilter: int, lookback_days: int):
    pro = _pro_api()
    name_map = _load_name_map(pro)
    trade_days = _recent_trade_days(pro, days)

    min_pct = float(os.getenv("SCAN_MIN_PCT_CHANGE", "2.0"))
    max_pct = float(os.getenv("SCAN_MAX_PCT_CHANGE", "9.5"))
    min_turnover = float(os.getenv("SCAN_MIN_TURNOVER_RATE", "3.0"))
    min_score = int(os.getenv("SCAN_MIN_SCORE", "55"))

    for trade_date in trade_days:
        try:
            frame = _load_day_universe(pro, trade_date)
        except Exception as exc:
            message = str(exc)
            if (
                "访问权限" in message
                or "permission" in message.lower()
                or "no permission" in message.lower()
            ):
                print("=== live_snapshot_fallback ===")
                print(f"fallback_reason={message}")
                cards = _build_live_snapshot_cards(
                    topn=topn,
                    prefilter=prefilter,
                    lookback_days=lookback_days,
                    min_pct=min_pct,
                    max_pct=max_pct,
                    min_turnover=min_turnover,
                    min_score=min_score,
                )
                _print_cards("live_snapshot", cards)
                return
            raise
        if frame.empty:
            print(f"=== {trade_date} ===")
            print("no_data")
            continue

        frame = frame[(frame["pct_chg"] >= min_pct) & (frame["pct_chg"] <= max_pct) & (frame["turnover_rate"] >= min_turnover)]
        frame = frame.sort_values("amount", ascending=False).head(prefilter)

        cards = []
        for _, row in frame.iterrows():
            history = _load_history(pro, row["ts_code"], trade_date, lookback_days)
            flows = history["flows"]
            card = build_trade_decision_card(
                code=_to_monitor_code(row["ts_code"]),
                name=name_map.get(row["ts_code"], row["ts_code"]),
                price=row.get("close"),
                pct_change=row.get("pct_chg"),
                turnover_rate=row.get("turnover_rate"),
                amount=row.get("amount"),
                closes=history["closes"],
                highs=history["highs"],
                lows=history["lows"],
                turnover_history=history["turnovers"],
                positive_flow_days=_positive_flow_days(flows),
                flow_total=sum(item["main_net_in"] for item in flows),
            )
            if card.score >= min_score:
                cards.append(card)

        cards.sort(
            key=lambda x: (x.score, float(x.amount or 0.0), float(x.turnover_rate or 0.0)),
            reverse=True,
        )
        _print_cards(trade_date, cards[:topn])


def main():
    parser = argparse.ArgumentParser(description="Replay recent trading days with stock_monitor rules.")
    parser.add_argument("--days", type=int, default=4)
    parser.add_argument("--topn", type=int, default=10)
    parser.add_argument("--prefilter", type=int, default=20)
    parser.add_argument("--lookback-days", type=int, default=30)
    args = parser.parse_args()
    simulate_recent_days(args.days, args.topn, args.prefilter, args.lookback_days)


if __name__ == "__main__":
    main()
