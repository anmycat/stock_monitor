import os
import time
import threading
from collections import deque
from contextlib import contextmanager

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

from logger import get_logger
from modules.analysis import (
    blend_risk_with_sentiment,
    load_bettafish_signal,
    ma_signal,
    price_metrics,
    risk_score,
)
from modules.sentiment import (
    analyze_news_sentiment,
)
from modules.market import (
    get_call_auction_top10_with_status,
    get_quote,
    get_source_health_snapshot,
    is_trading_day,
    now_bj,
)
from modules.notifier import notify_with_guard
from modules.sector_analysis import (
    auto_discover_hot_sectors,
    get_market_active_top,
    get_market_breadth,
    get_sector_list,
    load_watchlist,
)
from modules.fund_flow import (
    get_famous_trader_stocks,
)
from modules.factors import (
    score_stocks_by_factors,
)
from modules.human_thinking import (
    get_market_context,
    get_time_pattern,
    TradingReason,
)
from modules.trader_brain import (
    get_professional_trader,
)
from modules.stock_scanner import (
    scan_burst_fund_signals,
    scan_market_trade_candidates,
    scan_small_cap_monitor,
)
from modules.etf_tracker import (
    get_etf_trade_states,
    get_etf_stock_alerts,
    get_market_index_summary,
)
from modules.weekly_ops import (
    build_news_digest,
    check_daily_repo_status,
    fetch_finance_news,
    load_weekly_state,
    save_weekly_state,
)
from modules.briefing import generate_market_brief

try:
    from modules.dsa_integration import (
        get_dsa_quote_data,
        get_dsa_stock_info,
        get_dsa_analysis_available,
        get_dsa_model_list,
    )

    DSA_INTEGRATION_AVAILABLE = True
except ImportError:
    DSA_INTEGRATION_AVAILABLE = False

    def get_dsa_quote_data():
        return {}

    def get_dsa_stock_info(code):
        return None

    def get_dsa_analysis_available():
        return False

    def get_dsa_model_list():
        return []


logger = get_logger("guardian")
load_dotenv("config/.env")

WATCHLIST = load_watchlist()

PRICE_WINDOWS = {}
TURNOVER_SERIES = deque(maxlen=120)
VOLUME_SERIES = deque(maxlen=120)
TURNOVER_WINDOWS = {}
MARKET_INDEX_ORDER = [
    ("sh000001", "上证指数"),
    ("sz399001", "深证成指"),
    ("sh000300", "沪深300"),
    ("sz399006", "创业板指"),
]
MARKET_INDEX_LABEL = {
    "sh000001": "上证",
    "sz399001": "深证",
    "sh000300": "沪深300",
    "sz399006": "创业板",
}

_job_locks = {}
_scheduler = None


@contextmanager
def _job_execution_lock(job_name):
    lock = _job_locks.setdefault(job_name, threading.Lock())
    acquired = lock.acquire(blocking=False)
    if not acquired:
        logger.warning("job_skipped_already_running job=%s", job_name)
        yield False
        return
    try:
        yield True
    finally:
        lock.release()


def _safe_get_price(quote):
    price = quote.get("price")
    if price is None:
        return None
    try:
        return float(price)
    except (ValueError, TypeError):
        return None


def _safe_get_float(quote, key, default=None):
    value = quote.get(key)
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _format_amount_yi(amount):
    if amount is None:
        return "NA"
    try:
        value = float(amount)
    except (ValueError, TypeError):
        return "NA"
    return f"{value / 100000000.0:.1f}亿"


def _format_amount_wan(amount, decimals=0):
    if amount is None:
        return "NA"
    try:
        value = float(amount)
    except (ValueError, TypeError):
        return "NA"
    fmt = f"{{:.{int(decimals)}f}}"
    return f"{fmt.format(value / 10000.0)}万"


def _format_total_mv_yi(total_mv):
    if total_mv is None:
        return "NA"
    try:
        return f"{float(total_mv) / 100000000.0:.0f}亿"
    except (TypeError, ValueError):
        return "NA"


def _display_stock_code(code):
    text = str(code or "").strip().lower()
    if text.startswith(("sh", "sz")) and len(text) >= 8 and text[2:].isdigit():
        return text[2:]
    return str(code or "").strip()


def _format_price_text(value):
    if value is None:
        return "NA"
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "NA"


def _format_volume_text(value):
    if value is None:
        return "NA"
    try:
        vol = float(value)
    except (TypeError, ValueError):
        return "NA"
    if vol <= 0:
        return "NA"
    if vol >= 100000000:
        return f"{vol / 100000000.0:.2f}亿手"
    if vol >= 10000:
        return f"{vol / 10000.0:.1f}万手"
    return f"{vol:.0f}手"


def _format_change_shares_text(value):
    if value is None:
        return "NA"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "NA"
    sign = "+" if v > 0 else ""
    abs_v = abs(v)
    if abs_v >= 100000000:
        return f"{sign}{v / 100000000.0:.2f}亿股"
    if abs_v >= 10000:
        return f"{sign}{v / 10000.0:.2f}万股"
    return f"{sign}{v:.0f}股"


def _format_etf_change_text(value, mode):
    mode_text = str(mode or "amount").strip().lower()
    if mode_text in {"hold_pct_delta", "hold_pct_new"}:
        if value is None:
            return "NA"
        try:
            v = float(value)
        except (TypeError, ValueError):
            return "NA"
        sign = "+" if v > 0 else ""
        return f"{sign}{v:.2f}pct"
    return _format_change_shares_text(value)


def _format_market_shares_text(value):
    if value is None:
        return "NA"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "NA"
    if v <= 0:
        return "NA"
    if v >= 100000000:
        return f"{v / 100000000.0:.2f}亿股"
    if v >= 10000:
        return f"{v / 10000.0:.2f}万股"
    return f"{v:.0f}股"


def _pct_emoji(pct):
    try:
        pct_v = float(pct)
    except (TypeError, ValueError):
        return "⚪"
    if abs(pct_v) < 0.005:
        pct_v = 0.0
    if pct_v > 0:
        return "🟢"
    if pct_v < 0:
        return "🔴"
    return "⚪"


def _format_turnover_text(value):
    if value is None:
        return "NA"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "NA"
    if abs(v) >= 10:
        return f"{v:.1f}%"
    if abs(v) >= 1:
        return f"{v:.2f}%"
    return f"{v:.3f}%"


def _format_pct_ball(pct):
    if pct is None:
        return "⚪NA"
    try:
        pct_v = float(pct)
    except (TypeError, ValueError):
        return "⚪NA"
    if abs(pct_v) < 0.005:
        pct_v = 0.0
    if pct_v > 0:
        return f"🟢{pct_v:+.2f}%"
    if pct_v < 0:
        return f"🔴{pct_v:+.2f}%"
    return f"⚪{pct_v:+.2f}%"


def _format_distance_pct(value):
    if value is None:
        return "NA"
    try:
        return f"{float(value):+.2f}%"
    except (TypeError, ValueError):
        return "NA"


def _append_etf_trade_states(lines, etf_states, title="【ETF决策面板】"):
    if not etf_states:
        return

    lines.append("")
    lines.append(title)
    icon_map = {"BUYABLE": "🟢", "WATCH": "🟡", "AVOID": "❌"}
    for i, row in enumerate(etf_states[:3], 1):
        name = row.get("name", row.get("code", "ETF"))
        code = row.get("code", "")
        price = row.get("price")
        try:
            price_text = "NA" if price is None else f"{float(price):.3f}"
        except (TypeError, ValueError):
            price_text = "NA"
        pct_text = _format_pct_ball(row.get("pct_change"))
        status = row.get("status", "WATCH")
        status_text = row.get("status_text", "观望")
        action = row.get("action", "等")
        try:
            score = float(row.get("score") or 0.0)
        except (TypeError, ValueError):
            score = 0.0
        distance_text = _format_distance_pct(row.get("distance_to_support_pct"))
        support = row.get("support_price")
        try:
            support_text = "NA" if support is None else f"{float(support):.3f}"
        except (TypeError, ValueError):
            support_text = "NA"
        flow_text = row.get("flow_text", "主力流向NA")
        reason = "；".join(row.get("reasons", [])[:2])
        lines.append(
            f"  {i}. {name}({code}) {price_text} {pct_text} "
            f"{icon_map.get(status, '🟡')}{status_text} 指令:{action}"
        )
        lines.append(
            f"     支撑:{support_text} 距离:{distance_text} 资金:{flow_text} 评分:{score:.1f}"
        )
        if reason:
            lines.append(f"     理由:{reason}")


def _is_trading_weekday(dt=None):
    dt = dt or now_bj()
    return dt.weekday() < 5


def _in_session(dt=None):
    dt = dt or now_bj()
    hm = dt.strftime("%H:%M")
    return ("09:30" <= hm <= "11:30") or ("13:00" <= hm <= "14:58")


def _parse_hhmm(value, fallback):
    text = (value or fallback or "").strip()
    parts = text.split(":")
    if len(parts) != 2:
        parts = fallback.split(":")
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except Exception:
        hour, minute = 0, 0
    return hour, minute


def _load_etf_trade_panel(
    codes_key="etf_codes", max_codes=None, topn=None, quote_source=None
):
    if os.getenv("ETF_HOLDINGS_ENABLED", "true").lower() != "true":
        return []
    etf_codes = WATCHLIST.get(codes_key, ["sh510300", "sh159919"])
    if not etf_codes:
        return []
    if max_codes is not None:
        try:
            limit = max(1, int(max_codes))
            etf_codes = etf_codes[:limit]
        except (TypeError, ValueError):
            pass
    topn_text = os.getenv("ETF_STOCK_ALERT_TOPN", "").strip() or os.getenv(
        "ETF_DECISION_TOPN", "5"
    ).strip()
    try:
        resolved_topn = int(topn_text) if topn is None else int(topn)
    except (TypeError, ValueError):
        resolved_topn = 5
    resolved_source = str(
        quote_source
        or os.getenv("ETF_COMPONENT_QUOTE_SOURCE", "qtimg")
        or "qtimg"
    ).strip()
    if not resolved_source:
        resolved_source = "qtimg"
    return get_etf_stock_alerts(
        etf_codes, topn=resolved_topn, quote_source=resolved_source
    )


def _init_scheduler(tz_name):
    db_path = os.getenv("SCHEDULER_DB_PATH", "logs/apscheduler.sqlite").strip()
    if not db_path:
        db_path = "logs/apscheduler.sqlite"
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    jobstores = {"default": SQLAlchemyJobStore(url=f"sqlite:///{db_path}")}
    misfire_grace = int(os.getenv("SCHEDULER_MISFIRE_GRACE_SECONDS", "120"))
    max_instances = int(os.getenv("SCHEDULER_MAX_INSTANCES", "1"))
    coalesce = os.getenv("SCHEDULER_COALESCE", "true").lower() == "true"
    job_defaults = {
        "coalesce": coalesce,
        "max_instances": max_instances,
        "misfire_grace_time": misfire_grace,
    }
    return BackgroundScheduler(
        jobstores=jobstores, job_defaults=job_defaults, timezone=tz_name
    )


def _init_price_series(symbol):
    if symbol not in PRICE_WINDOWS:
        window = int(os.getenv("PRICE_WINDOW_SIZE", "30"))
        PRICE_WINDOWS[symbol] = deque(maxlen=max(5, window))
    if symbol not in TURNOVER_WINDOWS:
        TURNOVER_WINDOWS[symbol] = deque(maxlen=30)


def _collect_single_stock_state(symbol, quote_source):
    _init_price_series(symbol)
    quote = get_quote(symbol, source=quote_source)
    price = _safe_get_price(quote)
    if price is None or price <= 0:
        logger.warning("invalid_price symbol=%s price=%s", symbol, quote.get("price"))
        return None

    PRICE_WINDOWS[symbol].append(price)

    turnover = _safe_get_float(quote, "turnover_rate")
    volume = _safe_get_float(quote, "volume")
    if turnover is not None:
        TURNOVER_SERIES.append(turnover)
        TURNOVER_WINDOWS[symbol].append(turnover)
    if volume is not None:
        VOLUME_SERIES.append(volume)

    close_prices = list(PRICE_WINDOWS[symbol])
    signal_data = ma_signal(close_prices, window=5)
    metrics = price_metrics(close_prices)
    base_risk = risk_score(
        market_change=metrics["market_change"] * 100.0,
        volatility=metrics["volatility"],
    )
    sentiment_data = load_bettafish_signal()
    risk_data = blend_risk_with_sentiment(base_risk, sentiment_data)

    return {
        "quote": quote,
        "signal_data": signal_data,
        "metrics": metrics,
        "risk_data": risk_data,
        "sentiment_data": sentiment_data,
    }


def _collect_index_states():
    stock_states = {}
    index_source = os.getenv("INDEX_QUOTE_SOURCE") or os.getenv("QUOTE_SOURCE", "auto")
    index_source = index_source.strip() if isinstance(index_source, str) else "auto"
    if not index_source:
        index_source = "auto"
    for symbol, _ in MARKET_INDEX_ORDER:
        try:
            data = _collect_single_stock_state(symbol, index_source)
            if data:
                stock_states[symbol] = data
        except Exception as exc:
            logger.warning("index_state_failed symbol=%s err=%s", symbol, exc)
    return stock_states


def _index_name(symbol, raw_name, fallback_name):
    text = str(raw_name or "").strip()
    if not text:
        return fallback_name
    norm = text.lower().replace(".", "")
    sym = symbol.lower().replace(".", "")
    if norm in {sym, sym[2:]}:
        return fallback_name
    return text


def _index_display_name(symbol, raw_name, fallback_name):
    base = _index_name(symbol, raw_name, fallback_name)
    short = MARKET_INDEX_LABEL.get(symbol)
    if short:
        return short
    return base


def _format_morning_alert(
    auction_rows,
    news_items,
    sentiment_analysis=None,
    market_index=None,
    auction_meta=None,
    etf_panel=None,
    etf_states=None,
    auction_signals=None,
):
    lines = ["[stock_monitor] 交易日"]
    lines.append(f"📅 {now_bj().strftime('%Y-%m-%d %H:%M')}")

    if sentiment_analysis:
        sentiment = sentiment_analysis.get("overall_sentiment", "neutral")
        score = sentiment_analysis.get("score", 0.0)
        emoji = {"positive": "📈", "negative": "📉", "neutral": "➡️"}.get(sentiment, "➡️")
        lines.append(f"市场情绪: {emoji} {sentiment.upper()} ({score:+.2f})")

    if market_index:
        lines.append("")
        lines.append("【指数开盘】")
        for idx in market_index:
            symbol = str(idx.get("code") or "").strip().lower()
            fallback_name = MARKET_INDEX_LABEL.get(symbol, symbol or "指数")
            name = _index_display_name(symbol, idx.get("name"), fallback_name)
            open_price = _safe_get_float(idx, "open")
            change_pct = _safe_get_float(idx, "change_pct")
            prev_close = _safe_get_float(idx, "prev_close")
            if change_pct is not None:
                emoji = "🟢" if change_pct > 0 else "🔴" if change_pct < 0 else "⚪"
                open_text = "NA" if open_price is None else f"{open_price:.2f}"
                prev_close_text = "NA" if prev_close is None else f"{prev_close:.2f}"
                lines.append(
                    f"  {name}（{symbol}） 开盘{open_text} 昨收{prev_close_text} {emoji}{change_pct:+.2f}%"
                )

    _append_etf_trade_states(lines, etf_states, title="【ETF状态Top3】")
    _append_etf_panel(lines, etf_panel)

    lines.append("")
    lines.append("今日集合竞价情况：")
    if auction_signals:
        lines.append("  竞价信号（评分排序）：")
        for i, sig in enumerate(auction_signals[:10], 1):
            code = _display_stock_code(sig.code)
            name = sig.name or "未知"
            price_text = _format_price_text(sig.price)
            pct_text = _format_pct_ball(sig.pct_change)
            volume_text = _format_volume_text(sig.volume)
            heat_emoji = "🚀" if float(sig.score or 0) >= 80 else _pct_emoji(sig.pct_change)
            tags = "/".join(sig.tags[:2]) if sig.tags else ""
            tail = f" 🧠{float(sig.score or 0):.0f}"
            if tags:
                tail += f" {tags}"
            lines.append(
                f"  {i}. {heat_emoji} {name}（{code}） {price_text} {pct_text} 📦{volume_text}{tail}"
            )
    elif auction_rows:
        if auction_meta and auction_meta.get("stale"):
            lines.append("  备注：使用缓存数据（数据源异常）")
        for i, row in enumerate(auction_rows[:10], 1):
            code = _display_stock_code(row.get("code"))
            name = row.get("name") or "未知"
            price_text = _format_price_text(row.get("price"))
            pct_text = _format_pct_ball(row.get("pct_change"))
            volume_text = _format_volume_text(row.get("volume"))
            if volume_text == "NA":
                amount = row.get("amount")
                if amount is not None:
                    try:
                        volume_text = f"约{float(amount) / 10000.0:.0f}万(按成交额)"
                    except (TypeError, ValueError):
                        volume_text = "NA"
            lines.append(
                f"  {i}. {_pct_emoji(row.get('pct_change'))} {name}（{code}） {price_text} {pct_text} 📦{volume_text}"
            )
    else:
        if auction_meta and auction_meta.get("error"):
            lines.append("  暂无数据（数据源异常，已降级）")
        else:
            lines.append("  暂无数据")

    lines.append("")
    lines.append("新闻速览：")
    if news_items:
        digest = build_news_digest(news_items)
        lines.extend([f"  {line}" for line in digest.splitlines()])
    else:
        lines.append("  暂无新闻")
    return "\n".join(lines)


def _append_etf_panel(lines, etf_panel, title="【ETF持仓变化】", limit=100):
    if not etf_panel:
        return
    increased = etf_panel.get("increased", [])
    decreased = etf_panel.get("decreased", [])
    neutral = etf_panel.get("neutral", [])

    if not increased and not decreased and not neutral:
        return

    lines.append("")
    lines.append(title)

    if increased:
        lines.append(f"🟢🟢🟢 【跟买】ETF增持 ({len(increased)}只) 🟢🟢🟢")
        for i, row in enumerate(increased[:limit], 1):
            name = row.get("name", "")
            code = row.get("code", "")
            price = row.get("price", "NA")
            pct = row.get("pct_change", "NA")
            pct_str = str(pct) if pct != "NA" else "NA"
            if pct != "NA":
                try:
                    pct_val = float(pct.replace("%", ""))
                    if pct_val > 0:
                        pct_emoji = "🟢"
                    elif pct_val < 0:
                        pct_emoji = "🔴"
                    else:
                        pct_emoji = "⚪"
                    pct_str = f"{pct_emoji}{pct}"
                except:
                    pct_str = pct
            turnover = row.get("turnover_rate", "NA")
            etf = row.get("etf", "")
            lines.append(f"▶ {name}({code}) {price} {pct_str} 换手{turnover} [{etf}]")

    if decreased:
        lines.append("")
        lines.append(f"🔴🔴🔴 【跟卖】ETF减持 ({len(decreased)}只) 🔴🔴🔴")
        for i, row in enumerate(decreased[:limit], 1):
            name = row.get("name", "")
            code = row.get("code", "")
            price = row.get("price", "NA")
            pct = row.get("pct_change", "NA")
            pct_str = str(pct) if pct != "NA" else "NA"
            if pct != "NA":
                try:
                    pct_val = float(pct.replace("%", ""))
                    if pct_val > 0:
                        pct_emoji = "🟢"
                    elif pct_val < 0:
                        pct_emoji = "🔴"
                    else:
                        pct_emoji = "⚪"
                    pct_str = f"{pct_emoji}{pct}"
                except:
                    pct_str = pct
            turnover = row.get("turnover_rate", "NA")
            etf = row.get("etf", "")
            lines.append(f"▼ {name}({code}) {price} {pct_str} 换手{turnover} [{etf}]")

    if neutral and not increased and not decreased:
        lines.append("🟡 【观察】暂无调仓明细，以下为权重成分观察")
        for row in neutral[:limit]:
            name = row.get("name", "")
            code = row.get("code", "")
            price = row.get("price", "NA")
            pct = row.get("pct_change", "NA")
            pct_str = str(pct) if pct != "NA" else "NA"
            turnover = row.get("turnover_rate", "NA")
            etf = row.get("etf", "")
            lines.append(f"• {name}({code}) {price} {pct_str} 换手{turnover} [{etf}]")


def _format_intraday_opportunity(
    decision_cards,
    hot_sectors,
    burst_signals=None,
    small_cap_signals=None,
    famous_trader_stocks=None,
    factor_scored_stocks=None,
    market_context=None,
    time_pattern=None,
    market_assessment=None,
    etf_panel=None,
    etf_states=None,
):
    if decision_cards is None:
        decision_cards = []
    if famous_trader_stocks is None:
        famous_trader_stocks = []
    if factor_scored_stocks is None:
        factor_scored_stocks = []
    if burst_signals is None:
        burst_signals = []
    if small_cap_signals is None:
        small_cap_signals = []
    if market_context is None:
        market_context = {}
    if time_pattern is None:
        time_pattern = {}
    if market_assessment is None:
        market_assessment = {}

    reason_gen = TradingReason()
    all_reasons = []

    if market_context:
        all_reasons.extend(reason_gen.market_context_reason(market_context))
    if time_pattern:
        all_reasons.extend(reason_gen.time_period_reason(time_pattern))

    lines = ["[stock_monitor] 盘中交易机会"]
    lines.append(f"⏰ {now_bj().strftime('%Y-%m-%d %H:%M')}")

    if all_reasons:
        lines.append("")
        lines.append("【市场判断】")
        for reason in all_reasons[:3]:
            lines.append(f"  {reason}")

    ctx = market_context.get("context", "UNKNOWN") if market_context else "UNKNOWN"
    if ctx == "BULL":
        lines.append("  📈 总体偏多，可积极做多")
    elif ctx == "BEAR":
        lines.append("  🔴 总体偏弱，建议观望")
    elif ctx == "CONSOLIDATION":
        lines.append("  ➡️ 震荡格局，高抛低吸")
    else:
        lines.append("  ❓ 保持观察")

    if market_assessment:
        trend = market_assessment.get("trend", "")
        position = market_assessment.get("position", "")
        action = market_assessment.get("action", "")
        if trend:
            lines.append(f"  仓位建议: {position}")
            lines.append(f"  操作建议: {action}")

    lines.append("")
    _append_etf_trade_states(lines, etf_states, title="【ETF状态Top3】")
    _append_etf_panel(lines, etf_panel, title="【ETF监控】", limit=20)

    lines.append("")
    lines.append("【A 起爆资金】")
    if burst_signals:
        for i, row in enumerate(burst_signals[:10], 1):
            code = row.get("code", "")
            name = row.get("name", "")
            price_text = _format_price_text(row.get("price"))
            pct_text = _format_pct_ball(row.get("pct_change"))
            d3 = row.get("delta_pct_3s")
            d3_text = "NA" if d3 is None else f"{float(d3):+.2f}%"
            d_amt = _format_amount_wan(row.get("delta_amount_3s"), decimals=1)
            amount_text = _format_amount_yi(row.get("amount"))
            turnover_text = _format_turnover_text(row.get("turnover_rate"))
            lines.append(
                f"  {i}. {name}({code}) {price_text} {pct_text} "
                f"3秒{d3_text} 3秒增额{d_amt} 成交额{amount_text} 换手{turnover_text}"
            )
    else:
        lines.append("  暂无满足阈值的起爆资金个股")

    lines.append("")
    lines.append("【B 小盘股监控】")
    if small_cap_signals:
        for i, row in enumerate(small_cap_signals[:10], 1):
            code = row.get("code", "")
            name = row.get("name", "")
            price_text = _format_price_text(row.get("price"))
            pct_text = _format_pct_ball(row.get("pct_change"))
            turnover_text = _format_turnover_text(row.get("turnover_rate"))
            amount_text = _format_amount_wan(row.get("amount"), decimals=0)
            mv_text = _format_total_mv_yi(row.get("total_mv"))
            sector_text = row.get("sector") or "未识别板块"
            action = row.get("action") or "观察"
            lines.append(
                f"  {i}. {name}({code}) {price_text} {pct_text} 换手{turnover_text} "
                f"成交额{amount_text} 市值{mv_text} [{action}] {sector_text}"
            )
    else:
        lines.append("  暂无满足条件的小盘活跃股")

    lines.append("")
    lines.append("【C 现有策略扫盘】")
    if decision_cards:
        for i, card in enumerate(decision_cards[:10], 1):
            price_text = "NA" if card.price is None else f"{card.price:.2f}"
            pct = card.pct_change or 0
            if pct > 0:
                pct_emoji = "🟢"
            elif pct < 0:
                pct_emoji = "🔴"
            else:
                pct_emoji = "⚪"
            pct_text = "NA" if card.pct_change is None else f"{card.pct_change:+.2f}%"
            turnover_text = (
                "NA" if card.turnover_rate is None else f"{card.turnover_rate:.2f}%"
            )
            amount_text = (
                "NA" if card.amount is None else f"{float(card.amount) / 10000.0:.0f}万"
            )
            sector_text = card.sector or "未识别板块"

            action = card.action or ""
            if "买" in action or "关注" in action:
                action_emoji = "🟢买入"
            elif "减" in action or "卖" in action:
                action_emoji = "🔴卖出"
            elif "持" in action:
                action_emoji = "🟡持有"
            else:
                action_emoji = f"[{action}]"

            lines.append(
                f"  {i}. {card.name}({card.code}) {price_text} {pct_emoji}{pct_text} 换手{turnover_text} 成交额{amount_text} "
                f"评分{card.score} {action_emoji} {sector_text}"
            )
            lines.append(f"     结论: {card.summary}")
            check_text = " / ".join(
                f"{item.label}{'✓' if item.ok is True else '✗' if item.ok is False else '~'}"
                for item in card.checklist
            )
            lines.append(f"     检查: {check_text}")
            entry = "NA" if card.levels.entry is None else f"{card.levels.entry:.2f}"
            stop = "NA" if card.levels.stop is None else f"{card.levels.stop:.2f}"
            target = "NA" if card.levels.target is None else f"{card.levels.target:.2f}"
            lines.append(f"     点位: 买入{entry} 止损{stop} 目标{target}")
            lines.append(f"     风险: {card.risk_warning}")
    else:
        lines.append("  暂无满足条件的具体个股")

    if factor_scored_stocks:
        lines.append("")
        lines.append("【多因子选股】")
        for i, stock in enumerate(factor_scored_stocks[:8], 1):
            name = stock.get("name", "")
            code = stock.get("code", "")
            score = stock.get("score", 0)
            turnover = stock.get("turnover_rate", 0) or 0
            pct = stock.get("pct_change", 0) or 0
            if pct > 0:
                pct_emoji = "🟢"
            elif pct < 0:
                pct_emoji = "🔴"
            else:
                pct_emoji = "⚪"
            pe = stock.get("pe")
            pe_str = f"PE{pe:.1f}" if pe else ""
            reason = f"综合评分{score:.1f} {pe_str}{pct_emoji}{pct:+.2f}%"
            lines.append(
                f"  因子{i}. {name}({code})—换手{_format_turnover_text(turnover)}—{reason}"
            )

    if famous_trader_stocks:
        lines.append("")
        lines.append("【C扩展：游资关注】")
        for i, stock in enumerate(famous_trader_stocks[:10], 1):
            name = stock.get("name", "")
            code = stock.get("code", "")
            turnover = stock.get("turnover_rate")
            turnover_text = _format_turnover_text(turnover)
            net_amount = stock.get("net_amount", 0) or 0
            if net_amount > 0:
                amount_emoji = "🟢"
            elif net_amount < 0:
                amount_emoji = "🔴"
            else:
                amount_emoji = "⚪"
            reason = f"龙虎榜净买入{amount_emoji}{_format_amount_yi(net_amount)}"
            lines.append(f"  {i}. {name}({code})—换手{turnover_text}—{reason}")

    if hot_sectors:
        lines.append("")
        lines.append("【C扩展：热门板块】")
        for i, sec in enumerate(hot_sectors[:5], 1):
            pct = sec.get("avg_pct_change", 0)
            rising = sec.get("rising_count", 0)
            total = sec.get("stock_count", 0)
            turnover = sec.get("avg_turnover", 0)
            signal = sec.get("signal", "")
            emoji = "🔥" if signal == "STRONG_RISING" else "📈"
            reason = f"{rising}/{total}只上涨"
            lines.append(
                f"  {i}. {emoji}{sec['sector']}—换手{_format_turnover_text(turnover)}—{reason}"
            )
            for j, stock in enumerate(sec.get("top_stocks", [])[:5], 1):
                sp = stock.get("pct_change", 0)
                name = stock.get("name", "")[:6]
                code = stock.get("code", "")
                turnover_s = stock.get("turnover_rate", 0) or 0
                lines.append(
                    f"     {j}. {name}({code})—换手{_format_turnover_text(turnover_s)}—涨幅{sp:.2f}%"
                )

    return "\n".join(lines)


def _format_decision_dashboard_marketwide(
    title,
    stock_states,
    sector_analysis,
    market_breadth=None,
    active_stocks=None,
    etf_panel=None,
    etf_states=None,
):
    lines = [f"[stock_monitor] {title}"]
    lines.append(f"📅 {now_bj().strftime('%Y-%m-%d %H:%M')}")
    lines.append("")

    index_order = MARKET_INDEX_ORDER

    lines.append("【全市场总览】")
    for symbol, fallback_name in index_order:
        data = stock_states.get(symbol)
        if not data:
            continue
        quote = data.get("quote", {})
        name = _index_display_name(symbol, quote.get("name"), fallback_name)
        price = _safe_get_float(quote, "price", 0.0)
        pct = _safe_get_float(quote, "pct_change")
        pct_text = "NA" if pct is None else f"{pct:+.2f}%"
        emoji = (
            "🟢"
            if pct is not None and pct > 0
            else "🔴"
            if pct is not None and pct < 0
            else "⚪"
        )
        lines.append(f"  {name}（{symbol}） {price:.2f} {emoji}{pct_text}")

    if market_breadth:
        up = int(market_breadth.get("up", 0))
        down = int(market_breadth.get("down", 0))
        flat = int(market_breadth.get("flat", 0))
        sample = int(market_breadth.get("sample_size", 0))
        limit_up = int(market_breadth.get("limit_up", 0))
        limit_down = int(market_breadth.get("limit_down", 0))
        amount_total = market_breadth.get("amount_total")
        avg_turnover = market_breadth.get("avg_turnover")
        amount_text = (
            "NA"
            if amount_total is None
            else f"{float(amount_total) / 100000000.0:.0f}亿"
        )
        turnover_text = "NA" if avg_turnover is None else f"{float(avg_turnover):.2f}%"
        lines.append(f"  上涨:{up} 下跌:{down} 平盘:{flat} (样本{sample})")
        lines.append(
            f"  涨停:{limit_up} 跌停:{limit_down} 成交额:{amount_text} 平均换手:{turnover_text}"
        )
    else:
        lines.append("  市场宽度: 暂无数据")

    brief_lines = generate_market_brief(
        title,
        stock_states,
        market_breadth=market_breadth,
        active_stocks=active_stocks,
        sector_analysis=sector_analysis,
    )
    if brief_lines:
        lines.append("")
        lines.append("【市场摘要】")
        lines.extend([f"  {line}" for line in brief_lines])

    _append_etf_trade_states(lines, etf_states, title="【ETF状态Top3】")
    _append_etf_panel(lines, etf_panel)

    lines.append("")
    lines.append("【全市场活跃Top10】")
    if active_stocks:
        for i, row in enumerate(active_stocks[:10], 1):
            name = row.get("name", "")
            code = row.get("code", "")
            pct = row.get("pct_change")
            amount = row.get("amount")
            turnover = row.get("turnover_rate")
            pct_text = "NA" if pct is None else f"{float(pct):+.2f}%"
            amount_text = "NA" if amount is None else f"{float(amount) / 10000.0:.0f}万"
            turnover_text = _format_turnover_text(turnover)
            lines.append(
                f"  {i}. {name}({code}) {pct_text} 成交额{amount_text} 换手{turnover_text}"
            )
    else:
        lines.append("  暂无数据")

    lines.append("")
    lines.append("【全市场板块】")
    if sector_analysis:
        for sec in sector_analysis[:10]:
            signal = sec.get("signal", "MIXED")
            emoji = (
                "🔥"
                if signal == "STRONG_RISING"
                else "📈"
                if signal == "RISING"
                else "➡️"
            )
            pct = sec.get("avg_pct_change", sec.get("pct_change", 0.0)) or 0.0
            name = sec.get("sector", sec.get("name", ""))
            lines.append(f"  {emoji} {name} {pct:+.2f}%")
    else:
        lines.append("  暂无数据")

    return "\n".join(lines)


def _build_market_sector_recap(limit=10):
    sectors = get_sector_list() or []
    rows = []
    for sec in sectors:
        name = sec.get("name", "")
        pct = sec.get("pct_change")
        if not name or pct is None:
            continue
        pct_v = float(pct)
        if pct_v >= 3:
            signal = "STRONG_RISING"
        elif pct_v > 0:
            signal = "RISING"
        elif pct_v <= -2:
            signal = "WEAK_FALLING"
        else:
            signal = "MIXED"
        rows.append({"sector": name, "avg_pct_change": pct_v, "signal": signal})
    rows.sort(key=lambda x: x.get("avg_pct_change", 0), reverse=True)
    return rows[: max(3, int(limit))]


def _fallback_active_top_from_sectors(sector_analysis, limit=10):
    rows = []
    seen = set()
    for sec in sector_analysis or []:
        for stock in sec.get("top_stocks", []) or []:
            code = stock.get("code")
            if not code or code in seen:
                continue
            seen.add(code)
            rows.append(
                {
                    "code": code,
                    "name": stock.get("name"),
                    "price": stock.get("price"),
                    "pct_change": stock.get("pct_change"),
                    "amount": stock.get("amount"),
                    "turnover_rate": stock.get("turnover_rate"),
                }
            )
            if len(rows) >= limit:
                return rows
    return rows


def _format_source_health(snapshot, limit=8):
    if not snapshot:
        return "暂无数据"
    lines = []
    for row in snapshot[:limit]:
        name = row.get("name", "")
        score = row.get("score", 0)
        succ = row.get("success", 0)
        fail = row.get("fail", 0)
        last_ok = row.get("last_success_ago")
        last_err = row.get("last_error_ago")
        cooldown = row.get("cooldown")
        cfail = int(row.get("consecutive_failures", 0) or 0)
        circuit_state = row.get("circuit_state", "closed")
        open_remaining = int(row.get("open_remaining", 0) or 0)
        ok_text = "NA" if last_ok is None else f"{last_ok}s"
        err_text = "NA" if last_err is None else f"{last_err}s"
        cool_text = "yes" if cooldown else "no"
        circuit_text = (
            f"{circuit_state}({open_remaining}s)"
            if circuit_state == "open"
            else circuit_state
        )
        lines.append(
            f"  {name}: score={score} ok={succ} fail={fail} cfail={cfail} "
            f"last_ok={ok_text} last_err={err_text} cooldown={cool_text} circuit={circuit_text}"
        )
    return "\n".join(lines)


def _enabled_source_health_names():
    selected = (os.getenv("QUOTE_SOURCE", "auto") or "auto").strip().lower()
    index_selected = (os.getenv("INDEX_QUOTE_SOURCE", "") or "").strip().lower()
    pool_text = (
        os.getenv("QUOTE_SOURCE_POOL", "qtimg,eastmoney,sina,akshare,yfinance,pytdx,tushare,mkts")
        .strip()
        .lower()
    )
    pool = [name.strip() for name in pool_text.split(",") if name.strip()]
    known = {
        "qtimg",
        "sina",
        "eastmoney",
        "yfinance",
        "pytdx",
        "baostock",
        "akshare",
        "tushare",
        "mkts",
        "daily",
    }
    names = set()
    if selected in {"auto", "random"}:
        for item in pool:
            if item in known and item != "daily":
                names.add(f"_get_quote_{item}")
        if os.getenv("DAILY_STOCK_ANALYSIS_AUX_ENABLED", "true").lower() == "true":
            names.add("_get_quote_daily_local")
    elif selected in known:
        if selected == "daily":
            names.add("_get_quote_daily_local")
        else:
            names.add(f"_get_quote_{selected}")

    if index_selected and index_selected not in {"auto", "random"}:
        if index_selected == "daily":
            names.add("_get_quote_daily_local")
        elif index_selected in known:
            names.add(f"_get_quote_{index_selected}")
    return names


def _job_source_health_report():
    with _job_execution_lock("source_health_report") as acquired:
        if not acquired:
            return
        enabled = os.getenv("SOURCE_HEALTH_REPORT_ENABLED", "true").lower() == "true"
        if not enabled:
            return
        if not is_trading_day():
            return
        now = now_bj()
        if not _is_trading_weekday(now) or not _in_session(now):
            return
        snapshot = get_source_health_snapshot()
        active_names = _enabled_source_health_names()
        if active_names:
            snapshot = [
                row
                for row in snapshot
                if str((row or {}).get("name") or "") in active_names
            ]
        if not snapshot:
            return

        # Only notify if there are severe problems (score < 0.5 for any source)
        # This reduces unnecessary notifications while still alerting on real issues
        has_issues = False
        problem_sources = []
        for row in snapshot:
            if not isinstance(row, dict):
                continue
            source_name = row.get("name", "unknown")
            score = float(row.get("score", 1.0))
            circuit_state = str(row.get("circuit_state", "closed"))
            cfail = int(row.get("consecutive_failures", 0) or 0)
            if score < 0.5 or circuit_state == "open" or cfail >= 3:
                has_issues = True
                problem_sources.append(
                    f"{source_name}(score={score:.2f},state={circuit_state},cfail={cfail})"
                )

        if not has_issues:
            # All sources are healthy, log but don't send notification
            logger.debug("source_health_all_healthy skipping_notification")
            return

        logger.info(
            "source_health_issues_detected sources=%s", ",".join(problem_sources)
        )
        limit = int(os.getenv("SOURCE_HEALTH_REPORT_TOPN", "8"))
        message = (
            "[stock_monitor] 数据源健康报告\n"
            f"🕒 {now.strftime('%Y-%m-%d %H:%M')}\n"
            f"⚠️ 发现问题数据源: {', '.join(problem_sources)}\n\n"
            f"{_format_source_health(snapshot, limit=limit)}"
        )
        key = f"source_health_{now.strftime('%Y%m%d%H')}"
        result = notify_with_guard(key, message)
        logger.info("source_health_report notify=%s", result.get("sent"))


def _job_morning_call_auction_and_news():
    with _job_execution_lock("morning_call_auction_news") as acquired:
        if not acquired:
            return
        started = time.time()
        max_runtime = max(20, int(os.getenv("MORNING_MAX_RUNTIME_SECONDS", "90")))
        logger.info("morning_alert_start")

        news = []
        sentiment_analysis = {}
        news_text = "暂无新闻"
        market_index = []
        etf_states = []
        etf_panel = []
        auction_rows = []
        auction_stale = False
        auction_error = ""
        auction_signals = []
        stage = "init"
        
        def _budget_exhausted():
            return (time.time() - started) >= max_runtime

        try:
            stage = "news"
            news_limit = max(3, int(os.getenv("MORNING_NEWS_LIMIT", "10")))
            news = fetch_finance_news(limit=news_limit)
            sentiment_analysis = analyze_news_sentiment(news)
            news_text = build_news_digest(news)
            logger.info("morning_stage stage=news count=%s", len(news))
            if _budget_exhausted():
                logger.warning("morning_budget_exhausted after=news max_runtime=%s", max_runtime)

            stage = "trading_day"
            if not is_trading_day():
                message = f"[stock_monitor] 非交易日\n新闻速览：\n{news_text}"
                result = notify_with_guard("daily_news", message)
                logger.info(
                    "morning_alert non_trading_day notify=%s news_count=%s",
                    result.get("sent"),
                    len(news),
                )
                return

            if not _is_trading_weekday():
                logger.info("morning_alert skip_non_weekday")
                return

            stage = "market_index"
            if not _budget_exhausted():
                try:
                    market_index = get_market_index_summary()
                except Exception as exc:
                    logger.warning("market_index_fetch_failed err=%s", exc)

            all_etf_codes = WATCHLIST.get("etf_codes", []) + WATCHLIST.get(
                "etf_codes_low_vol", []
            )
            morning_etf_state_enabled = (
                os.getenv("MORNING_ETF_STATE_ENABLED", "false").lower() == "true"
            )
            fast_state_limit = max(
                0, int(os.getenv("MORNING_ETF_STATE_MAX_CODES", "0"))
            )
            state_codes = (
                all_etf_codes[:fast_state_limit]
                if morning_etf_state_enabled and fast_state_limit > 0
                else []
            )
            if state_codes and not _budget_exhausted():
                stage = "etf_states"
                try:
                    etf_states = get_etf_trade_states(
                        state_codes,
                        topn=int(os.getenv("ETF_DECISION_TOPN", "3")),
                        flow_days=int(os.getenv("ETF_FLOW_DAYS", "3")),
                    )
                except Exception as exc:
                    logger.warning("etf_trade_state_failed err=%s", exc)

            stage = "etf_panel"
            morning_panel_enabled = (
                os.getenv("MORNING_ETF_PANEL_ENABLED", "false").lower() == "true"
            )
            if morning_panel_enabled and not _budget_exhausted():
                morning_panel_codes = max(
                    1, int(os.getenv("MORNING_ETF_PANEL_MAX_CODES", "3"))
                )
                morning_panel_topn = max(
                    1, int(os.getenv("MORNING_ETF_PANEL_TOPN", "3"))
                )
                panel_source = os.getenv(
                    "MORNING_ETF_QUOTE_SOURCE",
                    os.getenv("ETF_COMPONENT_QUOTE_SOURCE", "qtimg"),
                )
                try:
                    etf_panel = _load_etf_trade_panel(
                        max_codes=morning_panel_codes,
                        topn=morning_panel_topn,
                        quote_source=panel_source,
                    )
                except Exception as exc:
                    logger.warning("etf_stock_alerts_failed err=%s", exc)

            stage = "auction_rows"
            limit = int(os.getenv("AUCTION_TOP10_LIMIT", "10"))
            auction_rows, auction_stale, auction_error = (
                get_call_auction_top10_with_status(limit=limit)
            )

            stage = "auction_signals"
            if not _budget_exhausted():
                try:
                    from modules.auction_engine import get_auction_signals

                    auction_signals = get_auction_signals(limit=limit)
                except Exception as exc:
                    logger.warning("auction_analysis_failed err=%s", exc)

            stage = "notify"
            auction_meta = {"stale": auction_stale, "error": auction_error}
            message = _format_morning_alert(
                auction_rows,
                news,
                sentiment_analysis,
                market_index,
                auction_meta,
                etf_panel,
                etf_states=etf_states,
                auction_signals=auction_signals,
            )
            result = notify_with_guard("morning_auction_news", message)
            panel = etf_panel if isinstance(etf_panel, dict) else {}
            inc = panel.get("total_increased", 0)
            dec = panel.get("total_decreased", 0)
            logger.info(
                "morning_alert notify=%s reason=%s auction_count=%s news_count=%s etf_inc=%s etf_dec=%s",
                result.get("sent"),
                result.get("reason"),
                len(auction_rows),
                len(news),
                inc,
                dec,
            )
        except Exception as exc:
            logger.exception("morning_alert_failed stage=%s err=%s", stage, exc)
            fallback = (
                "[stock_monitor] 09:25 集合竞价简报（降级）\n"
                f"📅 {now_bj().strftime('%Y-%m-%d %H:%M')}\n"
                "数据源波动，已降级为简版消息，请关注下一轮推送。"
            )
            result = notify_with_guard("morning_auction_news", fallback)
            logger.info(
                "morning_alert_fallback notify=%s reason=%s",
                result.get("sent"),
                result.get("reason"),
            )
        finally:
            elapsed_ms = int((time.time() - started) * 1000)
            logger.info("morning_alert_end elapsed_ms=%s", elapsed_ms)


def _job_intraday_scan():
    with _job_execution_lock("intraday_scan") as acquired:
        if not acquired:
            return
        if not is_trading_day():
            return
        now = now_bj()
        if not _is_trading_weekday(now) or not _in_session(now):
            return

        famous_trader_enabled = (
            os.getenv("FAMOUS_TRADER_ENABLED", "true").lower() == "true"
        )
        famous_trader_stocks = []
        if famous_trader_enabled:
            try:
                famous_trader_stocks = get_famous_trader_stocks()
            except Exception as exc:
                logger.warning("famous_trader_detect_failed err=%s", exc)
        market_context = {}
        time_pattern = {}
        market_assessment = {}

        try:
            trader = get_professional_trader()
            market_assessment = trader.assess_market()
            market_context = {
                "context": market_assessment.get("context", "UNKNOWN"),
                "description": market_assessment.get("description", ""),
            }
            time_pattern = {
                "period": market_assessment.get("time_period", ""),
                "advice": market_assessment.get("time_advice", ""),
            }
            logger.info(
                "trader: trend=%s confidence=%.2f position=%s",
                market_assessment.get("trend"),
                market_assessment.get("confidence", 0),
                market_assessment.get("position", ""),
            )
        except Exception as exc:
            logger.warning("trader_process_failed err=%s", exc)
            try:
                market_context = get_market_context()
                time_pattern = get_time_pattern()
            except Exception:
                pass

        hot_sectors = auto_discover_hot_sectors(
            min_stocks_rising=int(os.getenv("HOT_SECTOR_MIN_STOCKS", "5")),
            min_avg_pct=float(os.getenv("HOT_SECTOR_MIN_PCT", "2.0")),
        )

        factor_scored_stocks = []
        decision_cards = []
        burst_signals = []
        small_cap_signals = []
        etf_panel = []
        etf_states = []
        factor_scoring_enabled = (
            os.getenv("FACTOR_SCORING_ENABLED", "true").lower() == "true"
        )
        if factor_scoring_enabled:
            watch_codes = [
                s.get("code")
                for s in WATCHLIST.get("watch_stocks", [])
                if s.get("code")
            ]
            if watch_codes:
                try:
                    min_factor_score = float(os.getenv("FACTOR_MIN_SCORE", "60"))
                    factor_scored_stocks = score_stocks_by_factors(
                        watch_codes,
                        min_score=min_factor_score,
                    )
                except Exception as exc:
                    logger.warning("factor_scoring_failed err=%s", exc)
        try:
            burst_signals = scan_burst_fund_signals(
                limit=int(os.getenv("BURST_CANDIDATE_TOPN", "10")),
                universe_size=int(os.getenv("BURST_SCAN_UNIVERSE", "300")),
            )
        except Exception as exc:
            logger.warning("burst_scan_failed err=%s", exc)
        try:
            small_cap_signals = scan_small_cap_monitor(
                hot_sectors,
                limit=int(os.getenv("SMALL_CAP_TOPN", "10")),
                universe_size=int(os.getenv("SMALL_CAP_SCAN_UNIVERSE", "300")),
            )
        except Exception as exc:
            logger.warning("small_cap_scan_failed err=%s", exc)
        try:
            decision_cards = scan_market_trade_candidates(
                hot_sectors,
                limit=int(os.getenv("INTRADAY_CANDIDATE_TOPN", "10")),
                universe_size=int(os.getenv("INTRADAY_SCAN_UNIVERSE", "180")),
                market_context=market_context.get("context", ""),
            )
        except Exception as exc:
            logger.warning("market_trade_scan_failed err=%s", exc)
        try:
            etf_panel = _load_etf_trade_panel()
        except Exception as exc:
            logger.warning("etf_trade_panel_failed err=%s", exc)
        etf_codes = WATCHLIST.get("etf_codes", []) + WATCHLIST.get(
            "etf_codes_low_vol", []
        )
        if etf_codes:
            try:
                etf_states = get_etf_trade_states(
                    etf_codes,
                    topn=int(os.getenv("ETF_DECISION_TOPN", "3")),
                    flow_days=int(os.getenv("ETF_FLOW_DAYS", "3")),
                )
            except Exception as exc:
                logger.warning("etf_trade_state_failed err=%s", exc)

        if (
            burst_signals
            or small_cap_signals
            or decision_cards
            or hot_sectors
            or famous_trader_stocks
            or factor_scored_stocks
            or etf_panel
            or etf_states
        ):
            message = _format_intraday_opportunity(
                decision_cards,
                hot_sectors,
                burst_signals,
                small_cap_signals,
                famous_trader_stocks,
                factor_scored_stocks,
                market_context,
                time_pattern,
                market_assessment,
                etf_panel,
                etf_states=etf_states,
            )
            key = f"intraday_{now.strftime('%Y%m%d%H%M')}"
            result = notify_with_guard(key, message)
            relaxed_cnt = sum(
                1
                for card in (decision_cards or [])
                if str(card.metrics.get("scan_mode")) == "relaxed"
            )
            logger.info(
                "intraday_scan notify=%s market=%s burst=%s small=%s candidates=%s relaxed=%s sectors=%s famous=%s",
                result.get("sent"),
                market_context.get("context", "UNKNOWN"),
                len(burst_signals),
                len(small_cap_signals),
                len(decision_cards),
                relaxed_cnt,
                len(hot_sectors),
                len(famous_trader_stocks),
            )


def _job_etf_alert():
    """独立ETF持仓变化推送"""
    with _job_execution_lock("etf_alert") as acquired:
        if not acquired:
            return
        if not is_trading_day():
            return
        now = now_bj()
        if not _is_trading_weekday(now):
            return
        if now.hour < 9 or now.hour > 15:
            return

        etf_panel = _load_etf_trade_panel()
        etf_panel_low_vol = _load_etf_trade_panel(codes_key="etf_codes_low_vol")

        if not etf_panel and not etf_panel_low_vol:
            return

        etf_codes_low_vol = WATCHLIST.get("etf_codes_low_vol", [])
        etf_codes = WATCHLIST.get("etf_codes", [])
        etf_states = []
        all_etf_codes = etf_codes + etf_codes_low_vol
        if all_etf_codes:
            try:
                etf_states = get_etf_trade_states(
                    all_etf_codes,
                    topn=int(os.getenv("ETF_DECISION_TOPN", "3")),
                    flow_days=int(os.getenv("ETF_FLOW_DAYS", "3")),
                )
            except Exception as exc:
                logger.warning("etf_trade_state_failed err=%s", exc)

        lines = []
        lines.append(f"📊 [ETF监控] {now.strftime('%H:%M')}")
        _append_etf_trade_states(lines, etf_states, title="【ETF状态Top3】")

        def _format_etf_holding_row(row):
            name = row.get("name", "") or "未知"
            code = _display_stock_code(row.get("code", ""))
            price_text = _format_price_text(row.get("price"))
            market_volume_text = _format_market_shares_text(row.get("volume"))
            change_mode = row.get("etf_change_mode")
            change_text = _format_etf_change_text(
                row.get("etf_change_amount"), change_mode
            )
            if str(change_mode or "").lower() in {"hold_pct_delta", "hold_pct_new"}:
                change_label = "权重变化"
            else:
                change_label = "成交变化"
            hold_pct_text = str(row.get("etf_hold_pct") or "").strip()
            hold_tail = f"，当前权重 {hold_pct_text}" if hold_pct_text and hold_pct_text != "NA" else ""
            source_etf = row.get("etf", "")
            source_tail = f" [{source_etf}]" if source_etf else ""
            return (
                f"{name}（{code}），价格 {price_text}，成交量 {market_volume_text}，{change_label} {change_text}{hold_tail}{source_tail}"
            )

        def _append_group_section(title, panel, limit=15):
            inc_rows = (panel or {}).get("increased", [])[:limit]
            dec_rows = (panel or {}).get("decreased", [])[:limit]

            lines.append("")
            lines.append(f"【{title}】")

            if inc_rows:
                lines.append(f"🟢 ETF增持（{len(inc_rows)}只）")
                for idx, row in enumerate(inc_rows, 1):
                    lines.append(f"  {idx}. 🟢 {_format_etf_holding_row(row)}")
            else:
                lines.append("🟢 ETF增持：暂无")

            if dec_rows:
                lines.append(f"🔴 ETF减仓（{len(dec_rows)}只）")
                for idx, row in enumerate(dec_rows, 1):
                    lines.append(f"  {idx}. 🔴 {_format_etf_holding_row(row)}")
            else:
                lines.append("🔴 ETF减仓：暂无")

            return len(inc_rows), len(dec_rows)

        low_title = "低波红利ETF成份股变化（512890/515300/561590）"
        low_inc_cnt, low_dec_cnt = _append_group_section(low_title, etf_panel_low_vol)
        oth_inc_cnt, oth_dec_cnt = _append_group_section("其他ETF成份股变化", etf_panel)

        total_changed = low_inc_cnt + low_dec_cnt + oth_inc_cnt + oth_dec_cnt
        if total_changed <= 0:
            logger.info("etf_alert no_position_change skip_notify")
            return

        message = "\n".join(lines)
        key = f"etf_alert_{now.strftime('%Y%m%d%H%M')}"
        result = notify_with_guard(key, message)
        logger.info(
            "etf_alert notify=%s low_inc=%s low_dec=%s other_inc=%s other_dec=%s",
            result.get("sent"),
            low_inc_cnt,
            low_dec_cnt,
            oth_inc_cnt,
            oth_dec_cnt,
        )


def _job_noon_recap():
    with _job_execution_lock("noon_recap") as acquired:
        if not acquired:
            return
        if not is_trading_day():
            return
        now = now_bj()
        if not _is_trading_weekday(now):
            return

        stock_states = _collect_index_states()

        sector_analysis = auto_discover_hot_sectors(
            min_stocks_rising=max(3, int(os.getenv("HOT_SECTOR_MIN_STOCKS", "5")) - 1),
            min_avg_pct=float(os.getenv("HOT_SECTOR_MIN_PCT", "2.0")),
        ) or _build_market_sector_recap(limit=int(os.getenv("RECAP_SECTOR_TOPN", "10")))
        market_breadth = get_market_breadth(
            sample_size=int(os.getenv("MARKET_BREADTH_SAMPLE_SIZE", "5000"))
        )
        active_top = get_market_active_top(
            limit=int(os.getenv("MARKET_ACTIVE_TOPN", "10"))
        )
        if not active_top:
            active_top = _fallback_active_top_from_sectors(
                sector_analysis, limit=int(os.getenv("MARKET_ACTIVE_TOPN", "10"))
            )
        etf_panel = _load_etf_trade_panel()
        etf_codes = WATCHLIST.get("etf_codes", []) + WATCHLIST.get(
            "etf_codes_low_vol", []
        )
        etf_states = []
        try:
            etf_states = get_etf_trade_states(
                etf_codes,
                topn=int(os.getenv("ETF_DECISION_TOPN", "3")),
                flow_days=int(os.getenv("ETF_FLOW_DAYS", "3")),
            )
        except Exception as exc:
            logger.warning("etf_trade_state_failed err=%s", exc)
        message = _format_decision_dashboard_marketwide(
            "午间复盘",
            stock_states,
            sector_analysis,
            market_breadth,
            active_top,
            etf_panel,
            etf_states=etf_states,
        )
        key = f"noon_recap_{now.strftime('%Y%m%d')}"
        result = notify_with_guard(key, message)
        logger.info(
            "noon_recap notify=%s stocks=%s", result.get("sent"), len(stock_states)
        )


def _job_close_recap():
    with _job_execution_lock("close_recap") as acquired:
        if not acquired:
            return
        if not is_trading_day():
            return
        now = now_bj()
        if not _is_trading_weekday(now):
            return

        stock_states = _collect_index_states()

        sector_analysis = auto_discover_hot_sectors(
            min_stocks_rising=max(3, int(os.getenv("HOT_SECTOR_MIN_STOCKS", "5")) - 1),
            min_avg_pct=float(os.getenv("HOT_SECTOR_MIN_PCT", "2.0")),
        ) or _build_market_sector_recap(limit=int(os.getenv("RECAP_SECTOR_TOPN", "10")))
        market_breadth = get_market_breadth(
            sample_size=int(os.getenv("MARKET_BREADTH_SAMPLE_SIZE", "5000"))
        )
        active_top = get_market_active_top(
            limit=int(os.getenv("MARKET_ACTIVE_TOPN", "10"))
        )
        if not active_top:
            active_top = _fallback_active_top_from_sectors(
                sector_analysis, limit=int(os.getenv("MARKET_ACTIVE_TOPN", "10"))
            )
        etf_panel = _load_etf_trade_panel()
        etf_codes = WATCHLIST.get("etf_codes", []) + WATCHLIST.get(
            "etf_codes_low_vol", []
        )
        etf_states = []
        try:
            etf_states = get_etf_trade_states(
                etf_codes,
                topn=int(os.getenv("ETF_DECISION_TOPN", "3")),
                flow_days=int(os.getenv("ETF_FLOW_DAYS", "3")),
            )
        except Exception as exc:
            logger.warning("etf_trade_state_failed err=%s", exc)
        message = _format_decision_dashboard_marketwide(
            "全日复盘",
            stock_states,
            sector_analysis,
            market_breadth,
            active_top,
            etf_panel,
            etf_states=etf_states,
        )
        key = f"close_recap_{now.strftime('%Y%m%d')}"
        result = notify_with_guard(key, message)
        logger.info(
            "close_recap notify=%s stocks=%s sectors=%s",
            result.get("sent"),
            len(stock_states),
            len(sector_analysis),
        )


def _job_weekly_check():
    with _job_execution_lock("weekly_check") as acquired:
        if not acquired:
            return
        status = check_daily_repo_status()
        save_weekly_state({"daily_repo": status})
        message = (
            "[stock_monitor] 周一03:00 版本检查\n"
            f"repo_exists={status['repo_exists']} behind={status['behind']} "
            f"upgraded={status['upgraded']} error={status['error'] or 'none'}"
        )
        result = notify_with_guard("weekly_daily_repo_check", message)
        logger.info("weekly_check notify=%s", result.get("sent"))


def _job_monday_news():
    with _job_execution_lock("monday_news") as acquired:
        if not acquired:
            return
        state = load_weekly_state()
        daily_status = state.get("daily_repo", {})
        limit = int(os.getenv("NEWS_HEADLINE_LIMIT", "5"))
        news = fetch_finance_news(limit=limit)
        news_text = build_news_digest(news)
        message = (
            f"[stock_monitor] 周一早间要点\n"
            f"版本: behind={daily_status.get('behind')} upgraded={daily_status.get('upgraded')}\n"
            f"财经:\n{news_text}"
        )
        result = notify_with_guard("weekly_monday_news", message)
        logger.info("monday_news notify=%s", result.get("sent"))


def _job_generate_dsa_quote():
    """生成DSA兼容的行情数据文件（股票+ETF）"""
    import json
    from pathlib import Path

    with _job_execution_lock("dsa_quote") as acquired:
        if not acquired:
            return
        from modules.market import get_quote, is_trading_day, now_bj
        from modules.etf_tracker import get_etf_holdings

        now = now_bj()

        quote_path = os.getenv(
            "DAILY_STOCK_ANALYSIS_QUOTE_PATH",
            "data/daily_stock_analysis/latest_quote.json",
        )
        quote_dir = Path(quote_path).parent
        quote_dir.mkdir(parents=True, exist_ok=True)

        # 1. 获取自选股行情
        watchlist = WATCHLIST.get("watch_stocks", [])
        stock_codes = [w["code"] for w in watchlist if w.get("type") == "stock"]

        if not stock_codes:
            stock_codes = ["sh600519", "sh600036", "sz000858", "sz002594", "sh600900"]

        quote_data = {}
        for code in stock_codes[:20]:
            try:
                q = get_quote(code)
                if q and q.get("price"):
                    quote_data[code] = {
                        "name": q.get("name", code),
                        "price": q.get("price"),
                        "pct_change": q.get("pct_change", 0),
                        "turnover_rate": q.get("turnover_rate", 0),
                        "type": "stock",
                    }
            except Exception:
                continue

        # 2. 获取指数行情
        quote_data["sh000001"] = {
            "name": "上证指数",
            "price": 0,
            "pct_change": 0,
            "type": "index",
        }
        quote_data["sz399001"] = {
            "name": "深证成指",
            "price": 0,
            "pct_change": 0,
            "type": "index",
        }

        for idx_code in ["sh000001", "sz399001"]:
            try:
                q = get_quote(idx_code)
                if q and q.get("price"):
                    quote_data[idx_code] = {
                        "name": q.get("name", idx_code),
                        "price": q.get("price"),
                        "pct_change": q.get("pct_change", 0),
                        "type": "index",
                    }
            except Exception:
                continue

        # 3. 获取ETF行情（供DSA分析ETF用）
        etf_codes = WATCHLIST.get("etf_codes", []) + WATCHLIST.get(
            "etf_codes_low_vol", []
        )
        for etf_code in etf_codes[:10]:
            try:
                q = get_quote(etf_code)
                if q and q.get("price"):
                    quote_data[etf_code] = {
                        "name": q.get("name", etf_code),
                        "price": q.get("price"),
                        "pct_change": q.get("pct_change", 0),
                        "turnover_rate": q.get("turnover_rate", 0),
                        "type": "etf",
                    }
            except Exception:
                continue

        try:
            with open(quote_path, "w", encoding="utf-8") as f:
                json.dump(quote_data, f, ensure_ascii=False, indent=2)
            logger.info(
                "dsa_quote_generated path=%s stocks=%d", quote_path, len(quote_data)
            )
        except Exception as exc:
            logger.warning("dsa_quote_failed err=%s", exc)


def main():
    tz_name = os.getenv("TZ", "Asia/Shanghai")
    os.environ["TZ"] = tz_name
    if hasattr(time, "tzset"):
        time.tzset()

    auction_time = os.getenv("AUCTION_ALERT_TIME", "09:25")
    noon_time = os.getenv("NOON_RECAP_TIME", "11:35")
    close_time = os.getenv("CLOSE_RECAP_TIME", "15:10")
    weekly_check_time = os.getenv("WEEKLY_VERSION_CHECK_TIME", "03:00")
    monday_news_time = os.getenv("MONDAY_NEWS_PUSH_TIME", "08:30")

    intraday_scan_enabled = os.getenv("INTRADAY_SCAN_ENABLED", "true").lower() == "true"
    intraday_scan_interval = int(os.getenv("INTRADAY_SCAN_INTERVAL_MINUTES", "15"))
    source_health_interval = int(
        os.getenv("SOURCE_HEALTH_REPORT_INTERVAL_MINUTES", "60")
    )

    global _scheduler
    _scheduler = _init_scheduler(tz_name)

    auction_h, auction_m = _parse_hhmm(auction_time, "09:25")
    noon_h, noon_m = _parse_hhmm(noon_time, "11:35")
    close_h, close_m = _parse_hhmm(close_time, "15:10")
    weekly_h, weekly_m = _parse_hhmm(weekly_check_time, "03:00")
    monday_h, monday_m = _parse_hhmm(monday_news_time, "08:30")

    _scheduler.add_job(
        _job_morning_call_auction_and_news,
        CronTrigger(day_of_week="mon-fri", hour=auction_h, minute=auction_m),
        id="morning_call_auction_news",
        replace_existing=True,
    )

    if intraday_scan_enabled:
        _scheduler.add_job(
            _job_intraday_scan,
            "interval",
            minutes=max(15, intraday_scan_interval),
            id="intraday_scan",
            replace_existing=True,
        )

    _scheduler.add_job(
        _job_generate_dsa_quote,
        "interval",
        hours=1,
        id="dsa_quote",
        replace_existing=True,
    )

    _scheduler.add_job(
        _job_source_health_report,
        "interval",
        minutes=max(30, source_health_interval),
        id="source_health_report",
        replace_existing=True,
    )

    _scheduler.add_job(
        _job_noon_recap,
        CronTrigger(day_of_week="mon-fri", hour=noon_h, minute=noon_m),
        id="noon_recap",
        replace_existing=True,
    )
    _scheduler.add_job(
        _job_close_recap,
        CronTrigger(day_of_week="mon-fri", hour=close_h, minute=close_m),
        id="close_recap",
        replace_existing=True,
    )

    _scheduler.add_job(
        _job_etf_alert,
        CronTrigger(day_of_week="mon-fri", hour=9, minute=35),
        id="etf_alert_morning",
        replace_existing=True,
    )
    _scheduler.add_job(
        _job_etf_alert,
        CronTrigger(day_of_week="mon-fri", hour=10, minute=30),
        id="etf_alert_1030",
        replace_existing=True,
    )
    _scheduler.add_job(
        _job_etf_alert,
        CronTrigger(day_of_week="mon-fri", hour=14, minute=0),
        id="etf_alert_1400",
        replace_existing=True,
    )
    _scheduler.add_job(
        _job_weekly_check,
        CronTrigger(day_of_week="mon", hour=weekly_h, minute=weekly_m),
        id="weekly_check",
        replace_existing=True,
    )
    _scheduler.add_job(
        _job_monday_news,
        CronTrigger(day_of_week="mon", hour=monday_h, minute=monday_m),
        id="monday_news",
        replace_existing=True,
    )

    logger.info(
        "guardian_started tz=%s watch_stocks=%s sectors=%s",
        tz_name,
        len(WATCHLIST.get("watch_stocks", [])),
        len(WATCHLIST.get("sectors", {})),
    )

    _scheduler.start()
    try:
        while True:
            time.sleep(1)
    except Exception as exc:
        logger.exception("guardian_loop_error err=%s", exc)
        raise


if __name__ == "__main__":
    import argparse
    import signal
    import sys

    parser = argparse.ArgumentParser(description="Stock Guardian Daemon")
    parser.add_argument("--daemon", "-d", action="store_true", help="Run as daemon")
    parser.add_argument(
        "--health-check", action="store_true", help="Run health check and exit"
    )
    parser.add_argument(
        "--health-port", type=int, default=8080, help="Health check HTTP port"
    )
    args = parser.parse_args()

    if args.health_check:
        health_port = int(os.getenv("HEALTH_CHECK_PORT", args.health_port))
        try:
            from http.server import HTTPServer, BaseHTTPRequestHandler
            import json

            class HealthHandler(BaseHTTPRequestHandler):
                def do_GET(self):
                    if self.path == "/health" or self.path == "/":
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        resp = {
                            "status": "ok",
                            "timestamp": now_bj().isoformat(),
                            "is_trading_day": is_trading_day(),
                            "watch_stocks": len(WATCHLIST.get("watch_stocks", [])),
                        }
                        self.wfile.write(json.dumps(resp).encode())
                    else:
                        self.send_response(404)
                        self.end_headers()

                def log_message(self, format, *args):
                    pass

            server = HTTPServer(("", health_port), HealthHandler)
            logger.info("health_check_server_started port=%d", health_port)
            server.serve_forever()
        except Exception as exc:
            logger.error("health_check_failed err=%s", exc)
            sys.exit(1)
        sys.exit(0)

    if args.daemon:
        try:
            import daemon
            from daemon.pidfile import PIDFile

            pid_file = os.getenv("PID_FILE", "/var/run/guardian.pid")
            with daemon.DaemonContext(pidfile=PIDFile(pid_file)):
                main()
        except ImportError:
            logger.warning("python-daemon not installed, running in foreground")
            main()
    else:

        def _signal_handler(signum, frame):
            logger.info("received_signal=%d shutting_down", signum)
            if _scheduler:
                try:
                    _scheduler.shutdown(wait=False)
                except Exception:
                    pass
            sys.exit(0)

        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)
        main()
