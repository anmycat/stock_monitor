import json
import os

from .ai_engine import summarize
from .utils import to_float


def build_market_brief_payload(title, stock_states, market_breadth=None, active_stocks=None, sector_analysis=None):
    indices = []
    for symbol, data in (stock_states or {}).items():
        quote = data.get("quote", {})
        indices.append(
            {
                "symbol": symbol,
                "name": quote.get("name") or symbol,
                "price": to_float(quote.get("price")),
                "pct_change": to_float(quote.get("pct_change")),
            }
        )

    payload = {
        "title": title,
        "indices": indices[:4],
        "market_breadth": {
            "up": int((market_breadth or {}).get("up", 0)),
            "down": int((market_breadth or {}).get("down", 0)),
            "flat": int((market_breadth or {}).get("flat", 0)),
            "limit_up": int((market_breadth or {}).get("limit_up", 0)),
            "limit_down": int((market_breadth or {}).get("limit_down", 0)),
            "avg_turnover": to_float((market_breadth or {}).get("avg_turnover")),
            "amount_total_yi": (
                round(float((market_breadth or {}).get("amount_total")) / 100000000.0, 1)
                if (market_breadth or {}).get("amount_total") is not None
                else None
            ),
        },
        "hot_sectors": [
            {
                "sector": row.get("sector") or row.get("name"),
                "pct_change": to_float(row.get("avg_pct_change", row.get("pct_change"))),
                "signal": row.get("signal"),
            }
            for row in (sector_analysis or [])[:5]
        ],
        "active_stocks": [
            {
                "name": row.get("name"),
                "code": row.get("code"),
                "pct_change": to_float(row.get("pct_change")),
                "turnover_rate": to_float(row.get("turnover_rate")),
                "amount_wan": (
                    round(float(row.get("amount")) / 10000.0, 1)
                    if row.get("amount") is not None
                    else None
                ),
            }
            for row in (active_stocks or [])[:5]
        ],
    }
    return payload


def _fallback_lines(payload):
    breadth = payload.get("market_breadth", {})
    up = int(breadth.get("up", 0))
    down = int(breadth.get("down", 0))
    limit_up = int(breadth.get("limit_up", 0))
    limit_down = int(breadth.get("limit_down", 0))
    top_sector = ""
    sectors = payload.get("hot_sectors") or []
    if sectors:
        first = sectors[0]
        top_sector = f"{first.get('sector', '')}{(first.get('pct_change') or 0):+.2f}%"

    if up > down:
        summary = "市场偏强，资金更偏向进攻方向。"
    elif down > up:
        summary = "市场偏弱，防守情绪更明显。"
    else:
        summary = "市场分化，指数与个股节奏不完全同步。"

    action = "优先跟踪强势板块内高换手个股。"
    if top_sector:
        action = f"优先围绕 {top_sector} 所在主线做观察与筛选。"

    risk = "注意追高风险。"
    if limit_down > limit_up:
        risk = "跌停家数占优，尾盘回撤风险需要控制。"
    elif limit_up > 0:
        risk = "涨停扩散存在情绪过热风险，避免高位接力。"

    return [summary, action, risk]


def _normalize_ai_lines(text):
    if not text or text.startswith("AI Error:"):
        return None
    lines = [line.strip(" -") for line in text.splitlines() if line.strip()]
    output = []
    prefixes = ("市场结论", "执行建议", "风险提示")
    for prefix in prefixes:
        matched = next((line for line in lines if line.startswith(prefix)), None)
        if matched:
            output.append(matched)
    if len(output) == 3:
        return output
    if len(lines) >= 3:
        return [
            f"市场结论：{lines[0].split('：', 1)[-1].strip()}",
            f"执行建议：{lines[1].split('：', 1)[-1].strip()}",
            f"风险提示：{lines[2].split('：', 1)[-1].strip()}",
        ]
    return None


def generate_market_brief(title, stock_states, market_breadth=None, active_stocks=None, sector_analysis=None):
    payload = build_market_brief_payload(title, stock_states, market_breadth, active_stocks, sector_analysis)
    fallback = _fallback_lines(payload)
    if os.getenv("MARKET_BRIEF_AI_ENABLED", "true").lower() != "true":
        return [
            f"市场结论：{fallback[0]}",
            f"执行建议：{fallback[1]}",
            f"风险提示：{fallback[2]}",
        ]

    prompt = (
        "你是A股盘中复盘助理。请根据下面的市场结构化数据，输出恰好3行中文，不要标题，不要Markdown。\n"
        "格式固定为：\n"
        "市场结论：...\n"
        "执行建议：...\n"
        "风险提示：...\n"
        "要求：结论具体、面向交易，不要空话，不要免责声明。\n"
        f"数据：{json.dumps(payload, ensure_ascii=False)}"
    )
    lines = _normalize_ai_lines(summarize(prompt))
    if lines:
        return lines

    return [
        f"市场结论：{fallback[0]}",
        f"执行建议：{fallback[1]}",
        f"风险提示：{fallback[2]}",
    ]
