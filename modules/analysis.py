import json
import os
from statistics import pstdev


def ma_signal(close_prices, window=5):
    if len(close_prices) < window:
        return {"signal": "NO_DATA", "reason": f"need_{window}_prices"}
    moving_avg = sum(close_prices[-window:]) / window
    signal = "BUY" if close_prices[-1] > moving_avg else "SELL"
    return {"signal": signal, "reason": f"close_vs_ma{window}"}


def moving_average(close_prices, window):
    if len(close_prices) < window:
        return None
    return sum(close_prices[-window:]) / window


def risk_score(market_change, volatility):
    score = 50
    score += market_change * 10
    score += volatility * 5
    bounded = max(0, min(100, score))
    if bounded >= 70:
        level = "HIGH"
    elif bounded >= 40:
        level = "MEDIUM"
    else:
        level = "LOW"
    return {"score": bounded, "level": level}
def load_bettafish_signal(report_path=None):
    if os.getenv("BETTAFISH_AUX_ENABLED", "true").lower() != "true":
        return {"source": "bettafish", "available": False, "sentiment_score": 0.0}
    path = report_path or os.getenv("BETTAFISH_REPORT_PATH", "data/bettafish/latest_report.json")
    if not os.path.exists(path):
        return {"source": "bettafish", "available": False, "sentiment_score": 0.0}
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        score = float(payload.get("sentiment_score", 0.0))
        return {"source": "bettafish", "available": True, "sentiment_score": max(-1.0, min(1.0, score))}
    except Exception:
        return {"source": "bettafish", "available": False, "sentiment_score": 0.0}


def blend_risk_with_sentiment(risk_data, sentiment_data):
    score = float(risk_data["score"])
    sentiment = float(sentiment_data.get("sentiment_score", 0.0))
    # Negative sentiment increases risk; positive sentiment slightly reduces risk.
    adjusted = max(0.0, min(100.0, score + (-sentiment * 15.0)))
    level = "HIGH" if adjusted >= 70 else "MEDIUM" if adjusted >= 40 else "LOW"
    out = dict(risk_data)
    out["score"] = adjusted
    out["level"] = level
    out["sentiment"] = sentiment
    return out


def price_metrics(close_prices):
    if len(close_prices) < 2:
        return {"market_change": 0.0, "volatility": 0.0}
    latest = float(close_prices[-1])
    prev = float(close_prices[-2])
    market_change = ((latest - prev) / prev) if prev else 0.0
    volatility = float(pstdev(close_prices[-min(10, len(close_prices)) :])) if len(close_prices) >= 3 else 0.0
    return {"market_change": market_change, "volatility": volatility}
