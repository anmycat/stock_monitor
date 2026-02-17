def risk_score(market_change, volatility):
    score = 50
    score += market_change * 10
    score += volatility * 5
    return max(0, min(100, score))
