def ma5_signal(close_prices):
    if len(close_prices) < 5:
        return "NO_DATA"
    ma5 = sum(close_prices[-5:]) / 5
    if close_prices[-1] > ma5:
        return "BUY"
    else:
        return "SELL"
