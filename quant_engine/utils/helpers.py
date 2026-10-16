def format_number(value: float, decimals: int = 2) -> str:
    if abs(value) >= 1e8:
        return f"{value/1e8:.{decimals}f}亿"
    elif abs(value) >= 1e4:
        return f"{value/1e4:.{decimals}f}万"
    else:
        return f"{value:.{decimals}f}"


def calculate_change_pct(current: float, previous: float) -> float:
    if previous == 0:
        return 0
    return (current - previous) / previous * 100


def calculate_max_drawdown(equity_curve: list) -> float:
    """
    计算最大回撤（统一公共实现）
    
    Args:
        equity_curve: 资金曲线列表
        
    Returns:
        float: 最大回撤比例 (0-1)
    """
    if not equity_curve or len(equity_curve) < 2:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for value in equity_curve:
        if value > peak:
            peak = value
        if peak > 0:
            dd = (peak - value) / peak
            if dd > max_dd:
                max_dd = dd
    return max_dd


def calculate_position(base: float, market_factor: float, score: float) -> float:
    """
    计算仓位比例（统一公共实现）
    
    Args:
        base: 基础仓位 (0-1)
        market_factor: 市场状态因子 (0-1)
        score: 信号评分 (0-100)
        
    Returns:
        float: 最终仓位比例 (0-1)
    """
    score_factor = score / 100.0
    position = base * market_factor * score_factor
    return max(0.05, min(0.4, position))  # 5%-40%范围