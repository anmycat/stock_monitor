from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional


def _safe_float(value) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _avg(values: List[float], window: int) -> Optional[float]:
    seq = [float(v) for v in values if v is not None]
    if len(seq) < window:
        return None
    return sum(seq[-window:]) / float(window)


def _status_text(ok: Optional[bool]) -> str:
    if ok is True:
        return "满足"
    if ok is False:
        return "不满足"
    return "注意"


@dataclass
class ChecklistItem:
    label: str
    ok: Optional[bool]
    detail: str = ""

    def render(self) -> str:
        text = _status_text(self.ok)
        if self.detail:
            return f"{self.label}: {text} | {self.detail}"
        return f"{self.label}: {text}"


@dataclass
class TradeLevels:
    support: Optional[float] = None
    resistance: Optional[float] = None
    entry: Optional[float] = None
    stop: Optional[float] = None
    target: Optional[float] = None


@dataclass
class TradeDecisionCard:
    code: str
    name: str
    price: Optional[float]
    pct_change: Optional[float]
    turnover_rate: Optional[float]
    amount: Optional[float]
    sector: Optional[str]
    sector_signal: Optional[str]
    score: int
    signal: str
    action: str
    summary: str
    risk_warning: str
    checklist: List[ChecklistItem] = field(default_factory=list)
    levels: TradeLevels = field(default_factory=TradeLevels)
    metrics: Dict = field(default_factory=dict)

    def to_dict(self) -> Dict:
        payload = asdict(self)
        payload["checklist"] = [item.render() for item in self.checklist]
        return payload


def build_trade_decision_card(
    *,
    code: str,
    name: str,
    price,
    pct_change,
    turnover_rate,
    amount=None,
    closes: Optional[List[float]] = None,
    highs: Optional[List[float]] = None,
    lows: Optional[List[float]] = None,
    turnover_history: Optional[List[float]] = None,
    sector: Optional[str] = None,
    sector_signal: Optional[str] = None,
    sector_pct: Optional[float] = None,
    positive_flow_days: int = 0,
    flow_total: Optional[float] = None,
    bias_limit: float = 5.0,
    turnover_gate: float = 3.0,
    turnover_hot_low: float = 5.0,
    turnover_hot_high: float = 15.0,
    turnover_death: float = 20.0,
    volume_ratio_gate: float = 1.5,
) -> TradeDecisionCard:
    close_seq = [_safe_float(v) for v in (closes or [])]
    close_seq = [v for v in close_seq if v is not None]
    high_seq = [_safe_float(v) for v in (highs or [])]
    high_seq = [v for v in high_seq if v is not None]
    low_seq = [_safe_float(v) for v in (lows or [])]
    low_seq = [v for v in low_seq if v is not None]
    turnover_seq = [_safe_float(v) for v in (turnover_history or [])]
    turnover_seq = [v for v in turnover_seq if v is not None]

    price_value = _safe_float(price)
    pct_value = _safe_float(pct_change)
    turnover_value = _safe_float(turnover_rate)
    amount_value = _safe_float(amount)
    sector_pct_value = _safe_float(sector_pct)

    ma5 = _avg(close_seq, 5)
    ma10 = _avg(close_seq, 10)
    ma20 = _avg(close_seq, 20)

    bias = None
    if price_value is not None and ma5 not in (None, 0):
        bias = (price_value - ma5) / ma5 * 100.0

    turnover_avg5 = None
    volume_ratio = None
    if turnover_value is not None and turnover_seq:
        history = turnover_seq[-5:]
        turnover_avg5 = sum(history) / float(len(history))
        if turnover_avg5 > 0:
            volume_ratio = turnover_value / turnover_avg5

    support_candidates = [value for value in (ma20, ma10) if value is not None]
    if low_seq:
        support_candidates.append(min(low_seq[-10:]))
    support = max(support_candidates) if support_candidates else None
    resistance = max(high_seq[-10:]) if high_seq else None
    if resistance is None and close_seq:
        resistance = max(close_seq[-10:])
    entry = support if support is not None else price_value
    stop = (support * 0.97) if support is not None else (price_value * 0.95 if price_value is not None else None)
    if resistance is not None and price_value is not None and resistance <= price_value:
        target = price_value * 1.06
    else:
        target = resistance if resistance is not None else (price_value * 1.06 if price_value is not None else None)

    trend_ok = None if None in (ma5, ma10, ma20) else (ma5 > ma10 > ma20)
    bias_ok = None if bias is None else abs(bias) <= float(bias_limit)
    turnover_gate_ok = None if turnover_value is None else turnover_value >= float(turnover_gate)
    turnover_zone_ok = None if turnover_value is None else (float(turnover_hot_low) <= turnover_value <= float(turnover_hot_high))
    turnover_risk = turnover_value is not None and turnover_value > float(turnover_death)
    volume_ratio_ok = None if volume_ratio is None else volume_ratio >= float(volume_ratio_gate)
    fund_ok = None if flow_total is None and positive_flow_days <= 0 else (positive_flow_days >= 3 or (flow_total or 0) > 0)
    sector_ok = None
    if sector_signal or sector_pct_value is not None:
        sector_ok = (sector_signal in {"STRONG_RISING", "RISING"}) or ((sector_pct_value or 0) > 1.0)

    checklist = [
        ChecklistItem(
            "严禁追高（乖离率<=5%）",
            bias_ok,
            "乖离率NA" if bias is None else f"乖离率{bias:+.2f}%",
        ),
        ChecklistItem(
            "趋势交易（MA5>MA10>MA20）",
            trend_ok,
            "MA数据不足" if None in (ma5, ma10, ma20) else f"MA5 {ma5:.2f} MA10 {ma10:.2f} MA20 {ma20:.2f}",
        ),
        ChecklistItem(
            "换手率三部曲",
            turnover_gate_ok if turnover_zone_ok is None else (turnover_gate_ok and turnover_zone_ok),
            (
                "换手率NA"
                if turnover_value is None
                else f"换手{turnover_value:.2f}%"
                + ("（沸点区）" if turnover_risk else "")
            ),
        ),
        ChecklistItem(
            "成交量联动（量比>=1.5）",
            volume_ratio_ok,
            "量比NA" if volume_ratio is None else f"量比{volume_ratio:.2f}",
        ),
        ChecklistItem(
            "连续资金进入（3-5日）",
            fund_ok,
            f"连续{positive_flow_days}日 主力{(flow_total or 0) / 100000000.0:+.2f}亿" if (flow_total is not None or positive_flow_days) else "资金NA",
        ),
    ]

    score = 0
    if trend_ok is True:
        score += 25
    elif trend_ok is None:
        score += 8
    if bias_ok is True:
        score += 20
    elif bias_ok is None:
        score += 6
    if turnover_gate_ok is True:
        score += 10
    if turnover_zone_ok is True:
        score += 10
    elif turnover_zone_ok is None:
        score += 4
    if volume_ratio_ok is True:
        score += 15
    elif volume_ratio_ok is None:
        score += 5
    if fund_ok is True:
        score += 10
    elif fund_ok is None:
        score += 4
    if sector_ok is True:
        score += 10
    elif sector_ok is None:
        score += 4
    if pct_value is not None:
        if 2.0 <= pct_value <= 7.0:
            score += 10
        elif pct_value > 9.5 or pct_value < -3.0:
            score -= 10
    if turnover_risk:
        score -= 20
    score = max(0, min(100, int(round(score))))

    buy_ready = all(value is True for value in (trend_ok, bias_ok, turnover_gate_ok, turnover_zone_ok, volume_ratio_ok)) and (fund_ok is not False)
    avoid = turnover_risk or bias_ok is False or (pct_value is not None and pct_value > 9.5)
    if avoid:
        signal = "AVOID"
        action = "X不买"
        summary = "情绪过热或偏离过大，不做无效追价。"
    elif buy_ready:
        signal = "BUY"
        action = "买"
        summary = "趋势、换手、量比和资金基本共振，可按支撑位试仓。"
    else:
        signal = "WATCH"
        action = "等"
        summary = "核心条件未完全闭合，继续等量能或资金确认。"

    risk_parts = []
    if turnover_risk:
        risk_parts.append("换手率超过20%，筹码可能松动")
    if bias_ok is False and bias is not None:
        risk_parts.append(f"乖离率{bias:+.2f}%超过阈值")
    if fund_ok is False:
        risk_parts.append("近3-5日主力未形成连续流入")
    if pct_value is not None and pct_value < 0:
        risk_parts.append("当日涨跌幅偏弱")
    risk_warning = "；".join(risk_parts) if risk_parts else "暂无明显结构性风险"

    return TradeDecisionCard(
        code=code,
        name=name,
        price=price_value,
        pct_change=pct_value,
        turnover_rate=turnover_value,
        amount=amount_value,
        sector=sector,
        sector_signal=sector_signal,
        score=score,
        signal=signal,
        action=action,
        summary=summary,
        risk_warning=risk_warning,
        checklist=checklist,
        levels=TradeLevels(
            support=support,
            resistance=resistance,
            entry=entry,
            stop=stop,
            target=target,
        ),
        metrics={
            "ma5": ma5,
            "ma10": ma10,
            "ma20": ma20,
            "bias_ma5": bias,
            "turnover_avg5": turnover_avg5,
            "volume_ratio": volume_ratio,
            "positive_flow_days": positive_flow_days,
            "flow_total": flow_total,
            "sector_pct": sector_pct_value,
        },
    )
