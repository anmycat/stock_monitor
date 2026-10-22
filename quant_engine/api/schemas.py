"""API 请求/响应模型（P3 重构从 routes.py 整体迁出）

跨域复用，域 router 经 `from ..schemas import X` 引用。
"""
import re
from typing import Dict, List, Optional

from pydantic import BaseModel, field_validator, model_validator

# 入参校验正则（随 BacktestRequest 从 routes.py 迁入）
_CODE_RE = re.compile(r'^\d{6}\.(SZ|SH|BJ)$')
_DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')

class SignalResponse(BaseModel):
    code: str
    name: str
    action: str
    level: Optional[str]
    score: float
    position: float
    reason: List[str]
    strategy: str
    price: float
    target_price: Optional[float]
    stop_loss: Optional[float]
    # Kelly公式字段
    kelly_position: Optional[float] = None
    kelly_confidence: Optional[float] = None
    kelly_source: Optional[str] = None


class BacktestRequest(BaseModel):
    codes: List[str]
    start_date: str
    end_date: str
    initial_capital: float = 100000

    @field_validator('codes')
    @classmethod
    def validate_codes(cls, v):
        if not v or len(v) == 0:
            raise ValueError('codes不能为空')
        if len(v) > 20:
            raise ValueError('codes最多20只股票')
        for c in v:
            if not _CODE_RE.match(c):
                raise ValueError(f'无效股票代码: {c}')
        return v

    @field_validator('start_date', 'end_date')
    @classmethod
    def validate_date(cls, v):
        if not _DATE_RE.match(v):
            raise ValueError(f'日期格式应为YYYY-MM-DD: {v}')
        return v

    @field_validator('initial_capital')
    @classmethod
    def validate_capital(cls, v):
        if v < 1000 or v > 100000000:
            raise ValueError('初始资金应在1000-1亿之间')
        return v

    @model_validator(mode='after')
    def validate_date_order(self):
        if self.start_date > self.end_date:
            raise ValueError('start_date不能晚于end_date')
        return self


class MarketOverviewResponse(BaseModel):
    total: int
    up: int
    down: int
    flat: int
    avg_change: float
    suspect: bool = False  # B5（2026-10-21）：True=数据卫生断言未过、拒绝出厂脏统计（13 §4.4 事故防线）


class ETFResponse(BaseModel):
    code: str
    name: str
    category: str = ''
    price: float
    change_pct: float
    volume: float
    trend: str
    volume_desc: str = ''
    no_data: bool = False


class ETFHoldingItem(BaseModel):
    code: str
    name: str
    weight: float = 0
    shares: float = 0
    market_value: float = 0
    seq: int = 0


class ETFHoldingsOverviewItem(BaseModel):
    code: str
    name: str
    category: str = ''
    quarter: Optional[str] = None
    report_date: Optional[str] = None
    holdings_count: int = 0
    top: List[ETFHoldingItem] = []
    tracked: bool = False


class ETFHoldingsLatestResponse(BaseModel):
    code: str
    name: str
    quarter: Optional[str] = None
    report_date: Optional[str] = None
    updated_at: Optional[str] = None
    holdings_count: int = 0
    top: List[ETFHoldingItem] = []


class HoldingChangeItem(BaseModel):
    code: str
    name: str
    direction: Optional[str] = None
    weight: float = 0
    prev_weight: float = 0
    shares: float = 0
    prev_shares: float = 0
    shares_change_pct: Optional[float] = None
    weight_delta: Optional[float] = None
    market_value: float = 0


class ETFHoldingsChangesResponse(BaseModel):
    etf_code: str = ''
    etf_name: str = ''
    previous_quarter: Optional[str] = None
    current_quarter: Optional[str] = None
    current_report_date: str = ''
    previous_report_date: str = ''
    new_entries: List[HoldingChangeItem] = []
    exits: List[HoldingChangeItem] = []
    increased: List[HoldingChangeItem] = []
    decreased: List[HoldingChangeItem] = []
    unchanged_count: int = 0
    nav_change_pct: Optional[float] = None
    alert_count: Optional[int] = None
    generated_at: Optional[str] = None


class NewsBriefItem(BaseModel):
    time: str = ''
    source: str = ''
    title: str = ''
    url: str = ''


class NewsSectorGroup(BaseModel):
    sector: str
    count: int = 0
    shown: int = 0
    truncated: bool = False
    items: List[NewsBriefItem] = []


class NewsDigestResponse(BaseModel):
    generated_at: str = ''
    window_start: str = ''
    window_end: str = ''
    from_last_push: bool = False
    total: int = 0
    dedup_dropped: int = 0
    sector_count: int = 0
    hidden_sectors: int = 0
    hidden_items: int = 0
    sources: Dict[str, int] = {}
    sectors: List[NewsSectorGroup] = []


class NewsSectorSummaryItem(BaseModel):
    sector: str
    count: int = 0


class NewsSectorsResponse(BaseModel):
    generated_at: str = ''
    sectors: List[NewsSectorSummaryItem] = []
    hidden_sectors: int = 0
    hidden_items: int = 0


class CodeGuardIssueItem(BaseModel):
    severity: str
    type: str
    code: str
    name: str = ''
    message: str = ''
    detail: Dict = {}
    repaired: bool = False


class CodeGuardReportResponse(BaseModel):
    mode: str = 'static'
    checked: int = 0
    quoted: int = 0
    issue_count: int = 0
    error_count: int = 0
    warning_count: int = 0
    by_type: Dict[str, int] = {}
    issues: List[CodeGuardIssueItem] = []
    started_at: str = ''
    duration_sec: float = 0


class MarketSentimentResponse(BaseModel):
    sentiment: str
    score: float
    avg_change: float
    up_count: int
    down_count: int
    details: List[ETFResponse]


class CooldownStatusResponse(BaseModel):
    code: str
    is_cooled_down: bool


class SectorStock(BaseModel):
    code: str
    name: str
    change_pct: float


class SectorResponse(BaseModel):
    name: str
    avg_change: float
    up_count: int
    down_count: int
    total: int
    up_ratio: float
    stocks: List[SectorStock]


class AlertResponse(BaseModel):
    type: str
    level: str
    title: str
    detail: str
    icon: str
    timestamp: str


class ValidationReportResponse(BaseModel):
    total_checks: int
    failed_checks: int
    success_rate: float
    by_module: dict
    last_check: Optional[str]


class AISummaryResponse(BaseModel):
    market: str
    signals: str
    recommendation: str
    full_text: str
    model: Optional[str] = None
    summary_type: Optional[str] = None


class StockSearchResult(BaseModel):
    code: str
    name: str
    industry: Optional[str] = None


class LimitStock(BaseModel):
    code: str
    name: str
    price: float
    change_pct: float


class MarketActivityResponse(BaseModel):
    limit_up: dict
    limit_down: dict
    near_limit: dict
    stats: dict


class StrategyInfo(BaseModel):
    id: str
    name: str
    category: str
    description: str


class ScreeningResult(BaseModel):
    code: str
    name: str
    score: float
    level: Optional[str]
    action: str
    position: float
    factors: dict
    reasons: List[str]
    strategy: str
    price: float
    target_price: Optional[float]
    stop_loss_price: Optional[float]


class PositionResponse(BaseModel):
    code: str
    name: str
    entry_price: float
    quantity: float
    entry_date: str
    strategy: str
    current_price: float
    pnl: float
    pnl_pct: float


class PortfolioSummaryResponse(BaseModel):
    total_positions: int
    total_value: float
    total_cost: float
    total_pnl: float
    total_pnl_pct: float
    positions: List[PositionResponse]


class PostEvalResponse(BaseModel):
    run_id: str
    run_date: str
    total_signals: int
    win_count: int = 0
    win_rate: float = 0.0
    avg_pnl: float = 0.0
    avg_max_profit: float = 0.0
    avg_max_drawdown: float = 0.0
    # 注（B4，2026-10-21）：get_run_list 仅返回 run_id/run_date/total_signals/market_state 四键，
    # 统计字段由 evaluate_run 单点现算（不落列表缓存）；五个统计字段给默认值保证旧 run/未评估
    # run 不触发 pydantic ValidationError（修复前 /api/evaluation/runs 对任何非空 runs:list 必 500）。
    # 真实统计请调 /api/evaluation/{run_id}。


class HotspotLeaderStock(BaseModel):
    code: str
    name: str
    change_pct: float


class HotspotResponse(BaseModel):
    name: str
    score: float
    avg_change: float
    leader_stocks: List[HotspotLeaderStock]
    up_count: int
    total: int
    up_ratio: float

