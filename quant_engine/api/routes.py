import re
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional, Dict
from pydantic import BaseModel, field_validator, model_validator
from loguru import logger
from .dependencies import (
    datacenter, signal_generator, backtest_engine, etf_monitor,
    cooldown_manager, sector_monitor, market_alert, hot_money_monitor,
    strategy_registry, hotspot_service, etf_holdings_monitor, news_monitor,
    code_guard
)
from .auth import verify_api_key
from ..services.ai_summary import AISummary
from ..services.market_analyzer import MarketAnalyzer
from ..services.data_validator import DataValidator
from ..services.portfolio import PortfolioManager
from ..backtest.post_evaluation import PostEvaluation
from ..utils.stock_resolver import resolver

# 入参校验正则
_CODE_RE = re.compile(r'^\d{6}\.(SZ|SH|BJ)$')
_SUMMARY_TYPES = {'general', 'morning_review', 'daily_review', 'call_auction'}
_DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
_RUN_ID_RE = re.compile(r'^[A-Za-z0-9_-]{1,64}$')

router = APIRouter(prefix="/api")

ai_summary = AISummary()
market_analyzer = MarketAnalyzer(datacenter)
data_validator = DataValidator(datacenter.cache)
portfolio_manager = PortfolioManager(datacenter.cache)
post_evaluation = PostEvaluation(datacenter.cache, datacenter)


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


@router.get("/signals", response_model=List[SignalResponse], dependencies=[Depends(verify_api_key)])
def get_signals(max_signals: int = Query(3, ge=1, le=10)):
    try:
        # Redis缓存: 避免全量扫描1.2s
        cache_key = f'api_cache:signals:{max_signals}'
        cached = datacenter.cache.get(cache_key)
        if cached is not None:
            return [SignalResponse(**s) for s in cached]
        signals = signal_generator.scan_all(max_signals)
        result = [
            SignalResponse(
                code=s.code, name=s.name, action=s.action.value,
                level=s.level.value if s.level else None,
                score=s.score, position=s.position, reason=s.reason,
                strategy=s.strategy, price=s.price,
                target_price=s.target_price, stop_loss=s.stop_loss,
                kelly_position=s.kelly_position,
                kelly_confidence=s.kelly_confidence,
                kelly_source=s.kelly_source,
            )
            for s in signals
        ]
        datacenter.cache.set(cache_key, [s.model_dump() for s in result], ttl=300)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_signals: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/signals/{code}", response_model=SignalResponse, dependencies=[Depends(verify_api_key)])
def get_stock_signal(code: str):
    if not _CODE_RE.match(code):
        raise HTTPException(status_code=400, detail=f'无效股票代码格式: {code}，应为如000001.SZ')
    try:
        signal = signal_generator.scan_stock(code)
        if signal is None:
            raise HTTPException(status_code=404, detail="No signal found")
        return SignalResponse(
            code=signal.code,
            name=signal.name,
            action=signal.action.value,
            level=signal.level.value if signal.level else None,
            score=signal.score,
            position=signal.position,
            reason=signal.reason,
            strategy=signal.strategy,
            price=signal.price,
            target_price=signal.target_price,
            stop_loss=signal.stop_loss,
            kelly_position=signal.kelly_position,
            kelly_confidence=signal.kelly_confidence,
            kelly_source=signal.kelly_source,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_stock_signal: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/market", response_model=MarketOverviewResponse, dependencies=[Depends(verify_api_key)])
def get_market_overview():
    try:
        overview = datacenter.get_market_overview()
        return MarketOverviewResponse(**overview)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_market_overview: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/etf", response_model=List[ETFResponse], dependencies=[Depends(verify_api_key)])
def get_etf_status():
    try:
        # Redis缓存: 避免fund_etf_spot_em()冷启动18s
        cached = datacenter.cache.get('api_cache:etf')
        if cached is not None:
            return [ETFResponse(**etf) for etf in cached]
        etf_list = etf_monitor.get_etf_status()
        result = [ETFResponse(**etf) for etf in etf_list]
        datacenter.cache.set('api_cache:etf', [e.model_dump() for e in result], ttl=300)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_etf_status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


def _normalize_etf_code(code: str) -> str:
    """ETF代码归一化：接受 510300 / 510300.SH，统一返回6位代码"""
    code = code.strip()
    if not re.match(r'^\d{6}(\.(SH|SZ|BJ))?$', code):
        raise HTTPException(status_code=400, detail=f"无效ETF代码: {code}")
    return code.split('.')[0]


@router.get("/etf/holdings", response_model=List[ETFHoldingsOverviewItem],
            dependencies=[Depends(verify_api_key)])
def get_etf_holdings_overview():
    """热门ETF持仓监控总览（最新季度快照明细）"""
    try:
        cached = datacenter.cache.get('api_cache:etf_holdings')
        if cached is not None:
            return [ETFHoldingsOverviewItem(**item) for item in cached]
        overview = etf_holdings_monitor.get_overview()
        result = [ETFHoldingsOverviewItem(**item) for item in overview]
        datacenter.cache.set('api_cache:etf_holdings',
                             [item.model_dump() for item in result], ttl=300)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_etf_holdings_overview: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/etf/holdings/{code}", response_model=ETFHoldingsLatestResponse,
            dependencies=[Depends(verify_api_key)])
def get_etf_holdings_latest(code: str):
    """单只ETF最新季度持仓（前N大，默认100）"""
    try:
        symbol = _normalize_etf_code(code)
        latest = etf_holdings_monitor.get_latest(symbol)
        if not latest:
            raise HTTPException(status_code=404,
                                detail=f"暂无{symbol}持仓数据（盘后任务尚未抓取）")
        return ETFHoldingsLatestResponse(
            code=symbol, name=latest.get('etf_name') or symbol,
            quarter=latest.get('quarter'), report_date=latest.get('report_date'),
            updated_at=latest.get('updated_at'),
            holdings_count=latest.get('holdings_count', 0),
            top=[ETFHoldingItem(**h) for h in latest.get('top', [])],
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_etf_holdings_latest: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/etf/holdings/{code}/changes", response_model=ETFHoldingsChangesResponse,
            dependencies=[Depends(verify_api_key)])
def get_etf_holdings_changes(code: str):
    """单只ETF最近一次季度环比变化（新进/退出/增持/减持）"""
    try:
        symbol = _normalize_etf_code(code)
        changes = etf_holdings_monitor.get_changes(symbol)
        if not changes:
            raise HTTPException(status_code=404,
                                detail=f"暂无{symbol}持仓变化数据（需已存上季度快照）")
        return ETFHoldingsChangesResponse(**changes)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_etf_holdings_changes: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/news/latest", response_model=NewsDigestResponse,
            dependencies=[Depends(verify_api_key)])
def get_news_latest():
    """最近一次早间财经要闻摘要（9:15按板块推送的内容，含未展示板块统计）"""
    try:
        cached = datacenter.cache.get('news:latest')
        if not cached:
            raise HTTPException(status_code=404,
                                detail="暂无早间新闻摘要（每日9:15首次推送后生成）")
        return NewsDigestResponse(**cached)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_news_latest: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/news/sectors", response_model=NewsSectorsResponse,
            dependencies=[Depends(verify_api_key)])
def get_news_sectors():
    """最近一次早间新闻的板块分布（含推送文案未展示的板块计数）"""
    try:
        cached = datacenter.cache.get('news:sectors')
        if not cached:
            raise HTTPException(status_code=404,
                                detail="暂无板块分布（每日9:15首次推送后生成）")
        return NewsSectorsResponse(**cached)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_news_sectors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/news/digest", response_model=NewsDigestResponse,
            dependencies=[Depends(verify_api_key)])
def get_news_digest():
    """按需生成一次早间新闻摘要（实时抓取+去重+分类，不写入推送状态）"""
    try:
        digest = news_monitor.collect_digest()
        if not digest or not digest.get('total'):
            raise HTTPException(status_code=404,
                                detail="当前时间窗内无新增新闻")
        return NewsDigestResponse(**digest)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_news_digest: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/system/code-guard/check", response_model=CodeGuardReportResponse,
             dependencies=[Depends(verify_api_key)])
def run_code_guard_check(mode: str = Query("static", pattern="^(static|auction)$")):
    """手动触发代码一致性校验（static=盘前静态校验 / auction=集合竞价缓存校验）

    对全部持久化监控目标（热门ETF/ETF持仓成分股/stock_list）做
    代码-市场-名称-价格四重校验；market_mismatch 自动修复脏缓存。
    """
    try:
        report = (code_guard.check_static() if mode == 'static'
                  else code_guard.check_auction())
        if not report:
            raise HTTPException(status_code=500, detail="校验失败：无目标或数据源异常")
        return CodeGuardReportResponse(**report)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in run_code_guard_check: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/system/code-guard/report", response_model=CodeGuardReportResponse,
            dependencies=[Depends(verify_api_key)])
def get_code_guard_report():
    """最近一次代码一致性校验报告（每日盘前/集合竞价由 worker 生成）"""
    try:
        report = code_guard.get_report()
        if not report:
            raise HTTPException(status_code=404,
                                detail="暂无校验报告（每日9:00盘前校验后生成）")
        return CodeGuardReportResponse(**report)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_code_guard_report: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/market/sentiment", response_model=MarketSentimentResponse, dependencies=[Depends(verify_api_key)])
def get_market_sentiment():
    try:
        # 复用ETF Redis缓存
        cached_etf = datacenter.cache.get('api_cache:etf')
        if cached_etf is not None:
            valid_etfs = [e for e in cached_etf if not e.get('no_data')]
            up_count = sum(1 for e in valid_etfs if e.get('change_pct', 0) > 0)
            down_count = sum(1 for e in valid_etfs if e.get('change_pct', 0) < 0)
            avg_change = sum(e.get('change_pct', 0) for e in valid_etfs) / len(valid_etfs) if valid_etfs else 0
            if avg_change > 1: sentiment, score = 'bullish', 70
            elif avg_change > 0: sentiment, score = 'slightly_bullish', 60
            elif avg_change > -1: sentiment, score = 'neutral', 50
            elif avg_change > -2: sentiment, score = 'slightly_bearish', 40
            else: sentiment, score = 'bearish', 30
            details = [ETFResponse(**d) for d in cached_etf]
            return MarketSentimentResponse(sentiment=sentiment, score=score, avg_change=round(avg_change, 2),
                up_count=up_count, down_count=down_count, details=details)
        sentiment = etf_monitor.get_market_sentiment()
        details = [ETFResponse(**d) for d in sentiment.get('details', [])]
        return MarketSentimentResponse(
            sentiment=sentiment['sentiment'], score=sentiment['score'],
            avg_change=sentiment['avg_change'], up_count=sentiment['up_count'],
            down_count=sentiment['down_count'], details=details,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_market_sentiment: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/sectors", response_model=List[SectorResponse], dependencies=[Depends(verify_api_key)])
def get_sectors():
    try:
        # Redis缓存: 避免16个板块逐一查询
        cached = datacenter.cache.get('api_cache:sectors')
        if cached is not None:
            return [SectorResponse(**s) for s in cached]
        sectors = sector_monitor.get_sector_data()
        result = [SectorResponse(**s) for s in sectors]
        datacenter.cache.set('api_cache:sectors', [s.model_dump() for s in result], ttl=300)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_sectors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/sectors/hot", response_model=List[SectorResponse], dependencies=[Depends(verify_api_key)])
def get_hot_sectors(top_n: int = Query(5, ge=1, le=10)):
    try:
        sectors = sector_monitor.get_hot_sectors(top_n)
        return [SectorResponse(**s) for s in sectors]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_hot_sectors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/sectors/cold", response_model=List[SectorResponse], dependencies=[Depends(verify_api_key)])
def get_cold_sectors(top_n: int = Query(50, ge=1, le=50)):
    try:
        sectors = sector_monitor.get_cold_sectors(top_n)
        return [SectorResponse(**s) for s in sectors]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_cold_sectors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/alerts", response_model=List[AlertResponse], dependencies=[Depends(verify_api_key)])
def get_alerts():
    try:
        # Redis缓存: 60s内避免重复分析
        cached = datacenter.cache.get('api_cache:alerts')
        if cached is not None:
            return [AlertResponse(**a) for a in cached]
        market = market_analyzer.analyze_market()
        alerts = market_alert.detect_alerts(market)
        result = [AlertResponse(**a) for a in alerts]
        datacenter.cache.set('api_cache:alerts', [a.model_dump() for a in result], ttl=60)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_alerts: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/alerts/history", response_model=List[AlertResponse], dependencies=[Depends(verify_api_key)])
def get_alert_history(limit: int = Query(20, ge=1, le=50)):
    try:
        history = market_alert.get_history(limit)
        return [AlertResponse(**a) for a in history]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_alert_history: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/validation", response_model=ValidationReportResponse, dependencies=[Depends(verify_api_key)])
def get_validation_report():
    try:
        market = market_analyzer.analyze_market()
        market_data = datacenter.get_market_overview()
        is_valid, errors = data_validator.validate_market_data(market_data)
        data_validator.log_validation("market", is_valid, errors)
        report = data_validator.get_validation_report()
        return ValidationReportResponse(**report)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_validation_report: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/validation/stock/{code}", dependencies=[Depends(verify_api_key)])
def validate_stock(code: str):
    if not _CODE_RE.match(code):
        raise HTTPException(status_code=400, detail=f'无效股票代码格式: {code}')
    try:
        df = datacenter.get_daily_data(code, 30)
        is_valid, errors = data_validator.validate_stock_data(df, code)
        data_validator.log_validation(f"stock:{code}", is_valid, errors)
        return {
            "code": code,
            "is_valid": is_valid,
            "errors": errors,
            "data_rows": len(df) if df is not None else 0,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in validate_stock: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/cooldown/{code}", response_model=CooldownStatusResponse, dependencies=[Depends(verify_api_key)])
def get_cooldown_status(code: str):
    if not _CODE_RE.match(code):
        raise HTTPException(status_code=400, detail=f'无效股票代码格式: {code}')
    try:
        is_cooled = cooldown_manager.is_cooled_down(code)
        return CooldownStatusResponse(
            code=code,
            is_cooled_down=is_cooled,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_cooldown_status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/backtest", dependencies=[Depends(verify_api_key)])
def run_backtest(request: BacktestRequest):
    try:
        result = backtest_engine.run(
            request.codes,
            request.start_date,
            request.end_date,
            request.initial_capital,
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in run_backtest: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/summary", response_model=AISummaryResponse, dependencies=[Depends(verify_api_key)])
def get_ai_summary():
    try:
        # 先检查缓存中是否有最新总结（优先级：daily_review > morning_review > call_auction）
        cached = datacenter.cache.get("ai_summary:daily_review")
        if cached:
            return AISummaryResponse(**cached)
        cached = datacenter.cache.get("ai_summary:morning_review")
        if cached:
            return AISummaryResponse(**cached)
        cached = datacenter.cache.get("ai_summary:call_auction")
        if cached:
            return AISummaryResponse(**cached)
        
        # 缓存为空返回404，不在线生成(避免7s+阻塞)
        raise HTTPException(status_code=404, detail="AI总结尚未生成，请等待worker自动生成")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_ai_summary: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/summary/{summary_type}", response_model=AISummaryResponse, dependencies=[Depends(verify_api_key)])
def get_ai_summary_by_type(summary_type: str):
    """获取指定类型的AI总结"""
    if summary_type not in _SUMMARY_TYPES:
        raise HTTPException(status_code=400, detail=f'无效总结类型: {summary_type}，可选: {_SUMMARY_TYPES}')
    try:
        cache_key = f"ai_summary:{summary_type}"
        cached = datacenter.cache.get(cache_key)
        if cached:
            return AISummaryResponse(**cached)
        raise HTTPException(status_code=404, detail=f"Summary type '{summary_type}' not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_ai_summary_by_type: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


class StockSearchResult(BaseModel):
    code: str
    name: str
    industry: Optional[str] = None


@router.get("/search", response_model=List[StockSearchResult], dependencies=[Depends(verify_api_key)])
def search_stock(q: str = Query(..., min_length=1, max_length=20), limit: int = Query(10, ge=1, le=50)):
    """搜索股票（支持代码/名称/拼音）"""
    try:
        # 初始化缓存
        if not resolver._stock_cache:
            stocks_df = datacenter.get_stock_list()
            if not stocks_df.empty:
                resolver.update_cache(stocks_df.to_dict('records'))

        results = resolver.resolve(q, limit)
        return [StockSearchResult(**r) for r in results]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in search_stock: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# 游资监控接口
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


@router.get("/hot-money", response_model=MarketActivityResponse, dependencies=[Depends(verify_api_key)])
def get_hot_money():
    """获取游资监控数据"""
    try:
        # Redis缓存: 避免涨停板接口冷启动
        cached = datacenter.cache.get('api_cache:hot_money')
        if cached is not None:
            return MarketActivityResponse(**cached)
        activity = hot_money_monitor.get_hot_money_activity()
        datacenter.cache.set('api_cache:hot_money', activity, ttl=300)
        return MarketActivityResponse(**activity)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_hot_money: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


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


@router.get("/strategies", response_model=List[StrategyInfo], dependencies=[Depends(verify_api_key)])
def list_strategies():
    """列出所有可用策略"""
    try:
        strategies = strategy_registry.get_available_strategies()
        return [StrategyInfo(**{**s, 'category': s.get('category', s.get('type', ''))}) for s in strategies]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in list_strategies: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/screen/{strategy_id}", response_model=List[ScreeningResult], dependencies=[Depends(verify_api_key)])
def screen_stocks(strategy_id: str, max_output: int = Query(10, ge=1, le=50)):
    """使用指定策略进行选股"""
    if not re.match(r'^[a-zA-Z0-9_-]{1,50}$', strategy_id):
        raise HTTPException(status_code=400, detail=f'无效策略ID格式: {strategy_id}')
    try:
        market = market_analyzer.analyze_market()
        results = strategy_registry.screen(strategy_id, market, max_output)
        return [ScreeningResult(**r) for r in results]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in screen_stocks: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ====== Kelly公式接口 ======
@router.get("/kelly", dependencies=[Depends(verify_api_key)])
def get_kelly_info():
    """获取Kelly公式配置及各策略Kelly参数"""
    try:
        from ..services.position_sizer import get_position_sizer
        from ..services.signal_tracker import get_signal_tracker
        
        sizer = get_position_sizer()
        tracker = get_signal_tracker()
        
        # Kelly配置
        config = {
            'kelly_fraction': sizer.config.kelly_fraction,
            'max_kelly_position': sizer.config.max_kelly_position,
            'min_trades_required': sizer.config.min_trades_required,
            'use_dynamic_params': sizer.config.use_dynamic_params,
            'portfolio_kelly_enabled': sizer.config.portfolio_kelly_enabled,
            'max_portfolio_kelly': sizer.config.max_portfolio_kelly,
        }
        
        # 各策略Kelly参数
        strategy_perf = tracker.get_strategy_performance()
        strategies_kelly = {}
        for strategy_name, stats in strategy_perf.items():
            params = sizer.get_strategy_kelly_params(strategy_name)
            kelly_result = sizer.calculate_kelly_position(
                params['win_rate'],
                params['profit_loss_ratio'],
                num_positions=1
            )
            strategies_kelly[strategy_name] = {
                'win_rate': params['win_rate'],
                'profit_loss_ratio': params['profit_loss_ratio'],
                'trade_count': params['trade_count'],
                'source': params['source'],
                'full_kelly': kelly_result['full_kelly'],
                'fractional_kelly': kelly_result['fractional_kelly'],
                'kelly_confidence': kelly_result['kelly_confidence'],
                'has_edge': kelly_result['has_edge'],
            }
        
        # 全局信号指标
        metrics = tracker.get_signal_metrics(days=30)
        global_kelly = None
        if metrics.get('count', 0) > 0:
            global_params = sizer.calculate_kelly_position(
                metrics.get('win_rate', 0.5),
                metrics.get('profit_loss_ratio', 1.5),
                num_positions=1
            )
            global_kelly = {
                'win_rate': metrics.get('win_rate'),
                'profit_loss_ratio': metrics.get('profit_loss_ratio'),
                'full_kelly': global_params['full_kelly'],
                'fractional_kelly': global_params['fractional_kelly'],
                'kelly_confidence': global_params['kelly_confidence'],
                'trade_count': metrics.get('count'),
            }
        
        return {
            'config': config,
            'strategies': strategies_kelly,
            'global': global_kelly,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_kelly_info: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# 持仓管理接口
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


@router.get("/portfolio", response_model=PortfolioSummaryResponse, dependencies=[Depends(verify_api_key)])
def get_portfolio():
    """获取持仓摘要"""
    try:
        summary = portfolio_manager.get_portfolio_summary()
        return PortfolioSummaryResponse(**summary)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_portfolio: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/portfolio/positions", response_model=List[PositionResponse], dependencies=[Depends(verify_api_key)])
def get_positions():
    """获取所有持仓"""
    try:
        positions = portfolio_manager.get_positions()
        return [PositionResponse(**p) for p in positions]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_positions: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# 后验评估接口
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


@router.get("/evaluation/runs", response_model=List[PostEvalResponse], dependencies=[Depends(verify_api_key)])
def get_evaluation_runs(limit: int = Query(20, ge=1, le=50)):
    """获取运行列表"""
    try:
        runs = post_evaluation.get_run_list(limit)
        return [PostEvalResponse(**r) for r in runs]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_evaluation_runs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/evaluation/{run_id}", dependencies=[Depends(verify_api_key)])
def get_evaluation(run_id: str):
    """获取单次运行的评估结果"""
    if not _RUN_ID_RE.match(run_id):
        raise HTTPException(status_code=400, detail=f'无效运行ID格式: {run_id}')
    try:
        result = post_evaluation.evaluate_run(run_id)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_evaluation: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/evaluation/batch/summary", dependencies=[Depends(verify_api_key)])
def get_evaluation_batch(limit: int = Query(10, ge=1, le=50)):
    """批量评估汇总"""
    try:
        result = post_evaluation.evaluate_batch(limit)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_evaluation_batch: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# 热点发现接口
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


@router.get("/hotspots", response_model=List[HotspotResponse], dependencies=[Depends(verify_api_key)])
def get_hotspots(top_n: int = Query(50, ge=1, le=50)):
    """获取热点板块排名（热度评分 + 龙头股）"""
    try:
        # Redis缓存: 避免板块行情重复查询
        cache_key = f'api_cache:hotspots:{top_n}'
        cached = datacenter.cache.get(cache_key)
        if cached is not None:
            return [HotspotResponse(**h) for h in cached]
        hotspots = hotspot_service.get_hotspots(top_n)
        result = [HotspotResponse(**h) for h in hotspots]
        datacenter.cache.set(cache_key, [h.model_dump() for h in result], ttl=300)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_hotspots: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/health")
async def health_check():
    """系统健康检查端点(无需认证)"""
    from datetime import datetime
    from ..data.datacenter import get_market_phase
    
    health = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "components": {}
    }
    
    # 检查Redis连接状态
    try:
        redis_ok = datacenter.cache.is_connected
        health["components"]["redis"] = "healthy" if redis_ok else "unhealthy"
        if not redis_ok:
            health["status"] = "degraded"
    except Exception as e:
        health["components"]["redis"] = "unhealthy"
        health["status"] = "degraded"
        logger.error(f"Health check - Redis error: {e}")
    
    # 获取市场阶段
    try:
        health["market_phase"] = get_market_phase()
    except Exception as e:
        health["market_phase"] = "unknown"
        logger.error(f"Health check - Market phase error: {e}")
    
    # 检查数据源状态
    try:
        from ..data.source_manager import source_manager
        health["data_sources"] = source_manager.get_status_report()
    except Exception as e:
        health["data_sources"] = {}
        logger.error(f"Health check - Data sources error: {e}")
    
    return health
