"""杂项域路由（P3 重构从 routes.py 1122 行拆分，prd/15 §2.4）

URL/方法/响应结构与拆分前一一对应；模型见 ../schemas，共享依赖见 ../_shared。
"""
import re

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger

from .._shared import (
    _CODE_RE,
    backtest_engine,
    cooldown_manager,
    datacenter,
    hot_money_monitor,
    hotspot_service,
    market_analyzer,
    resolver,
    strategy_registry,
    verify_api_key,
)
from ..schemas import (
    BacktestRequest,
    CooldownStatusResponse,
    HotspotResponse,
    MarketActivityResponse,
    ScreeningResult,
    StockSearchResult,
    StrategyInfo,
)

router = APIRouter(prefix="/api")

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


@router.get("/search", response_model=list[StockSearchResult], dependencies=[Depends(verify_api_key)])
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


@router.get("/strategies", response_model=list[StrategyInfo], dependencies=[Depends(verify_api_key)])
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


@router.get("/screen/{strategy_id}", response_model=list[ScreeningResult], dependencies=[Depends(verify_api_key)])
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
        from ...services.risk.position_sizer import get_position_sizer
        from ...services.signals.signal_tracker import get_signal_tracker
        
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


@router.get("/hotspots", response_model=list[HotspotResponse], dependencies=[Depends(verify_api_key)])
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