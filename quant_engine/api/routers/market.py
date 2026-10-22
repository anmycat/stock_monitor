"""市场域路由（P3 重构从 routes.py 1122 行拆分，prd/15 §2.4）

URL/方法/响应结构与拆分前一一对应；模型见 ../schemas，共享依赖见 ../_shared。
"""
from fastapi import APIRouter, Depends, HTTPException
from loguru import logger

from .._shared import (
    datacenter,
    etf_monitor,
    verify_api_key,
)
from ..schemas import ETFResponse, MarketOverviewResponse, MarketSentimentResponse

router = APIRouter(prefix="/api")

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

