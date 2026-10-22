"""预警域路由（P3 重构从 routes.py 1122 行拆分，prd/15 §2.4）

URL/方法/响应结构与拆分前一一对应；模型见 ../schemas，共享依赖见 ../_shared。
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger

from .._shared import (
    datacenter,
    market_alert,
    market_analyzer,
    verify_api_key,
)
from ..schemas import AlertResponse

router = APIRouter(prefix="/api")

@router.get("/alerts", response_model=list[AlertResponse], dependencies=[Depends(verify_api_key)])
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


@router.get("/alerts/history", response_model=list[AlertResponse], dependencies=[Depends(verify_api_key)])
def get_alert_history(limit: int = Query(20, ge=1, le=50)):
    try:
        history = market_alert.get_history(limit)
        return [AlertResponse(**a) for a in history]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_alert_history: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

