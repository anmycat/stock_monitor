"""板块域路由（P3 重构从 routes.py 1122 行拆分，prd/15 §2.4）

URL/方法/响应结构与拆分前一一对应；模型见 ../schemas，共享依赖见 ../_shared。
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger

from .._shared import (
    datacenter,
    sector_monitor,
    verify_api_key,
)
from ..schemas import SectorResponse

router = APIRouter(prefix="/api")

@router.get("/sectors", response_model=list[SectorResponse], dependencies=[Depends(verify_api_key)])
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


@router.get("/sectors/hot", response_model=list[SectorResponse], dependencies=[Depends(verify_api_key)])
def get_hot_sectors(top_n: int = Query(5, ge=1, le=10)):
    try:
        sectors = sector_monitor.get_hot_sectors(top_n)
        return [SectorResponse(**s) for s in sectors]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_hot_sectors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/sectors/cold", response_model=list[SectorResponse], dependencies=[Depends(verify_api_key)])
def get_cold_sectors(top_n: int = Query(50, ge=1, le=50)):
    try:
        sectors = sector_monitor.get_cold_sectors(top_n)
        return [SectorResponse(**s) for s in sectors]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_cold_sectors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

