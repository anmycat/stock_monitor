"""ETF 域路由（P3 重构从 routes.py 1122 行拆分，prd/15 §2.4）

URL/方法/响应结构与拆分前一一对应；模型见 ../schemas，共享依赖见 ../_shared。
"""
import re
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from loguru import logger

from .._shared import (
    datacenter,
    etf_holdings_monitor,
    etf_monitor,
    verify_api_key,
)
from ..schemas import (
    ETFHoldingItem,
    ETFHoldingsChangesResponse,
    ETFHoldingsLatestResponse,
    ETFHoldingsOverviewItem,
    ETFResponse,
)

router = APIRouter(prefix="/api")

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

