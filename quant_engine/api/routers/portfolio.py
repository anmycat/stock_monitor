"""组合域路由（P3 重构从 routes.py 1122 行拆分，prd/15 §2.4）

URL/方法/响应结构与拆分前一一对应；模型见 ../schemas，共享依赖见 ../_shared。
"""

from fastapi import APIRouter, Depends, HTTPException
from loguru import logger

from .._shared import (
    portfolio_manager,
    verify_api_key,
)
from ..schemas import PortfolioSummaryResponse, PositionResponse

router = APIRouter(prefix="/api")

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


@router.get("/portfolio/positions", response_model=list[PositionResponse], dependencies=[Depends(verify_api_key)])
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

