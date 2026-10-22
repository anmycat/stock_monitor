"""域路由聚合（P3 重构，prd/15 §2.4）

main.py 经 export_router() 一次性挂载；URL 全集与原 routes.py 36 端点一致。
"""
from fastapi import APIRouter

from .alerts import router as alerts_router
from .etf import router as etf_router
from .evaluation import router as evaluation_router
from .market import router as market_router
from .misc import router as misc_router
from .news import router as news_router
from .portfolio import router as portfolio_router
from .sectors import router as sectors_router
from .signals import router as signals_router
from .summary import router as summary_router
from .system import router as system_router
from .validation import router as validation_router

def export_router() -> APIRouter:
    """聚合全部域路由（各域 router 自带 /api 前缀，此处不再叠加）"""
    combined = APIRouter()
    combined.include_router(alerts_router)
    combined.include_router(etf_router)
    combined.include_router(evaluation_router)
    combined.include_router(market_router)
    combined.include_router(misc_router)
    combined.include_router(news_router)
    combined.include_router(portfolio_router)
    combined.include_router(sectors_router)
    combined.include_router(signals_router)
    combined.include_router(summary_router)
    combined.include_router(system_router)
    combined.include_router(validation_router)
    return combined
