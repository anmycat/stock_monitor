"""系统域路由（P3 重构从 routes.py 1122 行拆分，prd/15 §2.4）

URL/方法/响应结构与拆分前一一对应；模型见 ../schemas，共享依赖见 ../_shared。
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger

from .._shared import (
    code_guard,
    datacenter,
    verify_api_key,
)
from ..schemas import CodeGuardReportResponse

router = APIRouter(prefix="/api")

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


@router.get("/health")
async def health_check():
    """系统健康检查端点(无需认证)"""
    from datetime import datetime

    from ...utils.calendar import get_market_phase
    
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
        from ...data.source_manager import source_manager
        health["data_sources"] = source_manager.get_status_report()
    except Exception as e:
        health["data_sources"] = {}
        logger.error(f"Health check - Data sources error: {e}")
    
    return health
