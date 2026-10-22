"""AI 总结域路由（P3 重构从 routes.py 1122 行拆分，prd/15 §2.4）

URL/方法/响应结构与拆分前一一对应；模型见 ../schemas，共享依赖见 ../_shared。
"""
from fastapi import APIRouter, Depends, HTTPException
from loguru import logger

from .._shared import (
    _SUMMARY_TYPES,
    datacenter,
    verify_api_key,
)
from ..schemas import AISummaryResponse

router = APIRouter(prefix="/api")

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

