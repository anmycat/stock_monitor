"""后验评估域路由（P3 重构从 routes.py 1122 行拆分，prd/15 §2.4）

URL/方法/响应结构与拆分前一一对应；模型见 ../schemas，共享依赖见 ../_shared。
"""
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List
from loguru import logger

from .._shared import (
    verify_api_key, post_evaluation, _RUN_ID_RE,
)
from ..schemas import PostEvalResponse


router = APIRouter(prefix="/api")

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

