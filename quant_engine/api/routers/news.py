"""新闻域路由（P3 重构从 routes.py 1122 行拆分，prd/15 §2.4）

URL/方法/响应结构与拆分前一一对应；模型见 ../schemas，共享依赖见 ../_shared。
"""
from fastapi import APIRouter, HTTPException, Depends
from loguru import logger

from .._shared import (
    datacenter, news_monitor,
    verify_api_key,
)
from ..schemas import NewsDigestResponse, NewsSectorsResponse


router = APIRouter(prefix="/api")

@router.get("/news/latest", response_model=NewsDigestResponse,
            dependencies=[Depends(verify_api_key)])
def get_news_latest():
    """最近一次早间财经要闻摘要（9:15按板块推送的内容，含未展示板块统计）"""
    try:
        cached = datacenter.cache.get('news:latest')
        if not cached:
            raise HTTPException(status_code=404,
                                detail="暂无早间新闻摘要（每日9:15首次推送后生成）")
        return NewsDigestResponse(**cached)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_news_latest: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/news/sectors", response_model=NewsSectorsResponse,
            dependencies=[Depends(verify_api_key)])
def get_news_sectors():
    """最近一次早间新闻的板块分布（含推送文案未展示的板块计数）"""
    try:
        cached = datacenter.cache.get('news:sectors')
        if not cached:
            raise HTTPException(status_code=404,
                                detail="暂无板块分布（每日9:15首次推送后生成）")
        return NewsSectorsResponse(**cached)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_news_sectors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/news/digest", response_model=NewsDigestResponse,
            dependencies=[Depends(verify_api_key)])
def get_news_digest():
    """按需生成一次早间新闻摘要（实时抓取+去重+分类，不写入推送状态）"""
    try:
        digest = news_monitor.collect_digest()
        if not digest or not digest.get('total'):
            raise HTTPException(status_code=404,
                                detail="当前时间窗内无新增新闻")
        return NewsDigestResponse(**digest)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_news_digest: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

