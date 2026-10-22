"""信号域路由（P3 重构从 routes.py 1122 行拆分，prd/15 §2.4）

URL/方法/响应结构与拆分前一一对应；模型见 ../schemas，共享依赖见 ../_shared。
"""
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger

from .._shared import (
    _CODE_RE,
    datacenter,
    signal_generator,
    verify_api_key,
)
from ..schemas import SignalResponse

router = APIRouter(prefix="/api")

@router.get("/signals", response_model=List[SignalResponse], dependencies=[Depends(verify_api_key)])
def get_signals(max_signals: int = Query(3, ge=1, le=10)):
    try:
        # Redis缓存: 避免全量扫描1.2s
        cache_key = f'api_cache:signals:{max_signals}'
        cached = datacenter.cache.get(cache_key)
        if cached is not None:
            return [SignalResponse(**s) for s in cached]
        signals = signal_generator.scan_all(max_signals)
        result = [
            SignalResponse(
                code=s.code, name=s.name, action=s.action.value,
                level=s.level.value if s.level else None,
                score=s.score, position=s.position, reason=s.reason,
                strategy=s.strategy, price=s.price,
                target_price=s.target_price, stop_loss=s.stop_loss,
                kelly_position=s.kelly_position,
                kelly_confidence=s.kelly_confidence,
                kelly_source=s.kelly_source,
            )
            for s in signals
        ]
        datacenter.cache.set(cache_key, [s.model_dump() for s in result], ttl=300)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_signals: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/signals/{code}", response_model=SignalResponse, dependencies=[Depends(verify_api_key)])
def get_stock_signal(code: str):
    if not _CODE_RE.match(code):
        raise HTTPException(status_code=400, detail=f'无效股票代码格式: {code}，应为如000001.SZ')
    try:
        signal = signal_generator.scan_stock(code)
        if signal is None:
            raise HTTPException(status_code=404, detail="No signal found")
        return SignalResponse(
            code=signal.code,
            name=signal.name,
            action=signal.action.value,
            level=signal.level.value if signal.level else None,
            score=signal.score,
            position=signal.position,
            reason=signal.reason,
            strategy=signal.strategy,
            price=signal.price,
            target_price=signal.target_price,
            stop_loss=signal.stop_loss,
            kelly_position=signal.kelly_position,
            kelly_confidence=signal.kelly_confidence,
            kelly_source=signal.kelly_source,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_stock_signal: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

