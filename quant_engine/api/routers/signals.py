"""信号域路由（P3 重构从 routes.py 1122 行拆分，prd/15 §2.4）

URL/方法/响应结构与拆分前一一对应；模型见 ../schemas，共享依赖见 ../_shared。
"""

import threading

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger

from ...services.signals.signal_generator import (
    api_signals_cache_key,
    signal_to_api_dict,
    warm_api_signals_cache,
)
from .._shared import (
    _CODE_RE,
    datacenter,
    signal_generator,
    verify_api_key,
)
from ..schemas import SignalResponse

router = APIRouter(prefix="/api")

# 全量扫描单飞锁：分钟级扫描不得并发叠加（并发会争抢 TDX 连接互相拖垮）。
# 冷缓存首请求独占扫描，其余请求 503 + Retry-After，30s 后重试即命中 warm 缓存。
_scan_lock = threading.Lock()

@router.get("/signals", response_model=list[SignalResponse], dependencies=[Depends(verify_api_key)])
def get_signals(max_signals: int = Query(3, ge=1, le=10)):
    try:
        # Redis缓存: 避免全量扫描（worker 交易时段持续预热，TTL 15 分钟）
        cache_key = api_signals_cache_key(max_signals)
        cached = datacenter.cache.get(cache_key)
        if cached is not None:
            return [SignalResponse(**s) for s in cached]
        # 护栏：Redis 不可用时拒绝冷扫兜底——无缓存的全量扫描会退化为 TDX 全池
        # 探活（分钟级），并与 worker/beat 争抢 TDX 连接，故障期反而放大故障
        if not datacenter.cache.is_connected:
            raise HTTPException(
                status_code=503,
                detail="Cache unavailable, signal scan disabled",
                headers={"Retry-After": "30"})
        # 单飞：并发冷缓存请求只放行一个扫描，其余快速失败而非堆积
        if not _scan_lock.acquire(blocking=False):
            raise HTTPException(
                status_code=503,
                detail="Signal scan in progress, please retry",
                headers={"Retry-After": "30"})
        try:
            signals = signal_generator.scan_all(max_signals)
            result = [SignalResponse(**signal_to_api_dict(s)) for s in signals]
            warm_api_signals_cache(
                datacenter.cache, signals, max_signals=max_signals)
            return result
        finally:
            _scan_lock.release()
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

