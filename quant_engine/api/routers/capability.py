"""数据源能力中心只读端点（I-02，2026-11-07）

schema 采用 DSA #2304 的 ``providers / datasets / priorities / as_of +
warnings`` 四段结构。纯观测，不触发任何取数，也不修改源状态。
"""
from __future__ import annotations

from fastapi import APIRouter, Depends

from ...data.capability import build_capability_report
from .._shared import datacenter, verify_api_key

router = APIRouter(prefix="/api", dependencies=[Depends(verify_api_key)])


@router.get("/data-capability")
def get_data_capability():
    """数据源健康快照：源状态、优先级、降级留痕、整帧异常与封存覆盖。"""
    try:
        return build_capability_report(cache=getattr(datacenter, 'cache', None))
    except Exception as e:
        from loguru import logger
        logger.warning(f"[I-02] capability report failed: {e}")
        return {
            'as_of': None, 'providers': [], 'datasets': [], 'priorities': [],
            'warnings': [f'report_unavailable: {type(e).__name__}'],
        }
