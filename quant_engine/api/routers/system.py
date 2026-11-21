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


@router.get("/system/doctor")
def system_doctor():
    """只读脱敏自检快照（D70/N-04，DSA `--doctor` 范式）。

    聚合健康 / 数据源能力 / 关键开关（**不输出任何密钥或 webhook**）/
    AI 主力模型标识。纯观测，不取数、不改状态、不参与信号。
    与 API_AUTH_TOKEN 同批启用（依赖 verify_api_key）。
    """
    from datetime import datetime

    from ...config import get_settings

    doctor: dict = {
        'status': 'ok',
        'checked_at': datetime.now().isoformat(),
        'components': {},
        'settings': {},
        'warnings': [],
    }
    # 健康（轻量直读，纯观测；不复用 async health_check 避免事件循环差异）
    try:
        redis_ok = bool(datacenter.cache.is_connected)
    except Exception:
        redis_ok = False
    doctor['components']['health'] = {
        'status': 'healthy' if redis_ok else 'degraded',
        'redis': 'healthy' if redis_ok else 'unhealthy',
    }
    if not redis_ok:
        doctor['status'] = 'degraded'
        doctor['warnings'].append('redis_unavailable')

    # 数据源能力（复用 I-02 报告，纯只读）
    try:
        from ...data.capability import build_capability_report
        cap = build_capability_report(cache=getattr(datacenter, 'cache', None))
        doctor['components']['data_capability'] = {
            'as_of': cap.get('as_of'),
            'providers': [
                {'name': p.get('name'), 'status': p.get('status')}
                for p in cap.get('providers', [])
            ],
            'warnings': cap.get('warnings', []),
        }
        if cap.get('warnings'):
            doctor['warnings'].extend(cap['warnings'][:5])
    except Exception as exc:
        doctor['components']['data_capability'] = {'error': type(exc).__name__}

    # 关键开关（只列布尔/数值，绝不输出密钥/webhook/token）
    try:
        s = get_settings()
        doctor['settings'] = {
            'api_port': s.API_PORT,
            'api_auth_enabled': bool(s.API_AUTH_TOKEN),  # 只报是否启用，不报值
            'em_global_throttle': bool(s.EM_GLOBAL_THROTTLE),
            'intraday_kline_correction': bool(s.INTRADAY_KLINE_CORRECTION),
            'kline_cross_check': bool(s.KLINE_CROSS_CHECK),
            'kline_eod_seal': bool(s.KLINE_EOD_SEAL),
            'stale_quote_reject_seconds': s.STALE_QUOTE_REJECT_SECONDS,
            'source_timeout_suppression': bool(s.SOURCE_TIMEOUT_SUPPRESSION),
            # 密钥存在性（布尔）而非值——便于确认主力模型已配置
            'unisound_key_configured': bool(s.UNISOUND_API_KEY),
            'unisound_model': s.UNISOUND_MODEL,
            'unisound_base_url': s.UNISOUND_BASE_URL,
            'unisound_max_tokens': s.UNISOUND_MAX_TOKENS,
            'deepseek_key_configured': bool(s.DEEPSEEK_API_KEY or s.OPENAI_API_KEY),
        }
        if not doctor['settings']['api_auth_enabled']:
            doctor['warnings'].append('api_auth_token_not_set')
    except Exception:
        doctor['components']['settings'] = {'error': 'unavailable'}

    return doctor


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
