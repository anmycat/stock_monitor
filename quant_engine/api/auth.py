"""API认证模块 - Token验证"""
from __future__ import annotations

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader
from loguru import logger

from ..config import get_settings

# API Key认证头
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

# 启动时检查认证配置
_auth_warned = False


def _mask(token: str | None) -> str:
    """Token 脱敏：只露前 4 后 2，供运维比对变体不泄露全串（P1-1）"""
    if not token:
        return '<empty>'
    t = str(token)
    if len(t) <= 6:
        return t[:2] + '***'
    return f'{t[:4]}***{t[-2:]}'


def _warn_no_auth():
    """启动时警告：未配置认证Token"""
    global _auth_warned
    if _auth_warned:
        return
    settings = get_settings()
    if not settings.API_AUTH_TOKEN:
        logger.warning(
            "🔓 [SECURITY] API_AUTH_TOKEN 未配置——所有 API/WS 端点无认证开放。"
            "生产环境请在 .env 配置 API_AUTH_TOKEN（deploy 脚本启动前有空值检查）"
        )
        _auth_warned = True


# I-01：显式豁免清单（健康检查必须匿名——部署脚本、容器探针、负载均衡都依赖它）。
# 除此之外所有 /api 路由都应挂 verify_api_key；新增路由漏挂会在启动期告警。
AUTH_EXEMPT_PATHS = frozenset({'/api/health'})


def audit_auth_coverage(app) -> dict:
    """启动期自检：找出未挂鉴权的 /api 路由（只读，不阻断启动）。

    DSA #2304 与 docs/34 I-01 建议「router 级统一挂载」，本项目选择
    逐路由挂载 + 启动期自检的等价形态：既保留 /api/health 匿名，又能
    在新增路由忘记挂依赖时立刻暴露，而不是等上线后才发现端点裸奔。
    """
    checked = 0
    missing: list[str] = []
    for route in getattr(app, 'routes', []):
        path = getattr(route, 'path', '')
        if not path.startswith('/api/') or path in AUTH_EXEMPT_PATHS:
            continue
        checked += 1
        dependant = getattr(route, 'dependant', None)
        calls = [getattr(d, 'call', None)
                 for d in (getattr(dependant, 'dependencies', None) or [])]
        raw = getattr(route, 'dependencies', None) or []
        if verify_api_key not in calls and not any(
                getattr(d, 'dependency', None) is verify_api_key for d in raw):
            missing.append(path)
    if missing:
        logger.warning(
            f"🔓 [SECURITY] {len(missing)} 个 /api 路由未挂鉴权: {', '.join(missing)}")
    return {'checked': checked, 'missing': missing,
            'exempt': sorted(AUTH_EXEMPT_PATHS)}


def verify_api_key(api_key: str = Security(api_key_header)) -> None:
    """
    验证API Key
    
    Args:
        api_key: 从请求头获取的API Key
        
    Raises:
        HTTPException: 当API Key无效时抛出401错误
    """
    _warn_no_auth()
    settings = get_settings()
    
    # 如果未配置token,跳过认证(向后兼容)
    if not settings.API_AUTH_TOKEN:
        return

    # 验证token
    if api_key != settings.API_AUTH_TOKEN:
        logger.warning("无效的API Key尝试")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    
    logger.debug("API Key验证成功")


def verify_websocket_token(token: str | None = None) -> bool:
    """
    验证WebSocket连接的Token
    
    Args:
        token: 从query参数获取的token
        
    Returns:
        bool: 验证是否通过
    """
    settings = get_settings()
    
    # 如果未配置token,跳过认证(向后兼容)
    if not settings.API_AUTH_TOKEN:
        logger.debug("WebSocket: API_AUTH_TOKEN未配置,跳过认证")
        return True

    # 验证token
    if token != settings.API_AUTH_TOKEN:
        logger.warning(f"WebSocket: 无效的Token尝试 (提供={_mask(token)})")
        return False

    logger.debug("WebSocket: Token验证成功")
    return True
