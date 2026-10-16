"""API认证模块 - Token验证"""
from fastapi import HTTPException, Security, status, WebSocket, Query
from fastapi.security import APIKeyHeader
from typing import Optional
from ..config import get_settings
from loguru import logger

# API Key认证头
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

# 启动时检查认证配置
_auth_warned = False


def _warn_no_auth():
    """启动时警告：未配置认证Token"""
    global _auth_warned
    if _auth_warned:
        return
    settings = get_settings()
    if not settings.API_AUTH_TOKEN:
        logger.warning("⚠️ API_AUTH_TOKEN未配置，所有API端点无需认证（生产环境建议配置）")
        _auth_warned = True


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


def verify_websocket_token(token: Optional[str] = None) -> bool:
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
        logger.warning("WebSocket: 无效的Token尝试")
        return False
    
    logger.debug("WebSocket: Token验证成功")
    return True
