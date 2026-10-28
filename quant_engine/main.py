import sys
import time
import uuid
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger

from .api.routers import export_router
from .api.websocket_enhanced import ws_router
from .config import get_settings

settings = get_settings()
logger.remove()
logger.configure(patcher=lambda record: record["extra"].setdefault("request_id", "-"))
logger.add(
    sys.stdout,
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {extra[request_id]} | {name}:{function}:{line} - {message}",
    backtrace=False,
    diagnose=False,
)

app = FastAPI(
    title="量化扫描系统 V5",
    description="个人级量化交易决策中台",
    version="5.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in settings.CORS_ORIGINS.split(",") if o.strip()],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-API-Key"],
)


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    """请求ID中间件: 每个请求分配唯一ID，用于日志关联追踪"""
    request_id = str(uuid.uuid4())[:8]
    request.state.request_id = request_id
    start = time.time()
    with logger.contextualize(request_id=request_id):
        try:
            response = await call_next(request)
            elapsed = round((time.time() - start) * 1000, 1)
            response.headers["X-Request-ID"] = request_id
            if request.url.path.startswith('/api/'):
                logger.info(f"{request.method} {request.url.path} -> {response.status_code} ({elapsed}ms)")
            return response
        except Exception as e:
            elapsed = round((time.time() - start) * 1000, 1)
            logger.error(f"{request.method} {request.url.path} -> ERROR ({elapsed}ms): {e}")
            raise


def _error_payload(code: str, message: str, request_id: str, details=None):
    payload = {
        "code": code,
        "message": message,
        "request_id": request_id,
    }
    if details is not None:
        payload["details"] = details
    return {"success": False, "error": payload}


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    request_id = getattr(request.state, "request_id", "-")
    message = exc.detail if isinstance(exc.detail, str) else "Request error"
    if exc.status_code >= 500:
        message = "Internal server error"
    logger.warning(f"[{request_id}] {request.method} {request.url.path} -> HTTP {exc.status_code}: {message}")
    return JSONResponse(
        status_code=exc.status_code,
        content=_error_payload(f"HTTP_{exc.status_code}", message, request_id, exc.detail if not isinstance(exc.detail, str) else None),
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    request_id = getattr(request.state, "request_id", "-")
    logger.warning(f"[{request_id}] {request.method} {request.url.path} -> validation error")
    return JSONResponse(
        status_code=422,
        content=_error_payload("VALIDATION_ERROR", "Validation error", request_id, exc.errors()),
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """全局异常处理器，不暴露内部错误详情"""
    request_id = getattr(request.state, "request_id", "-")
    logger.error(f"[{request_id}] Unhandled exception on {request.method} {request.url.path}: {exc}")
    return JSONResponse(
        status_code=500,
        content=_error_payload("INTERNAL_SERVER_ERROR", "Internal server error", request_id),
    )

app.include_router(export_router())
app.include_router(ws_router)

# B8：UI 静态资源挂载。优先 quant_engine/static（代码包内），
# 回退项目根 UX/static（Apple UI 原型，部署脚本已同步）。
# 路径基于 __file__，不依赖 CWD。
_APP_DIR = Path(__file__).parent
static_dir = _APP_DIR / "static"
if not static_dir.exists():
    static_dir = _APP_DIR.parent / "UX" / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/")
async def root():
    index_file = static_dir / "index.html"
    if index_file.exists():
        return FileResponse(str(index_file))
    return {"message": "量化扫描系统 V5", "docs": "/docs"}


@app.on_event("startup")
async def startup_event():
    settings = get_settings()
    logger.info("Starting quant engine v5.0.0")
    logger.info(
        f"API: {settings.API_HOST}:{settings.API_PORT} "
        f"reload={settings.API_RELOAD} auth={'enabled' if settings.API_AUTH_TOKEN else 'disabled'}"
    )


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down quant engine")
    # 释放Redis连接资源
    try:
        from .api.dependencies import datacenter
        datacenter.cache.close()
        logger.info("Redis连接已关闭")
    except Exception as e:  # noqa: BLE001 任务兜底
        logger.error(f"关闭Redis连接时出错: {e}")


if __name__ == "__main__":
    import uvicorn
    settings = get_settings()
    uvicorn.run(
        "quant_engine.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.API_RELOAD,
    )
