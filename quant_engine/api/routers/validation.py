"""校验域路由（P3 重构从 routes.py 1122 行拆分，prd/15 §2.4）

URL/方法/响应结构与拆分前一一对应；模型见 ../schemas，共享依赖见 ../_shared。
"""
from fastapi import APIRouter, HTTPException, Depends
from loguru import logger

from .._shared import (
    datacenter, verify_api_key, market_analyzer,
    data_validator, _CODE_RE,
)
from ..schemas import ValidationReportResponse


router = APIRouter(prefix="/api")

@router.get("/validation", response_model=ValidationReportResponse, dependencies=[Depends(verify_api_key)])
def get_validation_report():
    try:
        market_analyzer.analyze_market()
        market_data = datacenter.get_market_overview()
        is_valid, errors = data_validator.validate_market_data(market_data)
        data_validator.log_validation("market", is_valid, errors)
        report = data_validator.get_validation_report()
        return ValidationReportResponse(**report)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_validation_report: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/validation/stock/{code}", dependencies=[Depends(verify_api_key)])
def validate_stock(code: str):
    if not _CODE_RE.match(code):
        raise HTTPException(status_code=400, detail=f'无效股票代码格式: {code}')
    try:
        df = datacenter.get_daily_data(code, 30)
        is_valid, errors = data_validator.validate_stock_data(df, code)
        data_validator.log_validation(f"stock:{code}", is_valid, errors)
        return {
            "code": code,
            "is_valid": is_valid,
            "errors": errors,
            "data_rows": len(df) if df is not None else 0,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in validate_stock: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

