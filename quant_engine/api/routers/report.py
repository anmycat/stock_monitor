"""结构化日报域路由（D61）

提供"首席分析师日报"的读取接口：
  GET /api/report          最新日报（Redis 优先，回落 SQLite 归档）
  GET /api/report?date=   指定日期的日报
  GET /api/report/dates   可用日期列表（前端日期选择器数据源）

日报的**生成**由 worker 在每日总结后完成并归档，本路由只负责读取
（与 summary 路由一致：读取路径不在线生成，避免阻塞）。
"""
from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger

from ...data.store import get_history_store
from .._shared import datacenter, verify_api_key
from ..schemas import DailyReportResponse, ReportListItemSchema

router = APIRouter(prefix="/api")

# 归档里的日报类型标记（与 summary_type 区分，避免与自由文本总结混淆）
REPORT_TYPE = "daily_report"


def _report_from_cache() -> dict | None:
    """从 Redis 取最新日报"""
    cached = datacenter.cache.get(f"daily_report:{REPORT_TYPE}")
    if isinstance(cached, dict) and cached:
        return cached
    return None


def _report_from_archive(date: str | None = None) -> dict | None:
    """从 SQLite 归档取日报（按日期倒序，可指定日期）"""
    rows = get_history_store().get_summary_history(
        summary_type=REPORT_TYPE, limit=60)
    for row in rows:
        data = row.get('data') or {}
        # 结构化日报以 data.report 字段承载
        report = data.get('report') if isinstance(data, dict) else None
        if not isinstance(report, dict):
            continue
        if date and str(report.get('date')) != date:
            continue
        return report
    return None


@router.get("/report", response_model=DailyReportResponse,
            dependencies=[Depends(verify_api_key)])
def get_daily_report(date: str | None = Query(None, description="报告日期 YYYY-MM-DD")):
    """获取结构化日报

    指定 date 时从归档取历史；不指定时优先返回 Redis 里的最新日报。
    """
    try:
        if date:
            report = _report_from_archive(date)
            if not report:
                raise HTTPException(status_code=404, detail=f"未找到 {date} 的日报")
            return DailyReportResponse(**report)

        report = _report_from_cache() or _report_from_archive()
        if not report:
            raise HTTPException(status_code=404, detail="日报尚未生成，请等待 worker 自动生成")
        return DailyReportResponse(**report)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_daily_report: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/report/dates", response_model=list[ReportListItemSchema],
            dependencies=[Depends(verify_api_key)])
def get_report_dates(limit: int = Query(30, ge=1, le=100)):
    """可用日报日期列表（前端日期选择器数据源）"""
    try:
        rows = get_history_store().get_summary_history(
            summary_type=REPORT_TYPE, limit=limit)
        items: list[ReportListItemSchema] = []
        seen: set[str] = set()
        for row in rows:
            data = row.get('data') or {}
            report = data.get('report') if isinstance(data, dict) else None
            if not isinstance(report, dict):
                continue
            d = str(report.get('date') or '')
            if not d or d in seen:
                continue
            seen.add(d)
            gate = report.get('gate') or {}
            items.append(ReportListItemSchema(
                date=d,
                report_type=REPORT_TYPE,
                model=report.get('model'),
                generated_at=row.get('generated_at'),
                gate_action=gate.get('action'),
                gate_label=gate.get('label'),
                gate_score=gate.get('score'),
                position_advice=report.get('position_advice') or '',
            ))
        return items
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_report_dates: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/report/raw", dependencies=[Depends(verify_api_key)])
def get_daily_report_raw(date: str | None = Query(None)):
    """日报原始 JSON（调试用：含 AI 原始返回，便于排查结构化解析）"""
    try:
        report = (_report_from_archive(date) if date
                  else (_report_from_cache() or _report_from_archive()))
        if not report:
            raise HTTPException(status_code=404, detail="日报尚未生成")
        return json.loads(json.dumps(report, ensure_ascii=False, default=str))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API error in get_daily_report_raw: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
