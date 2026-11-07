from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class SignalAction(str, Enum):
    BUY = "BUY"
    WATCH = "WATCH"
    SKIP = "SKIP"


class SignalLevel(str, Enum):
    S = "S"
    A = "A"
    B = "B"


class Signal(BaseModel):
    code: str
    name: str
    action: SignalAction
    level: SignalLevel | None = None
    score: float
    position: float
    reason: list[str]
    timestamp: datetime
    strategy: str
    price: float
    target_price: float | None = None
    stop_loss: float | None = None
    context: dict | None = None
    # 资金流字段（东财 fflow，近5日主力净流入累计，元；None=降级不展示）
    capital_flow_5d: float | None = None
    # D64：现价与日K参考价（新增字段，不改 price 语义 → signals 表无需迁移）
    # current_price: 信号产生时刻的**实时报价**（scan_all 已取全市场行情，零额外请求）
    # price_ref:     日K最后一根收盘价（信号计算所用的参考基准）
    # 差值 > 0.5% 说明日K未含当日未完成 bar（降级源特征），卡片需同时给出两者
    current_price: float | None = None
    price_ref: float | None = None
    # I-06/I-09（2026-11-07）：D66 血缘标签的只读透出。
    # **不参与任何评分/风控/Kelly/通知决策**，仅供 API / 前端信号卡 / 日报证据展示。
    data_as_of: str | None = None       # 日K 末根日期（数据实际截止日）
    data_source: str | None = None      # 产出该日K 的源
    freshness: str | None = None        # current / lagging / stale / unknown
    cross_check: str | None = None      # 针对性跨源复核结论（None=未复核）
    frame_status: str | None = None     # I-04 整帧校验 ok / invalid / unknown
    # Kelly公式字段
    kelly_position: float | None = None        # Kelly建议仓位
    kelly_confidence: float | None = None      # Kelly信心指数(0-100)
    kelly_source: str | None = None            # 'dynamic' / 'default'

    @property
    def id(self) -> str:
        """信号唯一ID: code + strategy + timestamp"""
        ts = self.timestamp.strftime('%Y%m%d%H%M%S') if hasattr(self.timestamp, 'strftime') else str(self.timestamp)
        return f"{self.code}_{self.strategy}_{ts}"

    @property
    def level_factor(self) -> float:
        if self.level is None:
            return 0.5
        factors = {
            SignalLevel.S: 1.0,
            SignalLevel.A: 0.7,
            SignalLevel.B: 0.4,
        }
        return factors[self.level]
