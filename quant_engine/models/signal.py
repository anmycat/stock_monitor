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
