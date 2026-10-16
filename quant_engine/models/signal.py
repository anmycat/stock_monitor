from enum import Enum
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime


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
    level: Optional[SignalLevel] = None
    score: float
    position: float
    reason: List[str]
    timestamp: datetime
    strategy: str
    price: float
    target_price: Optional[float] = None
    stop_loss: Optional[float] = None
    context: Optional[Dict] = None
    # Kelly公式字段
    kelly_position: Optional[float] = None        # Kelly建议仓位
    kelly_confidence: Optional[float] = None      # Kelly信心指数(0-100)
    kelly_source: Optional[str] = None            # 'dynamic' / 'default'

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