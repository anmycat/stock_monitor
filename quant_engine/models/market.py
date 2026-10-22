from __future__ import annotations
from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class MarketType(str, Enum):
    BULL = "BULL"
    BULL_WEAK = "BULL_WEAK"
    SIDEWAYS = "SIDEWAYS"
    BEAR = "BEAR"


class MarketState(BaseModel):
    type: MarketType
    score: float
    trend: float
    volume_ratio: float
    breadth: float
    timestamp: datetime
    details: dict | None = None

    @property
    def factor(self) -> float:
        factors = {
            MarketType.BULL: 1.0,
            MarketType.BULL_WEAK: 0.7,
            MarketType.SIDEWAYS: 0.5,
            MarketType.BEAR: 0.2,
        }
        return factors[self.type]
