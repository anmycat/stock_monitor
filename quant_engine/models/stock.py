from __future__ import annotations
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class StockQuote(BaseModel):
    code: str
    name: str
    price: float
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float
    change_pct: float
    timestamp: datetime
    turnover_rate: float | None = None
    pe_ratio: float | None = None
    pb_ratio: float | None = None
