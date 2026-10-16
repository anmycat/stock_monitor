from pydantic import BaseModel
from typing import Optional
from datetime import datetime


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
    turnover_rate: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None