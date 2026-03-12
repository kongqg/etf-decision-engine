from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class RecordTradeRequest(BaseModel):
    executed_at: datetime
    symbol: str
    name: str
    side: str
    price: float = Field(gt=0)
    amount: float = Field(gt=0)
    quantity: float | None = Field(default=None, gt=0)
    fee: float = Field(default=0.0, ge=0)
    related_advice_id: int | None = None
    advice_item_id: int | None = None
    intent: str = ""
    weight_before: float = Field(default=0.0, ge=0.0)
    weight_after: float = Field(default=0.0, ge=0.0)
    note: str = ""
