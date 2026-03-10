from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class CapitalAdjustmentRequest(BaseModel):
    executed_at: datetime
    flow_type: str
    amount: float = Field(gt=0)
    note: str = ""

