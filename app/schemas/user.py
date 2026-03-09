from __future__ import annotations

from pydantic import BaseModel, Field


class InitUserRequest(BaseModel):
    initial_capital: float = Field(gt=0)
    risk_level: str = Field(default="中性")
    allow_gold: bool = True
    allow_bond: bool = True
    allow_overseas: bool = True
    min_trade_amount: float = Field(default=1000.0, gt=0)


class UserProfileResponse(BaseModel):
    user_id: int
    initial_capital: float
    cash_balance: float
    total_asset: float
    risk_level: str
    allow_gold: bool
    allow_bond: bool
    allow_overseas: bool
