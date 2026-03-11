from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from app.core.config import get_settings


RiskMode = Literal["conservative", "balanced", "aggressive"]


class InitUserRequest(BaseModel):
    initial_capital: float = Field(gt=0)
    risk_level: str = Field(default="中性")
    risk_mode: RiskMode = Field(default="balanced")
    allow_gold: bool = True
    allow_bond: bool = True
    allow_overseas: bool = True
    min_trade_amount: float = Field(default_factory=lambda: get_settings().default_min_advice_amount, gt=0)
    target_holding_days: int = Field(default=5, ge=0, le=40)


class UserProfileResponse(BaseModel):
    user_id: int
    initial_capital: float
    cash_balance: float
    total_asset: float
    risk_level: str
    risk_mode: RiskMode
    allow_gold: bool
    allow_bond: bool
    allow_overseas: bool


class UpdatePreferencesRequest(BaseModel):
    risk_level: str = Field(default="中性")
    risk_mode: RiskMode = Field(default="balanced")
    allow_gold: bool = True
    allow_bond: bool = True
    allow_overseas: bool = True
    min_trade_amount: float = Field(default_factory=lambda: get_settings().default_min_advice_amount, gt=0)
    target_holding_days: int = Field(default=5, ge=0, le=40)
    max_total_position_pct: float = Field(gt=0, le=1)
    max_single_position_pct: float = Field(gt=0, le=1)
    cash_reserve_pct: float = Field(ge=0, lt=1)


class PreferencesResponse(BaseModel):
    risk_level: str
    risk_mode: RiskMode
    allow_gold: bool
    allow_bond: bool
    allow_overseas: bool
    min_trade_amount: float
    target_holding_days: int
    max_total_position_pct: float
    max_single_position_pct: float
    cash_reserve_pct: float
