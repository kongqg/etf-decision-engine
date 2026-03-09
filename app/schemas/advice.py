from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel


class AdviceItemResponse(BaseModel):
    symbol: str
    name: str
    rank: int
    action: str
    suggested_amount: float
    suggested_pct: float
    trigger_price_low: float | None
    trigger_price_high: float | None
    stop_loss_pct: float
    take_profit_pct: float
    score: float
    score_gap: float
    reason_short: str
    risk_level: str


class AdviceResponse(BaseModel):
    id: int
    advice_date: date
    created_at: datetime
    session_mode: str
    action: str
    market_regime: str
    target_position_pct: float
    current_position_pct: float
    summary_text: str
    risk_text: str
    evidence: dict
    items: list[AdviceItemResponse]


class ExplanationResponse(BaseModel):
    advice_id: int
    overall: dict
    items: list[dict]
