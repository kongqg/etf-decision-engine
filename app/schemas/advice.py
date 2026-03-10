from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, Field


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
    latest_price: float | None = None
    lot_size: float | None = None
    min_advice_amount: float | None = None
    min_order_amount: float | None = None
    available_cash: float | None = None
    budget_gap_to_min_order: float | None = None
    is_executable: bool = True
    execution_status: str | None = None
    recommendation_bucket: str | None = None
    not_executable_reason: str | None = None
    execution_note: str | None = None
    small_account_override: bool = False


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
    executable_recommendations: list[AdviceItemResponse] = Field(default_factory=list)
    watchlist_recommendations: list[AdviceItemResponse] = Field(default_factory=list)
    show_watchlist_recommendations: bool = True
    budget_filter_enabled: bool = True


class ExplanationResponse(BaseModel):
    advice_id: int
    overall: dict
    items: list[dict]
