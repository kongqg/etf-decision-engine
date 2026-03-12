from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, Field


class AdviceItemResponse(BaseModel):
    symbol: str
    name: str
    rank: int
    action: str
    intent: str
    category: str
    current_weight: float
    target_weight: float
    delta_weight: float
    current_amount: float
    target_amount: float
    suggested_amount: float
    suggested_pct: float
    final_score: float
    intra_score: float
    category_score: float
    global_rank: int
    category_rank: int
    score_gap_vs_holding: float = 0.0
    replace_threshold_used: float = 0.0
    replacement_symbol: str = ""
    hold_days: int = 0
    is_new_position: bool = False
    reason_short: str
    risk_level: str
    score_breakdown: dict = Field(default_factory=dict)
    rationale: dict = Field(default_factory=dict)


class AdviceResponse(BaseModel):
    id: int
    advice_date: date
    created_at: datetime
    session_mode: str
    action: str
    display_action: str
    action_code: str
    reason_code: str
    market_regime: str
    target_position_pct: float
    current_position_pct: float
    summary_text: str
    risk_text: str
    evidence: dict = Field(default_factory=dict)
    items: list[AdviceItemResponse] = Field(default_factory=list)
    action_counts: dict[str, int] = Field(default_factory=dict)
    intent_counts: dict[str, int] = Field(default_factory=dict)
    target_portfolio: dict = Field(default_factory=dict)
    budget_context: dict = Field(default_factory=dict)
    candidate_summary: list[dict] = Field(default_factory=list)


class ExplanationResponse(BaseModel):
    advice_id: int
    overall: dict = Field(default_factory=dict)
    items: list[dict] = Field(default_factory=list)
