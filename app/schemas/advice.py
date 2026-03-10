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
    category: str | None = None
    asset_class: str | None = None
    trade_mode: str | None = None
    trade_mode_note: str | None = None
    execution_timing_mode: str | None = None
    execution_timing_label: str | None = None
    recommended_execution_windows: list[str] = Field(default_factory=list)
    avoid_execution_windows: list[str] = Field(default_factory=list)
    timing_note: str | None = None
    timing_rule_applied: bool = False
    timing_display_enabled: bool = True
    current_execution_phase: str | None = None
    latest_price: float | None = None
    lot_size: float | None = None
    fee_rate: float | None = None
    min_fee: float | None = None
    estimated_fee: float | None = None
    estimated_cost_rate: float | None = None
    is_cost_efficient: bool = True
    cost_reason: str | None = None
    min_advice_amount: float | None = None
    min_order_amount: float | None = None
    available_cash: float | None = None
    budget_gap_to_min_order: float | None = None
    is_budget_executable: bool = True
    passes_min_advice: bool = True
    is_executable: bool = True
    execution_status: str | None = None
    recommendation_bucket: str | None = None
    not_executable_reason: str | None = None
    execution_note: str | None = None
    small_account_override: bool = False
    is_budget_substitute: bool = False
    primary_asset_class: str | None = None
    budget_substitute_reason: str | None = None
    is_best_unaffordable: bool = False
    best_unaffordable_reason: str | None = None
    is_affordable_but_weak: bool = False
    weak_signal_reason: str | None = None
    asset_allocation_weight: float | None = None
    asset_class_signal_score: float | None = None


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
    best_unaffordable_recommendation: AdviceItemResponse | None = None
    affordable_but_weak_recommendations: list[AdviceItemResponse] = Field(default_factory=list)
    watchlist_recommendations: list[AdviceItemResponse] = Field(default_factory=list)
    cost_inefficient_recommendations: list[AdviceItemResponse] = Field(default_factory=list)
    show_watchlist_recommendations: bool = True
    show_cost_inefficient_recommendations: bool = True
    budget_filter_enabled: bool = True
    fee_filter_enabled: bool = True


class ExplanationResponse(BaseModel):
    advice_id: int
    overall: dict
    items: list[dict]
