from __future__ import annotations

from pydantic import BaseModel


class PortfolioResponse(BaseModel):
    cash_balance: float
    market_value: float
    total_asset: float
    current_position_pct: float
    cumulative_deposit_amount: float = 0.0
    cumulative_withdraw_amount: float = 0.0
    net_capital_flow_amount: float = 0.0
    current_capital_base: float = 0.0
    holdings: list[dict]
    capital_flows: list[dict] = []


class PerformanceResponse(BaseModel):
    cumulative_return_pct: float
    win_rate: float
    max_drawdown_pct: float
    advice_hit_rate: float
    cumulative_deposit_amount: float = 0.0
    cumulative_withdraw_amount: float = 0.0
    net_capital_flow_amount: float = 0.0
    current_capital_base: float = 0.0
    return_basis_note: str = ""
    curve: list[dict]
    capital_flows: list[dict]
    trades: list[dict]
