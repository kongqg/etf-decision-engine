from __future__ import annotations

from pydantic import BaseModel


class PortfolioResponse(BaseModel):
    cash_balance: float
    market_value: float
    total_asset: float
    current_position_pct: float
    holdings: list[dict]


class PerformanceResponse(BaseModel):
    cumulative_return_pct: float
    win_rate: float
    max_drawdown_pct: float
    advice_hit_rate: float
    curve: list[dict]
    trades: list[dict]
