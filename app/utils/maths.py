from __future__ import annotations

import math

import numpy as np
import pandas as pd


def safe_pct_change(new_value: float, old_value: float) -> float:
    if old_value in (0, None):
        return 0.0
    return (new_value / old_value - 1.0) * 100.0


def pct_rank(series: pd.Series, ascending: bool = True) -> pd.Series:
    if series.empty:
        return pd.Series(dtype=float)
    ranked = series.rank(pct=True, ascending=ascending)
    return ranked.fillna(0.0) * 100.0


def round_money(value: float) -> float:
    return float(round(value, 2))


def floor_to_lot_by_amount(amount: float, lot_size: float = 100.0) -> float:
    if amount <= 0:
        return 0.0
    return math.floor(amount / lot_size) * lot_size


def max_drawdown(values: list[float]) -> float:
    if not values:
        return 0.0
    peak = values[0]
    worst = 0.0
    for value in values:
        peak = max(peak, value)
        if peak == 0:
            continue
        drawdown = (value / peak - 1.0) * 100.0
        worst = min(worst, drawdown)
    return worst


def ensure_dataframe(data: list[dict]) -> pd.DataFrame:
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data)


def annualized_volatility(returns: pd.Series, window: int = 10) -> float:
    clean = returns.dropna()
    if clean.empty:
        return 0.0
    return float(clean.tail(window).std(ddof=0) * np.sqrt(window) * 100.0)
