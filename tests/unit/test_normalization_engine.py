from __future__ import annotations

import pandas as pd
import pytest

from app.services.normalization_engine import NormalizationEngine


def test_normalization_engine_applies_category_percentile_ranks():
    frame = pd.DataFrame(
        [
            {
                "symbol": "A",
                "decision_category": "stock_etf",
                "momentum_20d": 10.0,
                "momentum_10d": 8.0,
                "momentum_5d": 4.0,
                "trend_strength": 3.0,
                "volatility_20d": 12.0,
                "drawdown_20d": -4.0,
                "liquidity_score": 40.0,
            },
            {
                "symbol": "B",
                "decision_category": "stock_etf",
                "momentum_20d": 20.0,
                "momentum_10d": 9.0,
                "momentum_5d": 5.0,
                "trend_strength": 5.0,
                "volatility_20d": 8.0,
                "drawdown_20d": -2.0,
                "liquidity_score": 60.0,
            },
            {
                "symbol": "C",
                "decision_category": "bond_etf",
                "momentum_20d": 2.0,
                "momentum_10d": 1.0,
                "momentum_5d": 0.5,
                "trend_strength": 1.0,
                "volatility_20d": 2.0,
                "drawdown_20d": -0.5,
                "liquidity_score": 80.0,
            },
        ]
    )

    ranked = NormalizationEngine().apply(frame)

    row_a = ranked.loc[ranked["symbol"] == "A"].iloc[0]
    row_b = ranked.loc[ranked["symbol"] == "B"].iloc[0]
    row_c = ranked.loc[ranked["symbol"] == "C"].iloc[0]

    assert row_b["momentum_20d_rank"] == pytest.approx(100.0)
    assert row_a["momentum_20d_rank"] == pytest.approx(0.0)
    assert row_b["volatility_rank"] == pytest.approx(100.0)
    assert row_a["volatility_rank"] == pytest.approx(0.0)
    assert row_c["momentum_20d_rank"] == pytest.approx(100.0)
