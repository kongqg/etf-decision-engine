from __future__ import annotations

from datetime import date

import pandas as pd

from app.services.scoring_engine import ScoringEngine


def test_scoring_engine_builds_intra_category_and_final_scores():
    frame = pd.DataFrame(
        [
            {
                "trade_date": date(2026, 3, 11),
                "symbol": "A",
                "name": "Alpha",
                "decision_category": "stock_etf",
                "category_label": "Stock",
                "momentum_20d": 12.0,
                "momentum_10d": 8.0,
                "momentum_5d": 4.0,
                "trend_strength": 3.0,
                "volatility_20d": 10.0,
                "drawdown_20d": -4.0,
                "liquidity_score": 50.0,
                "above_ma20_flag": True,
            },
            {
                "trade_date": date(2026, 3, 11),
                "symbol": "B",
                "name": "Beta",
                "decision_category": "stock_etf",
                "category_label": "Stock",
                "momentum_20d": 20.0,
                "momentum_10d": 12.0,
                "momentum_5d": 7.0,
                "trend_strength": 5.0,
                "volatility_20d": 7.0,
                "drawdown_20d": -2.0,
                "liquidity_score": 70.0,
                "above_ma20_flag": True,
            },
            {
                "trade_date": date(2026, 3, 11),
                "symbol": "C",
                "name": "Gamma",
                "decision_category": "bond_etf",
                "category_label": "Bond",
                "momentum_20d": 5.0,
                "momentum_10d": 3.0,
                "momentum_5d": 1.0,
                "trend_strength": 1.0,
                "volatility_20d": 3.0,
                "drawdown_20d": -0.5,
                "liquidity_score": 90.0,
                "above_ma20_flag": True,
            },
            {
                "trade_date": date(2026, 3, 11),
                "symbol": "D",
                "name": "Delta",
                "decision_category": "bond_etf",
                "category_label": "Bond",
                "momentum_20d": -3.0,
                "momentum_10d": -2.0,
                "momentum_5d": -1.0,
                "trend_strength": -0.5,
                "volatility_20d": 6.0,
                "drawdown_20d": -3.5,
                "liquidity_score": 20.0,
                "above_ma20_flag": False,
            },
        ]
    )

    result = ScoringEngine().score(frame)
    scored = result["scored_df"]

    assert list(scored["symbol"])[:2] == ["B", "C"]
    assert scored.loc[scored["symbol"] == "B", "global_rank"].iloc[0] == 1
    assert scored.loc[scored["symbol"] == "B", "category_rank"].iloc[0] == 1
    assert scored.loc[scored["symbol"] == "A", "category_rank"].iloc[0] == 2
    assert scored.loc[scored["symbol"] == "B", "final_score"].iloc[0] > scored.loc[scored["symbol"] == "A", "final_score"].iloc[0]
    assert result["category_scores"][0]["category_score"] >= result["category_scores"][1]["category_score"]
