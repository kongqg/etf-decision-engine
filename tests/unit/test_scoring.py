import pandas as pd

from app.services.scoring_service import ScoringService


def test_scoring_prefers_stronger_momentum_and_trend():
    frame = pd.DataFrame(
        [
            {
                "symbol": "A",
                "category": "宽基",
                "asset_class": "股票",
                "momentum_3d": 4.0,
                "momentum_5d": 6.0,
                "momentum_10d": 8.0,
                "trend_strength": 5.0,
                "ma_gap_5": 1.8,
                "ma_gap_10": 2.0,
                "volatility_10d": 2.0,
                "drawdown_20d": -1.0,
                "avg_amount_20d": 100000000,
            },
            {
                "symbol": "B",
                "category": "宽基",
                "asset_class": "股票",
                "momentum_3d": 1.0,
                "momentum_5d": 1.2,
                "momentum_10d": 1.5,
                "trend_strength": 0.8,
                "ma_gap_5": 0.2,
                "ma_gap_10": 0.3,
                "volatility_10d": 3.0,
                "drawdown_20d": -2.5,
                "avg_amount_20d": 80000000,
            },
        ]
    )

    scored = ScoringService().score(frame)

    assert scored.iloc[0]["symbol"] == "A"
    assert scored.iloc[0]["total_score"] > scored.iloc[1]["total_score"]
    assert scored.iloc[0]["rank_in_asset_class"] == 1
