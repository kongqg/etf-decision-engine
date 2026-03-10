from datetime import date, timedelta

import pytest
import pandas as pd

from app.services.feature_engine import FeatureEngine
from app.utils.maths import safe_pct_change


def test_feature_engine_calculates_shared_base_features():
    start = date(2026, 2, 1)
    history = pd.DataFrame(
        [
            {
                "date": start + timedelta(days=offset),
                "close": 100 + offset,
                "amount": 1000000 + offset * 10000,
            }
            for offset in range(25)
        ]
    )

    payload = FeatureEngine().calculate(history)

    assert payload["momentum_3d"] == pytest.approx(safe_pct_change(124, 121))
    assert payload["momentum_5d"] == pytest.approx(safe_pct_change(124, 119))
    assert payload["momentum_10d"] == pytest.approx(safe_pct_change(124, 114))
    assert payload["momentum_20d"] == pytest.approx(safe_pct_change(124, 104))
    assert payload["ma20"] == pytest.approx(history["close"].tail(20).mean())
    assert payload["rolling_max_20d"] == pytest.approx(history["close"].tail(20).max())
    assert payload["avg_turnover_20d"] == pytest.approx(history["amount"].tail(20).mean())
    assert payload["liquidity_score"] > 0
    assert payload["above_ma20_flag"] is True
