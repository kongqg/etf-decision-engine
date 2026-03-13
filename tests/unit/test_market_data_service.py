from __future__ import annotations

from datetime import date

import pandas as pd

from app.services.market_data_service import MarketDataService


def test_load_history_range_uses_local_cache_when_available(tmp_path):
    service = MarketDataService()
    service.history_cache_enabled = True
    service.history_cache_dir = tmp_path

    cache_frame = pd.DataFrame(
        {
            "date": pd.bdate_range(start="2025-01-01", end="2025-01-10"),
            "close": [1.0 + idx * 0.01 for idx in range(8)],
            "amount": [20_000_000 + idx * 10_000 for idx in range(8)],
        }
    )
    cache_frame.to_csv(service._history_cache_path("510300"), index=False)

    service._load_history_from_akshare_range = lambda **kwargs: None

    bundle = service.load_history_range(
        symbol="510300",
        category="宽基",
        min_avg_amount=10_000_000,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 10),
        allow_fallback=False,
    )

    assert bundle["source"] == "akshare_cache"
    assert not bundle["history"].empty
    assert bundle["request_params"]["cache_hit"] is True
