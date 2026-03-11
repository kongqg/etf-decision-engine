from datetime import date

import pandas as pd

from app.services.data_quality_service import DataQualityService


def _history(end_date: date, *, periods: int = 40, stale_shift: int = 0, amount: float = 100000000.0) -> pd.DataFrame:
    dates = pd.bdate_range(end=pd.Timestamp(end_date) - pd.offsets.BDay(stale_shift), periods=periods)
    return pd.DataFrame(
        {
            "date": dates,
            "close": [100 + index for index in range(len(dates))],
            "amount": [amount for _ in range(len(dates))],
        }
    )


def test_assess_history_marks_stale_and_keeps_real_latest_date():
    service = DataQualityService()

    assessment = service.assess_history(
        symbol="510300",
        name="沪深300ETF",
        source="akshare",
        history=_history(date(2026, 3, 11), stale_shift=1),
        requested_trade_date=date(2026, 3, 11),
        min_avg_amount=10000000.0,
        anomaly_pct_change_threshold=8.0,
    )

    assert assessment.latest_row_date == date(2026, 3, 10)
    assert assessment.payload["stale_data_flag"] is True
    assert assessment.payload["requested_trade_date"] == "2026-03-11"
    assert assessment.payload["latest_row_date"] == "2026-03-10"
    assert assessment.payload["formal_eligible"] is False


def test_build_summary_blocks_when_critical_symbol_is_stale():
    service = DataQualityService()
    reports = [
        {
            "symbol": "510300",
            "source": "akshare",
            "latest_row_date": "2026-03-10",
            "stale_data_flag": True,
            "formal_eligible": False,
            "status": "partial",
            "checks": [],
            "failed_checks": 1,
        },
        {
            "symbol": "511990",
            "source": "akshare",
            "latest_row_date": "2026-03-11",
            "stale_data_flag": False,
            "formal_eligible": True,
            "status": "pass",
            "checks": [],
            "failed_checks": 0,
        },
    ]

    summary = service.build_summary(
        quality_reports=reports,
        expected_trade_date=date(2026, 3, 11),
        current_time=pd.Timestamp("2026-03-11T10:00:00"),
        session_mode="intraday",
    )

    assert summary["quality_status"] == "blocked"
    assert summary["formal_decision_ready"] is False
    assert "510300" in summary["missing_core_symbols"]
    assert summary["stale_ratio"] > 0
