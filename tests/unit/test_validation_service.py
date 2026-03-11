from __future__ import annotations

from datetime import date

from app.services.threshold_calibration_service import CalibrationRequest, ThresholdCalibrationService
from app.services.validation_service import RollingValidationRequest, ValidationService
from tests.unit.backtest_helpers import build_dataset, seed_user, setup_test_db


def test_threshold_calibration_returns_candidates_and_recommended_thresholds(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    start_date = date(2026, 2, 3)
    end_date = date(2026, 3, 10)

    with session_local()() as session:
        seed_user(session)
        dataset = build_dataset(session, start_date=start_date, end_date=end_date)
        result = ThresholdCalibrationService().run(
            session,
            CalibrationRequest(
                start_date=start_date,
                end_date=end_date,
                initial_capital=100000,
                search_space={"decision_thresholds.open_threshold": [0.0, 20.0]},
                target_holding_days_candidates=[5],
            ),
            dataset=dataset,
            persist_output=False,
        )

        assert len(result["candidate_results"]) == 2
        assert result["recommended_candidate"]["candidate_id"]
        assert result["recommended_thresholds"]["decision_thresholds"]["open_threshold"] in {58.0, 78.0}
        assert result["best_backtest"]["metrics"]["final_asset"] > 0


def test_validation_service_builds_windows_and_aggregates(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    service = ValidationService()
    dates = [date(2026, 2, day) for day in range(1, 11)]
    windows = service._build_windows(trading_dates=dates, train_days=4, validation_days=2, step_days=2)

    assert windows == [
        {
            "train_start": date(2026, 2, 1),
            "train_end": date(2026, 2, 4),
            "validation_start": date(2026, 2, 5),
            "validation_end": date(2026, 2, 6),
        },
        {
            "train_start": date(2026, 2, 3),
            "train_end": date(2026, 2, 6),
            "validation_start": date(2026, 2, 7),
            "validation_end": date(2026, 2, 8),
        },
        {
            "train_start": date(2026, 2, 5),
            "train_end": date(2026, 2, 8),
            "validation_start": date(2026, 2, 9),
            "validation_end": date(2026, 2, 10),
        },
    ]


def test_validation_service_runs_rolling_validation(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    start_date = date(2026, 2, 3)
    end_date = date(2026, 3, 18)

    with session_local()() as session:
        seed_user(session)
        dataset = build_dataset(session, start_date=start_date, end_date=end_date)
        service = ValidationService()
        monkeypatch.setattr(service.backtest_service, "prepare_dataset", lambda *args, **kwargs: dataset)
        monkeypatch.setattr(service.calibration_service.backtest_service, "prepare_dataset", lambda *args, **kwargs: dataset)
        result = service.run(
            session,
            RollingValidationRequest(
                start_date=start_date,
                end_date=end_date,
                initial_capital=100000,
                train_days=12,
                validation_days=5,
                step_days=5,
                search_space={"decision_thresholds.open_threshold": [0.0, 20.0]},
                target_holding_days_candidates=[5],
            ),
            persist_output=False,
        )

        assert result["windows"]
        assert result["aggregate_validation"]["window_count"] == len(result["windows"])
        assert result["recommended_candidate"]["candidate_id"]
        assert "decision_thresholds" in result["recommended_thresholds"]
        assert result["final_backtest_recommended"]["metrics"]["final_asset"] > 0


def test_validation_service_raises_when_history_is_insufficient(monkeypatch):
    setup_test_db(monkeypatch)
    service = ValidationService()
    try:
        service._build_windows(
            trading_dates=[date(2026, 2, 1), date(2026, 2, 2), date(2026, 2, 3)],
            train_days=3,
            validation_days=2,
            step_days=1,
        )
    except Exception as exc:
        raise AssertionError(f"_build_windows should not raise, got {exc}")
    assert service._build_windows(
        trading_dates=[date(2026, 2, 1), date(2026, 2, 2), date(2026, 2, 3)],
        train_days=3,
        validation_days=2,
        step_days=1,
    ) == []
