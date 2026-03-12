from __future__ import annotations

from datetime import date

from app.services.backtest_service import BacktestRequest, BacktestService

from tests.unit.backtest_helpers import build_dataset, seed_user, setup_test_db


def test_backtest_service_runs_minimal_score_based_backtest(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    with session_local()() as session:
        seed_user(session)
        dataset = build_dataset(
            session,
            start_date=date(2026, 1, 5),
            end_date=date(2026, 2, 20),
        )
        result = BacktestService().run(
            session,
            BacktestRequest(
                start_date=date(2026, 1, 5),
                end_date=date(2026, 2, 20),
                initial_capital=100000.0,
                risk_mode="balanced",
                config_overrides={
                    "selection.min_final_score_for_target": 0.0,
                    "selection.max_selected_total": 2,
                    "selection.max_selected_per_category": 1,
                },
            ),
            dataset=dataset,
            persist_output=False,
        )

    metrics = result["metrics"]
    assert result["run_type"] == "backtest"
    assert len(result["daily_curve"]) > 0
    assert len(result["daily_decisions"]) > 0
    assert metrics["trade_count"] >= 1
    assert metrics["open_count"] >= 1
    assert metrics["open_count"] + metrics["add_count"] + metrics["reduce_count"] + metrics["exit_count"] <= metrics["trade_count"]
