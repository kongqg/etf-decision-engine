from __future__ import annotations

from datetime import date

from app.services.backtest_service import BacktestRequest, BacktestService
from app.services.backtest_runner import BacktestRunner
from app.services.decision_engine import DecisionEngine
from app.services.portfolio_allocator import PortfolioAllocator
from app.services.scoring_engine import ScoringEngine

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


def test_backtest_service_respects_user_category_preferences(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    with session_local()() as session:
        seed_user(session)
        from app.services.user_service import UserService

        UserService().update_preferences(
            session,
            risk_level="中性",
            risk_mode="balanced",
            allow_gold=False,
            allow_bond=True,
            allow_overseas=True,
            min_trade_amount=1000.0,
            max_total_position_pct=0.7,
            max_single_position_pct=0.35,
            cash_reserve_pct=0.2,
        )
        dataset = build_dataset(
            session,
            start_date=date(2026, 1, 5),
            end_date=date(2026, 2, 20),
            symbols=["518880"],
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
                    "selection.max_selected_total": 1,
                    "selection.max_selected_per_category": 1,
                },
            ),
            dataset=dataset,
            persist_output=False,
        )

    assert result["effective_preferences"]["allow_gold"] is False
    assert all(not day["candidate_summary"] for day in result["daily_decisions"])


def test_backtest_service_blocks_formal_decision_when_history_is_fallback(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    with session_local()() as session:
        seed_user(session)
        dataset = build_dataset(
            session,
            start_date=date(2026, 1, 5),
            end_date=date(2026, 2, 20),
        )
        for payload in dataset["history_by_symbol"].values():
            payload["source"] = "fallback"
        result = BacktestService().run(
            session,
            BacktestRequest(
                start_date=date(2026, 1, 5),
                end_date=date(2026, 2, 20),
                initial_capital=100000.0,
                risk_mode="balanced",
                strict_data_quality=True,
            ),
            dataset=dataset,
            persist_output=False,
        )

    assert result["metrics"]["trade_count"] == 0
    assert result["daily_decisions"]
    assert all(day.get("blocked_reason") == "data_quality_not_ready" for day in result["daily_decisions"])


def test_backtest_runner_override_supports_execution_overlay_and_category_heads(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    with session_local()() as session:
        seed_user(session)
        runner = BacktestRunner()
        scoring_engine = ScoringEngine()
        allocator = PortfolioAllocator()
        decision_engine = DecisionEngine()

        runner._apply_overrides(
            scoring_engine=scoring_engine,
            allocator=allocator,
            decision_engine=decision_engine,
            overrides={
                "execution_overlay.breakout_entry_threshold": 68.0,
                "execution_overlay.horizon_buckets.medium.non_held.entry": 0.55,
                "execution_overlay.internals.default_target_holding_days": 45,
                "category_heads.stock_etf.entry.momentum_5d": 0.31,
                "selection.min_final_score_for_target": 61.0,
            },
        )

    assert decision_engine.execution_overlay_service.config.breakout_entry_threshold == 68.0
    assert decision_engine.execution_overlay_service.config.horizon_buckets["medium"]["non_held"]["entry"] == 0.55
    assert decision_engine.execution_overlay_service.config.default_target_holding_days == 45
    assert decision_engine.execution_overlay_service.category_heads["stock_etf"]["entry"]["momentum_5d"] == 0.31
    assert allocator.scoring_config["selection"]["min_final_score_for_target"] == 61.0
