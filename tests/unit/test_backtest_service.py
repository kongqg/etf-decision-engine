from __future__ import annotations

from datetime import date, datetime, timedelta

from app.services.backtest_service import BacktestRequest, BacktestService, SimulatedPosition, SimulatedTrade
from tests.unit.backtest_helpers import DEFAULT_SYMBOLS, build_dataset, seed_user, setup_test_db


TEST_ACTIVE_THRESHOLDS = {
    "fallback": {"offensive_threshold": 0.0},
    "decision_thresholds": {
        "open_threshold": 30.0,
        "strong_entry_threshold": 30.0,
        "strong_hold_threshold": 30.0,
    },
    "t0_controls": {"minimum_expected_edge_bps": 0.0},
}


def test_backtest_runs_day_by_day_and_records_curve_and_trades(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    start_date = date(2026, 2, 3)
    end_date = date(2026, 3, 10)

    with session_local()() as session:
        seed_user(session)
        dataset = build_dataset(session, start_date=start_date, end_date=end_date)
        result = BacktestService().run(
            session,
            BacktestRequest(
                start_date=start_date,
                end_date=end_date,
                initial_capital=100000,
                threshold_overrides=TEST_ACTIVE_THRESHOLDS,
            ),
            dataset=dataset,
            persist_output=False,
        )

        assert len(result["daily_curve"]) == len(dataset["trading_dates"])
        assert result["trades"]
        assert result["metrics"]["trade_count"] == len(result["trades"])
        assert result["metrics"]["final_asset"] > 0
        assert any(item["action_code"] in {"buy_open", "buy_add"} for item in result["signal_log"])


def test_backtest_does_not_peek_future_data(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    start_date = date(2026, 2, 3)
    end_date = date(2026, 3, 10)
    cutoff = date(2026, 2, 24)

    with session_local()() as session:
        seed_user(session)
        normal_dataset = build_dataset(session, start_date=start_date, end_date=end_date, cutoff=cutoff)
        future_crash_dataset = build_dataset(
            session,
            start_date=start_date,
            end_date=end_date,
            variant="future_crash",
            cutoff=cutoff,
        )
        service = BacktestService()
        normal = service.run(
            session,
            BacktestRequest(
                start_date=start_date,
                end_date=end_date,
                initial_capital=100000,
                threshold_overrides=TEST_ACTIVE_THRESHOLDS,
            ),
            dataset=normal_dataset,
            persist_output=False,
        )
        crashed = service.run(
            session,
            BacktestRequest(
                start_date=start_date,
                end_date=end_date,
                initial_capital=100000,
                threshold_overrides=TEST_ACTIVE_THRESHOLDS,
            ),
            dataset=future_crash_dataset,
            persist_output=False,
        )

        normal_prefix = [row for row in normal["daily_curve"] if row["date"] <= cutoff.isoformat()]
        crashed_prefix = [row for row in crashed["daily_curve"] if row["date"] <= cutoff.isoformat()]
        assert [(row["date"], row["action_code"], row["selected_symbol"]) for row in normal_prefix] == [
            (row["date"], row["action_code"], row["selected_symbol"]) for row in crashed_prefix
        ]


def test_threshold_overrides_change_backtest_result(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    start_date = date(2026, 2, 3)
    end_date = date(2026, 3, 10)

    with session_local()() as session:
        seed_user(session)
        dataset = build_dataset(session, start_date=start_date, end_date=end_date)
        service = BacktestService()
        baseline = service.run(
            session,
            BacktestRequest(
                start_date=start_date,
                end_date=end_date,
                initial_capital=100000,
                threshold_overrides=TEST_ACTIVE_THRESHOLDS,
            ),
            dataset=dataset,
            persist_output=False,
        )
        conservative_open = service.run(
            session,
            BacktestRequest(
                    start_date=start_date,
                    end_date=end_date,
                    initial_capital=100000,
                    threshold_overrides={
                        **TEST_ACTIVE_THRESHOLDS,
                        "decision_thresholds": {
                            **TEST_ACTIVE_THRESHOLDS["decision_thresholds"],
                            "open_threshold": 99.0,
                        },
                    },
                ),
                dataset=dataset,
                persist_output=False,
            )

        assert baseline["metrics"]["trade_count"] != conservative_open["metrics"]["trade_count"]


def test_backtest_trade_context_respects_t1_and_t0(monkeypatch):
    setup_test_db(monkeypatch)
    service = BacktestService()
    now = datetime(2026, 3, 10, 14, 30, 0)
    positions = {
        "510300": SimulatedPosition(symbol="510300", name="沪深300ETF", quantity=100.0, avg_cost=1.0, latest_buy_at=now),
        "518880": SimulatedPosition(
            symbol="518880",
            name="黄金ETF",
            quantity=100.0,
            avg_cost=4.0,
            latest_buy_at=now - timedelta(minutes=45),
        ),
    }
    trades = [
        SimulatedTrade(
            executed_at=now,
            symbol="510300",
            name="沪深300ETF",
            side="buy",
            quantity=100.0,
            price=1.02,
            amount=102.0,
            fee=1.0,
            realized_pnl=0.0,
            action_code="buy_open",
            note="same day buy",
        ),
        SimulatedTrade(
            executed_at=now - timedelta(minutes=45),
            symbol="518880",
            name="黄金ETF",
            side="buy",
            quantity=100.0,
            price=4.12,
            amount=412.0,
            fee=1.0,
            realized_pnl=0.0,
            action_code="buy_open",
            note="same day buy",
        ),
    ]
    trade_context = service._simulated_trade_context(trades=trades, positions=positions, current_time=now)

    t1_route = service.decision_engine._route_action(
        symbol="510300",
        action_code="sell_exit",
        tradability_mode="t1",
        session_mode="intraday",
        current_time=now,
        trade_context=trade_context,
        decision_score=70.0,
        entry_score=20.0,
        exit_score=90.0,
    )
    t0_route = service.decision_engine._route_action(
        symbol="518880",
        action_code="sell_exit",
        tradability_mode="t0",
        session_mode="intraday",
        current_time=now,
        trade_context=trade_context,
        decision_score=70.0,
        entry_score=20.0,
        exit_score=92.0,
    )

    assert t1_route["executable_now"] is False
    assert t1_route["blocked_reason"] == "planned_exit_next_session_due_to_t1"
    assert t0_route["executable_now"] is True


def test_prepare_dataset_raises_when_history_is_too_short(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    start_date = date(2026, 2, 3)
    end_date = date(2026, 3, 10)

    def short_history(**kwargs):
        import pandas as pd

        dates = pd.bdate_range(end=end_date, periods=5)
        return {
            "history": pd.DataFrame(
                {
                    "date": dates,
                    "close": [1.0, 1.01, 1.02, 1.03, 1.04],
                    "amount": [100000000.0] * 5,
                }
            ),
            "source": "akshare",
            "request_params": kwargs,
        }

    with session_local()() as session:
        seed_user(session)
        service = BacktestService()
        monkeypatch.setattr(service.market_data_service, "load_history_range", short_history)
        try:
            service.prepare_dataset(session, start_date=start_date, end_date=end_date, symbols=DEFAULT_SYMBOLS)
        except ValueError as exc:
            assert "预热" in str(exc)
        else:
            raise AssertionError("expected insufficient history error")


def test_backtest_execution_cost_reduces_final_asset(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    start_date = date(2026, 2, 3)
    end_date = date(2026, 3, 10)

    with session_local()() as session:
        seed_user(session)
        dataset = build_dataset(session, start_date=start_date, end_date=end_date)
        service = BacktestService()
        no_cost = service.run(
            session,
            BacktestRequest(
                start_date=start_date,
                end_date=end_date,
                initial_capital=100000,
                threshold_overrides=TEST_ACTIVE_THRESHOLDS,
                execution_cost_bps_override=0.0,
            ),
            dataset=dataset,
            persist_output=False,
        )
        high_cost = service.run(
            session,
            BacktestRequest(
                start_date=start_date,
                end_date=end_date,
                initial_capital=100000,
                threshold_overrides=TEST_ACTIVE_THRESHOLDS,
                execution_cost_bps_override=50.0,
            ),
            dataset=dataset,
            persist_output=False,
        )

        assert high_cost["metrics"]["total_execution_cost"] > no_cost["metrics"]["total_execution_cost"]
        assert high_cost["metrics"]["final_asset"] < no_cost["metrics"]["final_asset"]


def test_backtest_relaxes_core_fallback_gate_without_changing_live_rules(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    start_date = date(2026, 2, 3)
    end_date = date(2026, 3, 10)

    with session_local()() as session:
        seed_user(session)
        dataset = build_dataset(session, start_date=start_date, end_date=end_date)
        dataset["history_by_symbol"]["511990"]["source"] = "fallback"

        result = BacktestService().run(
            session,
            BacktestRequest(
                start_date=start_date,
                end_date=end_date,
                initial_capital=100000,
                threshold_overrides=TEST_ACTIVE_THRESHOLDS,
            ),
            dataset=dataset,
            persist_output=False,
        )

        assert all(row["quality_status"] != "blocked" for row in result["daily_curve"])
        assert any(row["quality_status"] == "weak" for row in result["daily_curve"])
        assert not any(item["reason"] == "data_quality_not_ready" for item in result["skipped_actions"])
        assert result["quality_overview"]["label"] == "需谨慎"
        assert result["quality_overview"]["formal_ready_ratio"] > 0


def test_backtest_parking_ignores_cost_gate(monkeypatch):
    setup_test_db(monkeypatch)
    service = BacktestService()
    item = {
        "symbol": "511990",
        "name": "华宝添益",
        "action_code": "park_in_money_etf",
        "action_reason": "进攻边际不足，新增可用仓位先停泊到货币ETF。",
        "suggested_amount": 100000.0,
        "min_order_amount": 10000.0,
        "min_advice_amount": 100.0,
        "executable_now": False,
        "blocked_reason": "信号虽达标，但扣除统一交易成本后优势不足，暂不操作。",
        "recommendation_bucket": "watchlist_recommendations",
    }

    service._relax_transition_item_for_backtest(
        item=item,
        available_cash=100000.0,
        request=BacktestRequest(
            start_date=date(2026, 2, 3),
            end_date=date(2026, 3, 10),
            initial_capital=100000.0,
        ),
    )

    assert item["executable_now"] is True
    assert item["blocked_reason"] == ""
    assert item["recommendation_bucket"] == "executable_recommendations"


def test_backtest_trade_context_only_tracks_same_day_for_t0_flip(monkeypatch):
    setup_test_db(monkeypatch)
    service = BacktestService()
    now = datetime(2026, 3, 10, 14, 30, 0)
    yesterday = now - timedelta(days=1)
    positions = {
        "518880": SimulatedPosition(
            symbol="518880",
            name="黄金ETF",
            quantity=100.0,
            avg_cost=4.0,
            latest_buy_at=yesterday,
        ),
    }
    trades = [
        SimulatedTrade(
            executed_at=yesterday,
            symbol="518880",
            name="黄金ETF",
            side="buy",
            quantity=100.0,
            price=4.12,
            amount=412.0,
            fee=1.0,
            realized_pnl=0.0,
            action_code="buy_open",
            note="previous day buy",
        )
    ]
    trade_context = service._simulated_trade_context(trades=trades, positions=positions, current_time=now)
    route = service.decision_engine._route_action(
        symbol="518880",
        action_code="sell_exit",
        tradability_mode="t0",
        session_mode="intraday",
        current_time=now,
        trade_context=trade_context,
        decision_score=70.0,
        entry_score=20.0,
        exit_score=92.0,
        thresholds=service._prepare_backtest_thresholds(TEST_ACTIVE_THRESHOLDS),
    )

    assert trade_context["last_trade_by_symbol"] == {}
    assert route["executable_now"] is True


def test_backtest_uses_affordable_substitute_when_primary_buy_is_unaffordable(monkeypatch):
    setup_test_db(monkeypatch)
    service = BacktestService()
    primary_item = {
        "symbol": "511260",
        "name": "十年国债ETF",
        "action_code": "buy_open",
        "blocked_reason": "当前建议金额只有 5000.00 元，至少需要 13440.40 元才能覆盖最小建议金额或一手门槛。",
        "executable_now": False,
        "decision_score": 60.0,
        "category_score": 57.0,
    }
    plan = {
        "primary_item": primary_item,
        "action_code": "buy_open",
        "summary_text": "债券ETF领先，准备开仓。",
        "winning_category": "bond_etf",
        "reason_code": "category_first_selection",
        "mapped_horizon_profile": "",
        "lifecycle_phase": "",
        "transition_plan": [primary_item],
        "recommendation_groups": {
            "executable_recommendations": [],
            "affordable_but_weak_recommendations": [
                {
                    "symbol": "518880",
                    "name": "黄金ETF",
                    "category": "gold_etf",
                    "asset_class": "黄金ETF",
                    "action_code": "no_trade",
                    "decision_score": 55.0,
                    "category_score": 58.0,
                    "suggested_amount": 1100.0,
                    "suggested_pct": 0.011,
                    "min_order_amount": 1100.0,
                    "min_advice_amount": 100.0,
                    "current_weight": 0.0,
                    "current_amount": 0.0,
                    "lot_size": 100.0,
                    "latest_price": 11.0,
                    "expected_edge_before_cost": 0.0,
                    "expected_edge_after_cost": -5.0,
                    "executable_now": False,
                    "blocked_reason": "",
                    "action_reason": "买得起但当前不建议买。",
                }
            ],
        },
        "facts": {},
    }
    plan_result = {"plan": plan}

    service._apply_backtest_plan_overrides(
        plan_result=plan_result,
        available_cash=2000.0,
        total_asset=2000.0,
        request=BacktestRequest(
            start_date=date(2026, 2, 3),
            end_date=date(2026, 3, 10),
            initial_capital=2000.0,
        ),
    )

    substitute = plan_result["plan"]["primary_item"]
    assert substitute["symbol"] == "518880"
    assert substitute["executable_now"] is True
    assert substitute["action_code"] == "buy_open"
    assert substitute["backtest_affordable_substitute"] is True
    assert "次优替代" in plan_result["plan"]["summary_text"]
