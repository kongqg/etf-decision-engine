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
