from datetime import datetime, timedelta
from types import SimpleNamespace

from app.services.decision_engine import DecisionEngine


def test_t0_allows_same_day_sell_when_controls_pass():
    engine = DecisionEngine()
    now = datetime(2026, 3, 10, 14, 30, 0)
    route = engine._route_action(
        symbol="518880",
        action_code="sell_exit",
        tradability_mode="t0",
        session_mode="intraday",
        current_time=now,
        trade_context={
            "same_day_buy_symbols": {"518880"},
            "trade_count_by_symbol_today": {"518880": 1},
            "round_trips_by_symbol_today": {"518880": 0},
            "last_trade_by_symbol": {
                "518880": SimpleNamespace(executed_at=now - timedelta(minutes=45), side="buy")
            },
            "days_held_map": {"518880": 1},
        },
        decision_score=70.0,
        entry_score=30.0,
        exit_score=92.0,
    )

    assert route["executable_now"] is True
    assert route["blocked_reason"] == ""


def test_t1_blocks_same_day_sell_after_same_day_buy():
    engine = DecisionEngine()
    route = engine._route_action(
        symbol="510300",
        action_code="sell_exit",
        tradability_mode="t1",
        session_mode="intraday",
        current_time=datetime(2026, 3, 10, 14, 30, 0),
        trade_context={
            "same_day_buy_symbols": {"510300"},
            "trade_count_by_symbol_today": {},
            "round_trips_by_symbol_today": {},
            "last_trade_by_symbol": {},
            "days_held_map": {"510300": 1},
        },
        decision_score=65.0,
        entry_score=20.0,
        exit_score=90.0,
    )

    assert route["executable_now"] is False
    assert route["blocked_reason"] == "planned_exit_next_session_due_to_t1"
