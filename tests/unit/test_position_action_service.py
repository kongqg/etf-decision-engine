from datetime import datetime, timedelta
from copy import deepcopy
from types import SimpleNamespace

from app.services.position_action_service import PositionActionService


def _preferences(min_trade_amount: float = 100.0):
    return SimpleNamespace(min_trade_amount=min_trade_amount)


def _row(**overrides):
    payload = {
        "symbol": "510300",
        "decision_score": 68.0,
        "entry_score": 70.0,
        "hold_score": 62.0,
        "exit_score": 24.0,
        "decision_category": "stock_etf",
        "close_price": 4.0,
        "lot_size": 100,
        "rank_drop": 0,
        "days_held": 5,
    }
    payload.update(overrides)
    return payload


def _position(weight_pct: float = 0.12, market_value: float = 12000.0, **overrides):
    payload = {
        "weight_pct": weight_pct,
        "market_value": market_value,
        "avg_cost": 4.0,
        "quantity": market_value / 4.0,
        "unrealized_pnl": 400.0,
    }
    payload.update(overrides)
    return payload


def _trade_context(last_trade_by_symbol=None):
    return {
        "same_day_buy_symbols": set(),
        "trade_count_by_symbol_today": {},
        "round_trips_by_symbol_today": {},
        "last_trade_by_symbol": last_trade_by_symbol or {},
        "days_held_map": {},
    }


def test_held_symbol_stays_hold_when_hold_score_strong_and_exit_score_low():
    service = PositionActionService()

    result = service.decide(
        row=_row(),
        position=_position(),
        preferences=_preferences(),
        total_asset=100000.0,
        available_cash=20000.0,
        current_position_pct=0.12,
        target_position_pct=0.5,
        target_weight=0.12,
        selected_category="stock_etf",
        offensive_edge=True,
        fallback_action="no_trade",
        trade_context=_trade_context(),
        current_time=datetime(2026, 3, 10, 10, 0, 0),
    )

    assert result["action_code"] == "hold"
    assert result["position_action"] == "hold_position"


def test_held_symbol_becomes_reduce_when_target_weight_drops_and_exit_pressure_rises():
    service = PositionActionService()

    result = service.decide(
        row=_row(exit_score=64.0, hold_score=44.0, rank_drop=1, days_held=6),
        position=_position(weight_pct=0.20, market_value=20000.0),
        preferences=_preferences(),
        total_asset=100000.0,
        available_cash=5000.0,
        current_position_pct=0.20,
        target_position_pct=0.5,
        target_weight=0.08,
        selected_category="stock_etf",
        offensive_edge=True,
        fallback_action="no_trade",
        trade_context=_trade_context(),
        current_time=datetime(2026, 3, 10, 10, 0, 0),
    )

    assert result["action_code"] == "reduce"
    assert result["position_action"] == "reduce_position"
    assert result["delta_weight"] < 0


def test_held_symbol_becomes_exit_when_exit_score_clearly_dominates():
    service = PositionActionService()

    result = service.decide(
        row=_row(exit_score=82.0, hold_score=35.0, decision_score=38.0, rank_drop=3),
        position=_position(weight_pct=0.18, market_value=18000.0),
        preferences=_preferences(),
        total_asset=100000.0,
        available_cash=5000.0,
        current_position_pct=0.18,
        target_position_pct=0.4,
        target_weight=0.0,
        selected_category="stock_etf",
        offensive_edge=False,
        fallback_action="park_in_money_etf",
        trade_context=_trade_context(),
        current_time=datetime(2026, 3, 10, 10, 0, 0),
    )

    assert result["action_code"] == "sell_exit"
    assert result["position_action"] == "exit_position"


def test_non_held_symbol_opens_when_category_and_thresholds_pass():
    service = PositionActionService()

    result = service.decide(
        row=_row(),
        position=None,
        preferences=_preferences(),
        total_asset=100000.0,
        available_cash=30000.0,
        current_position_pct=0.10,
        target_position_pct=0.50,
        target_weight=0.15,
        selected_category="stock_etf",
        offensive_edge=True,
        fallback_action="no_trade",
        trade_context=_trade_context(),
        current_time=datetime(2026, 3, 10, 10, 0, 0),
    )

    assert result["action_code"] == "buy_open"
    assert result["position_action"] == "open_position"


def test_weak_offensive_environment_falls_back_to_money_etf():
    service = PositionActionService()
    defensive_category = service.policy.defensive_category()

    result = service.decide(
        row=_row(symbol="511990", decision_category=defensive_category, entry_score=45.0, decision_score=52.0),
        position=None,
        preferences=_preferences(),
        total_asset=100000.0,
        available_cash=25000.0,
        current_position_pct=0.20,
        target_position_pct=0.20,
        target_weight=0.18,
        selected_category="stock_etf",
        offensive_edge=False,
        fallback_action="park_in_money_etf",
        trade_context=_trade_context(),
        current_time=datetime(2026, 3, 10, 10, 0, 0),
    )

    assert result["action_code"] == "park_in_money_etf"
    assert result["position_action"] == "park_in_money_etf"


def test_tiny_delta_does_not_trigger_noisy_rebalance():
    service = PositionActionService()

    result = service.decide(
        row=_row(exit_score=18.0, hold_score=66.0),
        position=_position(weight_pct=0.12, market_value=12000.0),
        preferences=_preferences(),
        total_asset=100000.0,
        available_cash=20000.0,
        current_position_pct=0.12,
        target_position_pct=0.40,
        target_weight=0.125,
        selected_category="stock_etf",
        offensive_edge=True,
        fallback_action="no_trade",
        trade_context=_trade_context(),
        current_time=datetime(2026, 3, 10, 10, 0, 0),
    )

    assert result["action_code"] == "hold"
    assert result["position_action"] == "hold_position"
    assert abs(result["delta_weight"]) < 0.01


def test_recent_exit_requires_stronger_signal_before_reopening():
    service = PositionActionService()
    current_time = datetime(2026, 3, 10, 10, 0, 0)

    result = service.decide(
        row=_row(decision_score=59.0, entry_score=64.0),
        position=None,
        preferences=_preferences(),
        total_asset=100000.0,
        available_cash=30000.0,
        current_position_pct=0.10,
        target_position_pct=0.50,
        target_weight=0.12,
        selected_category="stock_etf",
        offensive_edge=True,
        fallback_action="no_trade",
        trade_context=_trade_context(
            last_trade_by_symbol={
                "510300": SimpleNamespace(side="sell", executed_at=current_time - timedelta(days=1))
            }
        ),
        current_time=current_time,
    )

    assert result["action_code"] == "no_trade"
    assert result["position_action"] == "no_trade"


def test_threshold_override_can_loosen_open_signal():
    service = PositionActionService()
    thresholds = deepcopy(service.rules)
    thresholds["decision_thresholds"]["open_threshold"] = 56.0

    baseline = service.decide(
        row=_row(decision_score=57.0),
        position=None,
        preferences=_preferences(),
        total_asset=100000.0,
        available_cash=30000.0,
        current_position_pct=0.10,
        target_position_pct=0.50,
        target_weight=0.15,
        selected_category="stock_etf",
        offensive_edge=True,
        fallback_action="no_trade",
        trade_context=_trade_context(),
        current_time=datetime(2026, 3, 10, 10, 0, 0),
    )
    overridden = service.decide(
        row=_row(decision_score=57.0),
        position=None,
        preferences=_preferences(),
        total_asset=100000.0,
        available_cash=30000.0,
        current_position_pct=0.10,
        target_position_pct=0.50,
        target_weight=0.15,
        selected_category="stock_etf",
        offensive_edge=True,
        fallback_action="no_trade",
        trade_context=_trade_context(),
        current_time=datetime(2026, 3, 10, 10, 0, 0),
        thresholds=thresholds,
    )

    assert baseline["action_code"] == "no_trade"
    assert overridden["action_code"] == "buy_open"
