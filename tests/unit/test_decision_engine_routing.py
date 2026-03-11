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


def test_affordable_but_weak_recommendations_stay_visible_for_small_account():
    engine = DecisionEngine()
    items = engine._build_affordable_but_weak_recommendations(
        candidate_items=[
            {
                "symbol": "518880",
                "name": "黄金ETF",
                "category": "gold_etf",
                "asset_class": "黄金ETF",
                "action_code": "no_trade",
                "category_score": 52.0,
                "decision_score": 40.0,
                "score": 40.0,
                "min_order_amount": 1102.4,
                "entry_eligible": True,
                "filter_pass": True,
                "is_current_holding": False,
            },
            {
                "symbol": "511990",
                "name": "华宝添益",
                "category": "money_etf",
                "asset_class": "货币ETF",
                "action_code": "park_in_money_etf",
                "category_score": 46.0,
                "decision_score": 40.0,
                "score": 40.0,
                "min_order_amount": 9999.4,
                "entry_eligible": True,
                "filter_pass": True,
                "is_current_holding": False,
            },
            {
                "symbol": "510300",
                "name": "沪深300ETF",
                "category": "stock_etf",
                "asset_class": "股票ETF",
                "action_code": "hold",
                "category_score": 12.0,
                "decision_score": 40.0,
                "score": 40.0,
                "min_order_amount": 468.9,
                "entry_eligible": True,
                "filter_pass": True,
                "is_current_holding": True,
            },
        ],
        excluded_symbols={"511990"},
        total_asset=1996.0,
        available_cash=1527.1,
        offensive_edge=False,
        selected_category="",
        selected_category_label="",
    )

    assert [item["symbol"] for item in items] == ["518880"]
    assert items[0]["recommendation_bucket"] == "affordable_but_weak_recommendations"
    assert items[0]["execution_status"] == "买得起但当前不建议买"
    assert items[0]["is_affordable_but_weak"] is True
    assert items[0]["suggested_amount"] == 1102.4
    assert "类别分 52.0 还没达到出手阈值 55.0" in items[0]["weak_signal_reason"]
