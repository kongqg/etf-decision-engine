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


def test_execution_cost_blocks_order_when_after_cost_edge_is_negative():
    engine = DecisionEngine()
    payload = engine._compose_item_payload(
        row={
            "symbol": "510300",
            "name": "沪深300ETF",
            "rank_in_category": 1,
            "close_price": 1.2,
            "lot_size": 100.0,
            "fee_rate": 0.0003,
            "min_fee": 1.0,
            "decision_score": 60.0,
            "entry_score": 70.0,
            "hold_score": 50.0,
            "exit_score": 20.0,
            "category_score": 66.0,
            "score_gap": 0.0,
            "risk_level": "中",
            "decision_category": "stock_etf",
            "tradability_mode": "t1",
            "target_holding_days": 5,
            "mapped_horizon_profile": "swing",
            "lifecycle_phase": "build_phase",
            "breakdown_json": "{}",
            "filter_pass": True,
        },
        position=None,
        action_payload={
            "action_code": "buy_open",
            "position_action": "buy_open",
            "position_action_label": "开仓买入",
            "action_reason": "测试开仓",
            "suggested_amount": 2000.0,
            "suggested_pct": 0.2,
            "min_order_amount": 120.0,
            "current_weight": 0.0,
            "target_weight": 0.2,
            "delta_weight": 0.2,
            "current_amount": 0.0,
            "target_amount": 2000.0,
        },
        route_payload={
            "requires_order": True,
            "executable_now": True,
            "blocked_reason": "",
            "planned_exit_days": None,
            "planned_exit_rule_summary": "",
            "edge_bps": 4.0,
        },
        available_cash=5000.0,
        min_trade_amount=100.0,
        thresholds={
            "decision_thresholds": {"open_threshold": 58.0},
            "t0_controls": {"score_to_edge_bps_multiplier": 2.0},
        },
    )

    assert payload["expected_edge_before_cost"] == 4.0
    assert payload["expected_edge_after_cost"] == -1.0
    assert payload["estimated_execution_cost"] == 1.0
    assert payload["execution_cost_bps"] == 5.0
    assert payload["executable_now"] is False
    assert "扣除统一交易成本后优势不足" in payload["blocked_reason"]


def test_execution_cost_keeps_order_executable_when_edge_remains_positive():
    engine = DecisionEngine()
    payload = engine._compose_item_payload(
        row={
            "symbol": "518880",
            "name": "黄金ETF",
            "rank_in_category": 1,
            "close_price": 4.5,
            "lot_size": 100.0,
            "fee_rate": 0.0003,
            "min_fee": 1.0,
            "decision_score": 70.0,
            "entry_score": 82.0,
            "hold_score": 60.0,
            "exit_score": 20.0,
            "category_score": 68.0,
            "score_gap": 0.0,
            "risk_level": "中",
            "decision_category": "gold_etf",
            "tradability_mode": "t0",
            "target_holding_days": 5,
            "mapped_horizon_profile": "swing",
            "lifecycle_phase": "build_phase",
            "breakdown_json": "{}",
            "filter_pass": True,
        },
        position=None,
        action_payload={
            "action_code": "buy_open",
            "position_action": "buy_open",
            "position_action_label": "开仓买入",
            "action_reason": "测试开仓",
            "suggested_amount": 3000.0,
            "suggested_pct": 0.3,
            "min_order_amount": 450.0,
            "current_weight": 0.0,
            "target_weight": 0.3,
            "delta_weight": 0.3,
            "current_amount": 0.0,
            "target_amount": 3000.0,
        },
        route_payload={
            "requires_order": True,
            "executable_now": True,
            "blocked_reason": "",
            "planned_exit_days": None,
            "planned_exit_rule_summary": "",
            "edge_bps": 24.0,
        },
        available_cash=5000.0,
        min_trade_amount=100.0,
        thresholds={
            "decision_thresholds": {"open_threshold": 58.0},
            "t0_controls": {"score_to_edge_bps_multiplier": 2.0},
        },
    )

    assert payload["expected_edge_before_cost"] == 24.0
    assert payload["expected_edge_after_cost"] == 19.0
    assert payload["executable_now"] is True
    assert payload["blocked_reason"] == ""


def test_execution_cost_does_not_block_sell_exit_when_route_is_executable():
    engine = DecisionEngine()
    payload = engine._compose_item_payload(
        row={
            "symbol": "511990",
            "name": "华宝添益",
            "rank_in_category": 1,
            "close_price": 100.0,
            "lot_size": 100.0,
            "fee_rate": 0.0002,
            "min_fee": 1.0,
            "decision_score": -5.0,
            "entry_score": 50.0,
            "hold_score": 50.0,
            "exit_score": 50.0,
            "category_score": 46.0,
            "score_gap": 0.0,
            "risk_level": "低",
            "decision_category": "money_etf",
            "tradability_mode": "t0",
            "target_holding_days": 30,
            "mapped_horizon_profile": "rotation",
            "lifecycle_phase": "exit_phase",
            "breakdown_json": "{}",
            "filter_pass": True,
        },
        position={
            "weight_pct": 0.9,
            "market_value": 90000.0,
            "avg_cost": 100.0,
            "quantity": 900.0,
            "unrealized_pnl": -70.0,
        },
        action_payload={
            "action_code": "sell_exit",
            "position_action": "sell_exit",
            "position_action_label": "卖出退出",
            "action_reason": "释放停车资金",
            "suggested_amount": 90000.0,
            "suggested_pct": 0.9,
            "min_order_amount": 10000.0,
            "current_weight": 0.9,
            "target_weight": 0.0,
            "delta_weight": -0.9,
            "current_amount": 90000.0,
            "target_amount": 0.0,
        },
        route_payload={
            "requires_order": True,
            "executable_now": True,
            "blocked_reason": "",
            "planned_exit_days": None,
            "planned_exit_rule_summary": "",
            "edge_bps": 0.0,
        },
        available_cash=10000.0,
        min_trade_amount=100.0,
        thresholds={
            "decision_thresholds": {"full_exit_threshold": 72.0, "reduce_threshold": 58.0},
            "t0_controls": {"score_to_edge_bps_multiplier": 2.0},
        },
    )

    assert payload["expected_edge_after_cost"] < 0.0
    assert payload["executable_now"] is True
    assert payload["blocked_reason"] == ""


def test_defensive_parking_stays_hold_without_executable_offensive_replacement():
    engine = DecisionEngine()
    planned_rows = [
        {
            "symbol": "511990",
            "name": "华宝添益",
            "category": "money_etf",
            "action": "卖出退出",
            "action_code": "sell_exit",
            "position_action": "exit_position",
            "position_action_label": "卖出退出",
            "action_reason": "释放停车资金",
            "decision_score": -5.0,
            "entry_score": 50.0,
            "hold_score": 50.0,
            "exit_score": 50.0,
            "execution_cost_bps": 5.0,
            "current_weight": 0.9,
            "target_weight": 0.0,
            "is_current_holding": True,
            "executable_now": True,
            "is_executable": True,
            "blocked_reason": "",
            "execution_status": "可执行卖出",
            "execution_note": "释放停车资金",
            "recommendation_bucket": "watchlist_recommendations",
        },
        {
            "symbol": "518880",
            "name": "黄金ETF",
            "category": "gold_etf",
            "action": "暂不交易",
            "action_code": "no_trade",
            "position_action": "no_trade",
            "position_action_label": "暂不交易",
            "decision_score": 53.3,
            "entry_score": 87.0,
            "hold_score": 67.0,
            "exit_score": 46.25,
            "current_weight": 0.0,
            "target_weight": 0.35,
            "is_current_holding": False,
            "executable_now": False,
            "is_executable": False,
            "blocked_reason": "",
            "recommendation_bucket": "watchlist_recommendations",
        },
    ]

    stabilized = engine._stabilize_defensive_parking_rows(
        planned_rows=planned_rows,
        selected_category="gold_etf",
        offensive_edge=True,
        fallback_action="park_in_money_etf",
    )

    money_row = stabilized[0]
    assert money_row["action_code"] == "hold"
    assert money_row["position_action"] == "hold_position"
    assert "今天还没有形成可执行的新开仓" in money_row["action_reason"]
