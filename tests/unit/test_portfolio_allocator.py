from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from app.services.portfolio_allocator import PortfolioAllocator


def _preferences():
    return SimpleNamespace(
        max_total_position_pct=0.85,
        max_single_position_pct=0.35,
        cash_reserve_pct=0.0,
    )


def _market_regime():
    return {
        "market_regime": "risk_on",
        "budget_total_pct": 0.80,
        "budget_by_category": {
            "stock_etf": 0.60,
            "bond_etf": 0.30,
        },
    }


def test_allocator_keeps_existing_holding_when_replace_gap_is_not_enough():
    allocator = PortfolioAllocator()
    allocator.constraints["selection"]["max_selected_total"] = 1
    allocator.constraints["selection"]["max_selected_per_category"] = 1
    allocator.constraints["selection"]["hold_guard_global_rank"] = 1
    allocator.constraints["selection"]["hold_guard_category_rank"] = 1
    allocator.constraints["selection"]["replace_threshold"] = 8.0
    allocator.scoring_config["selection"]["min_final_score_for_target"] = 0.0

    scored_df = pd.DataFrame(
        [
            {"symbol": "NEW", "name": "New", "decision_category": "stock_etf", "final_score": 70.0, "intra_score": 70.0, "category_score": 65.0, "global_rank": 1, "category_rank": 1, "filter_pass": True},
            {"symbol": "OLD", "name": "Old", "decision_category": "stock_etf", "final_score": 65.0, "intra_score": 65.0, "category_score": 65.0, "global_rank": 2, "category_rank": 2, "filter_pass": True},
        ]
    )
    current_holdings = [
        {"symbol": "OLD", "name": "Old", "category": "stock_etf", "current_weight": 0.20, "current_amount": 20000.0, "hold_days": 5},
    ]

    allocation = allocator.build_target_portfolio(
        scored_df,
        current_holdings=current_holdings,
        preferences=_preferences(),
        market_regime=_market_regime(),
        risk_mode="balanced",
    )

    assert list(allocation["target_weights"]) == ["OLD"]


def test_allocator_allows_replacement_when_score_gap_is_large_enough():
    allocator = PortfolioAllocator()
    allocator.constraints["selection"]["max_selected_total"] = 1
    allocator.constraints["selection"]["max_selected_per_category"] = 1
    allocator.constraints["selection"]["hold_guard_global_rank"] = 1
    allocator.constraints["selection"]["hold_guard_category_rank"] = 1
    allocator.constraints["selection"]["replace_threshold"] = 8.0
    allocator.scoring_config["selection"]["min_final_score_for_target"] = 0.0

    scored_df = pd.DataFrame(
        [
            {"symbol": "NEW", "name": "New", "decision_category": "stock_etf", "final_score": 78.0, "intra_score": 78.0, "category_score": 68.0, "global_rank": 1, "category_rank": 1, "filter_pass": True},
            {"symbol": "OLD", "name": "Old", "decision_category": "stock_etf", "final_score": 65.0, "intra_score": 65.0, "category_score": 65.0, "global_rank": 2, "category_rank": 2, "filter_pass": True},
        ]
    )
    current_holdings = [
        {"symbol": "OLD", "name": "Old", "category": "stock_etf", "current_weight": 0.20, "current_amount": 20000.0, "hold_days": 5},
    ]

    allocation = allocator.build_target_portfolio(
        scored_df,
        current_holdings=current_holdings,
        preferences=_preferences(),
        market_regime=_market_regime(),
        risk_mode="balanced",
    )

    assert list(allocation["target_weights"]) == ["NEW"]


def test_allocator_does_not_apply_min_hold_guard_when_hold_days_unknown():
    allocator = PortfolioAllocator()
    allocator.constraints["selection"]["max_selected_total"] = 1
    allocator.constraints["selection"]["max_selected_per_category"] = 1
    allocator.constraints["selection"]["hold_guard_global_rank"] = 1
    allocator.constraints["selection"]["hold_guard_category_rank"] = 1
    allocator.constraints["selection"]["replace_threshold"] = 8.0
    allocator.constraints["selection"]["min_hold_days_before_replace"] = 2
    allocator.scoring_config["selection"]["min_final_score_for_target"] = 0.0

    scored_df = pd.DataFrame(
        [
            {"symbol": "NEW", "name": "New", "decision_category": "stock_etf", "final_score": 78.0, "intra_score": 78.0, "category_score": 68.0, "global_rank": 1, "category_rank": 1, "filter_pass": True},
            {"symbol": "OLD", "name": "Old", "decision_category": "stock_etf", "final_score": 65.0, "intra_score": 65.0, "category_score": 65.0, "global_rank": 2, "category_rank": 2, "filter_pass": True},
        ]
    )
    current_holdings = [
        {"symbol": "OLD", "name": "Old", "category": "stock_etf", "current_weight": 0.20, "current_amount": 20000.0, "hold_days": 0, "hold_days_known": False},
    ]

    allocation = allocator.build_target_portfolio(
        scored_df,
        current_holdings=current_holdings,
        preferences=_preferences(),
        market_regime=_market_regime(),
        risk_mode="balanced",
    )

    assert list(allocation["target_weights"]) == ["NEW"]
    assert allocation["replacement_trace"]["NEW"]["hold_days_known"] is False


def test_allocator_skips_execution_blocked_candidate_and_backfills_next_slot():
    allocator = PortfolioAllocator()
    allocator.constraints["selection"]["max_selected_total"] = 1
    allocator.constraints["selection"]["max_selected_per_category"] = 1
    allocator.constraints["selection"]["hold_guard_global_rank"] = 1
    allocator.constraints["selection"]["hold_guard_category_rank"] = 1
    allocator.scoring_config["selection"]["min_final_score_for_target"] = 0.0

    scored_df = pd.DataFrame(
        [
            {"symbol": "A", "name": "A", "decision_category": "stock_etf", "final_score": 82.0, "intra_score": 82.0, "category_score": 70.0, "global_rank": 1, "category_rank": 1, "filter_pass": True},
            {"symbol": "B", "name": "B", "decision_category": "stock_etf", "final_score": 79.0, "intra_score": 79.0, "category_score": 69.0, "global_rank": 2, "category_rank": 2, "filter_pass": True},
        ]
    )

    allocation = allocator.build_target_portfolio(
        scored_df,
        current_holdings=[],
        preferences=_preferences(),
        market_regime=_market_regime(),
        risk_mode="balanced",
        blocked_candidate_reasons={"A": "执行层未通过入场通道，因此不占用正式名额。"},
    )

    assert list(allocation["target_weights"]) == ["B"]
    assert allocation["selection_trace"]["A"]["blocked_stage"] == "execution_gate"
    assert "执行层未通过入场通道" in allocation["selection_trace"]["A"]["blocked_reason"]


def test_allocator_allows_replacement_when_incumbent_is_reduce_and_candidate_entry_allowed():
    allocator = PortfolioAllocator()
    allocator.constraints["selection"]["max_selected_total"] = 1
    allocator.constraints["selection"]["max_selected_per_category"] = 1
    allocator.constraints["selection"]["hold_guard_global_rank"] = 1
    allocator.constraints["selection"]["hold_guard_category_rank"] = 1
    allocator.constraints["selection"]["replace_threshold"] = 10.0
    allocator.constraints["selection"]["min_hold_days_before_replace"] = 5
    allocator.scoring_config["selection"]["min_final_score_for_target"] = 0.0

    scored_df = pd.DataFrame(
        [
            {"symbol": "NEW", "name": "New", "decision_category": "stock_etf", "final_score": 65.9, "intra_score": 73.0, "category_score": 49.0, "global_rank": 1, "category_rank": 1, "filter_pass": True},
            {"symbol": "OLD", "name": "Old", "decision_category": "stock_etf", "final_score": 56.0, "intra_score": 59.0, "category_score": 49.0, "global_rank": 2, "category_rank": 2, "filter_pass": True},
        ]
    )
    current_holdings = [
        {"symbol": "OLD", "name": "Old", "category": "stock_etf", "current_weight": 0.20, "current_amount": 20000.0, "hold_days": 0, "hold_days_known": False},
    ]

    allocation = allocator.build_target_portfolio(
        scored_df,
        current_holdings=current_holdings,
        preferences=_preferences(),
        market_regime=_market_regime(),
        risk_mode="balanced",
        overlay_hints={
            "OLD": {"position_state": "REDUCE", "entry_allowed": False},
            "NEW": {"position_state": "NONE", "entry_allowed": True},
        },
    )

    assert list(allocation["target_weights"]) == ["NEW"]
    assert allocation["replacement_trace"]["NEW"]["replace_allowed"] is True
    assert allocation["replacement_trace"]["NEW"]["state_based_replace_allowed"] is True


def test_allocator_allows_same_category_addition_when_category_slots_remain():
    allocator = PortfolioAllocator()
    allocator.constraints["selection"]["max_selected_total"] = 4
    allocator.constraints["selection"]["max_selected_per_category"] = 3
    allocator.constraints["selection"]["replace_threshold"] = 10.0
    allocator.constraints["selection"]["min_hold_days_before_replace"] = 5
    allocator.scoring_config["selection"]["min_final_score_for_target"] = 0.0

    scored_df = pd.DataFrame(
        [
            {"symbol": "NEW1", "name": "New1", "decision_category": "stock_etf", "final_score": 70.0, "intra_score": 75.0, "category_score": 50.0, "global_rank": 1, "category_rank": 1, "filter_pass": True},
            {"symbol": "NEW2", "name": "New2", "decision_category": "stock_etf", "final_score": 61.4, "intra_score": 66.0, "category_score": 49.0, "global_rank": 2, "category_rank": 2, "filter_pass": True},
            {"symbol": "OLD", "name": "Old", "decision_category": "stock_etf", "final_score": 56.0, "intra_score": 58.0, "category_score": 49.0, "global_rank": 3, "category_rank": 3, "filter_pass": True},
        ]
    )
    current_holdings = [
        {"symbol": "OLD", "name": "Old", "category": "stock_etf", "current_weight": 0.20, "current_amount": 20000.0, "hold_days": 1, "hold_days_known": True},
    ]

    allocation = allocator.build_target_portfolio(
        scored_df,
        current_holdings=current_holdings,
        preferences=_preferences(),
        market_regime=_market_regime(),
        risk_mode="balanced",
    )

    assert {"NEW1", "NEW2"}.issubset(set(allocation["target_weights"]))
    assert allocation["selection_trace"]["NEW2"]["selected"] is True
    assert allocation["selection_trace"]["NEW2"]["selected_reason"] == "同类别仍有新增名额，因此按并存新增进入目标组合候选。"
    assert "NEW2" not in allocation["replacement_trace"]
