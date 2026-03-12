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
