from types import SimpleNamespace

import pandas as pd

from app.services.allocation_service import AllocationService


def build_preferences(min_trade_amount: float = 100.0):
    return SimpleNamespace(
        risk_level="中性",
        max_total_position_pct=0.7,
        max_single_position_pct=0.35,
        cash_reserve_pct=0.2,
        min_trade_amount=min_trade_amount,
    )


def build_scored_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_buy_candidate_with_enough_budget_stays_executable():
    service = AllocationService()
    scored_df = build_scored_df(
        [
            {
                "symbol": "510300",
                "name": "沪深300ETF",
                "close_price": 1.2,
                "total_score": 88.0,
            }
        ]
    )

    plan = service.plan(
        scored_df=scored_df,
        positions_df=pd.DataFrame(),
        total_asset=10000.0,
        available_cash=5000.0,
        current_position_pct=0.0,
        preferences=build_preferences(),
        market_regime="中性",
    )

    assert plan["action"] == "买入"
    assert plan["reason_code"] == "buy_candidates"
    assert len(plan["items"]) == 1
    assert not plan["watchlist_items"]
    assert plan["items"][0]["symbol"] == "510300"
    assert plan["items"][0]["is_executable"] is True
    assert plan["items"][0]["min_order_amount"] == 120.0


def test_high_score_but_cash_can_cover_one_lot_becomes_executable_fallback():
    service = AllocationService()
    scored_df = build_scored_df(
        [
            {
                "symbol": "512100",
                "name": "中证1000ETF",
                "close_price": 5.0,
                "total_score": 90.0,
            }
        ]
    )

    plan = service.plan(
        scored_df=scored_df,
        positions_df=pd.DataFrame(),
        total_asset=1000.0,
        available_cash=1000.0,
        current_position_pct=0.0,
        preferences=build_preferences(),
        market_regime="中性",
    )

    assert plan["action"] == "买入"
    assert plan["reason_code"] == "buy_candidates_one_lot_override"
    assert [item["symbol"] for item in plan["items"]] == ["512100"]
    assert plan["items"][0]["is_executable"] is True
    assert plan["items"][0]["small_account_override"] is True
    assert plan["items"][0]["suggested_amount"] == 500.0


def test_small_budget_prefers_affordable_etf_in_main_recommendations():
    service = AllocationService()
    scored_df = build_scored_df(
        [
            {
                "symbol": "511010",
                "name": "国债ETF",
                "close_price": 140.0,
                "total_score": 90.0,
            },
            {
                "symbol": "510300",
                "name": "沪深300ETF",
                "close_price": 4.7,
                "total_score": 80.0,
            },
        ]
    )

    plan = service.plan(
        scored_df=scored_df,
        positions_df=pd.DataFrame(),
        total_asset=1000.0,
        available_cash=1000.0,
        current_position_pct=0.0,
        preferences=build_preferences(),
        market_regime="中性",
    )

    assert plan["action"] == "买入"
    assert plan["reason_code"] == "buy_candidates_one_lot_override"
    assert [item["symbol"] for item in plan["items"]] == ["510300"]
    assert [item["symbol"] for item in plan["watchlist_items"]] == ["511010"]


def test_truly_unaffordable_high_score_stays_in_watchlist():
    service = AllocationService()
    scored_df = build_scored_df(
        [
            {
                "symbol": "511010",
                "name": "国债ETF",
                "close_price": 140.0,
                "total_score": 90.0,
            }
        ]
    )

    plan = service.plan(
        scored_df=scored_df,
        positions_df=pd.DataFrame(),
        total_asset=1000.0,
        available_cash=1000.0,
        current_position_pct=0.0,
        preferences=build_preferences(),
        market_regime="中性",
    )

    assert plan["action"] == "不操作"
    assert plan["reason_code"] in {"watchlist_only_budget_limited", "amount_below_min_advice"}
    assert plan["items"] == []
    assert len(plan["watchlist_items"]) == 1
    assert plan["watchlist_items"][0]["symbol"] == "511010"
    assert plan["watchlist_items"][0]["is_executable"] is False
