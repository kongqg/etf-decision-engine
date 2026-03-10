from types import SimpleNamespace

import pandas as pd
import pytest

from app.services.allocation_service import AllocationService


def build_preferences(min_trade_amount: float = 100.0):
    return SimpleNamespace(
        risk_level="中性",
        max_total_position_pct=0.7,
        max_single_position_pct=0.35,
        cash_reserve_pct=0.2,
        min_trade_amount=min_trade_amount,
    )


def build_row(
    symbol: str,
    name: str,
    asset_class: str,
    category: str,
    close_price: float,
    total_score: float,
    trade_mode: str,
    **overrides,
):
    row = {
        "symbol": symbol,
        "name": name,
        "asset_class": asset_class,
        "category": category,
        "trade_mode": trade_mode,
        "close_price": close_price,
        "total_score": total_score,
        "momentum_3d": 1.2,
        "momentum_5d": 1.8,
        "momentum_10d": 2.4,
        "trend_strength": 1.4,
        "ma_gap_5": 0.8,
        "ma_gap_10": 1.2,
        "volatility_10d": 2.2,
        "drawdown_20d": -2.0,
        "avg_amount_20d": 120000000.0,
        "risk_level": "中",
        "lot_size": 100.0,
        "fee_rate": 0.0003,
        "min_fee": 1.0,
    }
    row.update(overrides)
    return row


def build_scored_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_stock_etf_t_plus_1_stays_executable():
    service = AllocationService()
    scored_df = build_scored_df(
        [
            build_row(
                symbol="510300",
                name="沪深300ETF",
                asset_class="股票",
                category="宽基",
                close_price=1.2,
                total_score=88.0,
                trade_mode="T+1",
            )
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
    assert plan["reason_code"] == "asset_class_buy_candidates"
    assert len(plan["items"]) == 1
    assert plan["items"][0]["symbol"] == "510300"
    assert plan["items"][0]["asset_class"] == "股票"
    assert plan["items"][0]["trade_mode"] == "T+1"
    assert plan["items"][0]["is_executable"] is True
    assert plan["items"][0]["min_order_amount"] == 120.0


@pytest.mark.parametrize(
    ("asset_class", "category", "symbol", "name"),
    [
        ("债券", "债券", "511010", "国债ETF"),
        ("黄金", "黄金", "518880", "黄金ETF"),
        ("货币", "货币", "511990", "华宝添益"),
    ],
)
def test_t0_assets_can_enter_executable_recommendations(asset_class, category, symbol, name):
    service = AllocationService()
    scored_df = build_scored_df(
        [
            build_row(
                symbol=symbol,
                name=name,
                asset_class=asset_class,
                category=category,
                close_price=12.0,
                total_score=82.0,
                trade_mode="T+0",
            )
        ]
    )

    plan = service.plan(
        scored_df=scored_df,
        positions_df=pd.DataFrame(),
        total_asset=10000.0,
        available_cash=5000.0,
        current_position_pct=0.0,
        preferences=build_preferences(),
        market_regime="防守",
    )

    assert plan["action"] == "买入"
    assert plan["items"][0]["asset_class"] == asset_class
    assert plan["items"][0]["trade_mode"] == "T+0"
    assert plan["items"][0]["is_executable"] is True


def test_high_score_but_unaffordable_goes_to_watchlist():
    service = AllocationService()
    scored_df = build_scored_df(
        [
            build_row(
                symbol="511010",
                name="国债ETF",
                asset_class="债券",
                category="债券",
                close_price=140.0,
                total_score=90.0,
                trade_mode="T+0",
            )
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
    assert plan["reason_code"] == "watchlist_only_budget_limited"
    assert plan["items"] == []
    assert [item["symbol"] for item in plan["watchlist_items"]] == ["511010"]
    assert plan["watchlist_items"][0]["is_executable"] is False
    assert plan["watchlist_items"][0]["trade_mode"] == "T+0"


def test_defensive_bond_with_small_negative_momentum_can_still_be_selected():
    service = AllocationService()
    scored_df = build_scored_df(
        [
            build_row(
                symbol="511010",
                name="国债ETF",
                asset_class="债券",
                category="债券",
                close_price=1.2,
                total_score=79.2,
                trade_mode="T+0",
                momentum_5d=-0.16,
                momentum_10d=-0.19,
                ma_gap_10=-0.08,
                trend_strength=-0.11,
            )
        ]
    )

    plan = service.plan(
        scored_df=scored_df,
        positions_df=pd.DataFrame(),
        total_asset=10000.0,
        available_cash=5000.0,
        current_position_pct=0.0,
        preferences=build_preferences(),
        market_regime="防守",
    )

    assert plan["action"] == "买入"
    assert plan["reason_code"] == "asset_class_buy_candidates"
    assert [item["symbol"] for item in plan["items"]] == ["511010"]
    assert plan["facts"]["active_asset_class_count"] >= 1


def test_high_score_but_fee_too_high_goes_to_cost_inefficient_recommendations():
    service = AllocationService()
    scored_df = build_scored_df(
        [
            build_row(
                symbol="510300",
                name="沪深300ETF",
                asset_class="股票",
                category="宽基",
                close_price=1.2,
                total_score=88.0,
                trade_mode="T+1",
                min_fee=120.0,
            )
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

    assert plan["action"] == "不操作"
    assert plan["reason_code"] == "cost_inefficient_only"
    assert plan["items"] == []
    assert len(plan["cost_inefficient_items"]) == 1
    assert plan["cost_inefficient_items"][0]["symbol"] == "510300"
    assert plan["cost_inefficient_items"][0]["estimated_cost_rate"] > service.settings.max_fee_rate_for_execution


def test_small_budget_prefers_affordable_asset_class_candidate_over_theoretical_top_pick():
    service = AllocationService()
    scored_df = build_scored_df(
        [
            build_row(
                symbol="511010",
                name="国债ETF",
                asset_class="债券",
                category="债券",
                close_price=140.0,
                total_score=92.0,
                trade_mode="T+0",
            ),
            build_row(
                symbol="510300",
                name="沪深300ETF",
                asset_class="股票",
                category="宽基",
                close_price=4.7,
                total_score=80.0,
                trade_mode="T+1",
            ),
        ]
    )

    plan = service.plan(
        scored_df=scored_df,
        positions_df=pd.DataFrame(),
        total_asset=2000.0,
        available_cash=1000.0,
        current_position_pct=0.0,
        preferences=build_preferences(),
        market_regime="防守",
    )

    assert plan["action"] == "买入"
    assert plan["reason_code"] == "buy_candidates_one_lot_override"
    assert [item["symbol"] for item in plan["items"]] == ["510300"]
    assert [item["symbol"] for item in plan["watchlist_items"]] == ["511010"]


def test_small_budget_can_use_budget_substitute_when_primary_asset_class_is_unaffordable():
    service = AllocationService()
    scored_df = build_scored_df(
        [
            build_row(
                symbol="511010",
                name="国债ETF",
                asset_class="债券",
                category="债券",
                close_price=140.0,
                total_score=92.0,
                trade_mode="T+0",
                momentum_5d=-0.16,
                momentum_10d=-0.19,
                ma_gap_10=-0.08,
                trend_strength=-0.11,
            ),
            build_row(
                symbol="510300",
                name="沪深300ETF",
                asset_class="股票",
                category="宽基",
                close_price=4.7,
                total_score=76.5,
                trade_mode="T+1",
                momentum_5d=0.2,
                momentum_10d=-0.3,
                ma_gap_10=-0.1,
                trend_strength=-0.05,
            ),
            build_row(
                symbol="512100",
                name="中证1000ETF",
                asset_class="股票",
                category="宽基",
                close_price=3.9,
                total_score=68.5,
                trade_mode="T+1",
                momentum_5d=-0.2,
                momentum_10d=-0.1,
                ma_gap_10=-0.05,
                trend_strength=-0.03,
            ),
        ]
    )

    plan = service.plan(
        scored_df=scored_df,
        positions_df=pd.DataFrame(),
        total_asset=2000.0,
        available_cash=1000.0,
        current_position_pct=0.0,
        preferences=build_preferences(),
        market_regime="防守",
    )

    assert plan["action"] == "买入"
    assert plan["reason_code"] == "budget_substitute_buy_candidates"
    assert [item["symbol"] for item in plan["items"]] == ["510300", "512100"]
    assert plan["items"][0]["is_budget_substitute"] is True
    assert plan["items"][0]["execution_status"] == "预算内替代执行"
    assert plan["items"][0]["primary_asset_class"] == "债券"
    assert plan["best_unaffordable_item"]["symbol"] == "511010"
    assert [item["symbol"] for item in plan["watchlist_items"]] == ["511010"]


def test_budget_substitute_candidates_are_removed_from_affordable_but_weak():
    service = AllocationService()
    scored_df = build_scored_df(
        [
            build_row(
                symbol="511010",
                name="国债ETF",
                asset_class="债券",
                category="债券",
                close_price=140.0,
                total_score=92.0,
                trade_mode="T+0",
                momentum_5d=-0.16,
                momentum_10d=-0.19,
                ma_gap_10=-0.08,
                trend_strength=-0.11,
            ),
            build_row(
                symbol="511990",
                name="华宝添益",
                asset_class="货币",
                category="货币",
                close_price=99.994,
                total_score=81.4,
                trade_mode="T+0",
                momentum_5d=-0.01,
                momentum_10d=-0.01,
                ma_gap_10=-0.01,
                trend_strength=-0.01,
            ),
            build_row(
                symbol="510300",
                name="沪深300ETF",
                asset_class="股票",
                category="宽基",
                close_price=4.678,
                total_score=74.3,
                trade_mode="T+1",
                momentum_5d=-0.2,
                momentum_10d=-0.3,
                ma_gap_10=-0.1,
                trend_strength=-0.05,
            ),
            build_row(
                symbol="512690",
                name="酒ETF",
                asset_class="股票",
                category="行业",
                close_price=0.508,
                total_score=34.7,
                trade_mode="T+1",
                momentum_5d=-0.2,
                momentum_10d=-0.5,
                ma_gap_10=-0.3,
                trend_strength=-0.2,
            ),
            build_row(
                symbol="513500",
                name="标普500ETF",
                asset_class="跨境",
                category="跨境",
                close_price=2.309,
                total_score=14.7,
                trade_mode="T+0",
                momentum_5d=-0.3,
                momentum_10d=-0.4,
                ma_gap_10=-0.2,
                trend_strength=-0.15,
            ),
        ]
    )

    plan = service.plan(
        scored_df=scored_df,
        positions_df=pd.DataFrame(),
        total_asset=2000.0,
        available_cash=2000.0,
        current_position_pct=0.0,
        preferences=build_preferences(),
        market_regime="防守",
    )

    assert plan["reason_code"] == "budget_substitute_buy_candidates"
    assert [item["symbol"] for item in plan["items"]] == ["510300"]
    assert [item["symbol"] for item in plan["affordable_but_weak_items"]] == ["512690", "513500"]


def test_budget_substitute_is_blocked_when_one_lot_would_break_position_cap():
    service = AllocationService()
    scored_df = build_scored_df(
        [
            build_row(
                symbol="511010",
                name="国债ETF",
                asset_class="债券",
                category="债券",
                close_price=140.0,
                total_score=92.0,
                trade_mode="T+0",
                momentum_5d=-0.16,
                momentum_10d=-0.19,
                ma_gap_10=-0.08,
                trend_strength=-0.11,
            ),
            build_row(
                symbol="510500",
                name="中证500ETF",
                asset_class="股票",
                category="宽基",
                close_price=8.418,
                total_score=76.5,
                trade_mode="T+1",
                momentum_5d=0.2,
                momentum_10d=-0.3,
                ma_gap_10=-0.1,
                trend_strength=-0.05,
            ),
        ]
    )

    plan = service.plan(
        scored_df=scored_df,
        positions_df=pd.DataFrame(),
        total_asset=1000.0,
        available_cash=1000.0,
        current_position_pct=0.0,
        preferences=build_preferences(),
        market_regime="防守",
    )

    assert plan["action"] == "不操作"
    assert plan["reason_code"] == "watchlist_only_budget_and_position_limited"
    assert plan["items"] == []
    assert plan["best_unaffordable_item"]["symbol"] == "511010"
    assert plan["best_unaffordable_item"]["is_best_unaffordable"] is True
    assert [item["symbol"] for item in plan["watchlist_items"]] == ["511010"]
    assert plan["facts"]["practical_buy_cap_amount"] == 250.0
    assert plan["facts"]["affordable_but_over_cap_count"] >= 1


def test_affordable_but_weak_candidates_are_kept_visible():
    service = AllocationService()
    scored_df = build_scored_df(
        [
            build_row(
                symbol="511010",
                name="国债ETF",
                asset_class="债券",
                category="债券",
                close_price=140.695,
                total_score=79.2,
                trade_mode="T+0",
                momentum_5d=-0.08,
                momentum_10d=-0.12,
                ma_gap_10=-0.04,
                trend_strength=-0.06,
            ),
            build_row(
                symbol="513500",
                name="标普500ETF",
                asset_class="跨境",
                category="跨境",
                close_price=2.305,
                total_score=18.0,
                trade_mode="T+0",
                momentum_5d=-0.3,
                momentum_10d=-0.4,
                ma_gap_10=-0.2,
                trend_strength=-0.15,
            ),
            build_row(
                symbol="512690",
                name="酒ETF",
                asset_class="股票",
                category="行业",
                close_price=0.507,
                total_score=41.3,
                trade_mode="T+1",
                momentum_5d=-0.2,
                momentum_10d=-0.5,
                ma_gap_10=-0.3,
                trend_strength=-0.2,
            ),
        ]
    )

    plan = service.plan(
        scored_df=scored_df,
        positions_df=pd.DataFrame(),
        total_asset=1000.0,
        available_cash=1000.0,
        current_position_pct=0.0,
        preferences=build_preferences(),
        market_regime="防守",
    )

    assert plan["action"] == "不操作"
    assert plan["reason_code"] == "watchlist_only_budget_limited"
    assert plan["best_unaffordable_item"]["symbol"] == "511010"
    assert [item["symbol"] for item in plan["affordable_but_weak_items"]] == ["512690", "513500"]
    assert all(item["recommendation_bucket"] == "affordable_but_weak_recommendations" for item in plan["affordable_but_weak_items"])
    assert all(item["execution_status"] == "买得起但当前不建议买" for item in plan["affordable_but_weak_items"])
    assert all(item["is_affordable_but_weak"] is True for item in plan["affordable_but_weak_items"])
    assert plan["facts"]["affordable_but_weak_count"] == 2
