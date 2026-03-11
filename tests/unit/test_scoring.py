import pandas as pd

from app.services.decision_policy_service import get_decision_policy_service
from app.services.scoring_service import ScoringService


def build_row(symbol: str, category: str, asset_class: str, trade_mode: str, **overrides):
    row = {
        "symbol": symbol,
        "name": symbol,
        "category": category,
        "asset_class": asset_class,
        "trade_mode": trade_mode,
        "close_price": 1.0,
        "avg_amount_20d": 100000000.0,
        "liquidity_score": 18.0,
        "momentum_3d": 1.0,
        "momentum_5d": 2.0,
        "momentum_10d": 3.0,
        "momentum_20d": 5.0,
        "trend_strength": 1.5,
        "ma20": 0.98,
        "volatility_5d": 1.2,
        "volatility_10d": 1.5,
        "volatility_20d": 2.0,
        "drawdown_20d": -2.0,
        "relative_strength_10d": 0.4,
        "above_ma20_flag": 1,
        "lot_size": 100.0,
        "fee_rate": 0.0003,
        "min_fee": 1.0,
        "risk_level": "中",
    }
    row.update(overrides)
    return row


def test_category_first_scoring_prefers_stronger_stock_category():
    frame = pd.DataFrame(
        [
            build_row(
                "510300",
                "宽基",
                "股票",
                "T+1",
                momentum_5d=5.0,
                momentum_10d=8.5,
                trend_strength=4.0,
                volatility_10d=0.2,
                drawdown_20d=-0.1,
                above_ma20_flag=1,
            ),
            build_row(
                "159915",
                "宽基",
                "股票",
                "T+1",
                momentum_5d=4.8,
                momentum_10d=8.0,
                trend_strength=3.8,
                volatility_10d=0.25,
                drawdown_20d=-0.15,
                above_ma20_flag=1,
            ),
            build_row(
                "511010",
                "债券",
                "债券",
                "T+0",
                momentum_5d=0.4,
                momentum_10d=0.8,
                trend_strength=0.5,
                volatility_10d=0.3,
                drawdown_20d=-0.8,
                above_ma20_flag=0,
            ),
            build_row(
                "518880",
                "黄金",
                "黄金",
                "T+0",
                momentum_5d=0.8,
                momentum_10d=1.2,
                trend_strength=0.7,
                volatility_10d=1.0,
                drawdown_20d=-1.5,
                above_ma20_flag=0,
            ),
            build_row(
                "513100",
                "跨境",
                "跨境",
                "T+0",
                momentum_5d=1.0,
                momentum_10d=1.5,
                trend_strength=0.9,
                volatility_10d=2.6,
                drawdown_20d=-2.8,
                above_ma20_flag=0,
            ),
            build_row(
                "511990",
                "货币",
                "货币",
                "T+0",
                momentum_5d=0.1,
                momentum_10d=0.2,
                trend_strength=0.1,
                volatility_10d=0.05,
                drawdown_20d=-0.1,
                liquidity_score=15.5,
                above_ma20_flag=0,
            ),
        ]
    )

    evaluation = ScoringService().evaluate(
        candidates_df=frame,
        positions_df=pd.DataFrame(),
        target_holding_days=5,
        previous_rank_map={},
        days_held_map={},
    )

    assert evaluation["selected_category"] == "stock_etf"
    assert evaluation["offensive_edge"] is True
    assert evaluation["scored_df"].iloc[0]["decision_category"] == "stock_etf"
    assert evaluation["category_scores_df"].iloc[0]["decision_category"] == "stock_etf"


def test_money_etf_uses_defensive_score_not_offensive_score():
    frame = pd.DataFrame(
        [
            build_row("511990", "货币", "货币", "T+0", volatility_10d=0.05, drawdown_20d=-0.05, liquidity_score=16.0),
            build_row("511010", "债券", "债券", "T+0", momentum_10d=0.3, trend_strength=0.2, volatility_10d=1.5, drawdown_20d=-1.8),
            build_row("510300", "宽基", "股票", "T+1", momentum_10d=0.2, trend_strength=0.1, volatility_10d=4.0, drawdown_20d=-5.0),
        ]
    )

    category_scores = ScoringService().score_categories(frame)
    money_row = category_scores[category_scores["decision_category"] == "money_etf"].iloc[0]

    assert money_row["category_score"] == money_row["defensive_score"]
    assert money_row["category_score"] != money_row["offensive_score"]


def test_action_threshold_override_changes_offensive_edge():
    frame = pd.DataFrame(
        [
            build_row(
                "510300",
                "宽基",
                "股票",
                "T+1",
                momentum_5d=5.0,
                momentum_10d=8.5,
                trend_strength=4.0,
                volatility_10d=0.2,
                drawdown_20d=-0.1,
                above_ma20_flag=1,
            ),
            build_row("511990", "货币", "货币", "T+0", liquidity_score=16.0, volatility_10d=0.05, drawdown_20d=-0.1),
        ]
    )
    service = ScoringService()
    base_thresholds = get_decision_policy_service().action_thresholds
    relaxed_thresholds = {
        **base_thresholds,
        "fallback": {
            **base_thresholds["fallback"],
            "offensive_threshold": 10.0,
        },
    }
    strict_thresholds = {
        **base_thresholds,
        "fallback": {
            **base_thresholds["fallback"],
            "offensive_threshold": 999.0,
        },
    }

    baseline = service.evaluate(
        candidates_df=frame,
        positions_df=pd.DataFrame(),
        target_holding_days=5,
        previous_rank_map={},
        days_held_map={},
        action_thresholds=relaxed_thresholds,
    )
    overridden = service.evaluate(
        candidates_df=frame,
        positions_df=pd.DataFrame(),
        target_holding_days=5,
        previous_rank_map={},
        days_held_map={},
        action_thresholds=strict_thresholds,
    )

    assert baseline["offensive_edge"] is True
    assert overridden["offensive_edge"] is False


def test_single_symbol_category_uses_absolute_formula_scores_instead_of_being_stuck_at_50():
    frame = pd.DataFrame(
        [
            build_row(
                "518880",
                "黄金",
                "黄金",
                "T+0",
                momentum_5d=8.0,
                momentum_10d=10.0,
                trend_strength=5.0,
                volatility_10d=0.4,
                drawdown_20d=-0.2,
                liquidity_score=25.0,
                above_ma20_flag=1,
            ),
            build_row(
                "510300",
                "宽基",
                "股票",
                "T+1",
                momentum_5d=-2.0,
                momentum_10d=-1.5,
                trend_strength=-1.0,
                volatility_10d=3.5,
                drawdown_20d=-5.0,
                liquidity_score=18.0,
                above_ma20_flag=0,
            ),
            build_row(
                "159915",
                "宽基",
                "股票",
                "T+1",
                momentum_5d=-1.8,
                momentum_10d=-1.2,
                trend_strength=-0.8,
                volatility_10d=3.2,
                drawdown_20d=-4.8,
                liquidity_score=17.0,
                above_ma20_flag=0,
            ),
            build_row(
                "511990",
                "货币",
                "货币",
                "T+0",
                momentum_5d=0.0,
                momentum_10d=0.1,
                trend_strength=0.1,
                volatility_10d=0.03,
                drawdown_20d=-0.05,
                liquidity_score=16.0,
                above_ma20_flag=0,
            ),
        ]
    )

    evaluation = ScoringService().evaluate(
        candidates_df=frame,
        positions_df=pd.DataFrame(),
        target_holding_days=30,
        previous_rank_map={},
        days_held_map={},
    )

    gold_row = evaluation["scored_df"][evaluation["scored_df"]["symbol"] == "518880"].iloc[0]

    assert gold_row["entry_score"] > 50.0
    assert gold_row["hold_score"] > 50.0
    assert gold_row["decision_score"] > 30.0


def test_defensive_money_etf_single_symbol_keeps_stable_baseline_scores():
    frame = pd.DataFrame(
        [
            build_row("511990", "货币", "货币", "T+0", liquidity_score=16.0, volatility_10d=0.03, drawdown_20d=-0.05),
            build_row("510300", "宽基", "股票", "T+1", momentum_10d=-1.5, trend_strength=-1.0, volatility_10d=3.5, drawdown_20d=-5.0),
            build_row("159915", "宽基", "股票", "T+1", momentum_10d=-1.2, trend_strength=-0.8, volatility_10d=3.2, drawdown_20d=-4.8),
        ]
    )

    evaluation = ScoringService().evaluate(
        candidates_df=frame,
        positions_df=pd.DataFrame(),
        target_holding_days=30,
        previous_rank_map={},
        days_held_map={},
    )

    money_row = evaluation["scored_df"][evaluation["scored_df"]["symbol"] == "511990"].iloc[0]

    assert money_row["entry_score"] == 50.0
    assert money_row["hold_score"] == 50.0
    assert money_row["exit_score"] == 50.0
