from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from app.services.execution_overlay_service import ExecutionOverlayService


class _PolicyStub:
    def get_category_label(self, category: str) -> str:
        return category


def _preferences():
    return SimpleNamespace(target_holding_days=30)


def _portfolio(total_asset: float = 100000.0) -> dict:
    return {"total_asset": total_asset}


def _base_row(symbol: str = "AAA", **overrides):
    payload = {
        "symbol": symbol,
        "name": f"ETF-{symbol}",
        "decision_category": "stock_etf",
        "category_label": "股票ETF",
        "global_rank": 1,
        "category_rank": 1,
        "final_score": 82.0,
        "intra_score": 80.0,
        "category_score": 70.0,
        "filter_pass": True,
        "risk_level": "中",
        "asset_class": "股票",
        "trade_mode": "T+1",
        "tradability_mode": "t1",
        "close_price": 106.0,
        "ma5": 103.0,
        "ma10": 102.0,
        "ma20": 100.0,
        "momentum_3d": 1.2,
        "momentum_5d": 2.8,
        "momentum_10d": 5.1,
        "momentum_20d": 9.4,
        "trend_strength": 4.3,
        "volatility_10d": 4.5,
        "volatility_20d": 5.5,
        "drawdown_20d": -4.0,
        "relative_strength_10d": 1.5,
        "liquidity_score": 60.0,
        "score_breakdown_json": "{}",
    }
    payload.update(overrides)
    return payload


def _build_items(
    frame: pd.DataFrame,
    *,
    current_holdings: list[dict] | None = None,
    target_weights: dict[str, float] | None = None,
    total_asset: float = 100000.0,
):
    return ExecutionOverlayService().build_action_items(
        scored_df=frame,
        current_holdings=current_holdings or [],
        allocation={
            "target_weights": target_weights or {},
            "replace_threshold": 8.0,
            "candidate_summary": [],
        },
        portfolio_summary=_portfolio(total_asset),
        preferences=_preferences(),
        policy=_PolicyStub(),
        min_trade_amount=1000.0,
    )


def test_channel_a_open_uses_pullback_rebound_entry():
    frame = pd.DataFrame([_base_row(drawdown_20d=-4.0, close_price=106.0, ma5=103.0, momentum_3d=1.0)])

    result = _build_items(frame, target_weights={"AAA": 0.20})

    assert result["items"][0]["action_code"] == "buy_open"
    assert result["items"][0]["rationale"]["entry_channel_used"] == "A"
    assert result["items"][0]["rationale"]["trend_filter_pass"] is True


def test_channel_b_open_uses_strong_breakout_exception():
    frame = pd.DataFrame([_base_row(drawdown_20d=-1.0, close_price=108.0, ma5=104.0, momentum_5d=3.0)])

    result = _build_items(frame, target_weights={"AAA": 0.20})

    assert result["items"][0]["action_code"] == "buy_open"
    assert result["items"][0]["rationale"]["entry_channel_used"] == "B"
    assert result["items"][0]["rationale"]["breakout_exception_pass"] is True


def test_existing_holding_can_buy_add_when_hold_state_and_entry_allowed_again():
    frame = pd.DataFrame([_base_row()])
    current_holdings = [
        {
            "symbol": "AAA",
            "name": "ETF-AAA",
            "category": "stock_etf",
            "current_weight": 0.08,
            "current_amount": 8000.0,
            "hold_days": 8,
        }
    ]

    result = _build_items(frame, current_holdings=current_holdings, target_weights={"AAA": 0.20})

    assert result["items"][0]["action_code"] == "buy_add"
    assert result["items"][0]["rationale"]["position_state"] == "HOLD"


def test_existing_holding_with_unknown_hold_days_is_still_treated_as_held():
    frame = pd.DataFrame([_base_row()])
    current_holdings = [
        {
            "symbol": "AAA",
            "name": "ETF-AAA",
            "category": "stock_etf",
            "current_weight": 0.08,
            "current_amount": 8000.0,
            "hold_days": 0,
            "hold_days_known": False,
        }
    ]

    result = _build_items(frame, current_holdings=current_holdings, target_weights={"AAA": 0.08})

    assert result["items"][0]["action_code"] == "hold"
    assert result["items"][0]["rationale"]["position_state"] == "HOLD"
    assert result["items"][0]["execution_trace"]["position_state"]["hold_days_known"] is False


def test_pullback_but_still_falling_does_not_open():
    frame = pd.DataFrame([_base_row(drawdown_20d=-4.0, close_price=101.0, ma5=103.0, momentum_3d=-0.5)])

    result = _build_items(frame, target_weights={"AAA": 0.20})

    assert result["items"] == []
    assert result["overlay_rows"]["AAA"]["pullback_zone_pass"] is True
    assert result["overlay_rows"]["AAA"]["rebound_confirmation_pass"] is False
    assert result["overlay_rows"]["AAA"]["entry_allowed"] is False


def test_reduce_when_trend_weakens_but_not_fully_broken():
    frame = pd.DataFrame([_base_row(close_price=99.0, ma20=100.0, momentum_20d=6.0)])
    current_holdings = [
        {
            "symbol": "AAA",
            "name": "ETF-AAA",
            "category": "stock_etf",
            "current_weight": 0.20,
            "current_amount": 20000.0,
            "hold_days": 12,
        }
    ]

    result = _build_items(frame, current_holdings=current_holdings, target_weights={"AAA": 0.20})

    assert result["items"][0]["action_code"] == "sell_reduce"
    assert result["items"][0]["rationale"]["position_state"] == "REDUCE"
    steps = result["items"][0]["execution_trace"]["position_state"]["reason_steps"]
    assert any("momentum_20d > 0" in step["condition"] for step in steps)
    assert any("reduced_target_weight = normal_target_weight × reduced_target_multiplier" in step["condition"] for step in steps)
    final_steps = result["items"][0]["execution_trace"]["final_action_calc"]["reason_steps"]
    assert any("因此最终动作 = sell_reduce" in step["condition"] for step in final_steps)


def test_exit_when_trend_is_broken():
    frame = pd.DataFrame([_base_row(close_price=98.0, ma20=100.0, momentum_20d=-1.0)])
    current_holdings = [
        {
            "symbol": "AAA",
            "name": "ETF-AAA",
            "category": "stock_etf",
            "current_weight": 0.20,
            "current_amount": 20000.0,
            "hold_days": 16,
        }
    ]

    result = _build_items(frame, current_holdings=current_holdings, target_weights={})

    assert result["items"][0]["action_code"] == "sell_exit"
    assert result["items"][0]["rationale"]["position_state"] == "EXIT"


def test_reduce_state_allows_new_same_category_open_without_forced_full_switch():
    frame = pd.DataFrame(
        [
            _base_row(
                symbol="OLD",
                category_rank=2,
                global_rank=2,
                final_score=68.0,
                intra_score=65.0,
                close_price=99.0,
                ma20=100.0,
                momentum_20d=3.0,
                drawdown_20d=-5.0,
            ),
            _base_row(
                symbol="NEW",
                category_rank=1,
                global_rank=1,
                final_score=84.0,
                intra_score=82.0,
                drawdown_20d=-4.0,
                close_price=107.0,
                ma5=103.0,
                momentum_3d=1.1,
            ),
        ]
    )
    current_holdings = [
        {
            "symbol": "OLD",
            "name": "ETF-OLD",
            "category": "stock_etf",
            "current_weight": 0.20,
            "current_amount": 20000.0,
            "hold_days": 10,
        }
    ]

    result = _build_items(frame, current_holdings=current_holdings, target_weights={"NEW": 0.20})
    item_by_symbol = {item["symbol"]: item for item in result["items"]}

    assert item_by_symbol["NEW"]["action_code"] == "buy_open"
    assert item_by_symbol["OLD"]["action_code"] == "sell_reduce"


def test_exit_state_allows_same_category_switch():
    frame = pd.DataFrame(
        [
            _base_row(
                symbol="OLD",
                category_rank=2,
                global_rank=2,
                final_score=68.0,
                intra_score=65.0,
                close_price=98.0,
                ma20=100.0,
                momentum_20d=-1.0,
                drawdown_20d=-5.0,
            ),
            _base_row(
                symbol="NEW",
                category_rank=1,
                global_rank=1,
                final_score=84.0,
                intra_score=82.0,
                drawdown_20d=-4.0,
                close_price=107.0,
                ma5=103.0,
                momentum_3d=1.1,
            ),
        ]
    )
    current_holdings = [
        {
            "symbol": "OLD",
            "name": "ETF-OLD",
            "category": "stock_etf",
            "current_weight": 0.20,
            "current_amount": 20000.0,
            "hold_days": 10,
        }
    ]

    result = _build_items(frame, current_holdings=current_holdings, target_weights={"NEW": 0.20})
    item_by_symbol = {item["symbol"]: item for item in result["items"]}

    assert item_by_symbol["NEW"]["action_code"] == "switch"
    assert item_by_symbol["OLD"]["action_code"] == "sell_exit"


def test_money_etf_exit_score_is_driven_by_offensive_opportunity_not_hold_days():
    frame = pd.DataFrame(
        [
            _base_row(
                symbol="CASH",
                decision_category="money_etf",
                close_price=100.1,
                ma5=100.0,
                ma10=100.0,
                ma20=100.0,
                momentum_3d=0.02,
                momentum_5d=0.05,
                momentum_10d=0.08,
                momentum_20d=0.10,
                trend_strength=0.10,
                volatility_10d=0.2,
                volatility_20d=0.3,
                drawdown_20d=-0.05,
                liquidity_score=90.0,
                relative_strength_10d=0.0,
            ),
            _base_row(
                symbol="STK",
                decision_category="stock_etf",
                close_price=108.0,
                ma5=104.0,
                ma10=103.0,
                ma20=100.0,
                momentum_3d=1.0,
                momentum_5d=3.0,
                momentum_10d=6.0,
                momentum_20d=12.0,
                trend_strength=5.0,
                volatility_10d=4.0,
                volatility_20d=5.0,
                drawdown_20d=-1.0,
                relative_strength_10d=2.0,
                liquidity_score=70.0,
            ),
        ]
    )
    current_holdings_short = [
        {
            "symbol": "CASH",
            "name": "ETF-CASH",
            "category": "money_etf",
            "current_weight": 0.20,
            "current_amount": 20000.0,
            "hold_days": 1,
            "hold_days_known": True,
        }
    ]
    current_holdings_long = [
        {
            "symbol": "CASH",
            "name": "ETF-CASH",
            "category": "money_etf",
            "current_weight": 0.20,
            "current_amount": 20000.0,
            "hold_days": 200,
            "hold_days_known": True,
        }
    ]

    result_short = _build_items(frame, current_holdings=current_holdings_short, target_weights={"CASH": 0.20})
    result_long = _build_items(frame, current_holdings=current_holdings_long, target_weights={"CASH": 0.20})

    assert result_short["overlay_rows"]["CASH"]["exit_score"] == result_long["overlay_rows"]["CASH"]["exit_score"]
    assert result_short["overlay_rows"]["CASH"]["exit_score"] > 0
