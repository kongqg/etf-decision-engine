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


def test_switch_requires_old_position_to_be_weak_and_new_entry_to_be_valid():
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

    assert item_by_symbol["NEW"]["action_code"] == "switch"
    assert item_by_symbol["OLD"]["action_code"] == "sell_exit"
