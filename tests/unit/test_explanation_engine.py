import json

import pandas as pd

from app.services.explanation_engine import ExplanationEngine


def test_explanation_contains_category_and_score_breakdowns():
    breakdown = {
        "category_breakdown": {"raw_metrics": {"category_momentum": 3.2, "category_trend": 1.8}},
        "entry_breakdown": [{"feature": "momentum_5d", "weight": 0.2, "raw_value": 3.0, "percentile": 90.0, "contribution": 18.0}],
        "hold_breakdown": [{"feature": "trend_strength", "weight": 0.3, "raw_value": 1.2, "percentile": 80.0, "contribution": 24.0}],
        "exit_breakdown": [{"feature": "time_decay", "weight": 0.2, "raw_value": 0.1, "percentile": 20.0, "contribution": 4.0}],
    }
    scored_df = pd.DataFrame(
        [
            {
                "symbol": "510300",
                "name": "沪深300ETF",
                "momentum_10d": 3.4,
                "trend_strength": 1.8,
                "relative_strength_10d": 0.8,
                "volatility_10d": 1.2,
                "drawdown_20d": -1.5,
                "breakdown_json": json.dumps(breakdown, ensure_ascii=False),
            }
        ]
    )
    explanation = ExplanationEngine().build(
        advice={
            "session_mode": "intraday",
            "summary_text": "股票ETF 先赢出，建议开仓。",
            "market_regime": "中性",
            "risk_text": "注意控制仓位。",
        },
        scored_df=scored_df,
        filtered_df=scored_df.copy(),
        portfolio_summary={"current_position_pct": 0.1},
        market_snapshot={"raw": {"source": {}, "quality_summary": {}}},
        plan={
            "action_code": "buy_open",
            "target_position_pct": 0.3,
            "executable_now": True,
            "facts": {
                "target_holding_days": 5,
                "open_threshold": 58.0,
                "selected_category_score": 66.0,
                "transition_count": 1,
                "holding_review_count": 0,
                "target_portfolio_mode": "offensive",
            },
            "category_scores": [{"category_label": "股票ETF", "category_score": 66.0, "symbol_count": 2, "raw_metrics": {"momentum_10d": 3.0, "trend_strength": 1.5, "breadth": 0.8, "volatility_10d": 1.2, "drawdown_20d": 1.5}}],
            "winning_category_label": "股票ETF",
            "selected_category_score": 66.0,
            "target_portfolio": {
                "mode": "offensive",
                "notes": ["测试目标组合说明。"],
            },
            "primary_item": {
                "symbol": "510300",
                "name": "沪深300ETF",
                "asset_class": "股票ETF",
                "category": "stock_etf",
                "action": "开仓买入",
                "action_code": "buy_open",
                "decision_score": 72.0,
                "entry_score": 85.0,
                "hold_score": 70.0,
                "exit_score": 20.0,
                "category_score": 66.0,
                "horizon_profile_label": "短线波段",
                "mapped_horizon_profile": "swing",
                "lifecycle_phase": "build_phase",
                "target_holding_days": 5,
                "trade_mode": "T+1",
                "executable_now": True,
                "blocked_reason": "",
                "planned_exit_days": None,
                "planned_exit_rule_summary": "",
                "reason_short": "测试",
                "execution_note": "测试",
                "current_weight": 0.0,
                "target_weight": 0.3,
                "delta_weight": 0.3,
                "current_amount": 0.0,
                "target_amount": 3000.0,
                "current_return_pct": 0.0,
                "is_current_holding": False,
                "entry_eligible": True,
                "filter_reasons": [],
                "transition_label": "纳入目标组合",
            },
            "transition_plan": [
                {
                    "symbol": "510300",
                    "name": "沪深300ETF",
                    "asset_class": "股票ETF",
                    "category": "stock_etf",
                    "action": "开仓买入",
                    "action_code": "buy_open",
                    "decision_score": 72.0,
                    "entry_score": 85.0,
                    "hold_score": 70.0,
                    "exit_score": 20.0,
                    "category_score": 66.0,
                    "horizon_profile_label": "短线波段",
                    "mapped_horizon_profile": "swing",
                    "lifecycle_phase": "build_phase",
                    "target_holding_days": 5,
                    "trade_mode": "T+1",
                    "executable_now": True,
                    "blocked_reason": "",
                    "planned_exit_days": None,
                    "planned_exit_rule_summary": "",
                    "reason_short": "测试",
                    "execution_note": "测试",
                    "execution_status": "可执行开仓",
                    "current_weight": 0.0,
                    "target_weight": 0.3,
                    "delta_weight": 0.3,
                    "current_amount": 0.0,
                    "target_amount": 3000.0,
                    "current_return_pct": 0.0,
                    "is_current_holding": False,
                    "entry_eligible": True,
                    "filter_reasons": [],
                    "transition_label": "纳入目标组合",
                }
            ],
            "portfolio_review_items": [],
            "recommendation_groups": {
                "executable_recommendations": [
                    {
                        "symbol": "510300",
                        "name": "沪深300ETF",
                        "asset_class": "股票ETF",
                        "category": "stock_etf",
                        "action": "开仓买入",
                        "action_code": "buy_open",
                        "decision_score": 72.0,
                        "entry_score": 85.0,
                        "hold_score": 70.0,
                        "exit_score": 20.0,
                        "category_score": 66.0,
                        "horizon_profile_label": "短线波段",
                        "mapped_horizon_profile": "swing",
                        "lifecycle_phase": "build_phase",
                        "target_holding_days": 5,
                        "trade_mode": "T+1",
                        "executable_now": True,
                        "blocked_reason": "",
                        "planned_exit_days": None,
                        "planned_exit_rule_summary": "",
                        "reason_short": "测试",
                        "execution_note": "测试",
                    }
                ],
                "watchlist_recommendations": [],
            },
        },
    )

    assert explanation["overall"]["category_scores"]
    assert explanation["overall"]["execution_rule"]["rule"]
    assert explanation["overall"]["portfolio_transition"]["rows"]
    assert explanation["items"][0]["category_breakdown"]
    assert explanation["items"][0]["score_breakdown"]["entry_details"]
