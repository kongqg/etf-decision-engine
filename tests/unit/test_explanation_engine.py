from __future__ import annotations

import json
from types import SimpleNamespace

from app.services.explanation_engine import ExplanationEngine
from app.web.presenters import serialize_explanations


def _sample_item() -> dict:
    score_breakdown = {
        "ranks": {
            "momentum_20d_rank": 80.0,
            "momentum_10d_rank": 70.0,
            "momentum_5d_rank": 65.0,
            "trend_rank": 90.0,
            "volatility_rank": 55.0,
            "drawdown_rank": 60.0,
            "liquidity_rank": 75.0,
        },
        "category_components": {
            "top_mean_intrascore": 76.0,
            "breadth_score": 64.0,
            "category_momentum_score": 68.0,
        },
        "scores": {
            "intra_score": 76.88,
            "category_score": 70.8,
            "final_score": 75.06,
        },
    }
    return {
        "symbol": "510300",
        "name": "沪深300ETF",
        "category": "stock_etf",
        "action": "buy",
        "action_code": "buy_open",
        "intent": "open",
        "current_weight": 0.0,
        "target_weight": 0.2,
        "delta_weight": 0.2,
        "current_amount": 0.0,
        "target_amount": 20000.0,
        "suggested_amount": 20000.0,
        "suggested_pct": 0.2,
        "score": 78.2,
        "score_gap": 0.0,
        "score_gap_vs_holding": 0.0,
        "replace_threshold_used": 8.0,
        "replacement_symbol": "",
        "final_score": 75.06,
        "intra_score": 76.88,
        "category_score": 70.8,
        "entry_score": 82.0,
        "hold_score": 63.0,
        "exit_score": 18.0,
        "decision_score": 78.2,
        "reason_short": "满足通道A，允许新开仓。",
        "action_reason": "满足通道A，允许新开仓。",
        "risk_level": "中",
        "asset_class": "股票",
        "trade_mode": "T+1",
        "tradability_mode": "t1",
        "execution_note": "该 ETF 按 T+1 口径展示执行提示。",
        "is_new_position": True,
        "hold_days": 0,
        "is_held": False,
        "latest_price": 4.213,
        "scores": {
            "entry_score": 82.0,
            "hold_score": 63.0,
            "exit_score": 18.0,
            "decision_score": 78.2,
            "intra_score": 76.88,
            "category_score": 70.8,
            "final_score": 75.06,
        },
        "score_breakdown": score_breakdown,
        "feature_snapshot": {
            "close_price": 4.213,
            "momentum_3d": 1.2,
            "momentum_5d": 2.8,
            "momentum_10d": 5.4,
            "momentum_20d": 8.9,
            "ma5": 4.102,
            "ma10": 4.033,
            "ma20": 3.921,
            "trend_strength": 7.4,
            "drawdown_20d": -3.8,
            "volatility_20d": 5.2,
            "liquidity_score": 66.0,
            "decision_category": "stock_etf",
            "tradability_mode": "t1",
        },
        "rationale": {
            "trend_filter_pass": True,
            "pullback_zone_pass": True,
            "rebound_confirmation_pass": True,
            "breakout_exception_pass": False,
            "entry_allowed": True,
            "entry_channel_used": "A",
            "entry_channel_label": "通道A：回撤后反弹",
            "position_state": "NONE",
            "position_state_label": "未持有",
            "action_reason": "满足通道A，允许新开仓。",
        },
        "execution_trace": {
            "entry_checks": {
                "entry_channel": "A",
                "entry_allowed": True,
                "channel_a": {
                    "trend_filter_pass": True,
                    "pullback_zone_pass": True,
                    "rebound_confirmation_pass": True,
                    "channel_a_pass": True,
                },
                "channel_b": {
                    "trend_filter_pass": True,
                    "drawdown_near_high_pass": False,
                    "entry_score_pass": True,
                    "momentum_5d_pass": True,
                    "close_above_ma5_pass": True,
                    "volatility_guard_pass": True,
                    "channel_b_pass": False,
                },
            },
            "position_state": {
                "current_weight": 0.0,
                "position_state": "NONE",
                "position_state_label": "未持有",
                "reason": "当前没有持仓，因此只进行入场可行性判断。",
                "reduced_target_weight": 0.0,
            },
            "switch_checks": {
                "old_state": "NONE",
                "new_entry_allowed": True,
                "new_target_weight": 0.2,
                "rebalance_band": 0.05,
                "switch_allowed": False,
                "score_gap": 0.0,
            },
            "target_weight_adjustment": {
                "normal_target_weight": 0.2,
                "current_weight": 0.0,
                "reduced_target_weight": 0.0,
                "effective_target_weight": 0.2,
                "branch": "open_or_add_to_normal",
            },
            "final_action_calc": {
                "current_weight": 0.0,
                "effective_target_weight": 0.2,
                "delta_weight": 0.2,
                "total_asset": 100000.0,
                "target_amount": 20000.0,
                "delta_amount": 20000.0,
                "min_trade_amount": 1000.0,
                "rebalance_band": 0.05,
                "action": "buy",
                "action_code": "buy_open",
                "action_reason": "满足通道A，允许新开仓。",
            },
        },
    }


def test_explanation_engine_builds_structured_item_payload():
    engine = ExplanationEngine()
    item = _sample_item()
    allocation = {
        "total_budget_pct": 0.8,
        "single_weight_cap": 0.35,
        "category_budget_caps": {"stock_etf": 0.6},
        "replace_threshold": 8.0,
        "candidate_summary": [
            {
                "symbol": "510300",
                "name": "沪深300ETF",
                "category": "stock_etf",
                "final_score": 75.06,
                "intra_score": 76.88,
                "category_score": 70.8,
                "global_rank": 1,
                "category_rank": 1,
                "selected": True,
            }
        ],
        "selection_trace": {
            "510300": {
                "selected": True,
                "selected_reason": "最终分、过滤条件和替换条件都通过，因此进入目标组合候选。",
                "blocked_reason": "",
                "protected": False,
                "protected_reasons": [],
                "meets_min_final_score": True,
                "min_final_score_for_target": 55.0,
            }
        },
        "replacement_trace": {},
        "allocation_trace": {
            "510300": {
                "total_budget_pct": 0.8,
                "single_weight_cap": 0.35,
                "category_cap": 0.6,
                "provisional_weight": 0.22,
                "normal_target_weight": 0.2,
                "cap_applied": True,
                "cap_reasons": ["单票上限截断"],
                "selected_for_allocation": True,
                "protected": False,
                "protected_reasons": [],
                "selected_reason": "最终分、过滤条件和替换条件都通过，因此进入目标组合候选。",
                "blocked_reason": "",
                "replacement_trace": {},
            }
        },
        "overlay_rows": {
            "510300": {
                "symbol": "510300",
                "name": "沪深300ETF",
                "decision_category": "stock_etf",
                "global_rank": 1,
                "category_rank": 1,
            }
        },
        "overlay_traces": {"510300": {"execution_trace": item["execution_trace"], "rationale": item["rationale"], "feature_snapshot": item["feature_snapshot"], "score_breakdown": item["score_breakdown"], "scores": item["scores"]}},
    }

    explanation = engine.build(
        market_regime={"market_regime": "risk_on"},
        allocation=allocation,
        items=[item],
        candidate_summary=allocation["candidate_summary"],
        portfolio_summary={"current_position_pct": 0.0, "cash_balance": 100000.0, "market_value": 0.0, "total_asset": 100000.0},
        quality_summary={"quality_status": "ok", "verification_status": "正常"},
        current_holdings=[],
    )

    detail = explanation["items"][0]
    assert "summary_card" in detail
    assert "intra_score_breakdown" in detail
    assert "category_score_breakdown" in detail
    assert "final_score_breakdown" in detail
    assert "decision_score_breakdown" in detail
    assert "allocation_trace" in detail
    assert "execution_trace" in detail
    assert "natural_language_summary" in detail
    assert detail["summary_card"]["symbol"] == "510300"
    assert detail["execution_trace"]["entry_checks"]["entry_allowed"] is True
    assert detail["decision_score_breakdown"]["decision_score"] == 78.2


def test_serialize_explanations_handles_legacy_item_payload_gracefully():
    records = [
        SimpleNamespace(
            scope="overall",
            explanation_json=json.dumps(
                {
                    "headline": "今天暂不交易",
                    "market_regime": "risk_on",
                    "candidate_summary": [],
                },
                ensure_ascii=False,
            ),
        ),
        SimpleNamespace(
            scope="item",
            explanation_json=json.dumps(
                {
                    "symbol": "518880",
                    "name": "黄金ETF",
                    "title": "黄金ETF / 观察",
                    "summary": "旧记录只有简短说明。",
                    "action": "no_trade",
                    "action_code": "no_trade",
                    "intent": "hold",
                    "category": "gold_etf",
                    "scores": {"final_score": 61.0},
                    "execution_overlay": {"entry_channel_used": "none", "position_state": "NONE"},
                },
                ensure_ascii=False,
            ),
        ),
    ]

    payload = serialize_explanations(records)

    item = payload["items"][0]
    assert item["summary_card"]["symbol"] == "518880"
    assert item["intra_score_breakdown"]["available"] is False
    assert item["allocation_trace"]["normal_target_weight"] == 0.0
    assert item["execution_trace"]["entry_checks"]["reason_steps"] == []
