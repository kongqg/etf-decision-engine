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


def _build_explanation(*, item: dict, allocation: dict, current_holdings: list[dict] | None = None) -> dict:
    engine = ExplanationEngine()
    return engine.build(
        market_regime={"market_regime": "risk_on"},
        allocation=allocation,
        items=[item],
        candidate_summary=allocation.get("candidate_summary", []),
        portfolio_summary={"current_position_pct": 0.0, "cash_balance": 100000.0, "market_value": 0.0, "total_asset": 100000.0},
        quality_summary={"quality_status": "ok", "verification_status": "正常"},
        current_holdings=current_holdings or [],
    )


def test_explanation_engine_builds_structured_item_payload():
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

    explanation = _build_explanation(item=item, allocation=allocation)

    detail = explanation["items"][0]
    assert "summary_card" in detail
    assert "intra_score_breakdown" in detail
    assert "category_score_breakdown" in detail
    assert "final_score_breakdown" in detail
    assert "decision_score_breakdown" in detail
    assert "allocation_trace" in detail
    assert "execution_trace" in detail
    assert "natural_language_summary" in detail
    assert "primary_reason_stage" in detail
    assert "decision_ladder" in detail
    assert "show_blocks" in detail
    assert detail["summary_card"]["symbol"] == "510300"
    assert detail["execution_trace"]["entry_checks"]["entry_allowed"] is True
    assert detail["decision_score_breakdown"]["decision_score"] == 78.2
    assert detail["primary_reason_stage"] == "final_action"
    assert detail["show_blocks"]["switch_checks"] is False


def test_high_score_but_execution_gate_blocked_uses_execution_gate_as_primary_reason():
    item = _sample_item()
    item.update(
        {
            "action": "no_trade",
            "action_code": "no_trade",
            "target_weight": 0.0,
            "delta_weight": 0.0,
            "target_amount": 0.0,
            "suggested_amount": 0.0,
            "suggested_pct": 0.0,
            "reason_short": "当前既不满足回撤后反弹，也不满足强趋势突破，因此暂不开仓。",
        }
    )
    item["execution_trace"]["entry_checks"]["entry_allowed"] = False
    item["execution_trace"]["entry_checks"]["entry_channel"] = "none"
    item["execution_trace"]["final_action_calc"]["action_code"] = "no_trade"
    item["execution_trace"]["final_action_calc"]["action_reason"] = "当前既不满足回撤后反弹，也不满足强趋势突破，因此暂不开仓。"
    allocation = {
        "total_budget_pct": 0.8,
        "single_weight_cap": 0.35,
        "category_budget_caps": {"stock_etf": 0.6},
        "replace_threshold": 8.0,
        "candidate_summary": [],
        "selection_trace": {
            "510300": {
                "selected": True,
                "selected_reason": "已进入目标组合候选。",
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
                "cap_applied": False,
                "cap_reasons": [],
                "selected_for_allocation": True,
                "selected_reason": "已进入目标组合候选。",
                "blocked_reason": "",
                "replacement_trace": {},
            }
        },
        "overlay_rows": {"510300": {"symbol": "510300", "name": "沪深300ETF", "decision_category": "stock_etf", "global_rank": 1, "category_rank": 1}},
        "overlay_traces": {"510300": {"execution_trace": item["execution_trace"], "rationale": item["rationale"], "feature_snapshot": item["feature_snapshot"], "score_breakdown": item["score_breakdown"], "scores": item["scores"]}},
    }

    detail = _build_explanation(item=item, allocation=allocation)["items"][0]

    assert detail["primary_reason_stage"] == "execution_gate"
    assert detail["show_blocks"]["entry_checks"] is True


def test_blocked_before_candidate_allocation_prefers_allocation_reason():
    item = _sample_item()
    item.update({"action": "no_trade", "action_code": "no_trade", "reason_short": "总入选数量已达到上限。"})
    item["execution_trace"]["entry_checks"]["entry_allowed"] = False
    allocation = {
        "total_budget_pct": 0.8,
        "single_weight_cap": 0.35,
        "category_budget_caps": {"stock_etf": 0.6},
        "replace_threshold": 8.0,
        "candidate_summary": [],
        "selection_trace": {
            "510300": {
                "selected": False,
                "blocked_stage": "slot_limit",
                "blocked_reason": "总入选数量已达到上限。",
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
                "provisional_weight": 0.0,
                "normal_target_weight": 0.0,
                "cap_applied": False,
                "cap_reasons": [],
                "selected_for_allocation": False,
                "selected_reason": "",
                "blocked_reason": "总入选数量已达到上限。",
                "replacement_trace": {},
            }
        },
        "overlay_rows": {"510300": {"symbol": "510300", "name": "沪深300ETF", "decision_category": "stock_etf", "global_rank": 5, "category_rank": 2}},
        "overlay_traces": {"510300": {"execution_trace": item["execution_trace"], "rationale": item["rationale"], "feature_snapshot": item["feature_snapshot"], "score_breakdown": item["score_breakdown"], "scores": item["scores"]}},
    }

    detail = _build_explanation(item=item, allocation=allocation)["items"][0]

    assert detail["primary_reason_stage"] == "allocation"
    assert detail["show_blocks"]["entry_checks"] is False


def test_current_holding_reduce_prefers_position_state():
    item = _sample_item()
    item.update(
        {
            "action": "sell",
            "action_code": "sell_reduce",
            "intent": "reduce",
            "current_weight": 0.2,
            "target_weight": 0.1,
            "delta_weight": -0.1,
            "current_amount": 20000.0,
            "target_amount": 10000.0,
            "reason_short": "中期趋势走弱但未完全破坏，先把仓位降到正常目标的一半。",
        }
    )
    item["execution_trace"]["position_state"]["position_state"] = "REDUCE"
    item["execution_trace"]["position_state"]["position_state_label"] = "减仓观察"
    item["execution_trace"]["position_state"]["current_weight"] = 0.2
    item["execution_trace"]["position_state"]["reduced_target_weight"] = 0.1
    item["execution_trace"]["position_state"]["reason"] = "20日动量仍为正，但价格已回到20日均线下方，趋势转弱。"
    item["execution_trace"]["final_action_calc"]["action_code"] = "sell_reduce"
    item["execution_trace"]["final_action_calc"]["action_reason"] = item["reason_short"]
    allocation = {
        "total_budget_pct": 0.8,
        "single_weight_cap": 0.35,
        "category_budget_caps": {"stock_etf": 0.6},
        "replace_threshold": 8.0,
        "candidate_summary": [],
        "selection_trace": {"510300": {"selected": True, "selected_reason": "已持有并保留。", "blocked_reason": "", "protected": False, "protected_reasons": [], "meets_min_final_score": True, "min_final_score_for_target": 55.0}},
        "replacement_trace": {},
        "allocation_trace": {"510300": {"total_budget_pct": 0.8, "single_weight_cap": 0.35, "category_cap": 0.6, "provisional_weight": 0.2, "normal_target_weight": 0.2, "cap_applied": False, "cap_reasons": [], "selected_for_allocation": True, "selected_reason": "已持有并保留。", "blocked_reason": "", "replacement_trace": {}}},
        "overlay_rows": {"510300": {"symbol": "510300", "name": "沪深300ETF", "decision_category": "stock_etf", "global_rank": 3, "category_rank": 2}},
        "overlay_traces": {"510300": {"execution_trace": item["execution_trace"], "rationale": item["rationale"], "feature_snapshot": item["feature_snapshot"], "score_breakdown": item["score_breakdown"], "scores": item["scores"]}},
    }

    detail = _build_explanation(item=item, allocation=allocation, current_holdings=[{"symbol": "510300", "current_weight": 0.2, "current_amount": 20000.0, "hold_days": 8, "hold_days_known": True}])["items"][0]

    assert detail["primary_reason_stage"] == "position_state"
    assert detail["show_blocks"]["position_state"] is True


def test_money_etf_uses_defensive_wording():
    item = _sample_item()
    item.update(
        {
            "symbol": "511990",
            "name": "华宝添益",
            "category": "money_etf",
            "action": "buy",
            "action_code": "buy_open",
            "reason_short": "当前市场仍偏防守，外部风险资产没有形成更好的可执行机会，因此把这只货币ETF作为防守停泊仓位。",
            "feature_snapshot": {**_sample_item()["feature_snapshot"], "decision_category": "money_etf"},
        }
    )
    item["rationale"]["entry_channel_used"] = "none"
    item["execution_trace"]["entry_checks"]["entry_allowed"] = False
    allocation = {
        "total_budget_pct": 0.3,
        "single_weight_cap": 0.3,
        "category_budget_caps": {"money_etf": 0.3},
        "replace_threshold": 8.0,
        "candidate_summary": [],
        "selection_trace": {"511990": {"selected": True, "selected_reason": "作为防守停泊仓位进入组合。", "blocked_reason": "", "protected": False, "protected_reasons": [], "meets_min_final_score": True, "min_final_score_for_target": 55.0}},
        "replacement_trace": {},
        "allocation_trace": {"511990": {"total_budget_pct": 0.3, "single_weight_cap": 0.3, "category_cap": 0.3, "provisional_weight": 0.3, "normal_target_weight": 0.3, "cap_applied": False, "cap_reasons": [], "selected_for_allocation": True, "selected_reason": "作为防守停泊仓位进入组合。", "blocked_reason": "", "replacement_trace": {}}},
        "overlay_rows": {"511990": {"symbol": "511990", "name": "华宝添益", "decision_category": "money_etf", "global_rank": 1, "category_rank": 1}},
        "overlay_traces": {"511990": {"execution_trace": item["execution_trace"], "rationale": item["rationale"], "feature_snapshot": item["feature_snapshot"], "score_breakdown": item["score_breakdown"], "scores": item["scores"]}},
    }

    detail = _build_explanation(item=item, allocation=allocation)["items"][0]

    assert "防守" in detail["primary_reason_text"]
    assert "停泊" in detail["natural_language_summary"]


def test_replacement_context_uses_incumbent_hold_days_when_available():
    item = _sample_item()
    item.update({"symbol": "159915", "name": "创业板ETF", "action": "no_trade", "action_code": "no_trade", "reason_short": "旧持仓与新候选分差不足，暂不替换。"})
    replacement = {
        "incumbent_symbol": "510300",
        "candidate_symbol": "159915",
        "score_gap": 6.0,
        "replace_threshold": 8.0,
        "hold_days": 7,
        "hold_days_known": True,
        "replace_allowed": False,
        "blocked_reason": "旧持仓与新候选分差不足，暂不替换。",
    }
    allocation = {
        "total_budget_pct": 0.8,
        "single_weight_cap": 0.35,
        "category_budget_caps": {"stock_etf": 0.6},
        "replace_threshold": 8.0,
        "candidate_summary": [],
        "selection_trace": {"159915": {"selected": False, "blocked_stage": "replacement", "blocked_reason": "旧持仓与新候选分差不足，暂不替换。", "protected": False, "protected_reasons": [], "meets_min_final_score": True, "min_final_score_for_target": 55.0}},
        "replacement_trace": {"159915": replacement},
        "allocation_trace": {"159915": {"total_budget_pct": 0.8, "single_weight_cap": 0.35, "category_cap": 0.6, "provisional_weight": 0.0, "normal_target_weight": 0.0, "cap_applied": False, "cap_reasons": [], "selected_for_allocation": False, "selected_reason": "", "blocked_reason": "旧持仓与新候选分差不足，暂不替换。", "replacement_trace": replacement}},
        "overlay_rows": {"159915": {"symbol": "159915", "name": "创业板ETF", "decision_category": "stock_etf", "global_rank": 4, "category_rank": 2}},
        "overlay_traces": {"159915": {"execution_trace": item["execution_trace"], "rationale": item["rationale"], "feature_snapshot": item["feature_snapshot"], "score_breakdown": item["score_breakdown"], "scores": item["scores"]}},
    }

    detail = _build_explanation(item=item, allocation=allocation)["items"][0]

    assert detail["comparison"]["hold_days"] == 7
    assert detail["comparison"]["hold_days_known"] is True


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
