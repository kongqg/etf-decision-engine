from __future__ import annotations

from app.web.pages import templates
from app.web.presenters import page_context


def _render_explanation(explanation: dict) -> str:
    templates.env.cache.clear()
    template = templates.env.get_template("explanation.html")
    original_url_for = templates.env.globals.get("url_for")
    templates.env.globals["url_for"] = lambda *args, **kwargs: "/static/mock"
    try:
        context = page_context("原因说明", "closed")
        context.update(
            {
                "request": object(),
                "data_status": None,
                "advice": {"id": 99, "summary_text": "测试建议摘要"},
                "explanation": explanation,
            }
        )
        return template.render(**context)
    finally:
        if original_url_for is not None:
            templates.env.globals["url_for"] = original_url_for


def test_explanation_page_renders_key_sections():
    explanation = {
        "overall": {
            "headline": "开仓或加仓目标 ETF",
            "market_regime": "偏进攻",
            "reasons": ["测试原因"],
            "portfolio": {"current_position_pct": 0.2, "cash_balance": 80000.0},
            "budget": {"total_budget_pct": 0.8, "single_weight_cap": 0.35},
            "candidate_summary": [
                {"symbol": "510300", "name": "沪深300ETF", "category": "股票ETF", "selected": True, "final_score": 75.1, "intra_score": 76.9, "category_score": 70.8, "global_rank": 1, "category_rank": 1}
            ],
        },
        "items": [
            {
                "symbol": "510300",
                "summary": "满足通道A，允许新开仓。",
                "category": "股票ETF",
                "summary_card": {
                    "name": "沪深300ETF",
                    "symbol": "510300",
                    "decision_category": "stock_etf",
                    "decision_category_label": "股票ETF",
                    "final_action_label": "开仓买入",
                    "effective_target_weight": 0.2,
                    "decision_score": 79.0,
                    "final_score": 75.1,
                    "entry_channel_label": "通道A：回撤后反弹",
                    "market_regime": "偏进攻",
                },
                "primary_reason_stage_label": "最终动作层",
                "primary_reason_text": "评分层和分仓层都通过了，而且执行层允许今天进场，因此形成正式买入动作。",
                "decision_ladder": [
                    {"title": "评分层", "status": "pass", "summary": "最终分 75.1，最低候选阈值 55.0，全局排名 1，类别排名 1。"},
                    {"title": "候选与分仓层", "status": "pass", "summary": "拿到 normal_target_weight 20.0% 的理论目标仓位。"},
                    {"title": "执行门禁层", "status": "pass", "summary": "入场通道 通道A：回撤后反弹 通过，因此允许执行层继续开仓/加仓。"},
                    {"title": "持仓管理层", "status": "info", "summary": "当前未持有，因此这一层不参与裁决。"},
                    {"title": "最终动作层", "status": "pass", "summary": "当前仓位 0.0%，执行仓位 20.0%，最终动作是开仓买入。"},
                ],
                "show_blocks": {
                    "feature_snapshot": True,
                    "score_breakdown": True,
                    "allocation_trace": True,
                    "replacement_trace": False,
                    "entry_checks": True,
                    "position_state": False,
                    "switch_checks": False,
                    "target_weight_adjustment": True,
                    "final_action_calc": True,
                    "head_formulas": False,
                },
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
                    "tradability_mode": "t1",
                },
                "intra_score_breakdown": {
                    "formula": "单票分 = ...",
                    "available": True,
                    "intra_score": 76.9,
                    "components": [{"label": "20日动量分位", "weight": 0.3, "rank_value": 80.0, "contribution": 24.0, "formula_text": "0.300 × 80.00 = 24.00"}],
                },
                "category_score_breakdown": {
                    "formula": "类别分 = ...",
                    "available": True,
                    "category_score": 70.8,
                    "top_mean_intrascore": 76.0,
                    "breadth_score": 64.0,
                    "category_momentum_score": 68.0,
                    "components": [{"label": "类别头部平均单票分", "weight": 0.5, "value": 76.0, "contribution": 38.0, "formula_text": "0.500 × 76.00 = 38.00"}],
                },
                "final_score_breakdown": {
                    "formula": "最终分 = ...",
                    "final_score": 75.1,
                    "decision_score": 79.0,
                    "global_rank": 1,
                    "category_rank": 1,
                    "minimum_candidate_threshold": 55.0,
                    "meets_minimum_candidate_threshold": True,
                    "entered_candidate_pool": True,
                    "eliminated_stage": "",
                    "eliminated_reason": "",
                    "components": [{"label": "单票分", "weight": 0.7, "value": 76.9, "contribution": 53.83, "formula_text": "0.700 × 76.90 = 53.83"}],
                },
                "allocation_trace": {
                    "total_budget_pct": 0.8,
                    "single_weight_cap": 0.35,
                    "category_cap": 0.6,
                    "provisional_weight": 0.22,
                    "normal_target_weight": 0.2,
                    "cap_applied": True,
                    "cap_reasons": ["单票上限截断"],
                    "protected": False,
                    "protected_reasons": [],
                    "selected_for_allocation": True,
                    "selected_reason": "通过筛选",
                    "blocked_reason": "",
                    "replacement_trace": {},
                },
                "execution_trace": {
                    "entry_checks": {
                        "entry_allowed": True,
                        "channel_a": {"trend_filter_pass": True, "pullback_zone_pass": True, "rebound_confirmation_pass": True, "channel_a_pass": True},
                        "channel_b": {"trend_filter_pass": True, "drawdown_near_high_pass": False, "momentum_5d_pass": True, "close_above_ma5_pass": True, "entry_score_pass": True, "volatility_guard_pass": True, "channel_b_pass": False},
                        "reason_steps": [
                            {"condition": "趋势过滤：momentum_20d > 0 且 close_price > ma20（当前 8.90，4.213 vs 3.921）", "passed": True, "passed_label": "满足", "meaning": "只有中期动量仍为正，且价格站在20日均线上方，才允许新的多头买入。", "conclusion": False},
                            {"condition": "最终入场结论：entry_channel = A，entry_allowed = True", "passed": True, "passed_label": "满足", "meaning": "最终只要通道A或通道B任意一个通过，就允许新开仓或加仓。", "conclusion": True},
                        ],
                    },
                    "position_state": {"current_weight": 0.0, "position_state_label": "未持有", "reduced_target_weight": 0.0, "reason": "当前没有持仓，因此只进行入场可行性判断。", "reason_steps": [{"condition": "当前仓位 = 0", "passed": True, "passed_label": "满足", "meaning": "当前没有持仓，因此这一层只做状态说明，不进入持仓管理动作。", "conclusion": True}]},
                    "switch_checks": {"old_state": "NONE", "new_entry_allowed": True, "new_target_weight": 0.2, "rebalance_band": 0.05, "score_gap": 0.0, "switch_allowed": False, "reason_steps": [{"condition": "本次没有进入同类别换仓判断", "passed": True, "passed_label": "满足", "meaning": "没有同类别旧持仓与新龙头形成直接替换关系。", "conclusion": True}]},
                    "target_weight_adjustment": {"normal_target_weight": 0.2, "current_weight": 0.0, "reduced_target_weight": 0.0, "effective_target_weight": 0.2, "branch": "open_or_add_to_normal", "reason_steps": [{"condition": "采用分支 = open_or_add_to_normal", "passed": True, "passed_label": "满足", "meaning": "入场条件成立，因此执行仓位直接采用 normal_target_weight。", "conclusion": False}]},
                    "final_action_calc": {"current_weight": 0.0, "effective_target_weight": 0.2, "delta_weight": 0.2, "total_asset": 100000.0, "target_amount": 20000.0, "delta_amount": 20000.0, "min_trade_amount": 1000.0, "rebalance_band": 0.05, "min_trade_blocked": False, "action_reason": "满足通道A，允许新开仓。", "reason_steps": [{"condition": "entry_allowed = True，target_gap = 20.00% > rebalance_band 5.00%", "passed": True, "passed_label": "满足", "meaning": "只有允许入场，而且目标仓位明显高于当前仓位，才值得真正开仓或加仓。", "conclusion": False}, {"condition": "因此最终动作 = buy_open", "passed": True, "passed_label": "满足", "meaning": "前面的仓位状态、目标仓位和交易门槛一起决定了最终动作。", "conclusion": True}]},
                },
                "natural_language_summary": "该 ETF 最终被选中，是因为最终分和执行决策分都靠前。",
                "weights": {"current_weight": 0.0, "normal_target_weight": 0.2, "effective_target_weight": 0.2},
            }
        ],
    }

    html = _render_explanation(explanation)

    assert "顶部摘要卡" in html
    assert "决策阶梯" in html
    assert "主原因：" in html
    assert "单票分拆解" in html
    assert "仓位分配卡" in html
    assert "最终动作卡" in html
    assert "510300" in html
    assert "momentum_20d &gt; 0" in html
    assert "因此最终动作 = buy_open" in html
    assert "换仓判断" not in html


def test_explanation_page_gracefully_degrades_for_legacy_payload():
    explanation = {
        "overall": {
            "headline": "今天暂不交易",
            "market_regime": "中性",
            "reasons": ["旧记录测试"],
            "portfolio": {"current_position_pct": 0.0, "cash_balance": 1000.0},
            "budget": {"total_budget_pct": 0.0, "single_weight_cap": 0.0},
            "candidate_summary": [],
        },
        "items": [
            {
                "symbol": "518880",
                "summary": "旧记录只有一句说明。",
                "summary_card": {
                    "name": "黄金ETF",
                    "symbol": "518880",
                    "decision_category": "gold_etf",
                    "decision_category_label": "黄金ETF",
                    "final_action_label": "暂不交易",
                    "effective_target_weight": 0.0,
                    "decision_score": 0.0,
                    "final_score": 61.0,
                    "entry_channel_label": "无",
                    "market_regime": "中性",
                },
                "primary_reason_stage_label": "最终动作层",
                "primary_reason_text": "旧记录未保存完整链路，但仍可查看结论。",
                "decision_ladder": [],
                "show_blocks": {
                    "feature_snapshot": False,
                    "score_breakdown": True,
                    "allocation_trace": False,
                    "replacement_trace": False,
                    "entry_checks": False,
                    "position_state": False,
                    "switch_checks": False,
                    "target_weight_adjustment": False,
                    "final_action_calc": True,
                    "head_formulas": False,
                },
                "feature_snapshot": {"tradability_mode": "t1"},
                "intra_score_breakdown": {"formula": "单票分 = ...", "available": False, "intra_score": 0.0, "components": []},
                "category_score_breakdown": {"formula": "类别分 = ...", "available": False, "category_score": 0.0, "top_mean_intrascore": 0.0, "breadth_score": 0.0, "category_momentum_score": 0.0, "components": []},
                "final_score_breakdown": {"formula": "最终分 = ...", "final_score": 61.0, "decision_score": 0.0, "global_rank": 0, "category_rank": 0, "minimum_candidate_threshold": 55.0, "meets_minimum_candidate_threshold": True, "entered_candidate_pool": False, "eliminated_stage": "", "eliminated_reason": "", "components": []},
                "allocation_trace": {"total_budget_pct": 0.0, "single_weight_cap": 0.0, "category_cap": 0.0, "provisional_weight": 0.0, "normal_target_weight": 0.0, "cap_applied": False, "cap_reasons": [], "protected": False, "protected_reasons": [], "selected_for_allocation": False, "selected_reason": "", "blocked_reason": "", "replacement_trace": {}},
                "execution_trace": {"entry_checks": {}, "position_state": {}, "switch_checks": {}, "target_weight_adjustment": {}, "final_action_calc": {}},
                "natural_language_summary": "旧记录未保存完整链路，但仍可查看结论。",
                "weights": {"current_weight": 0.0, "normal_target_weight": 0.0, "effective_target_weight": 0.0},
            }
        ],
    }

    html = _render_explanation(explanation)

    assert "旧记录未保存这一层的分项贡献" in html


def test_money_etf_page_uses_defensive_wording():
    explanation = {
        "overall": {
            "headline": "当前偏防守",
            "market_regime": "偏防守",
            "reasons": ["测试原因"],
            "portfolio": {"current_position_pct": 0.3, "cash_balance": 70000.0},
            "budget": {"total_budget_pct": 0.3, "single_weight_cap": 0.3},
            "candidate_summary": [],
        },
        "items": [
            {
                "symbol": "511990",
                "summary": "作为防守停泊仓位处理。",
                "summary_card": {
                    "name": "华宝添益",
                    "symbol": "511990",
                    "decision_category": "money_etf",
                    "decision_category_label": "货币ETF",
                    "final_action_label": "开仓买入",
                    "effective_target_weight": 0.3,
                    "decision_score": 61.0,
                    "final_score": 66.0,
                    "entry_channel_label": "无",
                    "market_regime": "偏防守",
                },
                "primary_reason_stage_label": "最终动作层",
                "primary_reason_text": "当前市场仍偏防守，外部风险资产没有形成更好的可执行机会，因此把这只货币ETF作为防守停泊仓位。",
                "decision_ladder": [{"title": "执行门禁层", "status": "info", "summary": "货币ETF按防守停泊逻辑解释，主因不是突破买点，而是外部机会成本和市场防守状态。"}],
                "show_blocks": {
                    "feature_snapshot": False,
                    "score_breakdown": False,
                    "allocation_trace": True,
                    "replacement_trace": False,
                    "entry_checks": False,
                    "position_state": False,
                    "switch_checks": False,
                    "target_weight_adjustment": True,
                    "final_action_calc": True,
                    "head_formulas": False,
                },
                "feature_snapshot": {"tradability_mode": "t0"},
                "intra_score_breakdown": {"formula": "单票分 = ...", "available": False, "intra_score": 0.0, "components": []},
                "category_score_breakdown": {"formula": "类别分 = ...", "available": False, "category_score": 0.0, "top_mean_intrascore": 0.0, "breadth_score": 0.0, "category_momentum_score": 0.0, "components": []},
                "final_score_breakdown": {"formula": "最终分 = ...", "final_score": 66.0, "decision_score": 61.0, "global_rank": 1, "category_rank": 1, "minimum_candidate_threshold": 55.0, "meets_minimum_candidate_threshold": True, "entered_candidate_pool": True, "eliminated_stage": "", "eliminated_reason": "", "components": []},
                "allocation_trace": {"total_budget_pct": 0.3, "single_weight_cap": 0.3, "category_cap": 0.3, "provisional_weight": 0.3, "normal_target_weight": 0.3, "cap_applied": False, "cap_reasons": [], "protected": False, "protected_reasons": [], "selected_for_allocation": True, "selected_reason": "作为防守停泊仓位进入组合。", "blocked_reason": "", "replacement_trace": {}},
                "execution_trace": {"entry_checks": {}, "position_state": {}, "switch_checks": {}, "target_weight_adjustment": {"reason_steps": []}, "final_action_calc": {"current_weight": 0.0, "effective_target_weight": 0.3, "delta_weight": 0.3, "total_asset": 100000.0, "target_amount": 30000.0, "delta_amount": 30000.0, "min_trade_amount": 1000.0, "rebalance_band": 0.05, "min_trade_blocked": False, "action_reason": "作为防守停泊仓位执行。", "reason_steps": []}},
                "natural_language_summary": "这只货币ETF这次承担的是防守停泊角色。",
                "weights": {"current_weight": 0.0, "normal_target_weight": 0.3, "effective_target_weight": 0.3},
            }
        ],
    }

    html = _render_explanation(explanation)

    assert "防守停泊仓位" in html
    assert "通道B：强趋势突破" not in html
