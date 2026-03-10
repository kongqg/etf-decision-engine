from __future__ import annotations

import json
from typing import Any

import pandas as pd

from app.core.session_mode import SESSION_MODE_HINTS
from app.services.decision_policy_service import get_decision_policy_service


class ExplanationEngine:
    def __init__(self) -> None:
        self.policy = get_decision_policy_service()

    def build(
        self,
        advice: dict[str, Any],
        scored_df: pd.DataFrame,
        filtered_df: pd.DataFrame,
        portfolio_summary: dict[str, Any],
        market_snapshot: dict[str, Any],
        plan: dict[str, Any],
    ) -> dict[str, Any]:
        recommendation_groups = plan["recommendation_groups"]
        primary_item = plan.get("primary_item")
        transition_plan = plan.get("transition_plan", [])
        portfolio_review_items = plan.get("portfolio_review_items", [])
        target_portfolio = plan.get("target_portfolio", {})
        raw = market_snapshot.get("raw", {})
        source = raw.get("source", {})
        quality_summary = raw.get("quality_summary", {})
        category_scores = plan.get("category_scores", [])
        action_counts = plan.get("action_counts", {})

        overall = {
            "mode": advice["session_mode"],
            "headline": advice["summary_text"],
            "market_regime": advice["market_regime"],
            "reasons": self._overall_reasons(plan, primary_item, category_scores),
            "risks": [
                advice["risk_text"],
                "系统只给辅助建议，不会自动下单；真实成交价格、滑点和执行纪律仍然决定结果。",
            ],
            "evidence": self._overall_evidence(plan, primary_item, portfolio_summary),
            "source_info": self._source_info(source, quality_summary),
            "source_note": source.get("note", ""),
            "session_hint": SESSION_MODE_HINTS[advice["session_mode"]],
            "execution_rule": self._execution_rule(advice, plan, primary_item),
            "category_scores": category_scores,
            "winning_category_explanation": self._winning_category_explanation(plan, category_scores),
            "target_portfolio": target_portfolio,
            "portfolio_transition": self._portfolio_transition_summary(target_portfolio, transition_plan),
            "portfolio_review_items": portfolio_review_items,
            "transition_plan": transition_plan,
            "daily_action_plan": transition_plan,
            "action_counts": action_counts,
            "executable_recommendations": recommendation_groups["executable_recommendations"],
            "watchlist_recommendations": recommendation_groups["watchlist_recommendations"],
        }

        items = []
        item_payloads = transition_plan or [
            *recommendation_groups["executable_recommendations"],
            *recommendation_groups["watchlist_recommendations"],
        ]
        for payload in item_payloads:
            row = scored_df[scored_df["symbol"] == payload["symbol"]]
            if row.empty:
                continue
            scored_row = row.iloc[0].to_dict()
            breakdown = self._parse_breakdown(scored_row.get("breakdown_json"))
            category_breakdown = breakdown.get("category_breakdown", {})
            items.append(
                {
                    "symbol": payload["symbol"],
                    "title": f"{payload['name']} 当前为什么是这个动作",
                    "summary": payload.get("action_reason") or payload.get("execution_note") or payload["reason_short"],
                    "category": payload["category"],
                    "category_label": payload["asset_class"],
                    "category_breakdown": category_breakdown,
                    "score_breakdown": {
                        "entry_score": payload["entry_score"],
                        "hold_score": payload["hold_score"],
                        "exit_score": payload["exit_score"],
                        "decision_score": payload["decision_score"],
                        "entry_details": breakdown.get("entry_breakdown", []),
                        "hold_details": breakdown.get("hold_breakdown", []),
                        "exit_details": breakdown.get("exit_breakdown", []),
                    },
                    "why_selected": self._why_selected(payload, breakdown),
                    "execution": {
                        "action": payload["action"],
                        "action_code": payload["action_code"],
                        "position_action": payload.get("position_action", ""),
                        "position_action_label": payload.get("position_action_label", payload["action"]),
                        "action_reason": payload.get("action_reason", payload.get("execution_note", "")),
                        "tradability_mode": payload["trade_mode"],
                        "executable_now": payload["executable_now"],
                        "blocked_reason": payload["blocked_reason"],
                        "planned_exit_days": payload.get("planned_exit_days"),
                        "planned_exit_rule_summary": payload.get("planned_exit_rule_summary", ""),
                    },
                    "position_context": {
                        "is_current_holding": bool(payload.get("is_current_holding")),
                        "current_weight": float(payload.get("current_weight", 0.0)),
                        "target_weight": float(payload.get("target_weight", 0.0)),
                        "delta_weight": float(payload.get("delta_weight", 0.0)),
                        "current_amount": float(payload.get("current_amount", 0.0)),
                        "target_amount": float(payload.get("target_amount", 0.0)),
                        "current_return_pct": float(payload.get("current_return_pct", 0.0)),
                        "days_held": int(payload.get("days_held", 0) or 0),
                        "rank_drop": int(payload.get("rank_drop", 0) or 0),
                        "entry_eligible": bool(payload.get("entry_eligible", True)),
                        "filter_reasons": list(payload.get("filter_reasons", [])),
                        "transition_label": payload.get("transition_label", ""),
                    },
                    "holding_profile": {
                        "target_holding_days": payload["target_holding_days"],
                        "mapped_horizon_profile": payload["mapped_horizon_profile"],
                        "horizon_profile_label": payload["horizon_profile_label"],
                        "lifecycle_phase": payload["lifecycle_phase"],
                    },
                    "proofs": [
                        {"label": "类别得分", "value": f"{payload['category_score']:.1f}"},
                        {"label": "入场分", "value": f"{payload['entry_score']:.1f}"},
                        {"label": "持有分", "value": f"{payload['hold_score']:.1f}"},
                        {"label": "退出分", "value": f"{payload['exit_score']:.1f}"},
                        {"label": "决策分", "value": f"{payload['decision_score']:.1f}"},
                        {"label": "当前权重", "value": f"{payload.get('current_weight', 0.0) * 100:.1f}%"},
                        {"label": "目标权重", "value": f"{payload.get('target_weight', 0.0) * 100:.1f}%"},
                        {"label": "权重变化", "value": f"{payload.get('delta_weight', 0.0) * 100:.1f}%"},
                        {"label": "近10日动量", "value": f"{float(scored_row['momentum_10d']):.2f}%"},
                        {"label": "趋势强度", "value": f"{float(scored_row['trend_strength']):.2f}%"},
                        {"label": "相对强弱", "value": f"{float(scored_row['relative_strength_10d']):.2f}%"},
                        {"label": "10日波动", "value": f"{float(scored_row['volatility_10d']):.2f}%"},
                        {"label": "20日回撤", "value": f"{float(scored_row['drawdown_20d']):.2f}%"},
                    ],
                }
            )

        return {"overall": overall, "items": items}

    def _overall_reasons(
        self,
        plan: dict[str, Any],
        primary_item: dict[str, Any] | None,
        category_scores: list[dict[str, Any]],
    ) -> list[str]:
        reasons = ["系统这次不是只看单只ETF，而是先生成目标组合，再决定当前组合如何过渡过去。"]
        if category_scores:
            top_category = category_scores[0]
            reasons.append(
                f"这次先按类别比较，当前领先的是 {top_category['category_label']}，类别得分 {top_category['category_score']:.1f}。"
            )
        if plan.get("portfolio_review_items"):
            reasons.append("所有当前持仓都会继续参加正式评估，即使它们本轮不适合继续新开仓，也会得到持有、减仓或退出判断。")
        if primary_item is not None:
            reasons.append(
                f"在 {primary_item['asset_class']} 内部，{primary_item['name']} 的决策分 {primary_item['decision_score']:.1f}，"
                f"对应 {primary_item['horizon_profile_label']} 的 {self._phase_label(primary_item['lifecycle_phase'])}。"
            )
            if primary_item["blocked_reason"]:
                reasons.append(f"系统给出了动作，但当前不能立刻执行，原因是 {primary_item['blocked_reason']}。")
            else:
                reasons.append(f"当前动作是 {primary_item['action']}，因为它最符合目标组合方向，而且入场分、持有分和退出分的组合最有利。")
        elif plan["action_code"] == "no_trade":
            reasons.append("这次没有任何进攻类别达到风险调整后的最低优势阈值，所以先不新增交易。")
        return reasons

    def _overall_evidence(
        self,
        plan: dict[str, Any],
        primary_item: dict[str, Any] | None,
        portfolio_summary: dict[str, Any],
    ) -> list[dict[str, str]]:
        facts = plan["facts"]
        evidence = [
            {"label": "当前仓位", "value": f"{portfolio_summary['current_position_pct'] * 100:.1f}%"},
            {"label": "目标仓位", "value": f"{plan['target_position_pct'] * 100:.1f}%"},
            {"label": "目标持有天数", "value": str(facts.get("target_holding_days", 0))},
            {"label": "胜出类别", "value": plan.get("winning_category_label", "-") or "-"},
            {"label": "胜出类别得分", "value": f"{plan['selected_category_score']:.1f}"},
            {"label": "是否可立即执行", "value": "是" if plan["executable_now"] else "否"},
            {
                "label": "目标组合模式",
                "value": {
                    "offensive": "进攻配置",
                    "defensive": "防守停车",
                    "no_trade": "仅复核不新增",
                }.get(str(facts.get("target_portfolio_mode", "no_trade")), "仅复核不新增"),
            },
            {"label": "过渡动作数", "value": str(facts.get("transition_count", 0))},
            {"label": "持仓复核数", "value": str(facts.get("holding_review_count", 0))},
        ]
        if primary_item is not None:
            evidence.extend(
                [
                    {"label": "映射周期", "value": primary_item["horizon_profile_label"]},
                    {"label": "生命周期", "value": self._phase_label(primary_item["lifecycle_phase"])},
                    {"label": "动作", "value": primary_item["action"]},
                    {"label": "标的决策分", "value": f"{primary_item['decision_score']:.1f}"},
                ]
            )
        return evidence

    def _winning_category_explanation(self, plan: dict[str, Any], category_scores: list[dict[str, Any]]) -> dict[str, Any]:
        if not category_scores:
            return {}
        top = category_scores[0]
        runner_up = category_scores[1] if len(category_scores) > 1 else None
        return {
            "winner": top,
            "runner_up": runner_up,
            "why": (
                f"{top['category_label']} 当前类别分最高。"
                if runner_up is None
                else f"{top['category_label']} 比第二名 {runner_up['category_label']} 高 {top['category_score'] - runner_up['category_score']:.1f} 分。"
            ),
        }

    def _execution_rule(self, advice: dict[str, Any], plan: dict[str, Any], primary_item: dict[str, Any] | None) -> dict[str, Any]:
        facts = plan["facts"]
        result_action = advice.get("action") or self.policy.action_label(str(plan.get("action_code", "no_trade")))
        return {
            "rule": "先决定目标组合属于哪个类别，再看每只标的相对当前持仓是该增、该减还是继续持有；最后再按 T+0/T+1、预算和时段限制决定能否立即执行。",
            "threshold": facts["open_threshold"],
            "top_score": float(primary_item["decision_score"]) if primary_item else 0.0,
            "result": result_action,
            "executable_now": plan["executable_now"],
            "blocked_reason": plan.get("blocked_reason", ""),
            "target_holding_days": facts.get("target_holding_days", 0),
            "selected_category_score": facts.get("selected_category_score", 0.0),
        }

    def _why_selected(self, payload: dict[str, Any], breakdown: dict[str, Any]) -> list[str]:
        category_breakdown = breakdown.get("category_breakdown", {})
        reasons = [
            f"{payload['asset_class']} 这条赛道当前类别得分 {payload['category_score']:.1f}。",
            f"这只 ETF 的入场分 {payload['entry_score']:.1f}、持有分 {payload['hold_score']:.1f}、退出分 {payload['exit_score']:.1f}。",
            f"映射周期是 {payload['horizon_profile_label']}，当前位于 {self._phase_label(payload['lifecycle_phase'])}。",
        ]
        if payload.get("is_current_holding"):
            reasons.append(
                f"你当前已经持有它，现有权重 {payload.get('current_weight', 0.0) * 100:.1f}%，目标权重 {payload.get('target_weight', 0.0) * 100:.1f}%。"
            )
        elif payload.get("target_weight", 0.0) > 0:
            reasons.append(f"它被纳入目标组合，目标权重 {payload.get('target_weight', 0.0) * 100:.1f}%。")
        if payload.get("is_current_holding") and not payload.get("entry_eligible", True):
            filter_reasons = payload.get("filter_reasons", [])
            filter_summary = "、".join(filter_reasons) if filter_reasons else "它本轮不适合继续新开仓"
            reasons.append(f"虽然它本轮不再适合新增仓位，但因为你已持有，所以仍要继续评估去留。筛选限制：{filter_summary}。")
        if payload.get("delta_weight", 0.0) > 0.01:
            reasons.append("目标组合希望提高这只标的的配置权重。")
        elif payload.get("delta_weight", 0.0) < -0.01:
            reasons.append("目标组合希望降低这只标的的配置权重。")
        if category_breakdown:
            raw_metrics = category_breakdown.get("raw_metrics", {})
            reasons.append(
                "类别层主要看 10 日动量、趋势、广度、波动和回撤。"
                f" 当前原始动量 {raw_metrics.get('category_momentum', 0):.2f}，趋势 {raw_metrics.get('category_trend', 0):.2f}。"
            )
        if payload["blocked_reason"]:
            reasons.append(f"动作已生成，但当前被 {payload['blocked_reason']} 阻塞。")
        return reasons

    def _portfolio_transition_summary(
        self,
        target_portfolio: dict[str, Any],
        transition_plan: list[dict[str, Any]],
    ) -> dict[str, Any]:
        rows = []
        for item in transition_plan:
            rows.append(
                {
                    "symbol": item["symbol"],
                    "name": item["name"],
                    "action": item["action"],
                    "execution_status": item["execution_status"],
                    "current_weight": float(item.get("current_weight", 0.0)),
                    "target_weight": float(item.get("target_weight", 0.0)),
                    "delta_weight": float(item.get("delta_weight", 0.0)),
                    "current_amount": float(item.get("current_amount", 0.0)),
                    "target_amount": float(item.get("target_amount", 0.0)),
                    "transition_label": item.get("transition_label", ""),
                    "executable_now": bool(item.get("executable_now", False)),
                    "blocked_reason": item.get("blocked_reason", ""),
                }
            )
        return {
            "mode": target_portfolio.get("mode", "no_trade"),
            "notes": target_portfolio.get("notes", []),
            "rows": rows,
        }

    def _source_info(self, source: dict[str, Any], quality_summary: dict[str, Any]) -> list[dict[str, str]]:
        return [
            {"label": "数据来源", "value": source.get("label", "-")},
            {"label": "数据类型", "value": source.get("data_type", quality_summary.get("data_type", "-"))},
            {"label": "验证状态", "value": quality_summary.get("verification_status", "-")},
            {"label": "最新可用日期", "value": quality_summary.get("latest_available_date", source.get("trade_date", "-"))},
            {"label": "是否支持实时执行", "value": "是" if quality_summary.get("supports_live_execution") else "否"},
        ]

    def _phase_label(self, phase: str) -> str:
        return {
            "build_phase": "建仓阶段",
            "hold_phase": "持有阶段",
            "exit_phase": "退出阶段",
        }.get(phase, phase)

    def _parse_breakdown(self, value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                payload = json.loads(value)
            except json.JSONDecodeError:
                return {}
            if isinstance(payload, dict):
                return payload
        return {}
