from __future__ import annotations

from typing import Any

from app.core.config import get_settings, load_yaml_config
from app.services.rulebook_service import RulebookService

ACTION_CODE_LABELS = {
    "buy_open": "开仓买入",
    "buy_add": "继续加仓",
    "hold": "继续持有",
    "sell_reduce": "减仓卖出",
    "sell_exit": "卖出退出",
    "switch": "同类换仓",
    "no_trade": "暂不交易",
}

ENTRY_CHANNEL_LABELS = {
    "none": "无",
    "A": "通道A：回撤后反弹",
    "B": "通道B：强趋势突破",
}

POSITION_STATE_LABELS = {
    "HOLD": "继续持有",
    "REDUCE": "减仓观察",
    "EXIT": "退出",
    "NONE": "未持有",
}

STAGE_LABELS = {
    "final_score": "最终分阈值层",
    "basic_filter": "基础过滤层",
    "slot_limit": "组合名额层",
    "replacement": "替换门槛层",
}

INTRA_COMPONENTS = [
    ("momentum_20d_rank", "20日动量分位", 0.30),
    ("momentum_10d_rank", "10日动量分位", 0.20),
    ("momentum_5d_rank", "5日动量分位", 0.10),
    ("trend_rank", "趋势强度分位", 0.15),
    ("liquidity_rank", "流动性分位", 0.10),
    ("volatility_rank", "20日波动友好度分位", 0.075),
    ("drawdown_rank", "20日回撤友好度分位", 0.075),
]

CATEGORY_COMPONENTS = [
    ("top_mean_intrascore", "类别头部平均单票分", 0.50),
    ("breadth_score", "类别广度分", 0.30),
    ("category_momentum_score", "类别动量分", 0.20),
]

FINAL_COMPONENTS = [
    ("intra_score", "单票分", 0.70),
    ("category_score", "类别分", 0.30),
]


class ExplanationTraceService:
    def __init__(self) -> None:
        settings = get_settings()
        self.scoring_config = load_yaml_config(settings.config_dir / "strategy_scoring.yaml")
        self.min_final_score = float(self.scoring_config.get("selection", {}).get("min_final_score_for_target", 55.0))
        self.rulebook_service = RulebookService()

    def build_item_payloads(
        self,
        *,
        market_regime: dict[str, Any],
        allocation: dict[str, Any],
        items: list[dict[str, Any]],
        candidate_summary: list[dict[str, Any]],
        current_holdings: list[dict[str, Any]],
        preferences: Any | None = None,
    ) -> list[dict[str, Any]]:
        item_by_symbol = {str(item["symbol"]): dict(item) for item in items}
        current_by_symbol = {str(row["symbol"]): dict(row) for row in current_holdings}
        overlay_rows = {
            str(symbol): dict(payload)
            for symbol, payload in (allocation.get("overlay_rows", {}) or {}).items()
            if isinstance(payload, dict)
        }
        overlay_traces = {
            str(symbol): dict(payload)
            for symbol, payload in (allocation.get("overlay_traces", {}) or {}).items()
            if isinstance(payload, dict)
        }
        allocation_trace = {
            str(symbol): dict(payload)
            for symbol, payload in (allocation.get("allocation_trace", {}) or {}).items()
            if isinstance(payload, dict)
        }
        selection_trace = {
            str(symbol): dict(payload)
            for symbol, payload in (allocation.get("selection_trace", {}) or {}).items()
            if isinstance(payload, dict)
        }
        replacement_trace = {
            str(symbol): dict(payload)
            for symbol, payload in (allocation.get("replacement_trace", {}) or {}).items()
            if isinstance(payload, dict)
        }

        ordered_symbols: list[str] = []
        for item in items:
            symbol = str(item["symbol"])
            if symbol not in ordered_symbols:
                ordered_symbols.append(symbol)
        for row in candidate_summary:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol", ""))
            if symbol and symbol not in ordered_symbols:
                ordered_symbols.append(symbol)

        payloads: list[dict[str, Any]] = []
        for symbol in ordered_symbols:
            action_item = item_by_symbol.get(symbol, {})
            row = overlay_rows.get(symbol, {})
            overlay_trace = overlay_traces.get(symbol, {})
            current = current_by_symbol.get(symbol, {})
            selection = selection_trace.get(symbol, {})
            replacement = replacement_trace.get(symbol, {})
            allocation_row = allocation_trace.get(symbol, {})
            score_breakdown = self._score_breakdown(action_item, overlay_trace, row)
            feature_snapshot = self._feature_snapshot(action_item, overlay_trace, row, score_breakdown)
            score_payload = self._scores(action_item, overlay_trace, row, score_breakdown)
            weight_payload = self._weights(action_item, overlay_trace, current, allocation_row)
            comparison = self._comparison(action_item, overlay_trace, replacement, allocation)
            action_code = str(action_item.get("action_code", overlay_trace.get("action_code", "no_trade")))
            action_bucket = str(action_item.get("action", overlay_trace.get("action", "no_trade")))
            summary_card = {
                "symbol": symbol,
                "name": str(action_item.get("name", row.get("name", selection.get("name", symbol)))),
                "decision_category": str(action_item.get("category", row.get("decision_category", selection.get("decision_category", "")))),
                "final_action": action_code,
                "final_action_label": ACTION_CODE_LABELS.get(action_code, action_code),
                "effective_target_weight": float(weight_payload.get("target_weight", 0.0)),
                "decision_score": float(score_payload.get("decision_score", score_payload.get("final_score", 0.0))),
                "final_score": float(score_payload.get("final_score", 0.0)),
                "entry_channel": str(self._nested(overlay_trace, "execution_trace", "entry_checks", "entry_channel") or action_item.get("rationale", {}).get("entry_channel_used", "none")),
                "entry_channel_label": ENTRY_CHANNEL_LABELS.get(
                    str(self._nested(overlay_trace, "execution_trace", "entry_checks", "entry_channel") or action_item.get("rationale", {}).get("entry_channel_used", "none")),
                    str(self._nested(overlay_trace, "execution_trace", "entry_checks", "entry_channel") or action_item.get("rationale", {}).get("entry_channel_used", "none")),
                ),
                "market_regime": str(market_regime.get("market_regime", "")),
            }
            intra_score_breakdown = self._intra_score_breakdown(score_breakdown, score_payload)
            category_score_breakdown = self._category_score_breakdown(score_breakdown, score_payload)
            final_score_breakdown = self._final_score_breakdown(
                score_payload=score_payload,
                selection=selection,
                allocation_row=allocation_row,
            )
            execution_trace = self._execution_trace(overlay_trace, weight_payload, action_item)
            decision_score_breakdown = self.rulebook_service.build_decision_score_breakdown(
                scores=score_payload,
                is_held=float(weight_payload.get("current_weight", 0.0) or 0.0) > 0,
                preferences=preferences,
            )
            natural_language_summary = self._natural_language_summary(
                summary_card=summary_card,
                selection=selection,
                allocation_row=allocation_row,
                execution_trace=execution_trace,
                comparison=comparison,
                action_reason=str(action_item.get("reason_short", overlay_trace.get("reason_short", ""))),
            )
            short_summary = str(action_item.get("reason_short", "")) or natural_language_summary

            payloads.append(
                {
                    "symbol": symbol,
                    "name": summary_card["name"],
                    "title": f"{summary_card['name']} / {summary_card['final_action_label']}",
                    "summary": short_summary,
                    "action": action_bucket,
                    "action_code": action_code,
                    "intent": str(action_item.get("intent", overlay_trace.get("intent", "hold"))),
                    "category": summary_card["decision_category"],
                    "scores": score_payload,
                    "ranks": {
                        "global_rank": int(score_payload.get("global_rank", 0) or 0),
                        "category_rank": int(score_payload.get("category_rank", 0) or 0),
                    },
                    "weights": weight_payload,
                    "comparison": comparison,
                    "execution_overlay": self._legacy_execution_overlay(overlay_trace, action_item),
                    "feature_snapshot": feature_snapshot,
                    "rank_snapshot": score_breakdown.get("ranks", {}),
                    "execution_note": str(action_item.get("execution_note", "")),
                    "summary_card": summary_card,
                    "intra_score_breakdown": intra_score_breakdown,
                    "category_score_breakdown": category_score_breakdown,
                    "final_score_breakdown": final_score_breakdown,
                    "decision_score_breakdown": decision_score_breakdown,
                    "allocation_trace": self._allocation_trace(allocation_row, selection, replacement),
                    "execution_trace": execution_trace,
                    "natural_language_summary": natural_language_summary,
                }
            )

        return payloads

    def _score_breakdown(self, action_item: dict[str, Any], overlay_trace: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
        breakdown = action_item.get("score_breakdown", {})
        if isinstance(breakdown, dict) and breakdown:
            return breakdown
        breakdown = overlay_trace.get("score_breakdown", {})
        if isinstance(breakdown, dict) and breakdown:
            return breakdown
        raw = row.get("score_breakdown_json", {})
        return raw if isinstance(raw, dict) else {}

    def _feature_snapshot(
        self,
        action_item: dict[str, Any],
        overlay_trace: dict[str, Any],
        row: dict[str, Any],
        score_breakdown: dict[str, Any],
    ) -> dict[str, Any]:
        snapshot = {}
        for source in (
            action_item.get("feature_snapshot", {}),
            overlay_trace.get("feature_snapshot", {}),
            score_breakdown.get("features", {}),
            row,
        ):
            if isinstance(source, dict):
                snapshot.update(source)
        return {
            "close_price": float(snapshot.get("close_price", snapshot.get("close", 0.0)) or 0.0),
            "momentum_3d": float(snapshot.get("momentum_3d", 0.0) or 0.0),
            "momentum_5d": float(snapshot.get("momentum_5d", 0.0) or 0.0),
            "momentum_10d": float(snapshot.get("momentum_10d", 0.0) or 0.0),
            "momentum_20d": float(snapshot.get("momentum_20d", 0.0) or 0.0),
            "ma5": float(snapshot.get("ma5", 0.0) or 0.0),
            "ma10": float(snapshot.get("ma10", 0.0) or 0.0),
            "ma20": float(snapshot.get("ma20", 0.0) or 0.0),
            "trend_strength": float(snapshot.get("trend_strength", 0.0) or 0.0),
            "drawdown_20d": float(snapshot.get("drawdown_20d", 0.0) or 0.0),
            "volatility_20d": float(snapshot.get("volatility_20d", 0.0) or 0.0),
            "liquidity_score": float(snapshot.get("liquidity_score", 0.0) or 0.0),
            "decision_category": str(snapshot.get("decision_category", row.get("decision_category", ""))),
            "tradability_mode": str(snapshot.get("tradability_mode", row.get("tradability_mode", ""))),
        }

    def _scores(
        self,
        action_item: dict[str, Any],
        overlay_trace: dict[str, Any],
        row: dict[str, Any],
        score_breakdown: dict[str, Any],
    ) -> dict[str, Any]:
        score_sources = [action_item.get("scores", {}), overlay_trace.get("scores", {}), score_breakdown.get("scores", {}), row]
        payload: dict[str, Any] = {}
        for source in score_sources:
            if isinstance(source, dict):
                payload.update(source)
        return {
            "entry_score": float(payload.get("entry_score", row.get("entry_score", 0.0)) or 0.0),
            "hold_score": float(payload.get("hold_score", row.get("hold_score", 0.0)) or 0.0),
            "exit_score": float(payload.get("exit_score", row.get("exit_score", 0.0)) or 0.0),
            "decision_score": float(payload.get("decision_score", row.get("decision_score", payload.get("final_score", 0.0))) or 0.0),
            "intra_score": float(payload.get("intra_score", row.get("intra_score", 0.0)) or 0.0),
            "category_score": float(payload.get("category_score", row.get("category_score", 0.0)) or 0.0),
            "final_score": float(payload.get("final_score", row.get("final_score", 0.0)) or 0.0),
            "global_rank": int(payload.get("global_rank", row.get("global_rank", score_breakdown.get("ranks_meta", {}).get("global_rank", 0))) or 0),
            "category_rank": int(payload.get("category_rank", row.get("category_rank", score_breakdown.get("ranks_meta", {}).get("category_rank", 0))) or 0),
        }

    def _weights(
        self,
        action_item: dict[str, Any],
        overlay_trace: dict[str, Any],
        current: dict[str, Any],
        allocation_row: dict[str, Any],
    ) -> dict[str, Any]:
        current_weight = float(action_item.get("current_weight", overlay_trace.get("current_weight", current.get("current_weight", 0.0))) or 0.0)
        target_weight = float(action_item.get("target_weight", overlay_trace.get("effective_target_weight", allocation_row.get("normal_target_weight", 0.0))) or 0.0)
        return {
            "current_weight": current_weight,
            "target_weight": target_weight,
            "normal_target_weight": float(overlay_trace.get("normal_target_weight", allocation_row.get("normal_target_weight", 0.0)) or 0.0),
            "effective_target_weight": target_weight,
            "delta_weight": float(action_item.get("delta_weight", overlay_trace.get("delta_weight", target_weight - current_weight)) or 0.0),
            "current_amount": float(action_item.get("current_amount", overlay_trace.get("current_amount", current.get("current_amount", 0.0))) or 0.0),
            "target_amount": float(action_item.get("target_amount", overlay_trace.get("target_amount", 0.0)) or 0.0),
            "delta_amount": float(self._nested(overlay_trace, "execution_trace", "final_action_calc", "delta_amount") or 0.0),
        }

    def _comparison(
        self,
        action_item: dict[str, Any],
        overlay_trace: dict[str, Any],
        replacement: dict[str, Any],
        allocation: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "replacement_symbol": str(action_item.get("replacement_symbol", replacement.get("candidate_symbol", replacement.get("incumbent_symbol", "")))),
            "score_gap_vs_holding": float(action_item.get("score_gap_vs_holding", replacement.get("score_gap", 0.0)) or 0.0),
            "replace_threshold_used": float(action_item.get("replace_threshold_used", allocation.get("replace_threshold", replacement.get("replace_threshold", 0.0))) or 0.0),
            "hold_days": int(action_item.get("hold_days", replacement.get("hold_days", 0)) or 0),
            "hold_days_known": bool(action_item.get("hold_days_known", replacement.get("hold_days_known", False))),
        }

    def _intra_score_breakdown(self, score_breakdown: dict[str, Any], score_payload: dict[str, Any]) -> dict[str, Any]:
        ranks = score_breakdown.get("ranks", {}) if isinstance(score_breakdown.get("ranks", {}), dict) else {}
        components = []
        for key, label, default_weight in INTRA_COMPONENTS:
            rank_value = float(ranks.get(key, 0.0) or 0.0)
            contribution = round(rank_value * default_weight, 4)
            components.append(
                {
                    "key": key,
                    "label": label,
                    "weight": default_weight,
                    "rank_value": rank_value,
                    "contribution": contribution,
                    "formula_text": f"{default_weight:.3f} × {rank_value:.2f} = {contribution:.2f}",
                }
            )
        return {
            "intra_score": float(score_payload.get("intra_score", 0.0)),
            "formula": "单票分 = 0.30×20日动量分位 + 0.20×10日动量分位 + 0.10×5日动量分位 + 0.15×趋势分位 + 0.10×流动性分位 + 0.075×波动友好度分位 + 0.075×回撤友好度分位",
            "components": components,
            "available": bool(ranks),
        }

    def _category_score_breakdown(self, score_breakdown: dict[str, Any], score_payload: dict[str, Any]) -> dict[str, Any]:
        category_components = score_breakdown.get("category_components", {}) if isinstance(score_breakdown.get("category_components", {}), dict) else {}
        components = []
        for key, label, default_weight in CATEGORY_COMPONENTS:
            value = float(category_components.get(key, 0.0) or 0.0)
            contribution = round(value * default_weight, 4)
            components.append(
                {
                    "key": key,
                    "label": label,
                    "weight": default_weight,
                    "value": value,
                    "contribution": contribution,
                    "formula_text": f"{default_weight:.3f} × {value:.2f} = {contribution:.2f}",
                }
            )
        return {
            "category_score": float(score_payload.get("category_score", 0.0)),
            "top_mean_intrascore": float(category_components.get("top_mean_intrascore", 0.0) or 0.0),
            "breadth_score": float(category_components.get("breadth_score", 0.0) or 0.0),
            "category_momentum_score": float(category_components.get("category_momentum_score", 0.0) or 0.0),
            "formula": "类别分 = 0.50×头部平均单票分 + 0.30×类别广度分 + 0.20×类别动量分",
            "components": components,
            "available": bool(category_components),
        }

    def _final_score_breakdown(
        self,
        *,
        score_payload: dict[str, Any],
        selection: dict[str, Any],
        allocation_row: dict[str, Any],
    ) -> dict[str, Any]:
        components = []
        for key, label, default_weight in FINAL_COMPONENTS:
            value = float(score_payload.get(key, 0.0) or 0.0)
            contribution = round(value * default_weight, 4)
            components.append(
                {
                    "key": key,
                    "label": label,
                    "weight": default_weight,
                    "value": value,
                    "contribution": contribution,
                    "formula_text": f"{default_weight:.3f} × {value:.2f} = {contribution:.2f}",
                }
            )
        eliminated_stage = str(selection.get("blocked_stage", ""))
        eliminated_reason = str(selection.get("blocked_reason", ""))
        if not eliminated_stage and bool(allocation_row.get("below_min_position_weight", False)):
            eliminated_stage = "budget"
            eliminated_reason = "理论仓位低于最小持仓权重，因此未形成正式目标仓位。"
        return {
            "final_score": float(score_payload.get("final_score", 0.0)),
            "decision_score": float(score_payload.get("decision_score", 0.0)),
            "global_rank": int(score_payload.get("global_rank", 0) or 0),
            "category_rank": int(score_payload.get("category_rank", 0) or 0),
            "minimum_candidate_threshold": float(selection.get("min_final_score_for_target", self.min_final_score) or self.min_final_score),
            "meets_minimum_candidate_threshold": bool(selection.get("meets_min_final_score", float(score_payload.get("final_score", 0.0)) >= self.min_final_score)),
            "entered_candidate_pool": bool(selection.get("selected", False) or float(allocation_row.get("normal_target_weight", 0.0)) > 0),
            "eliminated_stage": STAGE_LABELS.get(eliminated_stage, eliminated_stage),
            "eliminated_reason": eliminated_reason,
            "formula": "最终分 = 0.70×单票分 + 0.30×类别分",
            "components": components,
            "available": True,
        }

    def _allocation_trace(
        self,
        allocation_row: dict[str, Any],
        selection: dict[str, Any],
        replacement: dict[str, Any],
    ) -> dict[str, Any]:
        cap_reasons = allocation_row.get("cap_reasons", [])
        if not isinstance(cap_reasons, list):
            cap_reasons = []
        return {
            "total_budget_pct": float(allocation_row.get("total_budget_pct", 0.0) or 0.0),
            "single_weight_cap": float(allocation_row.get("single_weight_cap", 0.0) or 0.0),
            "category_cap": float(allocation_row.get("category_cap", 0.0) or 0.0),
            "provisional_weight": float(allocation_row.get("provisional_weight", 0.0) or 0.0),
            "normal_target_weight": float(allocation_row.get("normal_target_weight", 0.0) or 0.0),
            "cap_applied": bool(allocation_row.get("cap_applied", False)),
            "cap_reasons": cap_reasons,
            "remaining_budget_before": float(allocation_row.get("remaining_budget_before", 0.0) or 0.0),
            "category_remaining_before": float(allocation_row.get("category_remaining_before", 0.0) or 0.0),
            "selected_for_allocation": bool(allocation_row.get("selected_for_allocation", False)),
            "protected": bool(selection.get("protected", False)),
            "protected_reasons": selection.get("protected_reasons", []) if isinstance(selection.get("protected_reasons", []), list) else [],
            "selected_reason": str(selection.get("selected_reason", "")),
            "blocked_reason": str(selection.get("blocked_reason", "")),
            "replacement_trace": replacement if isinstance(replacement, dict) else {},
        }

    def _execution_trace(
        self,
        overlay_trace: dict[str, Any],
        weight_payload: dict[str, Any],
        action_item: dict[str, Any],
    ) -> dict[str, Any]:
        execution = overlay_trace.get("execution_trace", {})
        if not isinstance(execution, dict):
            execution = {}
        target_adjustment = dict(execution.get("target_weight_adjustment", {}))
        target_adjustment.setdefault("normal_target_weight", float(weight_payload.get("normal_target_weight", 0.0)))
        target_adjustment.setdefault("current_weight", float(weight_payload.get("current_weight", 0.0)))
        target_adjustment.setdefault("effective_target_weight", float(weight_payload.get("effective_target_weight", 0.0)))
        final_action_calc = dict(execution.get("final_action_calc", {}))
        final_action_calc.setdefault("current_weight", float(weight_payload.get("current_weight", 0.0)))
        final_action_calc.setdefault("effective_target_weight", float(weight_payload.get("effective_target_weight", 0.0)))
        final_action_calc.setdefault("delta_weight", float(weight_payload.get("delta_weight", 0.0)))
        final_action_calc.setdefault("target_amount", float(weight_payload.get("target_amount", 0.0)))
        final_action_calc.setdefault("delta_amount", float(weight_payload.get("delta_amount", 0.0)))
        final_action_calc.setdefault("action_code", str(action_item.get("action_code", overlay_trace.get("action_code", "no_trade"))))
        final_action_calc.setdefault("action_reason", str(action_item.get("reason_short", overlay_trace.get("reason_short", ""))))
        return {
            "entry_checks": execution.get("entry_checks", {}),
            "position_state": execution.get("position_state", {}),
            "switch_checks": execution.get("switch_checks", {}),
            "target_weight_adjustment": target_adjustment,
            "final_action_calc": final_action_calc,
        }

    def _legacy_execution_overlay(self, overlay_trace: dict[str, Any], action_item: dict[str, Any]) -> dict[str, Any]:
        rationale = overlay_trace.get("rationale", {})
        if isinstance(action_item.get("rationale", {}), dict):
            merged = dict(rationale)
            merged.update(action_item.get("rationale", {}))
            return merged
        return rationale if isinstance(rationale, dict) else {}

    def _natural_language_summary(
        self,
        *,
        summary_card: dict[str, Any],
        selection: dict[str, Any],
        allocation_row: dict[str, Any],
        execution_trace: dict[str, Any],
        comparison: dict[str, Any],
        action_reason: str,
    ) -> str:
        action_code = str(summary_card.get("final_action", "no_trade"))
        final_score = float(summary_card.get("final_score", 0.0))
        decision_score = float(summary_card.get("decision_score", 0.0))
        effective_target_weight = float(summary_card.get("effective_target_weight", 0.0)) * 100
        normal_target_weight = float(allocation_row.get("normal_target_weight", 0.0)) * 100
        entry_checks = execution_trace.get("entry_checks", {}) if isinstance(execution_trace.get("entry_checks", {}), dict) else {}
        entry_allowed = bool(entry_checks.get("entry_allowed", False))
        entry_channel = ENTRY_CHANNEL_LABELS.get(str(summary_card.get("entry_channel", "none")), str(summary_card.get("entry_channel", "none")))
        if action_code in {"buy_open", "buy_add", "switch"}:
            return (
                f"该 ETF 最终被选中，是因为最终分 {final_score:.1f}、执行决策分 {decision_score:.1f}，"
                f"仓位分配给出了 {normal_target_weight:.1f}% 的理论仓位。执行层中 {entry_channel} 通过，"
                f"因此有效执行仓位为 {effective_target_weight:.1f}%，最终动作为 {ACTION_CODE_LABELS.get(action_code, action_code)}。"
            )
        if action_code == "hold":
            return (
                f"该 ETF 当前继续持有。它的最终分为 {final_score:.1f}，执行决策分为 {decision_score:.1f}。"
                f"虽然执行层没有进一步放大仓位，但当前有效仓位仍保留在 {effective_target_weight:.1f}% 。{action_reason}"
            )
        if action_code in {"sell_reduce", "sell_exit"}:
            return (
                f"该 ETF 当前被要求{ACTION_CODE_LABELS.get(action_code, action_code)}。"
                f"主要原因是持仓状态已经转弱，执行层把有效仓位调整到 {effective_target_weight:.1f}% 。{action_reason}"
            )
        blocked_reason = str(selection.get("blocked_reason", "")) or action_reason or "当前没有形成正式可执行仓位。"
        replace_note = ""
        if comparison.get("replacement_symbol"):
            replace_note = (
                f" 与当前持仓比较时，分差只有 {float(comparison.get('score_gap_vs_holding', 0.0)):.1f}，"
                f"替换阈值为 {float(comparison.get('replace_threshold_used', 0.0)):.1f}。"
            )
        channel_note = "未通过入场通道。" if not entry_allowed else f"虽然入场通道 {entry_channel} 通过，但仍未形成正式执行仓位。"
        return (
            f"该 ETF 这次没有被正式执行。它的最终分为 {final_score:.1f}，执行决策分为 {decision_score:.1f}，"
            f"理论仓位为 {normal_target_weight:.1f}% ，但最终有效仓位被改写为 {effective_target_weight:.1f}% 。"
            f"{channel_note}{replace_note}{blocked_reason}"
        )

    def _nested(self, payload: dict[str, Any], *keys: str) -> Any:
        current: Any = payload
        for key in keys:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
        return current
