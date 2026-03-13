from __future__ import annotations

from collections import defaultdict
from typing import Any

import pandas as pd

from app.core.config import get_settings, load_yaml_config
from app.services.risk_mode_service import get_risk_mode_service


class PortfolioAllocator:
    def __init__(self) -> None:
        settings = get_settings()
        self.scoring_config = load_yaml_config(settings.config_dir / "strategy_scoring.yaml")
        self.constraints = load_yaml_config(settings.config_dir / "portfolio_constraints.yaml")
        self.risk_mode_service = get_risk_mode_service()

    def build_target_portfolio(
        self,
        scored_df: pd.DataFrame,
        *,
        current_holdings: list[dict[str, Any]],
        preferences: Any,
        market_regime: dict[str, Any],
        risk_mode: str | None,
        blocked_candidate_reasons: dict[str, str] | None = None,
        overlay_hints: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        if scored_df.empty:
            return {
                "selected": [],
                "target_weights": {},
                "total_budget_pct": 0.0,
                "single_weight_cap": 0.0,
                "category_budget_caps": {},
                "replace_threshold": 0.0,
                "selection_trace": {},
                "replacement_trace": {},
                "allocation_trace": {},
                "budget_trace": {},
                "candidate_summary": [],
            }

        risk_profile = self.risk_mode_service.resolve(risk_mode)
        blocked_candidate_reasons = {
            str(symbol): str(reason)
            for symbol, reason in (blocked_candidate_reasons or {}).items()
            if str(symbol)
        }
        overlay_hints = {
            str(symbol): dict(payload)
            for symbol, payload in (overlay_hints or {}).items()
            if str(symbol)
        }
        selection_cfg = self.constraints.get("selection", {})
        budget_cfg = self.constraints.get("budget", {})
        min_final_score = float(self.scoring_config.get("selection", {}).get("min_final_score_for_target", 55.0))
        max_total_selected = max(1, int(selection_cfg.get("max_selected_total", 3)))
        max_selected_per_category = max(1, int(selection_cfg.get("max_selected_per_category", 2)))
        hold_guard_global_rank = max(1, int(selection_cfg.get("hold_guard_global_rank", 5)))
        hold_guard_category_rank = max(1, int(selection_cfg.get("hold_guard_category_rank", 2)))
        replace_threshold = float(selection_cfg.get("replace_threshold", 8.0)) + risk_profile.replace_threshold_delta
        min_hold_days_before_replace = max(0, int(selection_cfg.get("min_hold_days_before_replace", 2)))

        current_by_symbol = {str(row["symbol"]): dict(row) for row in current_holdings}
        total_budget_pct = min(
            float(preferences.max_total_position_pct),
            max(0.0, 1.0 - float(getattr(preferences, "cash_reserve_pct", 0.0))),
            float(budget_cfg.get("max_total_weight", 0.8)),
            float(market_regime.get("budget_total_pct", market_regime.get("recommended_position_pct", 0.0))) * risk_profile.total_budget_multiplier,
        )
        single_weight_cap = min(
            float(preferences.max_single_position_pct),
            float(budget_cfg.get("max_single_weight", 0.35)) * risk_profile.single_weight_multiplier,
            total_budget_pct,
        )

        category_caps = {
            str(category): min(
                total_budget_pct,
                float(self.constraints.get("category_caps", {}).get(category, total_budget_pct)),
                float(market_regime.get("budget_by_category", {}).get(category, total_budget_pct)) * risk_profile.category_cap_multiplier,
            )
            for category in scored_df["decision_category"].dropna().astype(str).unique()
        }

        ranked = scored_df.sort_values(["final_score", "intra_score", "symbol"], ascending=[False, False, True]).reset_index(drop=True)
        ranked_rows = [row._asdict() for row in ranked.itertuples(index=False)]
        ranked_by_symbol = {
            str(row["symbol"]): row
            for row in ranked_rows
        }
        selection_trace: dict[str, dict[str, Any]] = {}
        replacement_trace: dict[str, dict[str, Any]] = {}

        for row_dict in ranked_rows:
            symbol = str(row_dict["symbol"])
            selection_trace[symbol] = {
                "symbol": symbol,
                "name": str(row_dict.get("name", symbol)),
                "decision_category": str(row_dict.get("decision_category", "")),
                "final_score": float(row_dict.get("final_score", 0.0)),
                "global_rank": int(row_dict.get("global_rank", 0) or 0),
                "category_rank": int(row_dict.get("category_rank", 0) or 0),
                "meets_min_final_score": float(row_dict.get("final_score", 0.0)) >= min_final_score,
                "min_final_score_for_target": min_final_score,
                "filter_pass": bool(row_dict.get("filter_pass", False)),
                "is_current_holding": symbol in current_by_symbol,
                "selected": False,
                "selected_stage": "",
                "selected_reason": "",
                "blocked_stage": "",
                "blocked_reason": "",
                "protected": False,
                "protected_reasons": [],
            }

        protected_symbols: list[str] = []
        for holding in current_holdings:
            symbol = str(holding["symbol"])
            row_dict = ranked_by_symbol.get(symbol)
            if row_dict is None:
                continue
            hold_days = int(holding.get("hold_days", 0) or 0)
            hold_days_known = bool(holding.get("hold_days_known", False))
            reasons: list[str] = []
            if int(row_dict.get("global_rank", 9999) or 9999) <= hold_guard_global_rank:
                reasons.append("全市场排名保护")
            if int(row_dict.get("category_rank", 9999) or 9999) <= hold_guard_category_rank:
                reasons.append("类别排名保护")
            if hold_days_known and hold_days < min_hold_days_before_replace:
                reasons.append("最短持有期保护")
            selection_trace[symbol].update(
                {
                    "hold_days": hold_days,
                    "hold_days_known": hold_days_known,
                }
            )
            if reasons:
                protected_symbols.append(symbol)
                selection_trace[symbol].update({"protected": True, "protected_reasons": reasons})

        selected_rows: list[dict[str, Any]] = []
        selected_symbols: set[str] = set()
        selected_by_category: dict[str, int] = defaultdict(int)

        def can_add(row_dict: dict[str, Any]) -> bool:
            category = str(row_dict["decision_category"])
            return len(selected_rows) < max_total_selected and selected_by_category[category] < max_selected_per_category

        for symbol in protected_symbols:
            row_dict = ranked_by_symbol.get(symbol)
            if row_dict is None:
                continue
            if can_add(row_dict):
                selected_rows.append(row_dict)
                selected_symbols.add(symbol)
                selected_by_category[str(row_dict["decision_category"])] += 1
                selection_trace[symbol].update(
                    {
                        "selected": True,
                        "selected_stage": "protected_hold",
                        "selected_reason": "当前持仓命中保护规则，先保留在目标组合中。",
                    }
                )

        incumbent_by_category: dict[str, dict[str, Any]] = {}
        current_holding_counts_by_category: dict[str, int] = defaultdict(int)
        for holding in current_holdings:
            symbol = str(holding["symbol"])
            row_dict = ranked_by_symbol.get(symbol)
            if row_dict is None:
                continue
            category = str(row_dict["decision_category"])
            current_holding_counts_by_category[category] += 1
            incumbent = incumbent_by_category.get(category)
            if incumbent is None or float(row_dict["final_score"]) > float(incumbent["final_score"]):
                merged = dict(row_dict)
                merged.update(current_by_symbol.get(symbol, {}))
                incumbent_by_category[category] = merged

        non_holding_selected_by_category: dict[str, int] = defaultdict(int)

        for row_dict in ranked_rows:
            symbol = str(row_dict["symbol"])
            category = str(row_dict["decision_category"])
            if symbol in selected_symbols:
                continue
            if float(row_dict.get("final_score", 0.0)) < min_final_score:
                selection_trace[symbol].update(
                    {
                        "blocked_stage": "final_score",
                        "blocked_reason": f"最终分 {float(row_dict.get('final_score', 0.0)):.1f} 低于最低候选阈值 {min_final_score:.1f}。",
                    }
                )
                continue
            if not bool(row_dict.get("filter_pass", False)) and symbol not in current_by_symbol:
                selection_trace[symbol].update(
                    {
                        "blocked_stage": "basic_filter",
                        "blocked_reason": "基础过滤未通过，因此不会进入新开仓候选池。",
                    }
                )
                continue
            if symbol not in current_by_symbol and symbol in blocked_candidate_reasons:
                selection_trace[symbol].update(
                    {
                        "blocked_stage": "execution_gate",
                        "blocked_reason": blocked_candidate_reasons[symbol],
                    }
                )
                continue
            if not can_add(row_dict):
                blocked_reason = "可选席位已满。"
                if selected_by_category[category] >= max_selected_per_category:
                    blocked_reason = "该类别已达到入选数量上限。"
                elif len(selected_rows) >= max_total_selected:
                    blocked_reason = "总入选数量已达到上限。"
                selection_trace[symbol].update(
                    {
                        "blocked_stage": "slot_limit",
                        "blocked_reason": blocked_reason,
                    }
                )
                continue

            if symbol not in current_by_symbol:
                incumbent = incumbent_by_category.get(category)
                category_addition_slot_available = (
                    current_holding_counts_by_category[category] + non_holding_selected_by_category[category]
                ) < max_selected_per_category
                if incumbent is not None and str(incumbent.get("symbol")) != symbol and not category_addition_slot_available:
                    score_gap = float(row_dict.get("final_score", 0.0)) - float(incumbent.get("final_score", 0.0))
                    hold_days = int(incumbent.get("hold_days", 0) or 0)
                    hold_days_known = bool(incumbent.get("hold_days_known", False))
                    candidate_hint = overlay_hints.get(symbol, {})
                    incumbent_hint = overlay_hints.get(str(incumbent.get("symbol", "")), {})
                    incumbent_state = str(incumbent_hint.get("position_state", "NONE"))
                    candidate_entry_allowed = bool(candidate_hint.get("entry_allowed", False))
                    state_based_replace_allowed = bool(
                        candidate_entry_allowed
                        and incumbent_state in {"REDUCE", "EXIT"}
                        and (not hold_days_known or hold_days >= min_hold_days_before_replace)
                    )
                    score_gap_replace_allowed = bool(
                        (not hold_days_known or hold_days >= min_hold_days_before_replace)
                        and score_gap >= replace_threshold
                    )
                    replace_allowed = bool(state_based_replace_allowed or score_gap_replace_allowed)
                    if hold_days_known and hold_days < min_hold_days_before_replace:
                        blocked_reason = "旧持仓仍处于最短持有期保护，暂不替换。"
                    elif state_based_replace_allowed:
                        blocked_reason = ""
                    elif score_gap < replace_threshold:
                        blocked_reason = "旧持仓与新候选分差不足，暂不替换。"
                    elif not candidate_entry_allowed:
                        blocked_reason = "新候选当前未通过入场通道，暂不替换。"
                    else:
                        blocked_reason = ""
                    replacement_payload = {
                        "incumbent_symbol": str(incumbent.get("symbol", "")),
                        "incumbent_name": str(incumbent.get("name", "")),
                        "candidate_symbol": symbol,
                        "candidate_name": str(row_dict.get("name", "")),
                        "score_gap": score_gap,
                        "replace_threshold": replace_threshold,
                        "hold_days": hold_days,
                        "hold_days_known": hold_days_known,
                        "incumbent_state": incumbent_state,
                        "candidate_entry_allowed": candidate_entry_allowed,
                        "state_based_replace_allowed": state_based_replace_allowed,
                        "score_gap_replace_allowed": score_gap_replace_allowed,
                        "replace_allowed": replace_allowed,
                        "blocked_reason": "" if replace_allowed else blocked_reason,
                    }
                    replacement_trace[symbol] = dict(replacement_payload, role="candidate")
                    replacement_trace[str(incumbent.get("symbol", ""))] = dict(replacement_payload, role="incumbent")
                    if not replace_allowed:
                        selection_trace[symbol].update(
                            {
                                "blocked_stage": "replacement",
                                "blocked_reason": replacement_payload["blocked_reason"],
                            }
                        )
                        continue

            selected_rows.append(row_dict)
            selected_symbols.add(symbol)
            selected_by_category[category] += 1
            if symbol not in current_by_symbol:
                non_holding_selected_by_category[category] += 1
            selection_trace[symbol].update(
                {
                    "selected": True,
                    "selected_stage": "candidate_selected",
                    "selected_reason": (
                        "同类别仍有新增名额，因此按并存新增进入目标组合候选。"
                        if symbol not in current_by_symbol
                        and (
                            current_holding_counts_by_category[category] + non_holding_selected_by_category[category]
                        ) <= max_selected_per_category
                        and symbol not in replacement_trace
                        else "最终分、过滤条件和替换条件都通过，因此进入目标组合候选。"
                    ),
                }
            )

        allocation_result = self._allocate_weights(
            selected_rows=selected_rows,
            total_budget_pct=total_budget_pct,
            single_weight_cap=single_weight_cap,
            category_caps=category_caps,
        )
        target_weights = allocation_result["weights"]

        candidate_summary = []
        watchlist_size = max(1, int(self.scoring_config.get("selection", {}).get("candidate_watchlist_size", 8)))
        for row_dict in ranked_rows[:watchlist_size]:
            candidate_summary.append(
                {
                    "symbol": row_dict["symbol"],
                    "name": row_dict["name"],
                    "category": row_dict["decision_category"],
                    "final_score": float(row_dict.get("final_score", 0.0)),
                    "intra_score": float(row_dict.get("intra_score", 0.0)),
                    "category_score": float(row_dict.get("category_score", 0.0)),
                    "global_rank": int(row_dict.get("global_rank", 0) or 0),
                    "category_rank": int(row_dict.get("category_rank", 0) or 0),
                    "selected": row_dict["symbol"] in target_weights,
                }
            )

        allocation_trace = {
            str(symbol): dict(trace)
            for symbol, trace in allocation_result.get("allocation_trace", {}).items()
        }
        for row_dict in ranked_rows:
            symbol = str(row_dict["symbol"])
            allocation_trace.setdefault(
                symbol,
                {
                    "symbol": symbol,
                    "decision_category": str(row_dict.get("decision_category", "")),
                    "selected_for_allocation": False,
                    "total_budget_pct": total_budget_pct,
                    "single_weight_cap": single_weight_cap,
                    "category_cap": float(category_caps.get(str(row_dict.get("decision_category", "")), total_budget_pct)),
                    "provisional_weight": 0.0,
                    "remaining_budget_before": total_budget_pct,
                    "category_remaining_before": float(category_caps.get(str(row_dict.get("decision_category", "")), total_budget_pct)),
                    "effective_cap_before_extra": 0.0,
                    "round1_weight": 0.0,
                    "round2_extra_weight": 0.0,
                    "allocated_weight_before_min_filter": 0.0,
                    "normal_target_weight": float(target_weights.get(symbol, 0.0)),
                    "cap_applied": False,
                    "cap_reasons": [],
                    "below_min_position_weight": False,
                },
            )
            allocation_trace[symbol]["selection_trace"] = selection_trace.get(symbol, {})
            allocation_trace[symbol]["replacement_trace"] = replacement_trace.get(symbol, {})
            allocation_trace[symbol]["selected_reason"] = selection_trace.get(symbol, {}).get("selected_reason", "")
            allocation_trace[symbol]["blocked_reason"] = selection_trace.get(symbol, {}).get("blocked_reason", "")
            allocation_trace[symbol]["protected"] = bool(selection_trace.get(symbol, {}).get("protected", False))
            allocation_trace[symbol]["protected_reasons"] = list(selection_trace.get(symbol, {}).get("protected_reasons", []))

        return {
            "selected": selected_rows,
            "target_weights": target_weights,
            "total_budget_pct": total_budget_pct,
            "single_weight_cap": single_weight_cap,
            "category_budget_caps": category_caps,
            "replace_threshold": replace_threshold,
            "selection_trace": selection_trace,
            "replacement_trace": replacement_trace,
            "allocation_trace": allocation_trace,
            "budget_trace": allocation_result.get("budget_trace", {}),
            "candidate_summary": candidate_summary,
        }

    def _allocate_weights(
        self,
        *,
        selected_rows: list[dict[str, Any]],
        total_budget_pct: float,
        single_weight_cap: float,
        category_caps: dict[str, float],
    ) -> dict[str, Any]:
        if not selected_rows or total_budget_pct <= 0:
            return {"weights": {}, "allocation_trace": {}, "budget_trace": {}}

        minimum_weight = float(self.constraints.get("budget", {}).get("min_position_weight", 0.08))
        rows = sorted(selected_rows, key=lambda row: float(row.get("final_score", 0.0)), reverse=True)
        total_score = sum(max(float(row.get("final_score", 0.0)), 0.01) for row in rows)
        provisional = {
            str(row["symbol"]): total_budget_pct * max(float(row.get("final_score", 0.0)), 0.01) / total_score
            for row in rows
        }

        allocated: dict[str, float] = {str(row["symbol"]): 0.0 for row in rows}
        category_used: dict[str, float] = defaultdict(float)
        remaining = total_budget_pct
        allocation_trace: dict[str, dict[str, Any]] = {}

        for row in rows:
            symbol = str(row["symbol"])
            category = str(row["decision_category"])
            desired = provisional[symbol]
            category_cap = float(category_caps.get(category, total_budget_pct))
            category_remaining_before = max(category_cap - category_used[category], 0.0)
            remaining_before = remaining
            cap = min(single_weight_cap, category_remaining_before, remaining)
            cap_reasons: list[str] = []
            if desired > single_weight_cap:
                cap_reasons.append("单票上限截断")
            if desired > category_remaining_before:
                cap_reasons.append("类别上限截断")
            if desired > remaining_before:
                cap_reasons.append("总预算剩余不足")
            if cap <= 0:
                allocation_trace[symbol] = {
                    "symbol": symbol,
                    "decision_category": category,
                    "selected_for_allocation": True,
                    "total_budget_pct": total_budget_pct,
                    "single_weight_cap": single_weight_cap,
                    "category_cap": category_cap,
                    "provisional_weight": desired,
                    "remaining_budget_before": remaining_before,
                    "category_remaining_before": category_remaining_before,
                    "effective_cap_before_extra": 0.0,
                    "round1_weight": 0.0,
                    "round2_extra_weight": 0.0,
                    "allocated_weight_before_min_filter": 0.0,
                    "normal_target_weight": 0.0,
                    "cap_applied": True,
                    "cap_reasons": cap_reasons or ["剩余预算或类别容量不足"],
                    "below_min_position_weight": False,
                }
                continue

            weight = min(desired, cap)
            if weight > 0:
                allocated[symbol] = weight
                category_used[category] += weight
                remaining -= weight
            allocation_trace[symbol] = {
                "symbol": symbol,
                "decision_category": category,
                "selected_for_allocation": True,
                "total_budget_pct": total_budget_pct,
                "single_weight_cap": single_weight_cap,
                "category_cap": category_cap,
                "provisional_weight": desired,
                "remaining_budget_before": remaining_before,
                "category_remaining_before": category_remaining_before,
                "effective_cap_before_extra": cap,
                "round1_weight": weight,
                "round2_extra_weight": 0.0,
                "allocated_weight_before_min_filter": weight,
                "normal_target_weight": 0.0,
                "cap_applied": bool(cap_reasons),
                "cap_reasons": cap_reasons,
                "below_min_position_weight": False,
            }

        if remaining > 0:
            for row in rows:
                symbol = str(row["symbol"])
                category = str(row["decision_category"])
                extra_cap = min(
                    single_weight_cap - allocated[symbol],
                    category_caps.get(category, total_budget_pct) - category_used[category],
                    remaining,
                )
                if extra_cap <= 0:
                    continue
                allocated[symbol] += extra_cap
                category_used[category] += extra_cap
                remaining -= extra_cap
                trace = allocation_trace.setdefault(
                    symbol,
                    {
                        "symbol": symbol,
                        "decision_category": category,
                        "selected_for_allocation": True,
                        "total_budget_pct": total_budget_pct,
                        "single_weight_cap": single_weight_cap,
                        "category_cap": float(category_caps.get(category, total_budget_pct)),
                        "provisional_weight": provisional[symbol],
                        "remaining_budget_before": total_budget_pct,
                        "category_remaining_before": float(category_caps.get(category, total_budget_pct)),
                        "effective_cap_before_extra": 0.0,
                        "round1_weight": 0.0,
                        "round2_extra_weight": 0.0,
                        "allocated_weight_before_min_filter": 0.0,
                        "normal_target_weight": 0.0,
                        "cap_applied": False,
                        "cap_reasons": [],
                        "below_min_position_weight": False,
                    },
                )
                trace["round2_extra_weight"] = float(trace.get("round2_extra_weight", 0.0)) + extra_cap
                trace["allocated_weight_before_min_filter"] = allocated[symbol]
                if remaining <= 0:
                    break

        min_weight_floor = min(minimum_weight, total_budget_pct)
        weights = {
            symbol: round(weight, 6)
            for symbol, weight in allocated.items()
            if weight >= min_weight_floor
        }
        for symbol, trace in allocation_trace.items():
            allocated_before_filter = float(allocated.get(symbol, 0.0))
            trace["allocated_weight_before_min_filter"] = allocated_before_filter
            trace["below_min_position_weight"] = bool(0.0 < allocated_before_filter < min_weight_floor)
            trace["normal_target_weight"] = float(weights.get(symbol, 0.0))
            if trace["below_min_position_weight"]:
                trace["cap_reasons"] = list(trace.get("cap_reasons", [])) + ["低于最小持仓权重，不纳入正式目标组合"]

        return {
            "weights": weights,
            "allocation_trace": allocation_trace,
            "budget_trace": {
                "total_budget_pct": total_budget_pct,
                "single_weight_cap": single_weight_cap,
                "minimum_weight": minimum_weight,
                "remaining_budget_after_allocation": remaining,
                "selected_count": len(rows),
            },
        }
