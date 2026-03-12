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
    ) -> dict[str, Any]:
        if scored_df.empty:
            return {
                "selected": [],
                "target_weights": {},
                "total_budget_pct": 0.0,
                "single_weight_cap": 0.0,
                "category_budget_caps": {},
                "candidate_summary": [],
            }

        risk_profile = self.risk_mode_service.resolve(risk_mode)
        selection_cfg = self.constraints.get("selection", {})
        budget_cfg = self.constraints.get("budget", {})
        min_final_score = float(self.scoring_config.get("selection", {}).get("min_final_score_for_target", 55.0))
        max_total_selected = max(1, int(selection_cfg.get("max_selected_total", 3)))
        max_selected_per_category = max(1, int(selection_cfg.get("max_selected_per_category", 2)))
        hold_guard_global_rank = max(1, int(selection_cfg.get("hold_guard_global_rank", 5)))
        hold_guard_category_rank = max(1, int(selection_cfg.get("hold_guard_category_rank", 2)))
        replace_threshold = float(selection_cfg.get("replace_threshold", 8.0)) + risk_profile.replace_threshold_delta
        min_hold_days_before_replace = max(0, int(selection_cfg.get("min_hold_days_before_replace", 2)))

        current_by_symbol = {
            str(row["symbol"]): dict(row)
            for row in current_holdings
        }
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
        protected_symbols: list[str] = []
        for holding in current_holdings:
            symbol = str(holding["symbol"])
            row = ranked[ranked["symbol"] == symbol]
            if row.empty:
                continue
            row_dict = row.iloc[0].to_dict()
            hold_days = int(holding.get("hold_days", 0) or 0)
            if (
                int(row_dict.get("global_rank", 9999) or 9999) <= hold_guard_global_rank
                or int(row_dict.get("category_rank", 9999) or 9999) <= hold_guard_category_rank
                or hold_days < min_hold_days_before_replace
            ):
                protected_symbols.append(symbol)

        selected_rows: list[dict[str, Any]] = []
        selected_by_category: dict[str, int] = defaultdict(int)

        def can_add(row_dict: dict[str, Any]) -> bool:
            category = str(row_dict["decision_category"])
            return (
                len(selected_rows) < max_total_selected
                and selected_by_category[category] < max_selected_per_category
            )

        for symbol in protected_symbols:
            row = ranked[ranked["symbol"] == symbol]
            if row.empty:
                continue
            row_dict = row.iloc[0].to_dict()
            if can_add(row_dict):
                selected_rows.append(row_dict)
                selected_by_category[str(row_dict["decision_category"])] += 1

        incumbent_by_category: dict[str, dict[str, Any]] = {}
        for holding in current_holdings:
            symbol = str(holding["symbol"])
            row = ranked[ranked["symbol"] == symbol]
            if row.empty:
                continue
            row_dict = row.iloc[0].to_dict()
            category = str(row_dict["decision_category"])
            incumbent = incumbent_by_category.get(category)
            if incumbent is None or float(row_dict["final_score"]) > float(incumbent["final_score"]):
                merged = dict(row_dict)
                merged.update(current_by_symbol.get(symbol, {}))
                incumbent_by_category[category] = merged

        for _, candidate in ranked.iterrows():
            row_dict = candidate.to_dict()
            symbol = str(row_dict["symbol"])
            category = str(row_dict["decision_category"])
            if symbol in {row["symbol"] for row in selected_rows}:
                continue
            if float(row_dict.get("final_score", 0.0)) < min_final_score:
                continue
            if not bool(row_dict.get("filter_pass", False)) and symbol not in current_by_symbol:
                continue
            if not can_add(row_dict):
                continue

            if symbol not in current_by_symbol:
                incumbent = incumbent_by_category.get(category)
                if incumbent is not None and str(incumbent.get("symbol")) != symbol:
                    score_gap = float(row_dict.get("final_score", 0.0)) - float(incumbent.get("final_score", 0.0))
                    hold_days = int(incumbent.get("hold_days", 0) or 0)
                    if hold_days < min_hold_days_before_replace or score_gap < replace_threshold:
                        continue

            selected_rows.append(row_dict)
            selected_by_category[category] += 1

        target_weights = self._allocate_weights(
            selected_rows=selected_rows,
            total_budget_pct=total_budget_pct,
            single_weight_cap=single_weight_cap,
            category_caps=category_caps,
        )

        candidate_summary = []
        watchlist_size = max(1, int(self.scoring_config.get("selection", {}).get("candidate_watchlist_size", 8)))
        for _, row in ranked.head(watchlist_size).iterrows():
            row_dict = row.to_dict()
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

        return {
            "selected": selected_rows,
            "target_weights": target_weights,
            "total_budget_pct": total_budget_pct,
            "single_weight_cap": single_weight_cap,
            "category_budget_caps": category_caps,
            "replace_threshold": replace_threshold,
            "candidate_summary": candidate_summary,
        }

    def _allocate_weights(
        self,
        *,
        selected_rows: list[dict[str, Any]],
        total_budget_pct: float,
        single_weight_cap: float,
        category_caps: dict[str, float],
    ) -> dict[str, float]:
        if not selected_rows or total_budget_pct <= 0:
            return {}

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

        for row in rows:
            symbol = str(row["symbol"])
            category = str(row["decision_category"])
            desired = provisional[symbol]
            cap = min(single_weight_cap, max(category_caps.get(category, total_budget_pct) - category_used[category], 0.0), remaining)
            if cap <= 0:
                continue
            weight = min(desired, cap)
            if weight > 0:
                allocated[symbol] = weight
                category_used[category] += weight
                remaining -= weight

        if remaining > 0:
            for row in rows:
                symbol = str(row["symbol"])
                category = str(row["decision_category"])
                extra_cap = min(single_weight_cap - allocated[symbol], category_caps.get(category, total_budget_pct) - category_used[category], remaining)
                if extra_cap <= 0:
                    continue
                allocated[symbol] += extra_cap
                category_used[category] += extra_cap
                remaining -= extra_cap
                if remaining <= 0:
                    break

        return {
            symbol: round(weight, 6)
            for symbol, weight in allocated.items()
            if weight >= min(minimum_weight, total_budget_pct)
        }

