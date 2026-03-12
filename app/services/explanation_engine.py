from __future__ import annotations

from typing import Any


class ExplanationEngine:
    def build(
        self,
        *,
        market_regime: dict[str, Any],
        allocation: dict[str, Any],
        items: list[dict[str, Any]],
        candidate_summary: list[dict[str, Any]],
        portfolio_summary: dict[str, Any],
        quality_summary: dict[str, Any],
    ) -> dict[str, Any]:
        reasons = [
            f"Market regime is {market_regime.get('market_regime', 'neutral')} and drives only the budget layer.",
            f"Total target budget is {float(allocation.get('total_budget_pct', 0.0)) * 100:.1f}%.",
            f"Replace threshold is {float(allocation.get('replace_threshold', 0.0)):.1f} FinalScore points.",
        ]
        if quality_summary:
            reasons.append(
                f"Data quality status is {quality_summary.get('quality_status', 'unknown')} "
                f"({quality_summary.get('verification_status', '')})."
            )
        if items:
            reasons.append(
                "Action items are generated only after comparing target weights against current holdings."
            )
        else:
            reasons.append("No ETF generated a tradable delta after score, budget, and replacement checks.")

        overall = {
            "headline": self._headline(items),
            "market_regime": market_regime.get("market_regime", "neutral"),
            "summary": self._headline(items),
            "reasons": reasons,
            "budget": {
                "total_budget_pct": float(allocation.get("total_budget_pct", 0.0)),
                "single_weight_cap": float(allocation.get("single_weight_cap", 0.0)),
                "category_budget_caps": allocation.get("category_budget_caps", {}),
            },
            "quality": quality_summary,
            "portfolio": {
                "current_position_pct": float(portfolio_summary.get("current_position_pct", 0.0)),
                "cash_balance": float(portfolio_summary.get("cash_balance", 0.0)),
                "market_value": float(portfolio_summary.get("market_value", 0.0)),
                "total_asset": float(portfolio_summary.get("total_asset", 0.0)),
            },
            "candidate_summary": candidate_summary,
        }

        item_details = []
        for item in items:
            item_details.append(
                {
                    "symbol": item["symbol"],
                    "title": f"{item['name']} / {item['intent']}",
                    "summary": item["reason_short"],
                    "action": item["action"],
                    "intent": item["intent"],
                    "scores": {
                        "intra_score": float(item["intra_score"]),
                        "category_score": float(item["category_score"]),
                        "final_score": float(item["final_score"]),
                    },
                    "ranks": {
                        "global_rank": int(item["global_rank"]),
                        "category_rank": int(item["category_rank"]),
                    },
                    "weights": {
                        "current_weight": float(item["current_weight"]),
                        "target_weight": float(item["target_weight"]),
                        "delta_weight": float(item["delta_weight"]),
                    },
                    "comparison": {
                        "replacement_symbol": item.get("replacement_symbol", ""),
                        "score_gap_vs_holding": float(item.get("score_gap_vs_holding", 0.0)),
                        "replace_threshold_used": float(item.get("replace_threshold_used", 0.0)),
                        "hold_days": int(item.get("hold_days", 0) or 0),
                    },
                    "feature_snapshot": item.get("score_breakdown", {}).get("features", {}),
                    "rank_snapshot": item.get("score_breakdown", {}).get("ranks", {}),
                    "execution_note": item.get("execution_note", ""),
                }
            )

        return {"overall": overall, "items": item_details}

    def _headline(self, items: list[dict[str, Any]]) -> str:
        if not items:
            return "No trade today"
        actions = {item["action"] for item in items}
        if "buy" in actions and "sell" in actions:
            return "Rebalance the portfolio"
        if "buy" in actions:
            return "Open or add selected ETFs"
        if "sell" in actions:
            return "Reduce or exit weak holdings"
        return "Hold current leaders"

