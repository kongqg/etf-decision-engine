from __future__ import annotations

from typing import Any

MARKET_REGIME_LABELS = {
    "risk_on": "偏进攻",
    "neutral": "中性",
    "risk_off": "偏防守",
}

QUALITY_STATUS_LABELS = {
    "ok": "正常",
    "weak": "需谨慎",
    "blocked": "已拦截",
}


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
        market_regime_label = self._market_regime_label(market_regime.get("market_regime", "neutral"))
        reasons = [
            f"当前市场状态是 {market_regime_label}，它主要影响预算层。",
            f"当前目标总预算为 {float(allocation.get('total_budget_pct', 0.0)) * 100:.1f}%。",
            f"当前替换阈值为 {float(allocation.get('replace_threshold', 0.0)):.1f} 分。",
        ]
        if quality_summary:
            quality_status = str(quality_summary.get("quality_status", "unknown"))
            reasons.append(
                f"当前数据质量状态为 {QUALITY_STATUS_LABELS.get(quality_status, quality_status)} "
                f"（{quality_summary.get('verification_status', '')}）。"
            )
        if items:
            reasons.append("系统会先算目标仓位，再和当前持仓对比，最后才生成具体动作。")
        else:
            reasons.append("在分数、预算和替换约束之后，没有 ETF 形成可执行的仓位变化。")

        overall = {
            "headline": self._headline(items),
            "market_regime": market_regime_label,
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
            return "今天暂不交易"
        actions = {item["action"] for item in items}
        if "buy" in actions and "sell" in actions:
            return "调仓换仓"
        if "buy" in actions:
            return "开仓或加仓目标 ETF"
        if "sell" in actions:
            return "减仓或退出转弱持仓"
        return "继续持有当前领先标的"

    def _market_regime_label(self, market_regime: str) -> str:
        return MARKET_REGIME_LABELS.get(str(market_regime), str(market_regime))
