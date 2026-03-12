from __future__ import annotations

from typing import Any

import pandas as pd

from app.core.config import get_settings, load_yaml_config


class MarketRegimeService:
    def __init__(self) -> None:
        settings = get_settings()
        self.config = load_yaml_config(settings.config_dir / "market_regime.yaml")
        self.thresholds = self.config.get("thresholds", {})
        self.budgets = self.config.get("budgets", {})

    def evaluate(self, features_df: pd.DataFrame) -> dict[str, Any]:
        if features_df.empty:
            return self._payload("risk_off", 0.0, 0.0, 0.0)

        broad_df = features_df[features_df["decision_category"] == "stock_etf"].copy()
        risk_df = features_df[features_df["decision_category"].isin(["stock_etf", "cross_border_etf", "gold_etf"])].copy()

        broad_index_score = float(
            50.0
            + broad_df["momentum_20d"].mean() * 2.0
            + broad_df["trend_strength"].mean() * 3.0
        ) if not broad_df.empty else 0.0
        trend_score = float(
            (risk_df["above_ma20_flag"].fillna(False).astype(bool).mean() * 100.0) * 0.6
            + risk_df["momentum_10d"].mean() * 1.5
        ) if not risk_df.empty else 0.0
        risk_appetite_score = float(
            50.0
            + (
                features_df[features_df["decision_category"].isin(["stock_etf", "cross_border_etf"])]["momentum_10d"].mean()
                - features_df[features_df["decision_category"].isin(["bond_etf", "money_etf"])]["momentum_10d"].mean()
            )
            * 2.5
        )

        composite_score = (broad_index_score * 0.4) + (trend_score * 0.35) + (risk_appetite_score * 0.25)
        risk_on_min = float(self.thresholds.get("risk_on_min_score", 60.0))
        neutral_min = float(self.thresholds.get("neutral_min_score", 40.0))

        if composite_score >= risk_on_min:
            regime = "risk_on"
        elif composite_score >= neutral_min:
            regime = "neutral"
        else:
            regime = "risk_off"

        return self._payload(
            regime,
            broad_index_score=round(broad_index_score, 2),
            risk_appetite_score=round(risk_appetite_score, 2),
            trend_score=round(trend_score, 2),
        )

    def _payload(
        self,
        regime: str,
        broad_index_score: float,
        risk_appetite_score: float,
        trend_score: float,
    ) -> dict[str, Any]:
        budget = self.budgets.get(regime, {})
        return {
            "market_regime": regime,
            "broad_index_score": broad_index_score,
            "risk_appetite_score": risk_appetite_score,
            "trend_score": trend_score,
            "recommended_position_pct": float(budget.get("total_budget_pct", 0.0)),
            "budget_total_pct": float(budget.get("total_budget_pct", 0.0)),
            "budget_by_category": {
                str(key): float(value)
                for key, value in budget.get("category_caps", {}).items()
            },
            "evidence": {
                "broad_index_score": broad_index_score,
                "risk_appetite_score": risk_appetite_score,
                "trend_score": trend_score,
            },
        }

