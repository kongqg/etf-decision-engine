from __future__ import annotations

import json
from typing import Any

import pandas as pd

from app.core.config import get_settings, load_yaml_config
from app.services.normalization_engine import NormalizationEngine


class ScoringEngine:
    def __init__(self) -> None:
        settings = get_settings()
        self.config = load_yaml_config(settings.config_dir / "strategy_scoring.yaml")
        self.normalization_engine = NormalizationEngine()
        self.intra_weights = {
            str(key): float(value)
            for key, value in self.config.get("intra_score_weights", {}).items()
        }
        self.category_config = self.config.get("category_score", {})
        self.category_weights = {
            str(key): float(value)
            for key, value in self.category_config.get("weights", {}).items()
        }
        self.final_weights = {
            str(key): float(value)
            for key, value in self.config.get("final_score_weights", {}).items()
        }
        self.top_n = max(1, int(self.category_config.get("top_n", 3)))
        self.breadth_thresholds = self.category_config.get("breadth_positive_thresholds", {})
        self.breadth_components = {
            str(key): float(value)
            for key, value in self.category_config.get("breadth_components", {}).items()
        }

    def score(self, frame: pd.DataFrame) -> dict[str, Any]:
        if frame.empty:
            return {"scored_df": frame.copy(), "category_scores": []}

        df = frame.copy()
        df = self.normalization_engine.apply(df)
        df["intra_score"] = self._weighted_sum(df, self.intra_weights)

        category_df = self._build_category_scores(df)
        merged = df.merge(
            category_df[["decision_category", "category_score", "top_mean_intrascore", "breadth_score", "category_momentum_score"]],
            on="decision_category",
            how="left",
        )
        merged["category_score"] = pd.to_numeric(merged["category_score"], errors="coerce").fillna(0.0)
        merged["final_score"] = (
            merged["intra_score"] * float(self.final_weights.get("intra_score", 0.7))
            + merged["category_score"] * float(self.final_weights.get("category_score", 0.3))
        )
        merged = merged.sort_values(["final_score", "intra_score", "symbol"], ascending=[False, False, True]).reset_index(drop=True)
        merged["global_rank"] = range(1, len(merged) + 1)
        merged["category_rank"] = (
            merged.groupby("decision_category")["final_score"]
            .rank(method="first", ascending=False)
            .astype(int)
        )
        merged["total_score"] = merged["final_score"]
        merged["rank_in_pool"] = merged["global_rank"]
        merged["breakdown_json"] = merged.apply(self._build_breakdown_json, axis=1)
        merged["score_breakdown_json"] = merged["breakdown_json"]
        return {
            "scored_df": merged,
            "category_scores": category_df.sort_values("category_score", ascending=False).to_dict(orient="records"),
        }

    def _build_category_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        grouped = pd.DataFrame(
            [self._category_row(category_df) for _, category_df in df.groupby("decision_category", dropna=False, sort=False)]
        )
        if grouped.empty:
            return grouped
        grouped["category_momentum_score"] = self._global_percentile(grouped["category_momentum_raw"])
        grouped["category_score"] = (
            grouped["top_mean_intrascore"] * float(self.category_weights.get("top_mean_intrascore", 0.5))
            + grouped["breadth_score"] * float(self.category_weights.get("breadth_score", 0.3))
            + grouped["category_momentum_score"] * float(self.category_weights.get("category_momentum_score", 0.2))
        )
        grouped["category_rank"] = grouped["category_score"].rank(method="first", ascending=False).astype(int)
        return grouped

    def _category_row(self, category_df: pd.DataFrame) -> pd.Series:
        ordered = category_df.sort_values("intra_score", ascending=False)
        top_mean = float(ordered["intra_score"].head(self.top_n).mean()) if not ordered.empty else 0.0
        breadth_score = self._breadth_score(category_df)
        category_momentum_raw = float(
            category_df["momentum_20d_rank"].mean() * 0.6
            + category_df["trend_rank"].mean() * 0.4
        )
        return pd.Series(
            {
                "decision_category": str(category_df["decision_category"].iloc[0]),
                "category_label": str(category_df["category_label"].iloc[0]) if "category_label" in category_df else str(category_df["decision_category"].iloc[0]),
                "symbol_count": int(len(category_df)),
                "top_mean_intrascore": top_mean,
                "breadth_score": breadth_score,
                "category_momentum_raw": category_momentum_raw,
            }
        )

    def _breadth_score(self, df: pd.DataFrame) -> float:
        if df.empty:
            return 0.0
        momentum_threshold = float(self.breadth_thresholds.get("momentum_20d_min", 0.0))
        trend_threshold = float(self.breadth_thresholds.get("trend_strength_min", 0.0))
        positive_momentum = (pd.to_numeric(df["momentum_20d"], errors="coerce").fillna(0.0) >= momentum_threshold).mean()
        positive_trend = (pd.to_numeric(df["trend_strength"], errors="coerce").fillna(0.0) >= trend_threshold).mean()
        above_ma20 = df["above_ma20_flag"].fillna(False).astype(bool).mean()
        return float(
            (
                positive_momentum * float(self.breadth_components.get("positive_momentum_20d", 0.4))
                + positive_trend * float(self.breadth_components.get("positive_trend_strength", 0.3))
                + above_ma20 * float(self.breadth_components.get("above_ma20_flag", 0.3))
            )
            * 100.0
        )

    def _weighted_sum(self, df: pd.DataFrame, weights: dict[str, float]) -> pd.Series:
        total = pd.Series(0.0, index=df.index)
        for column, weight in weights.items():
            total = total + pd.to_numeric(df[column], errors="coerce").fillna(0.0) * float(weight)
        return total

    def _global_percentile(self, series: pd.Series) -> pd.Series:
        count = len(series)
        if count == 0:
            return pd.Series(dtype=float)
        if count == 1:
            return pd.Series([100.0], index=series.index, dtype=float)
        ranks = pd.to_numeric(series, errors="coerce").fillna(0.0).rank(method="average", ascending=True)
        return ((ranks - 1) / max(count - 1, 1)).clip(lower=0.0, upper=1.0) * 100.0

    def _build_breakdown_json(self, row: pd.Series) -> str:
        payload = {
            "ranks": {
                "momentum_20d_rank": float(row.get("momentum_20d_rank", 0.0)),
                "momentum_10d_rank": float(row.get("momentum_10d_rank", 0.0)),
                "momentum_5d_rank": float(row.get("momentum_5d_rank", 0.0)),
                "trend_rank": float(row.get("trend_rank", 0.0)),
                "volatility_rank": float(row.get("volatility_rank", 0.0)),
                "drawdown_rank": float(row.get("drawdown_rank", 0.0)),
                "liquidity_rank": float(row.get("liquidity_rank", 0.0)),
            },
            "features": {
                "momentum_20d": float(row.get("momentum_20d", 0.0)),
                "momentum_10d": float(row.get("momentum_10d", 0.0)),
                "momentum_5d": float(row.get("momentum_5d", 0.0)),
                "trend_strength": float(row.get("trend_strength", 0.0)),
                "volatility_20d": float(row.get("volatility_20d", 0.0)),
                "drawdown_20d": float(row.get("drawdown_20d", 0.0)),
                "liquidity_score": float(row.get("liquidity_score", 0.0)),
                "above_ma20_flag": bool(row.get("above_ma20_flag", False)),
            },
            "weights": {
                "intra_score_weights": self.intra_weights,
                "final_score_weights": self.final_weights,
                "category_score_weights": self.category_weights,
            },
            "scores": {
                "intra_score": float(row.get("intra_score", 0.0)),
                "category_score": float(row.get("category_score", 0.0)),
                "final_score": float(row.get("final_score", 0.0)),
            },
            "ranks_meta": {
                "global_rank": int(row.get("global_rank", 0) or 0),
                "category_rank": int(row.get("category_rank", 0) or 0),
            },
            "category_components": {
                "top_mean_intrascore": float(row.get("top_mean_intrascore", 0.0)),
                "breadth_score": float(row.get("breadth_score", 0.0)),
                "category_momentum_score": float(row.get("category_momentum_score", 0.0)),
            },
        }
        return json.dumps(payload, ensure_ascii=False)
