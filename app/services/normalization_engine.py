from __future__ import annotations

from typing import Any

import pandas as pd

from app.core.config import get_settings, load_yaml_config


class NormalizationEngine:
    def __init__(self) -> None:
        settings = get_settings()
        config = load_yaml_config(settings.config_dir / "strategy_scoring.yaml")
        normalization = config.get("normalization", {})
        self.scale = float(normalization.get("scale", 100.0))
        self.directions = {
            str(key): str(value).strip().lower()
            for key, value in normalization.get("directions", {}).items()
        }

    def apply(self, frame: pd.DataFrame, *, category_column: str = "decision_category") -> pd.DataFrame:
        if frame.empty:
            return frame.copy()

        df = frame.copy()
        for feature, direction in self.directions.items():
            rank_column = self._rank_column_name(feature)
            df[rank_column] = (
                df.groupby(category_column, dropna=False)[feature]
                .transform(lambda series: self._percentile_rank(series, direction=direction))
                .fillna(0.0)
            )
        return df

    def _percentile_rank(self, series: pd.Series, *, direction: str) -> pd.Series:
        clean = pd.to_numeric(series, errors="coerce")
        count = len(clean)
        if count == 0:
            return pd.Series(dtype=float)
        if count == 1:
            return pd.Series([self.scale], index=clean.index, dtype=float)

        ranks = clean.rank(method="average", ascending=True)
        denominator = max(count - 1, 1)
        if direction == "lower":
            percentile = (count - ranks) / denominator
        else:
            percentile = (ranks - 1) / denominator
        return percentile.clip(lower=0.0, upper=1.0) * self.scale

    def _rank_column_name(self, feature: str) -> str:
        mapping = {
            "momentum_20d": "momentum_20d_rank",
            "momentum_10d": "momentum_10d_rank",
            "momentum_5d": "momentum_5d_rank",
            "trend_strength": "trend_rank",
            "volatility_20d": "volatility_rank",
            "drawdown_20d": "drawdown_rank",
            "liquidity_score": "liquidity_rank",
        }
        return mapping.get(feature, f"{feature}_rank")
