from __future__ import annotations

from typing import Any

import pandas as pd

from app.core.config import get_settings, load_yaml_config


class UniverseFilterService:
    def __init__(self) -> None:
        settings = get_settings()
        self.config = load_yaml_config(settings.config_dir / "strategy_filters.yaml")
        self.preference_exclusions = {
            str(category): str(flag)
            for category, flag in self.config.get("preference_exclusions", {}).items()
        }

    def apply(self, features_df: pd.DataFrame, preferences: Any | None = None) -> pd.DataFrame:
        if features_df.empty:
            return features_df.copy()

        df = features_df.copy()
        reasons: list[list[str]] = []
        passes: list[bool] = []

        min_history_days = int(self.config.get("min_history_days", 21))
        min_avg_amount = float(self.config.get("min_avg_amount_20d", 0.0))
        min_latest_amount = float(self.config.get("min_latest_amount", 0.0))
        max_volatility = float(self.config.get("max_volatility_20d", 999.0))
        exclude_fallback = bool(self.config.get("exclude_fallback_sources", True))
        exclude_stale = bool(self.config.get("exclude_stale_data", True))
        exclude_anomaly = bool(self.config.get("exclude_anomaly", True))

        for _, row in df.iterrows():
            row_reasons: list[str] = []
            category = str(row.get("decision_category") or row.get("category") or "")
            for excluded_category, preference_attr in self.preference_exclusions.items():
                if category == excluded_category and preferences is not None and not bool(getattr(preferences, preference_attr, False)):
                    row_reasons.append(f"preference_{preference_attr}_disabled")

            if not bool(row.get("formal_eligible", True)):
                row_reasons.append("data_not_formal_ready")
            if exclude_fallback and str(row.get("source_code", "")).lower() in {"fallback", "mock", "simulated"}:
                row_reasons.append("fallback_source")
            if exclude_stale and bool(row.get("stale_data_flag", False)):
                row_reasons.append("stale_data")
            if exclude_anomaly and bool(row.get("anomaly_flag", False)):
                row_reasons.append("anomaly_flag")
            if float(row.get("avg_amount_20d", 0.0) or 0.0) < max(float(row.get("min_avg_amount", 0.0) or 0.0), min_avg_amount):
                row_reasons.append("avg_amount_too_low")
            if float(row.get("latest_amount", 0.0) or 0.0) < min_latest_amount:
                row_reasons.append("latest_amount_too_low")
            if float(row.get("volatility_20d", 0.0) or 0.0) > max_volatility:
                row_reasons.append("volatility_too_high")
            if self._history_rows(row) < min_history_days:
                row_reasons.append("history_too_short")

            reasons.append(row_reasons)
            passes.append(not row_reasons)

        df["filter_reasons"] = reasons
        df["filter_pass"] = passes
        df["basic_filter_pass"] = passes
        df["basic_filter_reason"] = [";".join(items) for items in reasons]
        return df

    def _history_rows(self, row: pd.Series) -> int:
        latest_row_date = row.get("latest_row_date")
        trade_date = row.get("trade_date")
        if latest_row_date is None or trade_date is None:
            return 999
        try:
            return max((trade_date - latest_row_date).days, 0) + 21
        except TypeError:
            return 999

