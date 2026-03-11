from __future__ import annotations

import json
import math
from typing import Any

import pandas as pd

from app.services.decision_policy_service import get_decision_policy_service


class ScoringService:
    def __init__(self) -> None:
        self.policy = get_decision_policy_service()
        self.category_profiles = self.policy.category_profiles
        self.action_thresholds = self.policy.action_thresholds

    def score(self, candidates_df: pd.DataFrame) -> pd.DataFrame:
        if candidates_df.empty:
            return candidates_df
        default_days = int(self.policy.horizon_profiles.get("default_target_holding_days", 5))
        evaluation = self.evaluate(
            candidates_df=candidates_df,
            positions_df=pd.DataFrame(),
            target_holding_days=default_days,
            previous_rank_map={},
            days_held_map={},
        )
        return evaluation["scored_df"]

    def evaluate(
        self,
        *,
        candidates_df: pd.DataFrame,
        positions_df: pd.DataFrame,
        target_holding_days: int,
        previous_rank_map: dict[str, int],
        days_held_map: dict[str, int],
        action_thresholds: dict[str, Any] | None = None,
        category_score_adjustments: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        if candidates_df.empty:
            return {
                "scored_df": candidates_df,
                "category_scores_df": pd.DataFrame(),
                "selected_category": "",
                "selected_category_score": 0.0,
                "fallback_action": "no_trade",
                "offensive_edge": False,
            }

        df = self._prepare_frame(candidates_df)
        thresholds = action_thresholds or self.action_thresholds
        category_scores_df = self.score_categories(df, category_score_adjustments=category_score_adjustments)

        top_offensive = self._top_offensive_category(category_scores_df)
        offensive_threshold = float(thresholds.get("fallback", {}).get("offensive_threshold", 55.0))
        offensive_edge = bool(top_offensive and float(top_offensive["category_score"]) >= offensive_threshold)
        fallback_action = str(thresholds.get("fallback", {}).get("weak_offensive_action", "park_in_money_etf"))
        selected_category = str(top_offensive["decision_category"]) if top_offensive and offensive_edge else ""
        selected_category_score = float(top_offensive["category_score"]) if top_offensive and offensive_edge else 0.0

        scored_df = self.score_decision_universe(
            decision_df=df,
            category_scores_df=category_scores_df,
            target_holding_days=target_holding_days,
            previous_rank_map=previous_rank_map,
            days_held_map=days_held_map,
            offensive_edge=offensive_edge,
        )

        return {
            "scored_df": scored_df,
            "category_scores_df": category_scores_df,
            "selected_category": selected_category,
            "selected_category_score": selected_category_score,
            "fallback_action": fallback_action,
            "offensive_edge": offensive_edge,
        }

    def score_decision_universe(
        self,
        *,
        decision_df: pd.DataFrame,
        category_scores_df: pd.DataFrame,
        target_holding_days: int,
        previous_rank_map: dict[str, int],
        days_held_map: dict[str, int],
        offensive_edge: bool,
    ) -> pd.DataFrame:
        if decision_df.empty:
            return decision_df

        df = self._prepare_frame(decision_df)
        category_score_map = category_scores_df.set_index("decision_category").to_dict(orient="index")
        df["category_score"] = df["decision_category"].map(
            lambda category: float(category_score_map.get(category, {}).get("category_score", 0.0))
        )
        df["category_label"] = df["decision_category"].map(self.policy.get_category_label)
        return self.score_instruments(
            df=df,
            category_scores_df=category_scores_df,
            target_holding_days=target_holding_days,
            previous_rank_map=previous_rank_map,
            days_held_map=days_held_map,
            offensive_edge=offensive_edge,
        )

    def score_categories(
        self,
        df: pd.DataFrame,
        *,
        category_score_adjustments: dict[str, float] | None = None,
    ) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        if "decision_category" not in df.columns or "abs_drawdown_20d" not in df.columns:
            df = self._prepare_frame(df)

        grouped = (
            df.groupby("decision_category", dropna=False)
            .agg(
                category_momentum=("momentum_10d", "mean"),
                category_trend=("trend_strength", "mean"),
                category_breadth=("above_ma20_flag", "mean"),
                category_volatility=("volatility_10d", "mean"),
                category_drawdown=("abs_drawdown_20d", "mean"),
                defensive_liquidity=("liquidity_score", "mean"),
                symbol_count=("symbol", "count"),
            )
            .reset_index()
        )
        grouped["category_label"] = grouped["decision_category"].map(self.policy.get_category_label)

        grouped["category_momentum_score"] = self._rank_percentile(grouped["category_momentum"], higher_is_better=True)
        grouped["category_trend_score"] = self._rank_percentile(grouped["category_trend"], higher_is_better=True)
        grouped["category_breadth_score"] = self._rank_percentile(grouped["category_breadth"], higher_is_better=True)
        grouped["category_volatility_score"] = self._rank_percentile(
            grouped["category_volatility"],
            higher_is_better=True,
        )
        grouped["category_drawdown_score"] = self._rank_percentile(grouped["category_drawdown"], higher_is_better=True)
        grouped["defensive_liquidity_score"] = self._rank_percentile(
            grouped["defensive_liquidity"],
            higher_is_better=True,
        )

        offensive_weights = self.category_profiles.get("category_scoring", {}).get("offensive", {})
        defensive_weights = self.category_profiles.get("category_scoring", {}).get("money_etf", {})

        grouped["offensive_score"] = (
            float(offensive_weights.get("w_cat_mom", 0.0)) * grouped["category_momentum_score"]
            + float(offensive_weights.get("w_cat_trend", 0.0)) * grouped["category_trend_score"]
            + float(offensive_weights.get("w_cat_br", 0.0)) * grouped["category_breadth_score"]
            - float(offensive_weights.get("w_cat_vol", 0.0)) * grouped["category_volatility_score"]
            - float(offensive_weights.get("w_cat_dd", 0.0)) * grouped["category_drawdown_score"]
        )
        grouped["defensive_score"] = (
            float(defensive_weights.get("w_def_liq", 0.0)) * grouped["defensive_liquidity_score"]
            - float(defensive_weights.get("w_def_vol", 0.0)) * grouped["category_volatility_score"]
            - float(defensive_weights.get("w_def_dd", 0.0)) * grouped["category_drawdown_score"]
        )
        defensive_category = self.policy.defensive_category()
        grouped["category_score"] = grouped.apply(
            lambda row: float(row["defensive_score"])
            if row["decision_category"] == defensive_category
            else float(row["offensive_score"]),
            axis=1,
        )
        if category_score_adjustments:
            grouped["category_score"] = grouped.apply(
                lambda row: float(row["category_score"])
                + float(category_score_adjustments.get(str(row["decision_category"]), 0.0)),
                axis=1,
            )
        grouped = grouped.sort_values(["category_score", "category_momentum"], ascending=[False, False]).reset_index(drop=True)
        grouped["category_rank"] = grouped.index + 1
        grouped["breakdown_json"] = grouped.apply(self._category_breakdown_json, axis=1)
        return grouped

    def score_instruments(
        self,
        *,
        df: pd.DataFrame,
        category_scores_df: pd.DataFrame,
        target_holding_days: int,
        previous_rank_map: dict[str, int],
        days_held_map: dict[str, int],
        offensive_edge: bool,
    ) -> pd.DataFrame:
        if df.empty:
            return df

        frame = df.copy()
        frame["target_holding_days"] = target_holding_days
        if "profile_offensive_edge" not in frame.columns:
            frame["profile_offensive_edge"] = offensive_edge
        frame["mapped_horizon_profile"] = frame.apply(
            lambda row: self.policy.map_horizon_profile(
                target_holding_days,
                tradability_mode=str(row["tradability_mode"]),
                offensive_edge=bool(row.get("profile_offensive_edge", offensive_edge)),
            ),
            axis=1,
        )
        frame["planned_holding_days"] = frame["mapped_horizon_profile"].map(self.policy.planned_exit_days_for_profile)
        frame["days_held"] = frame["symbol"].map(lambda symbol: int(days_held_map.get(symbol, 0)))
        frame["previous_rank_in_category"] = frame["symbol"].map(lambda symbol: int(previous_rank_map.get(symbol, 0)))
        frame["volatility_spike"] = frame.apply(
            lambda row: float(row["volatility_5d"]) / max(float(row["volatility_20d"]), 1e-6),
            axis=1,
        )
        frame["time_decay"] = frame.apply(
            lambda row: min(float(row["days_held"]) / max(float(row["planned_holding_days"]), 1.0), 1.0),
            axis=1,
        )
        frame["remaining_days"] = frame.apply(
            lambda row: max(int(row["planned_holding_days"]) - int(row["days_held"]), 0),
            axis=1,
        )
        frame["lifecycle_phase"] = frame.apply(
            lambda row: self.policy.resolve_lifecycle_phase(
                int(row["planned_holding_days"]),
                int(row["remaining_days"]),
            ),
            axis=1,
        )

        frame = self._apply_formula_scores(frame)
        frame["decision_weights"] = frame.apply(
            lambda row: self.policy.get_phase_blending(str(row["mapped_horizon_profile"]), str(row["lifecycle_phase"])),
            axis=1,
        )
        frame["decision_score"] = frame.apply(
            lambda row: self._decision_score_from_weights(
                row["decision_weights"],
                float(row["entry_score"]),
                float(row["hold_score"]),
                float(row["exit_score"]),
            ),
            axis=1,
        )
        frame["total_score"] = frame["decision_score"]
        frame = frame.sort_values(
            ["category_score", "decision_score", "entry_score"],
            ascending=[False, False, False],
        ).reset_index(drop=True)
        frame["rank_in_pool"] = frame.index + 1
        frame["rank_in_category"] = (
            frame.groupby("decision_category")["decision_score"].rank(method="first", ascending=False).astype(int)
        )

        category_breakdown_map = category_scores_df.set_index("decision_category").to_dict(orient="index")
        frame["score_gap"] = frame.iloc[0]["decision_score"] - frame["decision_score"]
        frame["breakdown_json"] = frame.apply(
            lambda row: self._instrument_breakdown_json(row, category_breakdown_map.get(str(row["decision_category"]), {})),
            axis=1,
        )
        return frame

    def _prepare_frame(self, candidates_df: pd.DataFrame) -> pd.DataFrame:
        df = candidates_df.copy()
        if "decision_category" not in df.columns or "tradability_mode" not in df.columns:
            metadata = df.apply(
                lambda row: self.policy.classify(
                    symbol=str(row["symbol"]),
                    universe_category=str(row.get("category", "")),
                    asset_class=str(row.get("asset_class", row.get("category", ""))),
                    trade_mode=str(row.get("trade_mode", "")),
                ),
                axis=1,
                result_type="expand",
            )
            df["decision_category"] = metadata["category"]
            df["tradability_mode"] = metadata["tradability_mode"]
        for column, default_value in {
            "momentum_20d": 0.0,
            "ma20": df.get("close_price", 0.0),
            "ret_1d": df.get("pct_change", 0.0),
            "volatility_5d": 0.0,
            "volatility_20d": 0.0,
            "avg_turnover_20d": df.get("avg_amount_20d", 0.0),
            "liquidity_score": df.get("avg_amount_20d", 0.0),
            "category_return_10d": 0.0,
            "relative_strength_10d": 0.0,
            "above_ma20_flag": False,
        }.items():
            if column not in df.columns:
                df[column] = default_value

        if "liquidity_score" in df.columns and df["liquidity_score"].max() > 1000:
            df["liquidity_score"] = (df["liquidity_score"] + 1.0).map(math.log)

        if "above_ma20_flag" not in df.columns or df["above_ma20_flag"].isna().all():
            df["above_ma20_flag"] = (df["close_price"] > df["ma20"]).astype(int)
        else:
            df["above_ma20_flag"] = df["above_ma20_flag"].astype(int)

        category_return = df.groupby("decision_category")["momentum_10d"].transform("mean")
        df["category_return_10d"] = category_return.fillna(0.0)
        df["relative_strength_10d"] = df["momentum_10d"] - df["category_return_10d"]
        df["abs_drawdown_20d"] = df["drawdown_20d"].abs()
        return df

    def _apply_formula_scores(self, frame: pd.DataFrame) -> pd.DataFrame:
        df = frame.copy()
        head_configs = self.category_profiles.get("category_heads", {})
        entry_raw = []
        hold_raw = []
        entry_details = []
        hold_details = []

        for _, row in df.iterrows():
            category = str(row["decision_category"])
            head = head_configs.get(category, {})
            category_df = df[df["decision_category"] == category].copy()
            comparison_df = category_df if len(category_df) > 1 else df

            entry_payload = self._formula_for_row(
                reference_df=comparison_df,
                row_symbol=str(row["symbol"]),
                formula=head.get("entry", {}),
            )
            hold_payload = self._formula_for_row(
                reference_df=comparison_df,
                row_symbol=str(row["symbol"]),
                formula=head.get("hold", {}),
            )
            entry_raw.append(entry_payload["raw_score"])
            hold_raw.append(hold_payload["raw_score"])
            entry_details.append(entry_payload["details"])
            hold_details.append(hold_payload["details"])

        df["entry_raw_score"] = entry_raw
        df["hold_raw_score"] = hold_raw
        df["entry_breakdown"] = entry_details
        df["hold_breakdown"] = hold_details
        df["provisional_rank_in_category"] = (
            df.groupby("decision_category")["entry_raw_score"].rank(method="first", ascending=False).astype(int)
        )
        df["rank_drop"] = df.apply(
            lambda row: max(int(row["provisional_rank_in_category"]) - int(row["previous_rank_in_category"]), 0)
            if int(row["previous_rank_in_category"]) > 0
            else 0,
            axis=1,
        )

        exit_raw = []
        exit_details = []
        for _, row in df.iterrows():
            category = str(row["decision_category"])
            head = head_configs.get(category, {})
            category_df = df[df["decision_category"] == category].copy()
            comparison_df = category_df if len(category_df) > 1 else df
            exit_payload = self._formula_for_row(
                reference_df=comparison_df,
                row_symbol=str(row["symbol"]),
                formula=head.get("exit", {}),
            )
            exit_raw.append(exit_payload["raw_score"])
            exit_details.append(exit_payload["details"])
        df["exit_raw_score"] = exit_raw
        df["exit_breakdown"] = exit_details
        df["entry_score"] = df.groupby("decision_category")["entry_raw_score"].transform(
            lambda series: self._rank_percentile(series, higher_is_better=True)
        )
        df["hold_score"] = df.groupby("decision_category")["hold_raw_score"].transform(
            lambda series: self._rank_percentile(series, higher_is_better=True)
        )
        df["exit_score"] = df.groupby("decision_category")["exit_raw_score"].transform(
            lambda series: self._rank_percentile(series, higher_is_better=True)
        )
        category_sizes = df.groupby("decision_category")["symbol"].transform("count")
        defensive_category = self.policy.defensive_category()
        single_symbol_mask = (category_sizes <= 1) & (df["decision_category"] != defensive_category)
        if single_symbol_mask.any():
            for column, head_key in {
                "entry_score": "entry",
                "hold_score": "hold",
                "exit_score": "exit",
            }.items():
                df.loc[single_symbol_mask, column] = df.loc[single_symbol_mask].apply(
                    lambda row: self._normalize_formula_score(
                        raw_score=float(row[column.replace("_score", "_raw_score")]),
                        formula=head_configs.get(str(row["decision_category"]), {}).get(head_key, {}),
                    ),
                    axis=1,
                )
        return df

    def _formula_for_row(
        self,
        *,
        reference_df: pd.DataFrame,
        row_symbol: str,
        formula: dict[str, float],
    ) -> dict[str, Any]:
        raw_score = 0.0
        details = []
        row_index = reference_df.index[reference_df["symbol"] == row_symbol][0]
        for feature_name, weight in formula.items():
            values = self._feature_values(reference_df, feature_name)
            component_scores = self._rank_percentile(values, higher_is_better=True)
            raw_value = float(values.loc[row_index])
            component_value = float(component_scores.loc[row_index])
            contribution = float(weight) * component_value
            raw_score += contribution
            details.append(
                {
                    "feature": feature_name,
                    "weight": float(weight),
                    "raw_value": round(raw_value, 4),
                    "percentile": round(component_value, 2),
                    "contribution": round(contribution, 2),
                }
            )
        return {"raw_score": raw_score, "details": details}

    def _normalize_formula_score(self, *, raw_score: float, formula: dict[str, float]) -> float:
        if not formula:
            return 50.0
        min_raw = 0.0
        max_raw = 0.0
        for weight in formula.values():
            weight = float(weight)
            if weight >= 0:
                max_raw += weight * 100.0
            else:
                min_raw += weight * 100.0
        if math.isclose(max_raw, min_raw):
            return 50.0
        normalized = (float(raw_score) - min_raw) / (max_raw - min_raw) * 100.0
        return max(0.0, min(100.0, normalized))

    def _feature_values(
        self,
        df: pd.DataFrame,
        feature_name: str,
    ) -> pd.Series:
        if feature_name == "abs_drawdown_20d":
            return df["drawdown_20d"].abs().rename(feature_name)
        if feature_name == "volatility_spike":
            return df.get("volatility_spike", pd.Series([0.0] * len(df), index=df.index)).rename(feature_name)
        if feature_name == "time_decay":
            return df.get("time_decay", pd.Series([0.0] * len(df), index=df.index)).rename(feature_name)
        if feature_name == "rank_drop":
            return df.get("rank_drop", pd.Series([0.0] * len(df), index=df.index)).rename(feature_name)
        if feature_name in df.columns:
            return df[feature_name].astype(float)
        return pd.Series([0.0] * len(df), index=df.index, name=feature_name)

    def _decision_score_from_weights(self, weights: dict[str, float], entry_score: float, hold_score: float, exit_score: float) -> float:
        return (
            float(weights.get("entry", 0.0)) * entry_score
            + float(weights.get("hold", 0.0)) * hold_score
            - float(weights.get("exit", 0.0)) * exit_score
        )

    def _category_breakdown_json(self, row: pd.Series) -> str:
        payload = {
            "decision_category": row["decision_category"],
            "category_label": row["category_label"],
            "category_score": round(float(row["category_score"]), 2),
            "offensive_score": round(float(row["offensive_score"]), 2),
            "defensive_score": round(float(row["defensive_score"]), 2),
            "raw_metrics": {
                "category_momentum": round(float(row["category_momentum"]), 4),
                "category_trend": round(float(row["category_trend"]), 4),
                "category_breadth": round(float(row["category_breadth"]), 4),
                "category_volatility": round(float(row["category_volatility"]), 4),
                "category_drawdown": round(float(row["category_drawdown"]), 4),
                "defensive_liquidity": round(float(row["defensive_liquidity"]), 4),
            },
            "normalized_components": {
                "category_momentum_score": round(float(row["category_momentum_score"]), 2),
                "category_trend_score": round(float(row["category_trend_score"]), 2),
                "category_breadth_score": round(float(row["category_breadth_score"]), 2),
                "category_volatility_score": round(float(row["category_volatility_score"]), 2),
                "category_drawdown_score": round(float(row["category_drawdown_score"]), 2),
                "defensive_liquidity_score": round(float(row["defensive_liquidity_score"]), 2),
            },
        }
        return json.dumps(payload, ensure_ascii=False)

    def _instrument_breakdown_json(self, row: pd.Series, category_payload: dict[str, Any]) -> str:
        payload = {
            "symbol": row["symbol"],
            "name": row.get("name", row["symbol"]),
            "decision_category": row["decision_category"],
            "category_label": row["category_label"],
            "tradability_mode": row["tradability_mode"],
            "target_holding_days": int(row["target_holding_days"]),
            "mapped_horizon_profile": row["mapped_horizon_profile"],
            "horizon_profile_label": self.policy.get_profile_label(str(row["mapped_horizon_profile"])),
            "lifecycle_phase": row["lifecycle_phase"],
            "category_score": round(float(row["category_score"]), 2),
            "entry_score": round(float(row["entry_score"]), 2),
            "hold_score": round(float(row["hold_score"]), 2),
            "exit_score": round(float(row["exit_score"]), 2),
            "decision_score": round(float(row["decision_score"]), 2),
            "rank_in_pool": int(row["rank_in_pool"]),
            "rank_in_category": int(row["rank_in_category"]),
            "previous_rank_in_category": int(row.get("previous_rank_in_category", 0)),
            "rank_drop": int(row.get("rank_drop", 0)),
            "phase_weights": {
                key: round(float(value), 2) for key, value in row["decision_weights"].items()
            },
            "category_breakdown": category_payload,
            "entry_breakdown": row["entry_breakdown"],
            "hold_breakdown": row["hold_breakdown"],
            "exit_breakdown": row["exit_breakdown"],
        }
        return json.dumps(payload, ensure_ascii=False)

    def _top_offensive_category(self, category_scores_df: pd.DataFrame) -> dict[str, Any] | None:
        if category_scores_df.empty:
            return None
        offensive_categories = set(self.policy.offensive_categories())
        offensive_df = category_scores_df[category_scores_df["decision_category"].isin(offensive_categories)].copy()
        if offensive_df.empty:
            return None
        return offensive_df.sort_values(["category_score", "category_momentum"], ascending=[False, False]).iloc[0].to_dict()

    def _rank_percentile(self, series: pd.Series, higher_is_better: bool = True) -> pd.Series:
        if series.empty:
            return pd.Series(dtype=float)
        clean = pd.to_numeric(series, errors="coerce").fillna(0.0)
        if clean.nunique(dropna=False) <= 1:
            return pd.Series([50.0] * len(clean), index=clean.index, dtype=float)
        ranked = clean.rank(pct=True, ascending=higher_is_better)
        return ranked.fillna(0.0) * 100.0
