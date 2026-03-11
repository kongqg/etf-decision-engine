from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from functools import lru_cache
from types import SimpleNamespace
from typing import Any

from app.core.config import get_settings, load_yaml_config


RISK_MODE_LABELS = {
    "conservative": "稳一点",
    "balanced": "正常",
    "aggressive": "冲一点",
}

VALID_RISK_MODES = tuple(RISK_MODE_LABELS.keys())


@dataclass(frozen=True)
class EffectiveDecisionParameters:
    risk_mode: str
    risk_mode_label: str
    preferences: SimpleNamespace
    action_thresholds: dict[str, Any]
    allowed_categories: list[str] | None
    category_score_adjustments: dict[str, float]


class RiskModeService:
    def __init__(self) -> None:
        settings = get_settings()
        self.settings = settings
        self.config = load_yaml_config(settings.config_dir / "risk_modes.yaml")
        self.default_mode = self._normalize_mode(self.config.get("default_mode"))
        self.modes = self.config.get("modes", {})

    def resolve(self, preferences, action_thresholds: dict[str, Any]) -> EffectiveDecisionParameters:
        risk_mode = self._normalize_mode(getattr(preferences, "risk_mode", None))
        profile = self._profile_for_mode(risk_mode)

        effective_preferences = self._clone_preferences(preferences)
        effective_preferences.risk_mode = risk_mode
        effective_preferences.risk_mode_label = self.label_for_mode(risk_mode)

        base_max_total = float(getattr(preferences, "max_total_position_pct", 0.7))
        override_max_total = profile.get("max_total_position_pct")
        if override_max_total is not None:
            effective_preferences.max_total_position_pct = self._merge_numeric_preference(
                base_value=base_max_total,
                override_value=float(override_max_total),
                risk_mode=risk_mode,
                fallback=0.7,
            )
        else:
            effective_preferences.max_total_position_pct = base_max_total
        cash_reserve_pct = float(getattr(preferences, "cash_reserve_pct", 0.0))
        effective_preferences.max_total_position_pct = min(
            float(effective_preferences.max_total_position_pct),
            max(1.0 - cash_reserve_pct, 0.0),
        )

        base_target_holding_days = int(getattr(preferences, "target_holding_days", 5))
        override_target_holding_days = profile.get("target_holding_days")
        if override_target_holding_days is not None:
            effective_preferences.target_holding_days = int(
                self._merge_numeric_preference(
                    base_value=base_target_holding_days,
                    override_value=int(override_target_holding_days),
                    risk_mode=risk_mode,
                    fallback=5,
                )
            )
        else:
            effective_preferences.target_holding_days = base_target_holding_days

        thresholds = deepcopy(action_thresholds)
        fallback_rules = thresholds.setdefault("fallback", {})
        decision_thresholds = thresholds.setdefault("decision_thresholds", {})

        fallback_rules["offensive_threshold"] = self._shift_threshold(
            fallback_rules.get("offensive_threshold", 55.0),
            profile.get("offensive_threshold_delta", 0.0),
        )
        decision_thresholds["open_threshold"] = self._shift_threshold(
            decision_thresholds.get("open_threshold", 58.0),
            profile.get("open_threshold_delta", 0.0),
        )
        decision_thresholds["reduce_threshold"] = self._shift_threshold(
            decision_thresholds.get("reduce_threshold", 58.0),
            profile.get("reduce_threshold_delta", 0.0),
        )
        decision_thresholds["full_exit_threshold"] = self._shift_threshold(
            decision_thresholds.get("full_exit_threshold", 72.0),
            profile.get("full_exit_threshold_delta", 0.0),
        )
        if profile.get("fallback_action"):
            fallback_rules["weak_offensive_action"] = str(profile["fallback_action"])

        allowed_categories = profile.get("allowed_categories")
        if isinstance(allowed_categories, list):
            allowed_categories = [str(category) for category in allowed_categories if str(category).strip()]
        else:
            allowed_categories = None

        raw_adjustments = profile.get("category_score_adjustments", {})
        category_score_adjustments = {
            str(category): float(delta)
            for category, delta in raw_adjustments.items()
        }

        return EffectiveDecisionParameters(
            risk_mode=risk_mode,
            risk_mode_label=self.label_for_mode(risk_mode),
            preferences=effective_preferences,
            action_thresholds=thresholds,
            allowed_categories=allowed_categories,
            category_score_adjustments=category_score_adjustments,
        )

    def label_for_mode(self, risk_mode: str | None) -> str:
        normalized = self._normalize_mode(risk_mode)
        profile = self._profile_for_mode(normalized)
        return str(profile.get("label", RISK_MODE_LABELS.get(normalized, RISK_MODE_LABELS[self.default_mode])))

    def _profile_for_mode(self, risk_mode: str) -> dict[str, Any]:
        profile = self.modes.get(risk_mode)
        if isinstance(profile, dict):
            return profile
        return self.modes.get(self.default_mode, {})

    def _normalize_mode(self, risk_mode: Any) -> str:
        value = str(risk_mode or "").strip().lower()
        if value in VALID_RISK_MODES:
            return value
        return "balanced"

    def _clone_preferences(self, preferences) -> SimpleNamespace:
        payload = {
            key: value
            for key, value in vars(preferences).items()
            if not key.startswith("_")
        }
        return SimpleNamespace(**payload)

    def _merge_numeric_preference(
        self,
        *,
        base_value: float | int,
        override_value: float | int,
        risk_mode: str,
        fallback: float | int,
    ) -> float:
        base = float(base_value if base_value is not None else fallback)
        override = float(override_value)
        if risk_mode == "conservative":
            return min(base, override)
        if risk_mode == "aggressive":
            return max(base, override)
        return base

    def _shift_threshold(self, base_value: Any, delta: Any) -> float:
        shifted = float(base_value) + float(delta or 0.0)
        return max(0.0, min(100.0, shifted))


@lru_cache(maxsize=1)
def get_risk_mode_service() -> RiskModeService:
    return RiskModeService()
