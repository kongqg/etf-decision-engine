from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from app.core.config import get_settings, load_yaml_config


RISK_MODE_LABELS = {
    "conservative": "稳一点",
    "balanced": "正常",
    "aggressive": "冲一点",
}


@dataclass(frozen=True)
class RiskModeProfile:
    risk_mode: str
    label: str
    total_budget_multiplier: float
    single_weight_multiplier: float
    category_cap_multiplier: float
    replace_threshold_delta: float


class RiskModeService:
    def __init__(self) -> None:
        settings = get_settings()
        config = load_yaml_config(settings.config_dir / "risk_modes.yaml")
        self.default_mode = str(config.get("default_mode", "balanced")).strip().lower() or "balanced"
        self.modes = config.get("modes", {})

    def resolve(self, risk_mode: str | None) -> RiskModeProfile:
        normalized = self._normalize_mode(risk_mode)
        payload = self.modes.get(normalized, self.modes.get(self.default_mode, {}))
        return RiskModeProfile(
            risk_mode=normalized,
            label=str(payload.get("label", RISK_MODE_LABELS.get(normalized, "正常"))),
            total_budget_multiplier=float(payload.get("total_budget_multiplier", 1.0)),
            single_weight_multiplier=float(payload.get("single_weight_multiplier", 1.0)),
            category_cap_multiplier=float(payload.get("category_cap_multiplier", 1.0)),
            replace_threshold_delta=float(payload.get("replace_threshold_delta", 0.0)),
        )

    def label_for_mode(self, risk_mode: str | None) -> str:
        return self.resolve(risk_mode).label

    def _normalize_mode(self, value: Any) -> str:
        normalized = str(value or "").strip().lower()
        return normalized if normalized in self.modes else self.default_mode


@lru_cache(maxsize=1)
def get_risk_mode_service() -> RiskModeService:
    return RiskModeService()
