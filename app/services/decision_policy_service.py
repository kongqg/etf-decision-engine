from __future__ import annotations

from functools import lru_cache
from typing import Any

from app.core.config import get_settings, load_yaml_config


ACTION_LABELS = {
    "buy_open": "开仓买入",
    "buy_add": "继续加仓",
    "hold": "继续持有",
    "reduce": "减仓",
    "sell_exit": "卖出退出",
    "no_trade": "暂不交易",
    "park_in_money_etf": "转入货币ETF",
}


class DecisionPolicyService:
    def __init__(self) -> None:
        settings = get_settings()
        config_dir = settings.config_dir
        self.tradability_map = load_yaml_config(config_dir / "tradability_map.yaml")
        self.category_profiles = load_yaml_config(config_dir / "category_profiles.yaml")
        self.horizon_profiles = load_yaml_config(config_dir / "horizon_profiles.yaml")
        self.phase_blending = load_yaml_config(config_dir / "phase_blending.yaml")
        self.action_thresholds = load_yaml_config(config_dir / "action_thresholds.yaml")

    def classify(
        self,
        *,
        symbol: str,
        universe_category: str,
        asset_class: str,
        trade_mode: str | None = None,
    ) -> dict[str, str]:
        overrides = self.tradability_map.get("symbol_overrides", {}).get(symbol, {})
        category = overrides.get("category") or self._resolve_category(universe_category, asset_class)
        tradability_mode = overrides.get("tradability_mode") or self.tradability_map.get("tradability_defaults", {}).get(
            category,
            self._normalize_trade_mode(trade_mode),
        )
        return {
            "category": category,
            "category_label": self.get_category_label(category),
            "tradability_mode": self._normalize_trade_mode(tradability_mode),
        }

    def get_category_label(self, category: str) -> str:
        return str(self.tradability_map.get("display_names", {}).get(category, category))

    def offensive_categories(self) -> list[str]:
        return list(self.category_profiles.get("selection", {}).get("offensive_categories", []))

    def defensive_category(self) -> str:
        return str(self.category_profiles.get("selection", {}).get("defensive_category", "money_etf"))

    def max_selected_etfs(self) -> int:
        config_value = self.action_thresholds.get("selection", {}).get(
            "max_selected_etfs",
            self.category_profiles.get("selection", {}).get("max_selected_etfs", 1),
        )
        return max(1, int(config_value))

    def map_horizon_profile(
        self,
        target_holding_days: int,
        *,
        tradability_mode: str,
        offensive_edge: bool = True,
    ) -> str:
        mapping = self.horizon_profiles.get("mapping", {})
        if not offensive_edge:
            return str(mapping.get("offensive_edge_fallback_profile", "defensive_cash"))
        if target_holding_days <= 1:
            if tradability_mode == "t0":
                return "intraday_t0"
            return str(mapping.get("t1_intraday_fallback_profile", "swing"))
        if target_holding_days <= 10:
            return "swing"
        if target_holding_days <= 19:
            return str(mapping.get("intermediate_days_default_profile", "swing"))
        if target_holding_days <= 40:
            return "rotation"
        return "rotation"

    def get_profile_label(self, profile: str) -> str:
        payload = self.horizon_profiles.get("profiles", {}).get(profile, {})
        return str(payload.get("label", profile))

    def planned_exit_days_for_profile(self, profile: str) -> int:
        payload = self.horizon_profiles.get("profiles", {}).get(profile, {})
        return max(0, int(payload.get("planned_exit_days", 0)))

    def resolve_lifecycle_phase(self, planned_holding_days: int, remaining_days: int) -> str:
        thresholds = self.phase_blending.get("phase_thresholds", {})
        if planned_holding_days <= 0:
            return "exit_phase"
        remaining_ratio = remaining_days / max(planned_holding_days, 1)
        build_floor = float(thresholds.get("build_phase_min_remaining_ratio", 0.66))
        hold_floor = float(thresholds.get("hold_phase_min_remaining_ratio", 0.33))
        if remaining_ratio > build_floor:
            return "build_phase"
        if remaining_ratio > hold_floor:
            return "hold_phase"
        return "exit_phase"

    def get_phase_blending(self, profile: str, phase: str) -> dict[str, float]:
        profile_rules = self.phase_blending.get("profiles", {}).get(profile) or self.phase_blending.get("profiles", {}).get(
            "swing",
            {},
        )
        weights = profile_rules.get(phase) or {}
        return {
            "entry": float(weights.get("entry", 0.0)),
            "hold": float(weights.get("hold", 0.0)),
            "exit": float(weights.get("exit", 0.0)),
        }

    def session_executable_now(self, session_mode: str) -> bool:
        executable_modes = self.action_thresholds.get("session_rules", {}).get("executable_session_modes", ["intraday"])
        return session_mode in executable_modes

    def session_blocked_reason(self, session_mode: str) -> str:
        return str(
            self.action_thresholds.get("session_rules", {}).get("blocked_reason_by_session", {}).get(session_mode, "")
        )

    def action_label(self, action_code: str) -> str:
        return ACTION_LABELS.get(action_code, action_code)

    def _resolve_category(self, universe_category: str, asset_class: str) -> str:
        aliases = self.tradability_map.get("category_aliases", {})
        for category, payload in aliases.items():
            if universe_category in payload.get("universe_categories", []):
                return str(category)
            if asset_class in payload.get("asset_classes", []):
                return str(category)
        return "stock_etf"

    def _normalize_trade_mode(self, value: str | None) -> str:
        if value is None:
            return "t1"
        normalized = str(value).strip().lower().replace("+", "")
        if normalized in {"t0", "0"}:
            return "t0"
        return "t1"


@lru_cache(maxsize=1)
def get_decision_policy_service() -> DecisionPolicyService:
    return DecisionPolicyService()
