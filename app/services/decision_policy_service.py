from __future__ import annotations

from functools import lru_cache

from app.core.config import get_settings, load_yaml_config


ACTION_LABELS = {
    "buy": "buy",
    "hold": "hold",
    "sell": "sell",
    "no_trade": "no_trade",
}


class DecisionPolicyService:
    def __init__(self) -> None:
        settings = get_settings()
        self.tradability_map = load_yaml_config(settings.config_dir / "tradability_map.yaml")

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
            "category": str(category),
            "category_label": self.get_category_label(str(category)),
            "tradability_mode": self._normalize_trade_mode(str(tradability_mode)),
        }

    def get_category_label(self, category: str) -> str:
        return str(self.tradability_map.get("display_names", {}).get(category, category))

    def action_label(self, action: str) -> str:
        return ACTION_LABELS.get(action, action)

    def _resolve_category(self, universe_category: str, asset_class: str) -> str:
        aliases = self.tradability_map.get("category_aliases", {})
        for category, payload in aliases.items():
            if universe_category in payload.get("universe_categories", []):
                return str(category)
            if asset_class in payload.get("asset_classes", []):
                return str(category)
        return "stock_etf"

    def _normalize_trade_mode(self, value: str | None) -> str:
        normalized = str(value or "").strip().lower().replace("+", "")
        if normalized in {"t0", "0"}:
            return "t0"
        return "t1"


@lru_cache(maxsize=1)
def get_decision_policy_service() -> DecisionPolicyService:
    return DecisionPolicyService()

