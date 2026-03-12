from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

from app.core.config import get_settings, load_yaml_config
from app.utils.maths import round_money


ORDER_ACTIONS_WITH_COST = {"buy", "sell"}


@dataclass(frozen=True)
class ExecutionCostConfig:
    execution_cost_bps: float
    min_trade_amount: float


class ExecutionCostService:
    def __init__(self) -> None:
        self.settings = get_settings()
        raw = load_yaml_config(self.settings.config_dir / "execution_costs.yaml")
        self.config = ExecutionCostConfig(
            execution_cost_bps=float(raw.get("execution_cost_bps", 5.0)),
            min_trade_amount=float(raw.get("min_trade_amount", self.settings.default_min_advice_amount)),
        )

    def execution_cost_bps(self, override_bps: float | None = None) -> float:
        return float(self.config.execution_cost_bps if override_bps is None else override_bps)

    def execution_cost_rate(self, override_bps: float | None = None) -> float:
        return self.execution_cost_bps(override_bps) / 10_000.0

    def min_trade_amount(self) -> float:
        return float(self.config.min_trade_amount)

    def effective_min_trade_amount(self, user_min_trade_amount: float | None) -> float:
        base = float(user_min_trade_amount or 0.0)
        return max(base, self.min_trade_amount())

    def estimate_execution_cost(self, amount: float, *, override_bps: float | None = None) -> float:
        if amount <= 0:
            return 0.0
        return round_money(float(amount) * self.execution_cost_rate(override_bps))

    def expected_edge_before_cost(
        self,
        *,
        action_code: str,
        final_score: float,
        target_threshold: float = 55.0,
        replace_threshold: float = 8.0,
    ) -> float:
        if action_code not in ORDER_ACTIONS_WITH_COST:
            return 0.0
        threshold = target_threshold if action_code == "buy" else replace_threshold
        return round(max(float(final_score) - float(threshold), 0.0), 2)

    def expected_edge_after_cost(
        self,
        *,
        action_code: str,
        final_score: float,
        target_threshold: float = 55.0,
        replace_threshold: float = 8.0,
        override_bps: float | None = None,
    ) -> float:
        before_cost = self.expected_edge_before_cost(
            action_code=action_code,
            final_score=final_score,
            target_threshold=target_threshold,
            replace_threshold=replace_threshold,
        )
        return round(before_cost - self.execution_cost_bps(override_bps), 2)


@lru_cache(maxsize=1)
def get_execution_cost_service() -> ExecutionCostService:
    return ExecutionCostService()
