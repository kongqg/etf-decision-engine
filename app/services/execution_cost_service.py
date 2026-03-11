from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from app.core.config import get_settings, load_yaml_config
from app.utils.maths import round_money


ORDER_ACTIONS_WITH_COST = {"buy_open", "buy_add", "park_in_money_etf", "reduce", "sell_exit"}


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
        decision_score: float,
        exit_score: float,
        thresholds: dict[str, Any] | None = None,
        route_edge_bps: float = 0.0,
    ) -> float:
        if action_code not in ORDER_ACTIONS_WITH_COST:
            return 0.0

        resolved_thresholds = thresholds or {}
        decision_thresholds = resolved_thresholds.get("decision_thresholds", {})
        multiplier = float(resolved_thresholds.get("t0_controls", {}).get("score_to_edge_bps_multiplier", 2.0))

        if action_code in {"buy_open", "park_in_money_etf"}:
            reference_score = float(decision_score)
            reference_threshold = float(decision_thresholds.get("open_threshold", 58.0))
        elif action_code == "buy_add":
            reference_score = float(decision_score)
            reference_threshold = float(decision_thresholds.get("add_threshold", 64.0))
        elif action_code == "reduce":
            reference_score = float(exit_score)
            reference_threshold = float(decision_thresholds.get("reduce_threshold", 58.0))
        else:
            reference_score = float(exit_score)
            reference_threshold = float(decision_thresholds.get("full_exit_threshold", 72.0))

        edge_points = max(reference_score - reference_threshold, 0.0)
        score_edge_bps = edge_points * multiplier
        return round(max(score_edge_bps, float(route_edge_bps or 0.0)), 2)

    def expected_edge_after_cost(
        self,
        *,
        action_code: str,
        decision_score: float,
        exit_score: float,
        thresholds: dict[str, Any] | None = None,
        route_edge_bps: float = 0.0,
        override_bps: float | None = None,
    ) -> float:
        before_cost = self.expected_edge_before_cost(
            action_code=action_code,
            decision_score=decision_score,
            exit_score=exit_score,
            thresholds=thresholds,
            route_edge_bps=route_edge_bps,
        )
        return round(before_cost - self.execution_cost_bps(override_bps), 2)


@lru_cache(maxsize=1)
def get_execution_cost_service() -> ExecutionCostService:
    return ExecutionCostService()
