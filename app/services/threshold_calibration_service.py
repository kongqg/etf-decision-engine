from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd
import yaml
from sqlalchemy.orm import Session

from app.core.config import get_settings, load_yaml_config
from app.services.backtest_service import BacktestRequest, BacktestService


@dataclass
class CalibrationRequest:
    start_date: date
    end_date: date
    initial_capital: float
    use_live_trades: bool = False
    risk_mode: str | None = None
    slippage_bps: float | None = None
    execution_cost_bps_override: float | None = None
    fee_rate_override: float | None = None
    min_fee_override: float | None = None
    strict_data_quality: bool = True
    search_space: dict[str, list[float]] | None = None
    target_holding_days_candidates: list[int] | None = None
    run_label: str | None = None


class ThresholdCalibrationService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.config = load_yaml_config(self.settings.config_dir / "backtest.yaml")
        self.backtest_service = BacktestService()
        self.results_dir = Path(self.settings.base_dir) / "data" / "backtests"

    def run(
        self,
        session: Session,
        request: CalibrationRequest,
        *,
        dataset: dict[str, Any] | None = None,
        persist_output: bool = True,
    ) -> dict[str, Any]:
        dataset = dataset or self.backtest_service.prepare_dataset(
            session,
            start_date=request.start_date,
            end_date=request.end_date,
        )
        base_thresholds = deepcopy(self.backtest_service.decision_engine.policy.action_thresholds)
        search_space = request.search_space or self.config.get("calibration", {}).get("search_space", {})
        target_holding_days_candidates = request.target_holding_days_candidates or [
            int(value)
            for value in self.config.get("calibration", {}).get("target_holding_days_candidates", [5])
        ]
        candidates = self._build_candidates(
            base_thresholds=base_thresholds,
            search_space=search_space,
            target_holding_days_candidates=target_holding_days_candidates,
        )
        if not candidates:
            raise ValueError("当前没有可用的阈值候选组合。")

        candidate_results: list[dict[str, Any]] = []
        best_result: dict[str, Any] | None = None
        best_score: float | None = None
        best_candidate_meta: dict[str, Any] | None = None

        for candidate in candidates:
            result = self.backtest_service.run(
                session,
                BacktestRequest(
                    start_date=request.start_date,
                    end_date=request.end_date,
                    initial_capital=request.initial_capital,
                    use_live_trades=request.use_live_trades,
                    target_holding_days=int(candidate["target_holding_days"]),
                    risk_mode=request.risk_mode,
                    threshold_overrides=candidate["threshold_overrides"],
                    slippage_bps=request.slippage_bps,
                    execution_cost_bps_override=request.execution_cost_bps_override,
                    fee_rate_override=request.fee_rate_override,
                    min_fee_override=request.min_fee_override,
                    strict_data_quality=request.strict_data_quality,
                    run_label=request.run_label,
                ),
                dataset=dataset,
                persist_output=False,
                run_type="calibration_candidate",
                include_signal_log=False,
            )
            composite_score = self.backtest_service.calibration_score(result["metrics"])
            row = {
                "candidate_id": candidate["candidate_id"],
                "target_holding_days": int(candidate["target_holding_days"]),
                "threshold_deltas": candidate["threshold_deltas"],
                "threshold_overrides": candidate["threshold_overrides"],
                "metrics": result["metrics"],
                "overview": result["overview"],
                "quality_overview": result["quality_overview"],
                "composite_score": composite_score,
            }
            candidate_results.append(row)
            if (
                best_score is None
                or composite_score > best_score
                or (
                    composite_score == best_score
                    and float(result["metrics"]["max_drawdown_pct"]) > float(best_result["metrics"]["max_drawdown_pct"])
                )
            ):
                best_score = composite_score
                best_result = result
                best_candidate_meta = row

        if best_result is None or best_candidate_meta is None:
            raise ValueError("阈值校准没有产出有效结果。")

        top_candidates = sorted(candidate_results, key=lambda item: item["composite_score"], reverse=True)[:5]
        sensitivity_summary = self._sensitivity_summary(candidate_results)
        result = {
            "run_id": f"threshold_calibration_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}",
            "run_type": "threshold_calibration",
            "created_at": datetime.now().isoformat(),
            "request": {
                "start_date": request.start_date.isoformat(),
                "end_date": request.end_date.isoformat(),
                "initial_capital": float(request.initial_capital),
                "risk_mode": request.risk_mode or "balanced",
                "use_live_trades": bool(request.use_live_trades),
                "execution_cost_bps": float(
                    self.backtest_service.execution_cost_service.execution_cost_bps(request.execution_cost_bps_override)
                ),
                "search_space": search_space,
                "target_holding_days_candidates": target_holding_days_candidates,
            },
            "recommended_candidate": best_candidate_meta,
            "recommended_thresholds": best_result["effective_parameters"]["action_thresholds"],
            "default_vs_calibrated": self.backtest_service._compare_thresholds(
                base_thresholds,
                best_result["effective_parameters"]["action_thresholds"],
            ),
            "candidate_results": candidate_results,
            "top_candidates": top_candidates,
            "sensitivity_summary": sensitivity_summary,
            "best_backtest": best_result,
        }
        if persist_output:
            result["output_files"] = self._persist_result(result)
        else:
            result["output_files"] = {}
        return result

    def _build_candidates(
        self,
        *,
        base_thresholds: dict[str, Any],
        search_space: dict[str, list[float]],
        target_holding_days_candidates: list[int],
    ) -> list[dict[str, Any]]:
        keys = list(search_space.keys())
        if not keys:
            return []
        value_grid = [list(search_space[key]) for key in keys]
        candidates: list[dict[str, Any]] = []
        for target_holding_days in target_holding_days_candidates:
            for values in pd.MultiIndex.from_product(value_grid).tolist():
                threshold_overrides: dict[str, Any] = {}
                threshold_deltas: dict[str, float] = {}
                parts = [f"holding_days={int(target_holding_days)}"]
                for path, delta in zip(keys, values):
                    base_value = self.backtest_service._get_nested_value(base_thresholds, path.split("."))
                    effective_value = float(base_value) + float(delta)
                    self._set_nested_value(threshold_overrides, path.split("."), effective_value)
                    threshold_deltas[path] = float(delta)
                    parts.append(f"{path}={float(delta):+g}")
                candidates.append(
                    {
                        "candidate_id": "__".join(parts).replace(".", "_"),
                        "target_holding_days": int(target_holding_days),
                        "threshold_deltas": threshold_deltas,
                        "threshold_overrides": threshold_overrides,
                    }
                )
        return candidates

    def _sensitivity_summary(self, candidate_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, list[float]]] = {}
        for row in candidate_results:
            for path, delta in row["threshold_deltas"].items():
                key = f"{path}:{delta}"
                payload = grouped.setdefault(
                    key,
                    {"path": path, "delta": delta, "scores": [], "returns": [], "drawdowns": []},
                )
                payload["scores"].append(float(row["composite_score"]))
                payload["returns"].append(float(row["metrics"]["total_return_pct"]))
                payload["drawdowns"].append(abs(float(row["metrics"]["max_drawdown_pct"])))
            holding_key = f"target_holding_days:{int(row['target_holding_days'])}"
            holding_payload = grouped.setdefault(
                holding_key,
                {
                    "path": "target_holding_days",
                    "delta": int(row["target_holding_days"]),
                    "scores": [],
                    "returns": [],
                    "drawdowns": [],
                },
            )
            holding_payload["scores"].append(float(row["composite_score"]))
            holding_payload["returns"].append(float(row["metrics"]["total_return_pct"]))
            holding_payload["drawdowns"].append(abs(float(row["metrics"]["max_drawdown_pct"])))

        summary: list[dict[str, Any]] = []
        for payload in grouped.values():
            summary.append(
                {
                    "path": payload["path"],
                    "value": payload["delta"],
                    "avg_composite_score": round(sum(payload["scores"]) / max(len(payload["scores"]), 1), 4),
                    "avg_total_return_pct": round(sum(payload["returns"]) / max(len(payload["returns"]), 1), 2),
                    "avg_max_drawdown_pct": round(sum(payload["drawdowns"]) / max(len(payload["drawdowns"]), 1), 2),
                    "sample_count": len(payload["scores"]),
                }
            )
        summary.sort(key=lambda item: (item["path"], -item["avg_composite_score"]))
        return summary

    def _persist_result(self, result: dict[str, Any]) -> dict[str, str]:
        run_dir = self.results_dir / str(result["run_id"])
        run_dir.mkdir(parents=True, exist_ok=True)
        summary_path = run_dir / "summary.json"
        candidates_path = run_dir / "candidate_results.csv"
        thresholds_path = run_dir / "calibrated_action_thresholds.yaml"
        sensitivity_path = run_dir / "sensitivity_summary.csv"

        summary_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        pd.DataFrame(
            [
                {
                    "candidate_id": row["candidate_id"],
                    "target_holding_days": row["target_holding_days"],
                    "composite_score": row["composite_score"],
                    "total_return_pct": row["metrics"]["total_return_pct"],
                    "annualized_return_pct": row["metrics"]["annualized_return_pct"],
                    "max_drawdown_pct": row["metrics"]["max_drawdown_pct"],
                    "win_rate_pct": row["metrics"]["win_rate_pct"],
                    "trade_count": row["metrics"]["trade_count"],
                    "turnover_ratio": row["metrics"]["turnover_ratio"],
                    "threshold_deltas": json.dumps(row["threshold_deltas"], ensure_ascii=False),
                }
                for row in result["candidate_results"]
            ]
        ).to_csv(candidates_path, index=False, encoding="utf-8-sig")
        pd.DataFrame(result["sensitivity_summary"]).to_csv(sensitivity_path, index=False, encoding="utf-8-sig")
        thresholds_path.write_text(
            yaml.safe_dump(result["recommended_thresholds"], allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
        return {
            "summary_json": str(summary_path),
            "candidate_results_csv": str(candidates_path),
            "sensitivity_summary_csv": str(sensitivity_path),
            "calibrated_action_thresholds_yaml": str(thresholds_path),
        }

    def _set_nested_value(self, target: dict[str, Any], path: list[str], value: Any) -> None:
        current = target
        for key in path[:-1]:
            current = current.setdefault(key, {})
        current[path[-1]] = value
