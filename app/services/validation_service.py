from __future__ import annotations

import json
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
from app.services.threshold_calibration_service import CalibrationRequest, ThresholdCalibrationService


@dataclass
class RollingValidationRequest:
    start_date: date
    end_date: date
    initial_capital: float
    train_days: int | None = None
    validation_days: int | None = None
    step_days: int | None = None
    use_live_trades: bool = False
    risk_mode: str | None = None
    slippage_bps: float | None = None
    fee_rate_override: float | None = None
    min_fee_override: float | None = None
    strict_data_quality: bool = True
    search_space: dict[str, list[float]] | None = None
    target_holding_days_candidates: list[int] | None = None
    run_label: str | None = None


class ValidationService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.config = load_yaml_config(self.settings.config_dir / "backtest.yaml")
        self.backtest_service = BacktestService()
        self.calibration_service = ThresholdCalibrationService()
        self.results_dir = Path(self.settings.base_dir) / "data" / "backtests"

    def run(
        self,
        session: Session,
        request: RollingValidationRequest,
        *,
        persist_output: bool = True,
    ) -> dict[str, Any]:
        dataset = self.backtest_service.prepare_dataset(
            session,
            start_date=request.start_date,
            end_date=request.end_date,
        )
        trading_dates = [
            trade_date
            for trade_date in dataset["trading_dates"]
            if request.start_date <= trade_date <= request.end_date
        ]
        train_days = int(request.train_days or self.config.get("rolling_validation", {}).get("default_train_days", 60))
        validation_days = int(
            request.validation_days or self.config.get("rolling_validation", {}).get("default_validation_days", 20)
        )
        step_days = int(request.step_days or self.config.get("rolling_validation", {}).get("default_step_days", validation_days))
        windows = self._build_windows(
            trading_dates=trading_dates,
            train_days=train_days,
            validation_days=validation_days,
            step_days=step_days,
        )
        if not windows:
            raise ValueError("历史交易日不足，无法切出训练 + 验证窗口。")

        window_results: list[dict[str, Any]] = []
        chosen_candidates: list[dict[str, Any]] = []

        for index, window in enumerate(windows, start=1):
            calibration_result = self.calibration_service.run(
                session,
                CalibrationRequest(
                    start_date=window["train_start"],
                    end_date=window["train_end"],
                    initial_capital=request.initial_capital,
                    use_live_trades=request.use_live_trades,
                    risk_mode=request.risk_mode,
                    slippage_bps=request.slippage_bps,
                    fee_rate_override=request.fee_rate_override,
                    min_fee_override=request.min_fee_override,
                    strict_data_quality=request.strict_data_quality,
                    search_space=request.search_space,
                    target_holding_days_candidates=request.target_holding_days_candidates,
                    run_label=request.run_label,
                ),
                dataset=dataset,
                persist_output=False,
            )
            chosen = calibration_result["recommended_candidate"]
            chosen_candidates.append(chosen)

            calibrated_validation = self.backtest_service.run(
                session,
                BacktestRequest(
                    start_date=window["validation_start"],
                    end_date=window["validation_end"],
                    initial_capital=request.initial_capital,
                    use_live_trades=request.use_live_trades,
                    risk_mode=request.risk_mode,
                    target_holding_days=int(chosen["target_holding_days"]),
                    threshold_overrides=chosen["threshold_overrides"],
                    slippage_bps=request.slippage_bps,
                    fee_rate_override=request.fee_rate_override,
                    min_fee_override=request.min_fee_override,
                    strict_data_quality=request.strict_data_quality,
                    run_label=request.run_label,
                ),
                dataset=dataset,
                persist_output=False,
                run_type="rolling_validation_calibrated",
                include_signal_log=False,
            )
            baseline_validation = self.backtest_service.run(
                session,
                BacktestRequest(
                    start_date=window["validation_start"],
                    end_date=window["validation_end"],
                    initial_capital=request.initial_capital,
                    use_live_trades=request.use_live_trades,
                    risk_mode=request.risk_mode,
                    slippage_bps=request.slippage_bps,
                    fee_rate_override=request.fee_rate_override,
                    min_fee_override=request.min_fee_override,
                    strict_data_quality=request.strict_data_quality,
                    run_label=request.run_label,
                ),
                dataset=dataset,
                persist_output=False,
                run_type="rolling_validation_baseline",
                include_signal_log=False,
            )
            window_results.append(
                {
                    "window_index": index,
                    "train_start": window["train_start"].isoformat(),
                    "train_end": window["train_end"].isoformat(),
                    "validation_start": window["validation_start"].isoformat(),
                    "validation_end": window["validation_end"].isoformat(),
                    "chosen_candidate": chosen,
                    "training_best_metrics": calibration_result["recommended_candidate"]["metrics"],
                    "validation_calibrated_metrics": calibrated_validation["metrics"],
                    "validation_default_metrics": baseline_validation["metrics"],
                    "validation_calibrated_overview": calibrated_validation["overview"],
                    "validation_default_overview": baseline_validation["overview"],
                    "validation_calibrated_score": self.backtest_service.calibration_score(calibrated_validation["metrics"]),
                    "validation_default_score": self.backtest_service.calibration_score(baseline_validation["metrics"]),
                }
            )

        recommended_candidate = self._choose_final_candidate(window_results)
        full_range_calibrated = self.backtest_service.run(
            session,
            BacktestRequest(
                start_date=request.start_date,
                end_date=request.end_date,
                initial_capital=request.initial_capital,
                use_live_trades=request.use_live_trades,
                risk_mode=request.risk_mode,
                target_holding_days=int(recommended_candidate["target_holding_days"]),
                threshold_overrides=recommended_candidate["threshold_overrides"],
                slippage_bps=request.slippage_bps,
                fee_rate_override=request.fee_rate_override,
                min_fee_override=request.min_fee_override,
                strict_data_quality=request.strict_data_quality,
                run_label=request.run_label,
            ),
            dataset=dataset,
            persist_output=False,
            run_type="rolling_validation_recommended",
            include_signal_log=False,
        )
        full_range_default = self.backtest_service.run(
            session,
            BacktestRequest(
                start_date=request.start_date,
                end_date=request.end_date,
                initial_capital=request.initial_capital,
                use_live_trades=request.use_live_trades,
                risk_mode=request.risk_mode,
                slippage_bps=request.slippage_bps,
                fee_rate_override=request.fee_rate_override,
                min_fee_override=request.min_fee_override,
                strict_data_quality=request.strict_data_quality,
                run_label=request.run_label,
            ),
            dataset=dataset,
            persist_output=False,
            run_type="rolling_validation_default",
            include_signal_log=False,
        )

        aggregate = self._aggregate_window_results(window_results)
        result = {
            "run_id": f"rolling_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}",
            "run_type": "rolling_validation",
            "created_at": datetime.now().isoformat(),
            "request": {
                "start_date": request.start_date.isoformat(),
                "end_date": request.end_date.isoformat(),
                "initial_capital": float(request.initial_capital),
                "train_days": train_days,
                "validation_days": validation_days,
                "step_days": step_days,
                "risk_mode": request.risk_mode or "balanced",
                "use_live_trades": bool(request.use_live_trades),
            },
            "windows": window_results,
            "aggregate_validation": aggregate,
            "recommended_candidate": recommended_candidate,
            "recommended_thresholds": full_range_calibrated["effective_parameters"]["action_thresholds"],
            "default_vs_recommended": self.backtest_service._compare_thresholds(
                self.backtest_service.decision_engine.policy.action_thresholds,
                full_range_calibrated["effective_parameters"]["action_thresholds"],
            ),
            "final_backtest_recommended": full_range_calibrated,
            "final_backtest_default": full_range_default,
        }
        if persist_output:
            result["output_files"] = self._persist_result(result)
        else:
            result["output_files"] = {}
        return result

    def _build_windows(
        self,
        *,
        trading_dates: list[date],
        train_days: int,
        validation_days: int,
        step_days: int,
    ) -> list[dict[str, date]]:
        if len(trading_dates) < train_days + validation_days:
            return []
        windows: list[dict[str, date]] = []
        start_index = 0
        while start_index + train_days + validation_days <= len(trading_dates):
            train_slice = trading_dates[start_index : start_index + train_days]
            validation_slice = trading_dates[start_index + train_days : start_index + train_days + validation_days]
            windows.append(
                {
                    "train_start": train_slice[0],
                    "train_end": train_slice[-1],
                    "validation_start": validation_slice[0],
                    "validation_end": validation_slice[-1],
                }
            )
            start_index += step_days
        return windows

    def _choose_final_candidate(self, window_results: list[dict[str, Any]]) -> dict[str, Any]:
        grouped: dict[str, dict[str, Any]] = {}
        for window in window_results:
            chosen = window["chosen_candidate"]
            key = json.dumps(
                {
                    "target_holding_days": chosen["target_holding_days"],
                    "threshold_deltas": chosen["threshold_deltas"],
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            payload = grouped.setdefault(
                key,
                {
                    "candidate": chosen,
                    "selection_count": 0,
                    "validation_scores": [],
                },
            )
            payload["selection_count"] += 1
            payload["validation_scores"].append(float(window["validation_calibrated_score"]))
        ranked = sorted(
            grouped.values(),
            key=lambda item: (
                sum(item["validation_scores"]) / max(len(item["validation_scores"]), 1),
                item["selection_count"],
            ),
            reverse=True,
        )
        return ranked[0]["candidate"]

    def _aggregate_window_results(self, window_results: list[dict[str, Any]]) -> dict[str, Any]:
        if not window_results:
            return {}
        calibrated_scores = [float(row["validation_calibrated_score"]) for row in window_results]
        default_scores = [float(row["validation_default_score"]) for row in window_results]
        calibrated_returns = [float(row["validation_calibrated_metrics"]["total_return_pct"]) for row in window_results]
        default_returns = [float(row["validation_default_metrics"]["total_return_pct"]) for row in window_results]
        calibrated_drawdowns = [abs(float(row["validation_calibrated_metrics"]["max_drawdown_pct"])) for row in window_results]
        default_drawdowns = [abs(float(row["validation_default_metrics"]["max_drawdown_pct"])) for row in window_results]
        calibrated_wins = sum(1 for cal, base in zip(calibrated_scores, default_scores) if cal > base)
        return {
            "window_count": len(window_results),
            "avg_validation_calibrated_score": round(sum(calibrated_scores) / len(calibrated_scores), 4),
            "avg_validation_default_score": round(sum(default_scores) / len(default_scores), 4),
            "avg_validation_calibrated_return_pct": round(sum(calibrated_returns) / len(calibrated_returns), 2),
            "avg_validation_default_return_pct": round(sum(default_returns) / len(default_returns), 2),
            "avg_validation_calibrated_drawdown_pct": round(sum(calibrated_drawdowns) / len(calibrated_drawdowns), 2),
            "avg_validation_default_drawdown_pct": round(sum(default_drawdowns) / len(default_drawdowns), 2),
            "calibrated_beats_default_windows": calibrated_wins,
        }

    def _persist_result(self, result: dict[str, Any]) -> dict[str, str]:
        run_dir = self.results_dir / str(result["run_id"])
        run_dir.mkdir(parents=True, exist_ok=True)
        summary_path = run_dir / "summary.json"
        windows_path = run_dir / "window_results.csv"
        thresholds_path = run_dir / "recommended_action_thresholds.yaml"

        summary_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        pd.DataFrame(
            [
                {
                    "window_index": row["window_index"],
                    "train_start": row["train_start"],
                    "train_end": row["train_end"],
                    "validation_start": row["validation_start"],
                    "validation_end": row["validation_end"],
                    "validation_calibrated_score": row["validation_calibrated_score"],
                    "validation_default_score": row["validation_default_score"],
                    "validation_calibrated_return_pct": row["validation_calibrated_metrics"]["total_return_pct"],
                    "validation_default_return_pct": row["validation_default_metrics"]["total_return_pct"],
                    "validation_calibrated_drawdown_pct": row["validation_calibrated_metrics"]["max_drawdown_pct"],
                    "validation_default_drawdown_pct": row["validation_default_metrics"]["max_drawdown_pct"],
                    "chosen_threshold_deltas": json.dumps(row["chosen_candidate"]["threshold_deltas"], ensure_ascii=False),
                    "chosen_target_holding_days": row["chosen_candidate"]["target_holding_days"],
                }
                for row in result["windows"]
            ]
        ).to_csv(windows_path, index=False, encoding="utf-8-sig")
        thresholds_path.write_text(
            yaml.safe_dump(result["recommended_thresholds"], allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
        return {
            "summary_json": str(summary_path),
            "window_results_csv": str(windows_path),
            "recommended_action_thresholds_yaml": str(thresholds_path),
        }
