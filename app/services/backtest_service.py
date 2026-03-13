from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from sqlalchemy.orm import Session

from app.core.config import get_settings, load_yaml_config
from app.repositories.market_repo import list_universe
from app.repositories.user_repo import get_preferences
from app.services.backtest_runner import BacktestRunConfig, BacktestRunner
from app.services.execution_cost_service import get_execution_cost_service
from app.services.market_data_service import MarketDataService


@dataclass
class BacktestRequest:
    start_date: date
    end_date: date
    initial_capital: float
    use_live_trades: bool = False
    risk_mode: str | None = None
    slippage_bps: float | None = None
    execution_cost_bps_override: float | None = None
    strict_data_quality: bool = True
    config_overrides: dict[str, Any] | None = None


class BacktestService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.config = load_yaml_config(self.settings.config_dir / "backtest.yaml")
        self.market_data_service = MarketDataService()
        self.execution_cost_service = get_execution_cost_service()
        self.results_dir = Path(self.settings.base_dir) / "data" / "backtests"
        self.results_dir.mkdir(parents=True, exist_ok=True)

    def prepare_dataset(self, session: Session, *, start_date: date, end_date: date) -> dict[str, Any]:
        warmup_start = start_date - timedelta(days=self.settings.min_refresh_history_days * 3)
        universe = list_universe(session)
        history_by_symbol: dict[str, dict[str, Any]] = {}
        trading_dates: set[date] = set()
        for etf in universe:
            bundle = self.market_data_service.load_history_range(
                symbol=etf.symbol,
                category=etf.category,
                min_avg_amount=etf.min_avg_amount,
                start_date=warmup_start,
                end_date=end_date,
            )
            history = bundle["history"].copy()
            history["date"] = pd.to_datetime(history["date"])
            history_by_symbol[etf.symbol] = {
                "etf": etf,
                "history": history,
                "source": bundle["source"],
                "request_params": bundle["request_params"],
            }
            trading_dates.update(
                trade_date.date()
                for trade_date in history["date"].tolist()
                if start_date <= trade_date.date() <= end_date
            )
        return {
            "start_date": start_date,
            "end_date": end_date,
            "warmup_start": warmup_start,
            "history_by_symbol": history_by_symbol,
            "trading_dates": sorted(trading_dates),
        }

    def run(
        self,
        session: Session,
        request: BacktestRequest,
        *,
        dataset: dict[str, Any] | None = None,
        persist_output: bool = True,
    ) -> dict[str, Any]:
        prepared = dataset or self.prepare_dataset(session, start_date=request.start_date, end_date=request.end_date)
        runner = BacktestRunner()
        result = runner.run(
            prepared,
            BacktestRunConfig(
                start_date=request.start_date,
                end_date=request.end_date,
                initial_capital=float(request.initial_capital),
                risk_mode=str(request.risk_mode or "balanced"),
                slippage_bps=request.slippage_bps,
                execution_cost_bps_override=request.execution_cost_bps_override,
                strict_data_quality=request.strict_data_quality,
                config_overrides=request.config_overrides,
            ),
            base_preferences=get_preferences(session),
        )
        if persist_output:
            result["output_files"] = self._persist_run(result)
        else:
            result["output_files"] = {}
        return result

    def load_saved_run(self, run_id: str) -> dict[str, Any] | None:
        summary_path = self.results_dir / run_id / "summary.json"
        if not summary_path.exists():
            return None
        return json.loads(summary_path.read_text(encoding="utf-8"))

    def list_saved_runs(self, limit: int = 12) -> list[dict[str, Any]]:
        rows = []
        for summary_path in sorted(self.results_dir.glob("*/summary.json"), reverse=True):
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
            rows.append(
                {
                    "run_id": payload.get("run_id"),
                    "run_type": payload.get("run_type", "backtest"),
                    "created_at": payload.get("created_at"),
                    "start_date": payload.get("request", {}).get("start_date"),
                    "end_date": payload.get("request", {}).get("end_date"),
                    "overview": payload.get("overview", {}),
                }
            )
        return rows[:limit]

    def _persist_run(self, result: dict[str, Any]) -> dict[str, str]:
        run_dir = self.results_dir / str(result["run_id"])
        run_dir.mkdir(parents=True, exist_ok=True)
        summary_path = run_dir / "summary.json"
        daily_curve_path = run_dir / "daily_curve.csv"
        trades_path = run_dir / "trades.csv"
        decisions_path = run_dir / "daily_decisions.json"
        params_path = run_dir / "params.yaml"

        summary_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        pd.DataFrame(result.get("daily_curve", [])).to_csv(daily_curve_path, index=False, encoding="utf-8-sig")
        pd.DataFrame(result.get("trades", [])).to_csv(trades_path, index=False, encoding="utf-8-sig")
        decisions_path.write_text(json.dumps(result.get("daily_decisions", []), ensure_ascii=False, indent=2), encoding="utf-8")
        params_path.write_text(
            yaml.safe_dump(result.get("request", {}), allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
        return {
            "summary": str(summary_path),
            "daily_curve": str(daily_curve_path),
            "trades": str(trades_path),
            "daily_decisions": str(decisions_path),
            "params": str(params_path),
        }
