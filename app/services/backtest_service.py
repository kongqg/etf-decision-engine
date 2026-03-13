from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from time import perf_counter
from typing import Any

import pandas as pd
import yaml
from sqlalchemy.orm import Session

from app.core.config import get_settings, load_yaml_config
from app.repositories.market_repo import list_universe
from app.repositories.user_repo import get_preferences
from app.services.backtest_runner import BacktestRunConfig, BacktestRunner
from app.services.data_quality_service import DataQualityService
from app.services.execution_cost_service import get_execution_cost_service
from app.services.feature_engine import FeatureEngine
from app.services.market_data_service import MarketDataService
from app.services.normalization_engine import NormalizationEngine


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
    profile: bool = False


class BacktestService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.config = load_yaml_config(self.settings.config_dir / "backtest.yaml")
        self.market_data_service = MarketDataService()
        self.data_quality_service = DataQualityService()
        self.execution_cost_service = get_execution_cost_service()
        self.feature_engine = FeatureEngine()
        self.normalization_engine = NormalizationEngine()
        self.risk_rules = load_yaml_config(self.settings.config_dir / "risk_rules.yaml")
        self.anomaly_threshold = float(self.risk_rules.get("anomaly_pct_change_threshold", 9.0))
        self.historical_data_config = self.config.get("historical_data", {})
        self.results_dir = Path(self.settings.base_dir) / "data" / "backtests"
        self.results_dir.mkdir(parents=True, exist_ok=True)

    def load_raw_dataset(
        self,
        session: Session,
        *,
        start_date: date,
        end_date: date,
        require_formal_history: bool = True,
    ) -> dict[str, Any]:
        warmup_start = start_date - timedelta(days=self.settings.min_refresh_history_days * 3)
        universe = list_universe(session)
        history_by_symbol: dict[str, dict[str, Any]] = {}
        trading_dates: set[date] = set()
        real_source_symbols = 0
        symbols_with_requested_end = 0
        for etf in universe:
            bundle = self.market_data_service.load_history_range(
                symbol=etf.symbol,
                category=etf.category,
                min_avg_amount=etf.min_avg_amount,
                start_date=warmup_start,
                end_date=end_date,
                allow_fallback=not require_formal_history,
            )
            history = bundle["history"].copy()
            history["date"] = pd.to_datetime(history["date"])
            source = str(bundle["source"])
            latest_available_date = (
                pd.Timestamp(history["date"].max()).date()
                if not history.empty
                else None
            )
            history_by_symbol[etf.symbol] = {
                "etf": etf,
                "history": history,
                "source": source,
                "request_params": bundle["request_params"],
            }
            if source not in {"fallback", "mock", "simulated", "unavailable"}:
                real_source_symbols += 1
            if latest_available_date is not None and latest_available_date >= end_date:
                symbols_with_requested_end += 1
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
            "historical_source_summary": {
                "symbol_count": len(universe),
                "real_source_symbols": real_source_symbols,
                "real_source_ratio": (real_source_symbols / len(universe)) if universe else 0.0,
                "symbols_with_requested_end": symbols_with_requested_end,
                "end_coverage_ratio": (symbols_with_requested_end / len(universe)) if universe else 0.0,
            },
        }

    def prepare_precomputed_dataset(self, dataset: dict[str, Any]) -> dict[str, Any]:
        """把同一时间区间内可复用的日级特征和质量摘要预先算好，供多次回测直接复用。"""
        if dataset.get("daily_feature_frames") and dataset.get("daily_quality_summaries"):
            return dataset

        started_at = perf_counter()
        trading_dates = list(dataset.get("trading_dates", []))
        history_by_symbol = dict(dataset.get("history_by_symbol", {}))
        if not trading_dates or not history_by_symbol:
            prepared = dict(dataset)
            prepared["daily_feature_frames"] = {}
            prepared["daily_quality_summaries"] = {}
            prepared["profiling"] = {
                "dataset_cache_prepare_sec": round(perf_counter() - started_at, 4),
                "dataset_symbol_count": len(history_by_symbol),
                "dataset_trading_days": len(trading_dates),
            }
            return prepared

        trade_timestamps = {
            trade_date: pd.Timestamp(trade_date).to_datetime64()
            for trade_date in trading_dates
        }
        daily_rows: dict[date, list[dict[str, Any]]] = {trade_date: [] for trade_date in trading_dates}
        daily_quality_reports: dict[date, list[dict[str, Any]]] = {trade_date: [] for trade_date in trading_dates}
        symbol_daily_rows: dict[str, dict[date, dict[str, Any]]] = {}
        symbol_quality_payloads: dict[str, dict[date, dict[str, Any]]] = {}

        for symbol, payload in history_by_symbol.items():
            history = payload["history"].sort_values("date").reset_index(drop=True)
            date_values = history["date"].to_numpy()
            etf = payload["etf"]
            decision_meta = BacktestRunner.classify_symbol(etf=etf)
            per_symbol_rows: dict[date, dict[str, Any]] = {}
            per_symbol_quality: dict[date, dict[str, Any]] = {}

            for trade_date in trading_dates:
                cutoff_index = int(date_values.searchsorted(trade_timestamps[trade_date], side="right"))
                truncated = history.iloc[:cutoff_index]
                quality_report = self.data_quality_service.assess_history(
                    symbol=str(etf.symbol),
                    name=str(etf.name),
                    source=str(payload.get("source", "akshare")),
                    history=truncated,
                    requested_trade_date=trade_date,
                    min_avg_amount=float(etf.min_avg_amount),
                    anomaly_pct_change_threshold=self.anomaly_threshold,
                )
                per_symbol_quality[trade_date] = quality_report.payload
                daily_quality_reports[trade_date].append(quality_report.payload)
                if len(quality_report.clean_history) < 21:
                    continue

                features = self.feature_engine.calculate(quality_report.clean_history)
                row = {
                    "trade_date": trade_date,
                    "symbol": etf.symbol,
                    "name": etf.name,
                    "category": etf.category,
                    "decision_category": decision_meta["category"],
                    "category_label": decision_meta["category_label"],
                    "asset_class": etf.asset_class,
                    "market": etf.market,
                    "risk_level": etf.risk_level,
                    "trade_mode": etf.trade_mode,
                    "lot_size": etf.lot_size,
                    "fee_rate": etf.fee_rate,
                    "min_fee": etf.min_fee,
                    "tradability_mode": decision_meta["tradability_mode"],
                    "formal_eligible": bool(quality_report.payload.get("formal_eligible", False)),
                    "source_code": str(payload.get("source", "akshare")),
                    "stale_data_flag": bool(quality_report.payload.get("stale_data_flag", False)),
                    "latest_row_date": (
                        date.fromisoformat(quality_report.payload["latest_row_date"])
                        if quality_report.payload.get("latest_row_date")
                        else trade_date
                    ),
                    "quality_status": self._quality_status_label(str(quality_report.payload.get("status", ""))),
                    "anomaly_flag": False,
                    "min_avg_amount": etf.min_avg_amount,
                    **features,
                }
                per_symbol_rows[trade_date] = row
                daily_rows[trade_date].append(row)

            symbol_daily_rows[symbol] = per_symbol_rows
            symbol_quality_payloads[symbol] = per_symbol_quality

        daily_feature_frames: dict[date, pd.DataFrame] = {}
        daily_quality_summaries: dict[date, dict[str, Any]] = {}
        for trade_date in trading_dates:
            frame = pd.DataFrame(daily_rows[trade_date])
            if not frame.empty:
                category_return = frame.groupby("decision_category")["momentum_10d"].transform("mean")
                frame["category_return_10d"] = category_return.fillna(0.0)
                frame["relative_strength_10d"] = frame["momentum_10d"] - frame["category_return_10d"]
                frame = self.normalization_engine.apply(frame)
            daily_feature_frames[trade_date] = frame
            daily_quality_summaries[trade_date] = self.data_quality_service.build_summary(
                quality_reports=daily_quality_reports[trade_date],
                expected_trade_date=trade_date,
                current_time=pd.Timestamp(trade_date).replace(hour=15, minute=0),
                session_mode="backtest",
            )

        prepared = dict(dataset)
        prepared["daily_feature_frames"] = daily_feature_frames
        prepared["daily_quality_summaries"] = daily_quality_summaries
        prepared["symbol_daily_rows"] = symbol_daily_rows
        prepared["symbol_quality_payloads"] = symbol_quality_payloads
        prepared["profiling"] = {
            **dict(dataset.get("profiling", {})),
            "dataset_cache_prepare_sec": round(perf_counter() - started_at, 4),
            "dataset_symbol_count": len(history_by_symbol),
            "dataset_trading_days": len(trading_dates),
        }
        return prepared

    def prepare_dataset(
        self,
        session: Session,
        *,
        start_date: date,
        end_date: date,
        require_formal_history: bool = True,
    ) -> dict[str, Any]:
        raw_dataset = self.load_raw_dataset(
            session,
            start_date=start_date,
            end_date=end_date,
            require_formal_history=require_formal_history,
        )
        prepared = self.prepare_precomputed_dataset(raw_dataset)
        if require_formal_history:
            self._ensure_formal_history_ready(prepared)
        return prepared

    def run(
        self,
        session: Session,
        request: BacktestRequest,
        *,
        dataset: dict[str, Any] | None = None,
        persist_output: bool = True,
    ) -> dict[str, Any]:
        prepare_started_at = perf_counter()
        prepared = dataset or self.prepare_dataset(
            session,
            start_date=request.start_date,
            end_date=request.end_date,
            require_formal_history=request.strict_data_quality,
        )
        prepare_elapsed = perf_counter() - prepare_started_at
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
                profile=request.profile,
            ),
            base_preferences=get_preferences(session),
        )
        if request.profile:
            result["profiling"] = {
                "dataset_prepare_sec": round(prepare_elapsed, 4),
                **dict(prepared.get("profiling", {})),
                **dict(result.get("profiling", {})),
            }
        if persist_output:
            result["output_files"] = self._persist_run(result)
        else:
            result["output_files"] = {}
        return result

    def _ensure_formal_history_ready(self, dataset: dict[str, Any]) -> None:
        """正式回测默认要求历史区间以真实历史数据为主，不允许 fallback 静默冒充正式实验。"""
        summary = dict(dataset.get("historical_source_summary", {}))
        symbol_count = int(summary.get("symbol_count", 0))
        real_source_ratio = float(summary.get("real_source_ratio", 0.0))
        end_coverage_ratio = float(summary.get("end_coverage_ratio", 0.0))
        minimum_real_source_ratio = float(self.historical_data_config.get("minimum_real_source_ratio", 0.8))
        minimum_formal_day_ratio = float(self.historical_data_config.get("minimum_formal_day_ratio", 0.8))

        if symbol_count <= 0:
            raise ValueError("没有可用标的，无法准备正式历史回测数据。")
        if real_source_ratio < minimum_real_source_ratio or end_coverage_ratio < minimum_real_source_ratio:
            raise ValueError(
                "历史正式数据不足，当前区间真实历史覆盖率过低。"
                f"真实源覆盖 {real_source_ratio:.0%}，结束日覆盖 {end_coverage_ratio:.0%}。"
            )

        daily_summaries = dataset.get("daily_quality_summaries", {})
        trading_days = len(dataset.get("trading_dates", []))
        if trading_days <= 0:
            raise ValueError("历史区间没有有效交易日，无法准备正式回测。")
        formal_ready_days = sum(
            1
            for payload in daily_summaries.values()
            if bool(payload.get("formal_decision_ready"))
        )
        formal_day_ratio = formal_ready_days / trading_days if trading_days else 0.0
        if formal_day_ratio < minimum_formal_day_ratio:
            raise ValueError(
                "历史正式数据不足，当前区间可正式决策交易日占比过低。"
                f"正式可用交易日占比 {formal_day_ratio:.0%}。"
            )

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

    def _quality_status_label(self, status: str) -> str:
        if status == "pass":
            return "ok"
        if status == "partial":
            return "weak"
        return "blocked"
