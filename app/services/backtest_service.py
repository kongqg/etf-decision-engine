from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from itertools import product
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pandas as pd
import yaml
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings, load_yaml_config
from app.db.models import ETFUniverse, Trade
from app.repositories.market_repo import list_universe
from app.repositories.user_repo import get_preferences
from app.services.data_quality_service import DataQualityService
from app.services.decision_engine import DecisionEngine
from app.services.execution_cost_service import get_execution_cost_service
from app.services.market_data_service import MarketDataService
from app.services.market_regime_service import MarketRegimeService
from app.services.risk_mode_service import get_risk_mode_service
from app.services.user_service import UserService
from app.utils.maths import max_drawdown, round_money


BACKTEST_COST_BLOCKED_REASON = "信号虽达标，但扣除统一交易成本后优势不足，暂不操作。"
BACKTEST_T0_BLOCKED_REASON_HINTS = ("同日反手", "T+0")


METRIC_EXPLANATIONS = {
    "total_return_pct": {
        "professional": "累计收益率，表示回测区间结束时总资产相对初始资金的整体涨跌幅。",
        "plain": "这段时间最后到底赚了多少，或者亏了多少。",
    },
    "annualized_return_pct": {
        "professional": "把区间收益按年化口径折算后的收益率，便于和不同长度区间比较。",
        "plain": "如果把这段表现粗略换算成一年，大概能有多快的赚钱速度。",
    },
    "max_drawdown_pct": {
        "professional": "历史净值从阶段高点回落到后续低点的最大跌幅。",
        "plain": "过去最惨的时候，账户最多缩水了多少。",
    },
    "win_rate_pct": {
        "professional": "盈利平仓交易占全部平仓交易的比例。",
        "plain": "10 次卖出里，大概有几次是赚钱离场的。",
    },
    "trade_count": {
        "professional": "区间内实际成交的模拟交易笔数。",
        "plain": "这套策略在这段时间一共动了多少次手。",
    },
    "total_execution_cost": {
        "professional": "回测区间内所有模拟成交累计扣除的统一执行成本。",
        "plain": "这段历史里，一共被交易摩擦吃掉了多少钱。",
    },
    "turnover_ratio": {
        "professional": "累计成交额相对平均总资产的比率，用来衡量换手强度。",
        "plain": "它调仓勤不勤，来回折腾多不多。",
    },
    "stability_score": {
        "professional": "按正收益日占比构造的简化稳定性指标，越高代表日度表现越平顺。",
        "plain": "赚钱和亏钱的日子比起来，它整体稳不稳。",
    },
}


@dataclass
class BacktestRequest:
    start_date: date
    end_date: date
    initial_capital: float
    use_live_trades: bool = False
    target_holding_days: int | None = None
    risk_mode: str | None = None
    threshold_overrides: dict[str, Any] | None = None
    slippage_bps: float | None = None
    execution_cost_bps_override: float | None = None
    fee_rate_override: float | None = None
    min_fee_override: float | None = None
    strict_data_quality: bool = True
    run_label: str | None = None


@dataclass
class SimulatedPosition:
    symbol: str
    name: str
    quantity: float
    avg_cost: float
    last_price: float = 0.0
    market_value: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    weight_pct: float = 0.0
    last_action_suggestion: str = "继续持有"
    latest_buy_at: datetime | None = None


@dataclass
class SimulatedTrade:
    executed_at: datetime
    symbol: str
    name: str
    side: str
    quantity: float
    price: float
    amount: float
    fee: float
    realized_pnl: float
    action_code: str
    note: str
    source: str = "backtest"


class BacktestService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.config = load_yaml_config(self.settings.config_dir / "backtest.yaml")
        self.market_data_service = MarketDataService()
        self.market_regime_service = MarketRegimeService()
        self.data_quality_service = DataQualityService()
        self.decision_engine = DecisionEngine()
        self.execution_cost_service = get_execution_cost_service()
        self.risk_mode_service = get_risk_mode_service()
        self.user_service = UserService()
        self.results_dir = Path(self.settings.base_dir) / "data" / "backtests"
        self.default_slippage_bps = float(self.config.get("execution", {}).get("default_slippage_bps", 3.0))
        self.annualization_days = int(self.config.get("execution", {}).get("annualization_days", 252))

    def prepare_dataset(
        self,
        session: Session,
        *,
        start_date: date,
        end_date: date,
        symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        warmup_start = start_date - timedelta(days=self.settings.min_refresh_history_days * 3)
        universe_rows = list_universe(session)
        if symbols:
            selected = set(symbols)
            universe_rows = [row for row in universe_rows if row.symbol in selected]
        if not universe_rows:
            raise ValueError("当前没有可用于回测的 ETF 标的。")

        history_by_symbol: dict[str, dict[str, Any]] = {}
        available_dates: set[date] = set()
        warmup_ready_symbols: list[str] = []
        critical_missing: list[str] = []

        for etf in universe_rows:
            bundle = self.market_data_service.load_history_range(
                symbol=etf.symbol,
                category=etf.category,
                min_avg_amount=float(etf.min_avg_amount),
                start_date=warmup_start,
                end_date=end_date,
            )
            history = bundle["history"].copy().sort_values("date").reset_index(drop=True)
            history["date"] = pd.to_datetime(history["date"], errors="coerce")
            if history.empty:
                critical_missing.append(etf.symbol)
                continue
            history_by_symbol[etf.symbol] = {
                "etf": etf,
                "history": history,
                "source": bundle["source"],
                "request_params": bundle["request_params"],
            }
            symbol_dates = {
                pd.Timestamp(value).date()
                for value in history["date"].tolist()
                if start_date <= pd.Timestamp(value).date() <= end_date
            }
            available_dates.update(symbol_dates)
            warmup_rows = int((history["date"] < pd.Timestamp(start_date)).sum())
            if warmup_rows >= self.data_quality_service.minimum_history_rows:
                warmup_ready_symbols.append(etf.symbol)
            elif etf.symbol in self.data_quality_service.critical_symbols:
                critical_missing.append(etf.symbol)

        trading_dates = sorted(available_dates)
        if not trading_dates:
            raise ValueError("指定区间内没有可用历史交易日。")

        coverage_ratio = len(warmup_ready_symbols) / max(len(universe_rows), 1)
        if critical_missing:
            raise ValueError(
                "开始日期前可用历史样本不足，核心 ETF 缺少预热窗口："
                + ", ".join(sorted(set(critical_missing)))
            )
        if coverage_ratio < self.data_quality_service.minimum_formal_coverage_ratio:
            raise ValueError(
                "开始日期前可用预热样本不足，"
                f"当前覆盖率 {coverage_ratio * 100:.1f}%，"
                f"至少需要 {self.data_quality_service.minimum_formal_coverage_ratio * 100:.1f}%。"
            )

        return {
            "start_date": start_date,
            "end_date": end_date,
            "warmup_start": warmup_start,
            "universe": {row.symbol: row for row in universe_rows},
            "history_by_symbol": history_by_symbol,
            "trading_dates": trading_dates,
        }

    def run(
        self,
        session: Session,
        request: BacktestRequest,
        *,
        dataset: dict[str, Any] | None = None,
        persist_output: bool = True,
        run_type: str = "backtest",
        include_signal_log: bool = True,
    ) -> dict[str, Any]:
        self._validate_request(request)
        dataset = dataset or self.prepare_dataset(
            session,
            start_date=request.start_date,
            end_date=request.end_date,
        )
        trading_dates = [
            trade_date
            for trade_date in dataset["trading_dates"]
            if request.start_date <= trade_date <= request.end_date
        ]
        if not trading_dates:
            raise ValueError("指定区间内没有可回放的历史交易日。")

        base_preferences = self._base_preferences(session)
        if request.risk_mode:
            base_preferences.risk_mode = request.risk_mode
        if request.target_holding_days is not None:
            base_preferences.target_holding_days = int(request.target_holding_days)
        effective_parameters = self.risk_mode_service.resolve(base_preferences, self.decision_engine.policy.action_thresholds)
        effective_preferences = effective_parameters.preferences
        effective_thresholds = deepcopy(effective_parameters.action_thresholds)
        if request.threshold_overrides:
            effective_thresholds = self._merge_nested_dicts(effective_thresholds, request.threshold_overrides)
        effective_thresholds = self._prepare_backtest_thresholds(effective_thresholds)

        run_id = f"{run_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
        cash_balance = float(request.initial_capital)
        positions: dict[str, SimulatedPosition] = {}
        trades: list[SimulatedTrade] = []
        skipped_actions: list[dict[str, Any]] = []
        daily_curve: list[dict[str, Any]] = []
        signal_log: list[dict[str, Any]] = []
        quality_status_counts = {"ok": 0, "weak": 0, "blocked": 0}
        previous_rank_map: dict[str, int] = {}
        baseline_notes: list[str] = []

        if request.use_live_trades:
            cash_balance, live_notes = self._seed_with_live_trades(
                session=session,
                start_date=request.start_date,
                positions=positions,
                cash_balance=cash_balance,
                trades=trades,
            )
            baseline_notes.extend(live_notes)

        for trade_date in trading_dates:
            current_time = datetime.combine(trade_date, time(14, 50))
            daily_market = self._build_daily_market_state(
                dataset=dataset,
                trade_date=trade_date,
                current_time=current_time,
                relax_quality_gate=True,
            )
            quality_status_counts[daily_market["quality_summary"]["quality_status"]] = (
                quality_status_counts.get(daily_market["quality_summary"]["quality_status"], 0) + 1
            )

            self._mark_to_market(
                positions=positions,
                price_map=daily_market["price_map"],
                cash_balance=cash_balance,
            )
            portfolio_summary = self._portfolio_summary(positions=positions, cash_balance=cash_balance)
            positions_df = self._positions_dataframe(positions)
            trade_context = self._simulated_trade_context(
                trades=trades,
                positions=positions,
                current_time=current_time,
            )
            plan_result = self.decision_engine.build_plan_from_context(
                feature_df=daily_market["feature_df"],
                portfolio_summary=portfolio_summary,
                positions_df=positions_df,
                preferences=effective_preferences,
                current_time=current_time,
                market_snapshot=daily_market["market_snapshot"],
                session_mode="intraday",
                trade_context=trade_context,
                previous_rank_map=previous_rank_map,
                action_thresholds=effective_thresholds,
                allowed_categories=set(effective_parameters.allowed_categories or []),
                category_score_adjustments=effective_parameters.category_score_adjustments,
            )
            self._apply_backtest_plan_overrides(
                plan_result=plan_result,
                available_cash=float(portfolio_summary["cash_balance"]),
                total_asset=float(portfolio_summary["total_asset"]),
                request=request,
            )

            if request.strict_data_quality and plan_result["quality_gate"]["blocked"]:
                skipped_actions.append(
                    {
                        "date": trade_date.isoformat(),
                        "reason": "data_quality_not_ready",
                        "detail": plan_result["quality_gate"]["blocking_reasons"],
                    }
                )

            day_trades, day_skips, cash_balance = self._execute_transition_plan(
                transition_plan=plan_result["plan"]["transition_plan"],
                positions=positions,
                cash_balance=cash_balance,
                current_time=current_time,
                request=request,
            )
            trades.extend(day_trades)
            skipped_actions.extend(day_skips)
            self._mark_to_market(
                positions=positions,
                price_map=daily_market["price_map"],
                cash_balance=cash_balance,
            )
            portfolio_summary = self._portfolio_summary(positions=positions, cash_balance=cash_balance)
            previous_rank_map = (
                {
                    str(row["symbol"]): int(row["rank_in_category"])
                    for _, row in plan_result["scored_df"].iterrows()
                }
                if not plan_result["scored_df"].empty
                else {}
            )

            primary_item = plan_result["plan"].get("primary_item")
            daily_curve.append(
                {
                    "date": trade_date.isoformat(),
                    "total_asset": round_money(float(portfolio_summary["total_asset"])),
                    "cash_balance": round_money(float(portfolio_summary["cash_balance"])),
                    "market_value": round_money(float(portfolio_summary["market_value"])),
                    "position_pct": round(float(portfolio_summary["current_position_pct"]) * 100, 2),
                    "cumulative_return_pct": round(
                        (float(portfolio_summary["total_asset"]) / float(request.initial_capital) - 1.0) * 100,
                        2,
                    ),
                    "action_code": str(plan_result["plan"]["action_code"]),
                    "action_label": self.decision_engine.policy.action_label(str(plan_result["plan"]["action_code"])),
                    "summary_text": str(plan_result["plan"]["summary_text"]),
                    "quality_status": str(plan_result["quality_gate"]["quality_status"]),
                    "blocking_reasons": list(plan_result["quality_gate"]["blocking_reasons"]),
                    "warning_reasons": list(plan_result["quality_gate"]["warning_reasons"]),
                    "winning_category": str(plan_result["plan"].get("winning_category", "")),
                    "selected_symbol": str(primary_item.get("symbol", "")) if primary_item else "",
                    "selected_name": str(primary_item.get("name", "")) if primary_item else "",
                }
            )
            if include_signal_log:
                signal_log.append(
                    {
                        "date": trade_date.isoformat(),
                        "action_code": str(plan_result["plan"]["action_code"]),
                        "reason_code": str(plan_result["plan"]["reason_code"]),
                        "summary_text": str(plan_result["plan"]["summary_text"]),
                        "quality_status": str(plan_result["quality_gate"]["quality_status"]),
                        "category_scores": plan_result["plan"]["category_scores"],
                        "transition_plan": plan_result["plan"]["transition_plan"],
                        "action_counts": plan_result["plan"]["action_counts"],
                    }
                )

        metrics = self._build_metrics(
            request=request,
            daily_curve=daily_curve,
            trades=trades,
        )
        source_distribution = self._source_distribution(dataset)
        quality_overview = self._quality_overview(
            quality_status_counts=quality_status_counts,
            total_days=len(daily_curve),
            source_distribution=source_distribution,
        )
        overview = self._headline_summary(metrics=metrics, quality_overview=quality_overview)
        beginner_summary = self._beginner_summary(metrics=metrics, quality_overview=quality_overview)
        result = {
            "run_id": run_id,
            "run_type": run_type,
            "created_at": datetime.now().isoformat(),
            "request": {
                "start_date": request.start_date.isoformat(),
                "end_date": request.end_date.isoformat(),
                "initial_capital": float(request.initial_capital),
                "use_live_trades": bool(request.use_live_trades),
                "risk_mode": str(getattr(effective_preferences, "risk_mode", "balanced")),
                "risk_mode_label": str(getattr(effective_preferences, "risk_mode_label", "正常")),
                "target_holding_days": int(getattr(effective_preferences, "target_holding_days", 5)),
                "execution_cost_bps": float(self._resolved_execution_cost_bps(request)),
                "strict_data_quality": bool(request.strict_data_quality),
                "threshold_overrides": request.threshold_overrides or {},
                "run_label": request.run_label or "",
            },
            "effective_parameters": {
                "risk_mode": str(getattr(effective_preferences, "risk_mode", "balanced")),
                "risk_mode_label": str(getattr(effective_preferences, "risk_mode_label", "正常")),
                "max_total_position_pct": float(getattr(effective_preferences, "max_total_position_pct", 0.7)),
                "max_single_position_pct": float(getattr(effective_preferences, "max_single_position_pct", 0.35)),
                "cash_reserve_pct": float(getattr(effective_preferences, "cash_reserve_pct", 0.2)),
                "target_holding_days": int(getattr(effective_preferences, "target_holding_days", 5)),
                "execution_cost_bps": float(self._resolved_execution_cost_bps(request)),
                "action_thresholds": effective_thresholds,
            },
            "overview": overview,
            "metrics": metrics,
            "quality_overview": quality_overview,
            "metric_explanations": METRIC_EXPLANATIONS,
            "beginner_summary": beginner_summary,
            "professional_summary": {
                "headline": overview["one_line_conclusion"],
                "notes": [
                    *baseline_notes,
                    f"回测默认按统一交易成本 {self._resolved_execution_cost_bps(request):.1f} bps 计入；如果显式传了旧版费率覆盖，则以覆盖参数为准。",
                ],
                "default_vs_effective_thresholds": self._compare_thresholds(
                    self.decision_engine.policy.action_thresholds,
                    effective_thresholds,
                ),
            },
            "daily_curve": daily_curve,
            "trades": [self._serialize_trade(item) for item in trades if item.source == "backtest"],
            "skipped_actions": skipped_actions,
            "signal_log": signal_log,
            "output_files": {},
        }
        if persist_output:
            result["output_files"] = self._persist_result(result)
        return result

    def list_saved_runs(self, limit: int = 10) -> list[dict[str, Any]]:
        if not self.results_dir.exists():
            return []
        rows: list[dict[str, Any]] = []
        for path in sorted(self.results_dir.iterdir(), key=lambda item: item.stat().st_mtime, reverse=True):
            summary_file = path / "summary.json"
            if not summary_file.exists():
                continue
            try:
                payload = json.loads(summary_file.read_text(encoding="utf-8"))
            except Exception:
                continue
            rows.append(
                {
                    "run_id": payload.get("run_id", path.name),
                    "run_type": payload.get("run_type", "backtest"),
                    "created_at": payload.get("created_at", ""),
                    "start_date": payload.get("request", {}).get("start_date", ""),
                    "end_date": payload.get("request", {}).get("end_date", ""),
                    "overview": payload.get("overview", {}),
                }
            )
            if len(rows) >= limit:
                break
        return rows

    def load_saved_run(self, run_id: str) -> dict[str, Any] | None:
        summary_file = self.results_dir / run_id / "summary.json"
        if not summary_file.exists():
            return None
        return json.loads(summary_file.read_text(encoding="utf-8"))

    def calibration_candidates(self) -> list[dict[str, Any]]:
        calibration_config = self.config.get("calibration", {})
        search_space = calibration_config.get("search_space", {})
        holding_days_candidates = [
            int(value) for value in calibration_config.get("target_holding_days_candidates", [5])
        ]
        keys = list(search_space.keys())
        values = [list(search_space[key]) for key in keys]
        rows: list[dict[str, Any]] = []
        for holding_days in holding_days_candidates:
            for combination in product(*values):
                threshold_deltas: dict[str, float] = {}
                parts = [f"holding_days={holding_days}"]
                for key, delta in zip(keys, combination):
                    threshold_deltas[key] = float(delta)
                    parts.append(f"{key}={float(delta):+g}")
                rows.append(
                    {
                        "candidate_id": "__".join(parts).replace(".", "_"),
                        "target_holding_days": holding_days,
                        "threshold_deltas": threshold_deltas,
                        "description": "，".join(parts),
                    }
                )
        return rows

    def _validate_request(self, request: BacktestRequest) -> None:
        if request.end_date < request.start_date:
            raise ValueError("结束日期不能早于开始日期。")
        if request.initial_capital <= 0:
            raise ValueError("初始资金必须大于 0。")

    def _base_preferences(self, session: Session) -> SimpleNamespace:
        preferences = get_preferences(session)
        if preferences is not None:
            payload = {
                key: value
                for key, value in vars(preferences).items()
                if not key.startswith("_")
            }
            return SimpleNamespace(**payload)
        defaults = self.user_service._risk_profile_defaults("中性")
        return SimpleNamespace(
            risk_level="中性",
            risk_mode="balanced",
            allow_gold=True,
            allow_bond=True,
            allow_overseas=True,
            min_trade_amount=float(self.settings.default_min_advice_amount),
            target_holding_days=5,
            max_total_position_pct=float(defaults["max_total_position_pct"]),
            max_single_position_pct=float(defaults["max_single_position_pct"]),
            cash_reserve_pct=float(defaults["cash_reserve_pct"]),
        )

    def _prepare_backtest_thresholds(self, thresholds: dict[str, Any]) -> dict[str, Any]:
        resolved = deepcopy(thresholds)
        t0_controls = resolved.setdefault("t0_controls", {})
        t0_controls["minimum_expected_edge_bps"] = 0.0
        t0_controls["cooldown_minutes_after_trade"] = 0
        t0_controls["max_decisions_per_symbol_per_day"] = max(
            int(t0_controls.get("max_decisions_per_symbol_per_day", 4)),
            99,
        )
        t0_controls["max_round_trips_per_day"] = max(
            int(t0_controls.get("max_round_trips_per_day", 2)),
            99,
        )
        return resolved

    def _build_daily_market_state(
        self,
        *,
        dataset: dict[str, Any],
        trade_date: date,
        current_time: datetime,
        relax_quality_gate: bool = False,
    ) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        quality_reports: list[dict[str, Any]] = []
        for symbol, payload in dataset["history_by_symbol"].items():
            etf: ETFUniverse = payload["etf"]
            history = payload["history"]
            window = history[history["date"] <= pd.Timestamp(trade_date)].copy()
            assessment = self.data_quality_service.assess_history(
                symbol=symbol,
                name=etf.name,
                source=str(payload["source"]),
                history=window,
                requested_trade_date=trade_date,
                min_avg_amount=float(etf.min_avg_amount),
                anomaly_pct_change_threshold=float(self.market_data_service.risk_rules["anomaly_pct_change_threshold"]),
            )
            quality_reports.append(assessment.payload)
            if assessment.clean_history.empty:
                continue
            features = self.market_data_service.feature_engine.calculate(assessment.clean_history)
            anomaly_flag = (
                abs(features["pct_change"]) >= float(self.market_data_service.risk_rules["anomaly_pct_change_threshold"])
                or features["volatility_10d"] >= float(self.market_data_service.risk_rules["anomaly_volatility_threshold"])
            )
            rows.append(
                {
                    "symbol": etf.symbol,
                    "name": etf.name,
                    "category": etf.category,
                    "asset_class": etf.asset_class,
                    "market": etf.market,
                    "benchmark": etf.benchmark,
                    "risk_level": etf.risk_level,
                    "min_avg_amount": etf.min_avg_amount,
                    "settlement_note": etf.settlement_note,
                    "trade_mode": etf.trade_mode,
                    "lot_size": etf.lot_size,
                    "fee_rate": etf.fee_rate,
                    "min_fee": etf.min_fee,
                    "latest_row_date": assessment.latest_row_date.isoformat() if assessment.latest_row_date else "",
                    "source_code": str(payload["source"]),
                    "stale_data_flag": bool(assessment.payload["stale_data_flag"]),
                    "quality_status": str(assessment.payload["status"]),
                    "formal_eligible": bool(assessment.payload["formal_eligible"]),
                    "source_request_json": json.dumps(payload["request_params"], ensure_ascii=False),
                    "anomaly_flag": anomaly_flag,
                    **features,
                }
            )

        feature_df = pd.DataFrame(rows)
        feature_df = self.market_data_service._apply_decision_metadata(feature_df)
        formal_market_df = feature_df[feature_df["formal_eligible"]] if not feature_df.empty else pd.DataFrame()
        snapshot_payload = self.market_regime_service.evaluate(formal_market_df if not formal_market_df.empty else feature_df)
        quality_summary = self.data_quality_service.build_summary(
            quality_reports=quality_reports,
            expected_trade_date=trade_date,
            current_time=current_time,
            session_mode="intraday",
        )
        if relax_quality_gate:
            quality_summary = self._relax_quality_summary_for_backtest(
                quality_summary=quality_summary,
                quality_reports=quality_reports,
                expected_trade_date=trade_date,
            )
        source_counts = self._source_distribution(dataset)
        quality_summary["source_counts"] = source_counts
        market_snapshot = {
            "market_regime": snapshot_payload["market_regime"],
            "broad_index_score": snapshot_payload["broad_index_score"],
            "risk_appetite_score": snapshot_payload["risk_appetite_score"],
            "trend_score": snapshot_payload["trend_score"],
            "recommended_position_pct": snapshot_payload["recommended_position_pct"],
            "data_source": "mixed" if len([key for key, value in source_counts.items() if value > 0]) > 1 else next(iter(source_counts), ""),
            "quality_status": quality_summary["quality_status"],
            "formal_decision_ready": bool(quality_summary["formal_decision_ready"]),
            "raw": {
                "quality_summary": quality_summary,
                "source": {"source_counts": source_counts},
                "formulas": snapshot_payload["formulas"],
                "evidence": snapshot_payload["evidence"],
            },
        }
        price_map = {
            str(row["symbol"]): float(row["close_price"])
            for _, row in feature_df.iterrows()
        }
        return {
            "feature_df": feature_df,
            "market_snapshot": market_snapshot,
            "quality_summary": quality_summary,
            "price_map": price_map,
        }

    def _relax_quality_summary_for_backtest(
        self,
        *,
        quality_summary: dict[str, Any],
        quality_reports: list[dict[str, Any]],
        expected_trade_date: date,
    ) -> dict[str, Any]:
        summary = deepcopy(quality_summary)
        if str(summary.get("quality_status", "")) != "blocked":
            return summary

        real_symbol_count = sum(
            1 for item in quality_reports if str(item.get("source", "")) not in {"fallback", "mock", "simulated"}
        )
        if real_symbol_count <= 0:
            return summary

        migrated_warnings = list(summary.get("warning_reasons", []))
        blocking_reasons = [str(item) for item in summary.get("blocking_reasons", []) if str(item).strip()]
        if blocking_reasons:
            migrated_warnings.append("回测模式允许在部分核心标的缺少最新真实历史时继续回放，但结果只宜保守参考。")
            migrated_warnings.extend(blocking_reasons)

        summary["quality_status"] = "weak"
        summary["verification_status"] = "回测弱质量可继续"
        summary["reliability_level"] = "中低"
        summary["formal_decision_ready"] = True
        summary["supports_formal_decision"] = True
        summary["blocking_reasons"] = []
        summary["warning_reasons"] = list(dict.fromkeys(migrated_warnings))
        summary["backtest_quality_relaxed"] = True
        summary["freshness_label"] = (
            f"回测模式允许部分弱质量历史数据继续回放；请求交易日 {expected_trade_date.isoformat()}。"
            "线上正式建议仍然要求最新正式数据。"
        )
        return summary

    def _seed_with_live_trades(
        self,
        *,
        session: Session,
        start_date: date,
        positions: dict[str, SimulatedPosition],
        cash_balance: float,
        trades: list[SimulatedTrade],
    ) -> tuple[float, list[str]]:
        rows = list(
            session.scalars(
                select(Trade).where(Trade.executed_at < datetime.combine(start_date, time.min)).order_by(Trade.executed_at)
            )
        )
        notes: list[str] = []
        for row in rows:
            if row.side == "buy":
                total_cost = float(row.amount) + float(row.fee)
                cash_balance -= total_cost
                if cash_balance < -1e-6:
                    raise ValueError("历史真实交易回放后现金为负，请提高初始资金或关闭“使用历史真实交易记录”。")
                position = positions.get(row.symbol)
                if position is None:
                    positions[row.symbol] = SimulatedPosition(
                        symbol=row.symbol,
                        name=row.name,
                        quantity=float(row.quantity),
                        avg_cost=total_cost / max(float(row.quantity), 1.0),
                        latest_buy_at=row.executed_at,
                    )
                else:
                    new_quantity = position.quantity + float(row.quantity)
                    position.avg_cost = (
                        position.avg_cost * position.quantity + total_cost
                    ) / max(new_quantity, 1.0)
                    position.quantity = new_quantity
                    position.latest_buy_at = row.executed_at
            elif row.side == "sell":
                position = positions.get(row.symbol)
                if position is None:
                    continue
                sell_quantity = min(position.quantity, float(row.quantity))
                realized_pnl = (float(row.amount) - float(row.fee)) - position.avg_cost * sell_quantity
                position.realized_pnl += realized_pnl
                position.quantity -= sell_quantity
                cash_balance += float(row.amount) - float(row.fee)
                if position.quantity <= 1e-6:
                    positions.pop(row.symbol, None)
            trades.append(
                SimulatedTrade(
                    executed_at=row.executed_at,
                    symbol=row.symbol,
                    name=row.name,
                    side=row.side,
                    quantity=float(row.quantity),
                    price=float(row.price),
                    amount=float(row.amount),
                    fee=float(row.fee),
                    realized_pnl=float(row.realized_pnl),
                    action_code="historical_live_seed",
                    note="回测起点前的真实历史成交，用于初始化仓位。",
                    source="live_seed",
                )
            )
        if rows:
            notes.append(f"已在回测开始前导入 {len(rows)} 笔真实历史成交，作为初始持仓背景。")
        return cash_balance, notes

    def _execute_transition_plan(
        self,
        *,
        transition_plan: list[dict[str, Any]],
        positions: dict[str, SimulatedPosition],
        cash_balance: float,
        current_time: datetime,
        request: BacktestRequest,
    ) -> tuple[list[SimulatedTrade], list[dict[str, Any]], float]:
        executed: list[SimulatedTrade] = []
        skipped: list[dict[str, Any]] = []
        for item in transition_plan:
            action_code = str(item.get("action_code", ""))
            if action_code not in {"buy_open", "buy_add", "reduce", "sell_exit", "park_in_money_etf"}:
                continue
            if not bool(item.get("executable_now", False)):
                skipped.append(
                    {
                        "date": current_time.date().isoformat(),
                        "symbol": str(item.get("symbol", "")),
                        "action_code": action_code,
                        "reason": str(item.get("blocked_reason", "not_executable_now")),
                    }
                )
                continue
            trade, cash_balance, skip_reason = self._execute_single_action(
                item=item,
                positions=positions,
                cash_balance=cash_balance,
                current_time=current_time,
                request=request,
            )
            if trade is not None:
                executed.append(trade)
            if skip_reason:
                skipped.append(
                    {
                        "date": current_time.date().isoformat(),
                        "symbol": str(item.get("symbol", "")),
                        "action_code": action_code,
                        "reason": skip_reason,
                    }
                )
        return executed, skipped, cash_balance

    def _execute_single_action(
        self,
        *,
        item: dict[str, Any],
        positions: dict[str, SimulatedPosition],
        cash_balance: float,
        current_time: datetime,
        request: BacktestRequest,
    ) -> tuple[SimulatedTrade | None, float, str]:
        action_code = str(item["action_code"])
        symbol = str(item["symbol"])
        name = str(item["name"])
        lot_size = max(float(item.get("lot_size", self.settings.default_lot_size)), 1.0)
        base_price = float(item.get("latest_price", 0.0))
        if base_price <= 0:
            return None, cash_balance, "missing_price"
        fee_rate, min_fee = self._resolved_trade_cost(item=item, request=request)
        slippage_bps = float(request.slippage_bps if request.slippage_bps is not None else self.default_slippage_bps)
        position = positions.get(symbol)

        if action_code in {"buy_open", "buy_add", "park_in_money_etf"}:
            trade_price = base_price * (1.0 + slippage_bps / 10_000.0)
            budget = min(float(item.get("suggested_amount", 0.0)), cash_balance)
            quantity = self._max_buy_quantity(
                price=trade_price,
                budget=budget,
                lot_size=lot_size,
                fee_rate=fee_rate,
                min_fee=min_fee,
            )
            if quantity <= 0:
                return None, cash_balance, "budget_or_lot_not_enough"
            gross_amount = round_money(quantity * trade_price)
            fee = round_money(max(gross_amount * fee_rate, min_fee))
            total_cost = gross_amount + fee
            if total_cost > cash_balance + 1e-6:
                return None, cash_balance, "cash_not_enough"
            cash_balance -= total_cost
            if position is None:
                positions[symbol] = SimulatedPosition(
                    symbol=symbol,
                    name=name,
                    quantity=quantity,
                    avg_cost=total_cost / quantity,
                    latest_buy_at=current_time,
                    last_action_suggestion=str(item.get("execution_status", "继续持有")),
                )
            else:
                new_quantity = position.quantity + quantity
                position.avg_cost = (position.avg_cost * position.quantity + total_cost) / max(new_quantity, 1.0)
                position.quantity = new_quantity
                position.latest_buy_at = current_time
                position.last_action_suggestion = str(item.get("execution_status", "继续持有"))
            trade = SimulatedTrade(
                executed_at=current_time,
                symbol=symbol,
                name=name,
                side="buy",
                quantity=quantity,
                price=round(trade_price, 4),
                amount=gross_amount,
                fee=fee,
                realized_pnl=0.0,
                action_code=action_code,
                note=str(item.get("action_reason", "")),
            )
            return trade, cash_balance, ""

        if position is None or position.quantity <= 0:
            return None, cash_balance, "position_not_found"
        trade_price = base_price * (1.0 - slippage_bps / 10_000.0)
        requested_amount = float(item.get("suggested_amount", 0.0))
        quantity = self._sell_quantity(
            position=position,
            trade_price=trade_price,
            requested_amount=requested_amount,
            lot_size=lot_size,
            action_code=action_code,
        )
        if quantity <= 0:
            return None, cash_balance, "sell_quantity_zero"
        gross_amount = round_money(quantity * trade_price)
        fee = round_money(max(gross_amount * fee_rate, min_fee))
        realized_pnl = round_money(gross_amount - fee - position.avg_cost * quantity)
        cash_balance += gross_amount - fee
        position.quantity -= quantity
        position.realized_pnl += realized_pnl
        position.last_action_suggestion = str(item.get("execution_status", "继续持有"))
        if position.quantity <= 1e-6:
            positions.pop(symbol, None)
        trade = SimulatedTrade(
            executed_at=current_time,
            symbol=symbol,
            name=name,
            side="sell",
            quantity=quantity,
            price=round(trade_price, 4),
            amount=gross_amount,
            fee=fee,
            realized_pnl=realized_pnl,
            action_code=action_code,
            note=str(item.get("action_reason", "")),
        )
        return trade, cash_balance, ""

    def _max_buy_quantity(
        self,
        *,
        price: float,
        budget: float,
        lot_size: float,
        fee_rate: float,
        min_fee: float,
    ) -> float:
        if budget <= 0 or price <= 0:
            return 0.0
        unit_amount = price * lot_size
        max_lots = int(budget // unit_amount)
        while max_lots > 0:
            gross_amount = unit_amount * max_lots
            fee = max(gross_amount * fee_rate, min_fee)
            if gross_amount + fee <= budget + 1e-6:
                return max_lots * lot_size
            max_lots -= 1
        return 0.0

    def _sell_quantity(
        self,
        *,
        position: SimulatedPosition,
        trade_price: float,
        requested_amount: float,
        lot_size: float,
        action_code: str,
    ) -> float:
        if action_code == "sell_exit":
            return position.quantity
        if trade_price <= 0:
            return 0.0
        requested_lots = int(requested_amount // (trade_price * lot_size)) if requested_amount > 0 else 0
        quantity = min(position.quantity, requested_lots * lot_size)
        if quantity <= 0 and position.quantity > 0 and action_code == "reduce":
            quantity = min(position.quantity, lot_size)
        return quantity

    def _mark_to_market(
        self,
        *,
        positions: dict[str, SimulatedPosition],
        price_map: dict[str, float],
        cash_balance: float,
    ) -> None:
        market_value = 0.0
        for symbol, position in list(positions.items()):
            last_price = float(price_map.get(symbol, position.last_price))
            position.last_price = last_price
            position.market_value = round_money(position.quantity * last_price)
            position.unrealized_pnl = round_money((last_price - position.avg_cost) * position.quantity)
            market_value += position.market_value
            if position.quantity <= 1e-6:
                positions.pop(symbol, None)
        denominator = cash_balance + market_value
        for position in positions.values():
            position.weight_pct = position.market_value / denominator if denominator else 0.0

    def _portfolio_summary(
        self,
        *,
        positions: dict[str, SimulatedPosition],
        cash_balance: float,
    ) -> dict[str, Any]:
        market_value = sum(position.market_value for position in positions.values())
        total_asset = cash_balance + market_value
        holdings = [
            {
                "symbol": position.symbol,
                "name": position.name,
                "quantity": position.quantity,
                "avg_cost": position.avg_cost,
                "last_price": position.last_price,
                "market_value": position.market_value,
                "unrealized_pnl": position.unrealized_pnl,
                "realized_pnl": position.realized_pnl,
                "weight_pct": position.market_value / total_asset if total_asset else 0.0,
                "last_action_suggestion": position.last_action_suggestion,
            }
            for position in sorted(positions.values(), key=lambda item: item.market_value, reverse=True)
        ]
        return {
            "cash_balance": round_money(cash_balance),
            "market_value": round_money(market_value),
            "total_asset": round_money(total_asset),
            "current_position_pct": market_value / total_asset if total_asset else 0.0,
            "holdings": holdings,
        }

    def _positions_dataframe(self, positions: dict[str, SimulatedPosition]) -> pd.DataFrame:
        if not positions:
            return pd.DataFrame()
        return pd.DataFrame(
            [
                {
                    "symbol": position.symbol,
                    "name": position.name,
                    "quantity": position.quantity,
                    "avg_cost": position.avg_cost,
                    "last_price": position.last_price,
                    "market_value": position.market_value,
                    "unrealized_pnl": position.unrealized_pnl,
                    "realized_pnl": position.realized_pnl,
                    "weight_pct": position.weight_pct,
                    "last_action_suggestion": position.last_action_suggestion,
                }
                for position in positions.values()
            ]
        )

    def _simulated_trade_context(
        self,
        *,
        trades: list[SimulatedTrade],
        positions: dict[str, SimulatedPosition],
        current_time: datetime,
    ) -> dict[str, Any]:
        today = current_time.date()
        trade_count_by_symbol_today: dict[str, int] = {}
        same_day_buy_symbols: set[str] = set()
        round_trip_inputs: dict[str, dict[str, int]] = {}
        last_trade_by_symbol: dict[str, SimulatedTrade] = {}

        for trade in trades:
            symbol = trade.symbol
            if trade.executed_at.date() == today:
                trade_count_by_symbol_today[symbol] = trade_count_by_symbol_today.get(symbol, 0) + 1
                round_trip_inputs.setdefault(symbol, {"buy": 0, "sell": 0})[trade.side] += 1
                if trade.side == "buy":
                    same_day_buy_symbols.add(symbol)
                last_trade_by_symbol[symbol] = trade

        round_trips_by_symbol_today = {
            symbol: min(counts.get("buy", 0), counts.get("sell", 0))
            for symbol, counts in round_trip_inputs.items()
        }
        days_held_map = {
            symbol: max((today - position.latest_buy_at.date()).days + 1, 0)
            for symbol, position in positions.items()
            if position.latest_buy_at is not None
        }
        return {
            "same_day_buy_symbols": same_day_buy_symbols,
            "trade_count_by_symbol_today": trade_count_by_symbol_today,
            "round_trips_by_symbol_today": round_trips_by_symbol_today,
            "last_trade_by_symbol": last_trade_by_symbol,
            "days_held_map": days_held_map,
        }

    def _apply_backtest_plan_overrides(
        self,
        *,
        plan_result: dict[str, Any],
        available_cash: float,
        total_asset: float,
        request: BacktestRequest,
    ) -> None:
        plan = plan_result.get("plan", {})
        transition_plan = plan.get("transition_plan", [])
        if not transition_plan:
            return

        for item in transition_plan:
            self._relax_transition_item_for_backtest(item=item, available_cash=available_cash, request=request)

        substitute_item = self._build_affordable_substitute_for_backtest(
            plan=plan,
            available_cash=available_cash,
            total_asset=total_asset,
            request=request,
        )
        if substitute_item is not None:
            transition_plan.append(substitute_item)
            transition_plan.sort(
                key=lambda item: (
                    self.decision_engine._transition_priority(str(item.get("action_code", ""))),
                    abs(float(item.get("delta_weight", 0.0))),
                    float(item.get("decision_score", 0.0)),
                ),
                reverse=True,
            )
            recommendation_groups = plan.get("recommendation_groups", {})
            executable_rows = list(recommendation_groups.get("executable_recommendations", []))
            executable_rows.append(substitute_item)
            recommendation_groups["executable_recommendations"] = executable_rows
            recommendation_groups["affordable_but_weak_recommendations"] = [
                item
                for item in recommendation_groups.get("affordable_but_weak_recommendations", [])
                if str(item.get("symbol", "")) != str(substitute_item.get("symbol", ""))
            ]
            plan["primary_item"] = substitute_item
            plan["action_code"] = str(substitute_item.get("action_code", "buy_open"))
            plan["executable_now"] = True
            plan["blocked_reason"] = ""
            plan["mapped_horizon_profile"] = str(substitute_item.get("mapped_horizon_profile", ""))
            plan["lifecycle_phase"] = str(substitute_item.get("lifecycle_phase", ""))
            summary_text = str(plan.get("summary_text", "")).strip()
            substitute_note = str(substitute_item.get("backtest_substitute_note", "")).strip()
            plan["summary_text"] = f"{summary_text} {substitute_note}".strip()
        else:
            primary_item = plan.get("primary_item")
            plan["executable_now"] = bool(primary_item and primary_item.get("executable_now", False))
            plan["blocked_reason"] = str(primary_item.get("blocked_reason", "")) if primary_item else ""

        action_counts = self.decision_engine._count_position_actions(transition_plan)
        plan["action_counts"] = action_counts
        plan["daily_action_plan"] = transition_plan
        if isinstance(plan.get("facts"), dict):
            plan["facts"]["action_counts"] = action_counts

    def _relax_transition_item_for_backtest(
        self,
        *,
        item: dict[str, Any],
        available_cash: float,
        request: BacktestRequest,
    ) -> None:
        if bool(item.get("executable_now", False)):
            return

        action_code = str(item.get("action_code", ""))
        blocked_reason = str(item.get("blocked_reason", "")).strip()
        if not blocked_reason:
            return

        if (
            action_code == "park_in_money_etf"
            and blocked_reason == BACKTEST_COST_BLOCKED_REASON
            and self._buy_item_has_enough_budget(item=item, available_cash=available_cash, request=request)
        ):
            self._mark_item_executable_for_backtest(
                item=item,
                execution_status="可执行转入货币ETF",
                note="回测按日频回放时，防守停车动作不再额外要求覆盖 alpha 型执行优势。",
            )
            return

        if any(hint in blocked_reason for hint in BACKTEST_T0_BLOCKED_REASON_HINTS):
            if action_code in {"buy_open", "buy_add", "park_in_money_etf"} and not self._buy_item_has_enough_budget(
                item=item,
                available_cash=available_cash,
                request=request,
            ):
                return
            self._mark_item_executable_for_backtest(
                item=item,
                execution_status=self._executable_status_for_action(action_code),
                note="回测按日频逐日回放时，不再套用盘中型 T+0 限制。",
            )

    def _build_affordable_substitute_for_backtest(
        self,
        *,
        plan: dict[str, Any],
        available_cash: float,
        total_asset: float,
        request: BacktestRequest,
    ) -> dict[str, Any] | None:
        primary_item = plan.get("primary_item")
        if not isinstance(primary_item, dict):
            return None
        if str(primary_item.get("action_code", "")) not in {"buy_open", "buy_add"}:
            return None
        blocked_reason = str(primary_item.get("blocked_reason", ""))
        if "一手门槛" not in blocked_reason and "最小建议金额" not in blocked_reason:
            return None

        candidates = list(plan.get("recommendation_groups", {}).get("affordable_but_weak_recommendations", []))
        if not candidates:
            return None

        winning_category = str(plan.get("winning_category", ""))
        sorted_candidates = sorted(
            candidates,
            key=lambda item: (
                str(item.get("category", "")) == winning_category,
                float(item.get("decision_score", item.get("score", 0.0))),
                float(item.get("category_score", 0.0)),
                -float(item.get("min_order_amount", 0.0)),
            ),
            reverse=True,
        )
        candidate = deepcopy(sorted_candidates[0])
        symbol = str(candidate.get("symbol", ""))
        minimum_budget = max(
            float(candidate.get("min_advice_amount", candidate.get("min_trade_amount", 0.0))),
            self._minimum_buy_budget_for_backtest(item=candidate, request=request),
        )
        if not symbol or available_cash < minimum_budget:
            return None

        action_code = "buy_open"
        action_label = self.decision_engine.policy.action_label(action_code)
        suggested_amount = self._minimum_buy_budget_for_backtest(item=candidate, request=request)
        suggested_pct = round(suggested_amount / total_asset, 4) if total_asset else 0.0
        substitute_note = (
            f"主推荐 {primary_item.get('name', primary_item.get('symbol', '这只ETF'))} 因一手门槛暂不可执行，"
            f"回测改用买得起的次优替代 {candidate.get('name', symbol)}。这个替代只用于回测，不影响线上正式建议。"
        )
        candidate.update(
            {
                "action": action_label,
                "action_code": action_code,
                "position_action": action_code,
                "position_action_label": action_label,
                "action_reason": substitute_note,
                "reason_short": substitute_note,
                "suggested_amount": suggested_amount,
                "suggested_pct": suggested_pct,
                "target_weight": suggested_pct,
                "delta_weight": suggested_pct - float(candidate.get("current_weight", 0.0)),
                "target_amount": suggested_amount,
                "executable_now": True,
                "is_executable": True,
                "blocked_reason": "",
                "execution_status": "回测替代开仓",
                "execution_note": substitute_note,
                "recommendation_bucket": "executable_recommendations",
                "planned_exit_days": None,
                "planned_exit_rule_summary": "",
                "backtest_affordable_substitute": True,
                "backtest_substitute_for_symbol": str(primary_item.get("symbol", "")),
                "backtest_substitute_note": substitute_note,
            }
        )
        return candidate

    def _buy_item_has_enough_budget(
        self,
        *,
        item: dict[str, Any],
        available_cash: float,
        request: BacktestRequest,
    ) -> bool:
        min_required = max(
            float(item.get("min_advice_amount", item.get("min_trade_amount", 0.0))),
            self._minimum_buy_budget_for_backtest(item=item, request=request),
        )
        suggested_amount = float(item.get("suggested_amount", 0.0))
        return suggested_amount >= min_required and available_cash >= min_required

    def _minimum_buy_budget_for_backtest(
        self,
        *,
        item: dict[str, Any],
        request: BacktestRequest,
    ) -> float:
        base_price = float(item.get("latest_price", 0.0))
        lot_size = max(float(item.get("lot_size", self.settings.default_lot_size)), 1.0)
        min_order_amount = float(item.get("min_order_amount", 0.0))
        if base_price <= 0 or lot_size <= 0:
            return round_money(max(min_order_amount, 0.0))

        slippage_bps = float(request.slippage_bps if request.slippage_bps is not None else self.default_slippage_bps)
        trade_price = base_price * (1.0 + slippage_bps / 10_000.0)
        gross_amount = round_money(trade_price * lot_size)
        fee_rate, min_fee = self._resolved_trade_cost(item=item, request=request)
        fee = round_money(max(gross_amount * fee_rate, min_fee))
        return round_money(max(min_order_amount, gross_amount + fee))

    def _mark_item_executable_for_backtest(
        self,
        *,
        item: dict[str, Any],
        execution_status: str,
        note: str,
    ) -> None:
        action_reason = str(item.get("action_reason", "")).strip()
        item["executable_now"] = True
        item["is_executable"] = True
        item["blocked_reason"] = ""
        item["planned_exit_rule_summary"] = ""
        item["recommendation_bucket"] = "executable_recommendations"
        item["execution_status"] = execution_status
        item["execution_note"] = f"{action_reason} {note}".strip()
        item["backtest_execution_override_reason"] = note

    def _executable_status_for_action(self, action_code: str) -> str:
        return {
            "buy_open": "可执行开仓",
            "buy_add": "可执行加仓",
            "reduce": "可执行减仓",
            "sell_exit": "可执行卖出",
            "park_in_money_etf": "可执行转入货币ETF",
        }.get(action_code, "可执行")

    def _build_metrics(
        self,
        *,
        request: BacktestRequest,
        daily_curve: list[dict[str, Any]],
        trades: list[SimulatedTrade],
    ) -> dict[str, Any]:
        if not daily_curve:
            return {
                "total_return_pct": 0.0,
                "annualized_return_pct": 0.0,
                "max_drawdown_pct": 0.0,
                "win_rate_pct": 0.0,
                "trade_count": 0,
                "total_execution_cost": 0.0,
                "turnover_ratio": 0.0,
                "stability_score": 0.0,
            }
        asset_curve = [float(item["total_asset"]) for item in daily_curve]
        final_asset = asset_curve[-1]
        total_return_pct = (final_asset / float(request.initial_capital) - 1.0) * 100.0
        trade_days = len(daily_curve)
        if trade_days >= 2 and request.initial_capital > 0:
            annualized_return_pct = (
                (final_asset / float(request.initial_capital)) ** (self.annualization_days / trade_days) - 1.0
            ) * 100.0
        else:
            annualized_return_pct = 0.0
        realized_trades = [trade for trade in trades if trade.side == "sell" and trade.source == "backtest"]
        winning_trades = [trade for trade in realized_trades if trade.realized_pnl > 0]
        win_rate_pct = len(winning_trades) / len(realized_trades) * 100.0 if realized_trades else 0.0
        daily_assets = pd.Series(asset_curve, dtype=float)
        daily_returns = daily_assets.pct_change().fillna(0.0)
        positive_day_ratio = float((daily_returns > 0).mean()) if not daily_returns.empty else 0.0
        average_asset = float(daily_assets.mean()) if not daily_assets.empty else 0.0
        trade_amount = sum(float(trade.amount) for trade in trades if trade.source == "backtest")
        turnover_ratio = trade_amount / average_asset if average_asset > 0 else 0.0
        return {
            "total_return_pct": round(total_return_pct, 2),
            "annualized_return_pct": round(annualized_return_pct, 2),
            "max_drawdown_pct": round(max_drawdown(asset_curve), 2),
            "win_rate_pct": round(win_rate_pct, 2),
            "trade_count": len([trade for trade in trades if trade.source == "backtest"]),
            "total_execution_cost": round_money(
                sum(float(trade.fee) for trade in trades if trade.source == "backtest")
            ),
            "turnover_ratio": round(turnover_ratio, 2),
            "stability_score": round(positive_day_ratio * 100.0, 2),
            "positive_day_ratio_pct": round(positive_day_ratio * 100.0, 2),
            "final_asset": round_money(final_asset),
        }

    def _resolved_execution_cost_bps(self, request: BacktestRequest) -> float:
        return float(self.execution_cost_service.execution_cost_bps(request.execution_cost_bps_override))

    def _resolved_trade_cost(
        self,
        *,
        item: dict[str, Any],
        request: BacktestRequest,
    ) -> tuple[float, float]:
        if request.fee_rate_override is not None or request.min_fee_override is not None:
            fee_rate = (
                float(request.fee_rate_override)
                if request.fee_rate_override is not None
                else float(item.get("fee_rate", self.settings.default_fee_rate))
            )
            min_fee = (
                float(request.min_fee_override)
                if request.min_fee_override is not None
                else float(item.get("min_fee", self.settings.default_min_fee))
            )
            return fee_rate, min_fee

        execution_cost_bps = self._resolved_execution_cost_bps(request)
        return execution_cost_bps / 10_000.0, 0.0

    def calibration_score(self, metrics: dict[str, Any]) -> float:
        weights = self.config.get("calibration", {}).get("objective_weights", {})
        total_return_weight = float(weights.get("total_return_weight", 1.0))
        max_drawdown_weight = float(weights.get("max_drawdown_weight", 0.7))
        stability_weight = float(weights.get("stability_weight", 20.0))
        turnover_penalty_weight = float(weights.get("turnover_penalty_weight", 1.5))
        trade_count_penalty_weight = float(weights.get("trade_count_penalty_weight", 0.12))
        turnover_penalty = max(float(metrics.get("turnover_ratio", 0.0)) - 2.0, 0.0) * turnover_penalty_weight
        trade_count_penalty = max(float(metrics.get("trade_count", 0.0)) - 40.0, 0.0) * trade_count_penalty_weight
        return round(
            total_return_weight * float(metrics.get("total_return_pct", 0.0))
            + max_drawdown_weight * float(metrics.get("max_drawdown_pct", 0.0))
            + stability_weight * (float(metrics.get("stability_score", 0.0)) / 100.0)
            - turnover_penalty
            - trade_count_penalty,
            4,
        )

    def _quality_overview(
        self,
        *,
        quality_status_counts: dict[str, int],
        total_days: int,
        source_distribution: dict[str, int],
    ) -> dict[str, Any]:
        formal_ready_days = quality_status_counts.get("ok", 0) + quality_status_counts.get("weak", 0)
        ready_ratio = formal_ready_days / total_days if total_days else 0.0
        demo_only = bool(source_distribution.get("fallback", 0) and not source_distribution.get("akshare", 0))
        mixed_with_fallback = bool(source_distribution.get("fallback", 0) and source_distribution.get("akshare", 0))
        label = (
            "正式数据"
            if ready_ratio >= 0.9 and not demo_only and not mixed_with_fallback
            else "需谨慎"
            if ready_ratio >= 0.5
            else "演示数据"
        )
        note = (
            "大部分交易日都有可用于正式决策的数据。"
            if label == "正式数据"
            else "这次回放里有不少交易日数据偏弱，结果更适合做参考。"
            if label == "需谨慎"
            else "这次回放主要依赖回退/演示数据，只能看策略逻辑，不宜据此下结论。"
        )
        return {
            "quality_status_counts": quality_status_counts,
            "formal_ready_ratio": round(ready_ratio, 4),
            "source_distribution": source_distribution,
            "label": label,
            "note": note,
        }

    def _headline_summary(self, *, metrics: dict[str, Any], quality_overview: dict[str, Any]) -> dict[str, Any]:
        total_return = float(metrics["total_return_pct"])
        max_drawdown_pct = abs(float(metrics["max_drawdown_pct"]))
        turnover_ratio = float(metrics["turnover_ratio"])
        if total_return > 8 and max_drawdown_pct < 12:
            overall = "好"
        elif total_return > 0 and max_drawdown_pct < 20:
            overall = "一般"
        else:
            overall = "差"
        if max_drawdown_pct < 8 and turnover_ratio < 1.5:
            risk_level = "低"
        elif max_drawdown_pct < 18 and turnover_ratio < 3:
            risk_level = "中"
        else:
            risk_level = "高"
        one_line = (
            f"这段历史里总收益 {total_return:.2f}%，最大回撤 {abs(float(metrics['max_drawdown_pct'])):.2f}%，"
            f"整体属于“{overall}”，风险级别偏{risk_level}。"
        )
        if quality_overview["label"] != "正式数据":
            one_line = f"{one_line} 但数据质量是“{quality_overview['label']}”，要更谨慎解读。"
        return {
            "overall_performance": overall,
            "risk_level": risk_level,
            "one_line_conclusion": one_line,
        }

    def _beginner_summary(self, *, metrics: dict[str, Any], quality_overview: dict[str, Any]) -> list[str]:
        total_return = float(metrics["total_return_pct"])
        max_drawdown_pct = abs(float(metrics["max_drawdown_pct"]))
        risk_tone = "更稳一些" if abs(float(metrics["max_drawdown_pct"])) < 10 else "波动偏大一些"
        recommendation = "新手也可以先小资金观察" if total_return > 0 and max_drawdown_pct < 12 else "不建议新手直接照搬"
        verdict = "看起来还不错" if total_return > 0 else "需要谨慎"
        return [
            f"这段历史里，账户整体{'赚钱' if total_return >= 0 else '亏钱'}了 {abs(total_return):.2f}%。",
            f"中间最难受的时候，大概回撤了 {max_drawdown_pct:.2f}%。",
            f"从历史表现看，这套策略{risk_tone}，更适合能接受阶段波动、愿意按规则执行的人。",
            f"如果你是第一次用，当前结果属于“{verdict}”，{recommendation}。",
            f"另外要记住：这只是历史模拟，不代表未来还会一样。{quality_overview['note']}",
        ]

    def _compare_thresholds(self, base_thresholds: dict[str, Any], effective_thresholds: dict[str, Any]) -> list[dict[str, Any]]:
        paths = [
            "fallback.offensive_threshold",
            "decision_thresholds.open_threshold",
            "decision_thresholds.add_threshold",
            "decision_thresholds.hold_threshold",
            "decision_thresholds.reduce_threshold",
            "decision_thresholds.full_exit_threshold",
            "decision_thresholds.strong_entry_threshold",
            "decision_thresholds.strong_hold_threshold",
            "selection.minimum_category_score_delta",
        ]
        rows: list[dict[str, Any]] = []
        for path in paths:
            base_value = self._get_nested_value(base_thresholds, path.split("."))
            effective_value = self._get_nested_value(effective_thresholds, path.split("."))
            if base_value is None and effective_value is None:
                continue
            rows.append(
                {
                    "path": path,
                    "base_value": base_value,
                    "effective_value": effective_value,
                    "delta": None if base_value is None or effective_value is None else round(float(effective_value) - float(base_value), 4),
                }
            )
        return rows

    def _persist_result(self, result: dict[str, Any]) -> dict[str, str]:
        run_dir = self.results_dir / str(result["run_id"])
        run_dir.mkdir(parents=True, exist_ok=True)
        summary_path = run_dir / "summary.json"
        curve_path = run_dir / "daily_curve.csv"
        trades_path = run_dir / "trades.csv"
        skipped_path = run_dir / "skipped_actions.csv"
        signals_path = run_dir / "signal_log.json"
        thresholds_path = run_dir / "effective_action_thresholds.yaml"

        summary_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        pd.DataFrame(result.get("daily_curve", [])).to_csv(curve_path, index=False, encoding="utf-8-sig")
        pd.DataFrame(result.get("trades", [])).to_csv(trades_path, index=False, encoding="utf-8-sig")
        pd.DataFrame(result.get("skipped_actions", [])).to_csv(skipped_path, index=False, encoding="utf-8-sig")
        signals_path.write_text(json.dumps(result.get("signal_log", []), ensure_ascii=False, indent=2), encoding="utf-8")
        thresholds_path.write_text(
            yaml.safe_dump(result.get("effective_parameters", {}).get("action_thresholds", {}), allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
        return {
            "summary_json": str(summary_path),
            "daily_curve_csv": str(curve_path),
            "trades_csv": str(trades_path),
            "skipped_actions_csv": str(skipped_path),
            "signal_log_json": str(signals_path),
            "effective_action_thresholds_yaml": str(thresholds_path),
        }

    def _serialize_trade(self, trade: SimulatedTrade) -> dict[str, Any]:
        payload = asdict(trade)
        payload["executed_at"] = trade.executed_at.isoformat()
        return payload

    def _source_distribution(self, dataset: dict[str, Any]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for payload in dataset["history_by_symbol"].values():
            source = str(payload["source"])
            counts[source] = counts.get(source, 0) + 1
        return counts

    def _merge_nested_dicts(self, base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
        merged = deepcopy(base)
        for key, value in overrides.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = self._merge_nested_dicts(merged[key], value)
            else:
                merged[key] = value
        return merged

    def _get_nested_value(self, payload: dict[str, Any], path: list[str]) -> Any:
        current: Any = payload
        for key in path:
            if not isinstance(current, dict) or key not in current:
                return None
            current = current[key]
        return current
