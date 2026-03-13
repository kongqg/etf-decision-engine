from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import date, datetime, time
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pandas as pd

from app.core.config import get_settings, load_yaml_config
from app.services.decision_engine import DecisionEngine
from app.services.data_quality_service import DataQualityService
from app.services.decision_policy_service import get_decision_policy_service
from app.services.execution_cost_service import ExecutionCostService
from app.services.feature_engine import FeatureEngine
from app.services.market_regime_service import MarketRegimeService
from app.services.portfolio_allocator import PortfolioAllocator
from app.services.scoring_engine import ScoringEngine
from app.services.universe_filter_service import UniverseFilterService
from app.utils.maths import max_drawdown, round_money


@dataclass
class BacktestRunConfig:
    start_date: date
    end_date: date
    initial_capital: float
    risk_mode: str = "balanced"
    slippage_bps: float | None = None
    execution_cost_bps_override: float | None = None
    strict_data_quality: bool = True
    config_overrides: dict[str, Any] | None = None


class BacktestRunner:
    def __init__(self) -> None:
        settings = get_settings()
        self.settings = settings
        self.backtest_config = load_yaml_config(settings.config_dir / "backtest.yaml")
        self.policy = get_decision_policy_service()
        self.feature_engine = FeatureEngine()

    def run(self, dataset: dict[str, Any], request: BacktestRunConfig, *, base_preferences: Any | None = None) -> dict[str, Any]:
        market_regime_service = MarketRegimeService()
        filter_service = UniverseFilterService()
        decision_engine = DecisionEngine()
        data_quality_service = DataQualityService()
        scoring_engine = ScoringEngine()
        allocator = PortfolioAllocator()
        execution_cost_service = ExecutionCostService()
        self._apply_overrides(
            scoring_engine=scoring_engine,
            allocator=allocator,
            decision_engine=decision_engine,
            overrides=request.config_overrides or {},
        )
        decision_engine.allocator = allocator
        decision_engine.execution_cost_service = execution_cost_service

        slippage_bps = float(
            request.slippage_bps
            if request.slippage_bps is not None
            else self.backtest_config.get("execution", {}).get("default_slippage_bps", 3.0)
        )
        annualization_days = int(self.backtest_config.get("execution", {}).get("annualization_days", 252))

        preferences = self._build_preferences(
            request=request,
            base_preferences=base_preferences,
            default_target_holding_days=decision_engine.execution_overlay_service.config.default_target_holding_days,
        )
        cash_balance = float(request.initial_capital)
        positions: dict[str, dict[str, Any]] = {}
        trades: list[dict[str, Any]] = []
        daily_curve: list[dict[str, Any]] = []
        daily_decisions: list[dict[str, Any]] = []
        realized_holding_days: list[int] = []
        replacement_days = 0

        for trade_date in dataset["trading_dates"]:
            if trade_date < request.start_date or trade_date > request.end_date:
                continue

            features_df, quality_summary = self._build_daily_features(
                dataset=dataset,
                trade_date=trade_date,
                data_quality_service=data_quality_service,
            )
            if features_df.empty:
                continue

            formal_scope = features_df[features_df["formal_eligible"]].copy()
            regime_input = formal_scope if not formal_scope.empty else features_df
            market_regime = market_regime_service.evaluate(regime_input)
            portfolio_summary = self._portfolio_summary(positions=positions, cash_balance=cash_balance, scored_df=features_df)
            current_holdings = self._current_holdings(positions, portfolio_summary)

            if request.strict_data_quality and not bool(quality_summary.get("formal_decision_ready", True)):
                self._mark_positions(positions=positions, scored_df=features_df)
                portfolio_summary = self._portfolio_summary(positions=positions, cash_balance=cash_balance, scored_df=features_df)
                daily_curve.append(
                    {
                        "date": trade_date.isoformat(),
                        "total_asset": round_money(portfolio_summary["total_asset"]),
                        "cash_balance": round_money(cash_balance),
                        "market_value": round_money(portfolio_summary["market_value"]),
                    }
                )
                daily_decisions.append(
                    {
                        "date": trade_date.isoformat(),
                        "market_regime": market_regime["market_regime"],
                        "items": [],
                        "candidate_summary": [],
                        "target_weights": {},
                        "quality_summary": quality_summary,
                        "blocked_reason": "data_quality_not_ready",
                    }
                )
                for position in positions.values():
                    if position["quantity"] > 0:
                        position["hold_days"] += 1
                continue

            filtered_df = filter_service.apply(features_df, preferences)
            scoring_result = scoring_engine.score(filtered_df)
            scored_df = scoring_result["scored_df"]
            allocation, items = decision_engine._build_allocation_and_items(
                scored_df=scored_df,
                current_holdings=current_holdings,
                portfolio_summary=portfolio_summary,
                preferences=preferences,
                market_regime=market_regime,
            )

            replacement_days += 1 if any(item.get("replacement_symbol") for item in items if item["intent"] == "open") else 0
            day_trades, cash_balance, exit_holding_days = self._execute_items(
                items=items,
                scored_df=scored_df,
                positions=positions,
                cash_balance=cash_balance,
                trade_date=trade_date,
                slippage_bps=slippage_bps,
                execution_cost_service=execution_cost_service,
                request=request,
            )
            trades.extend(day_trades)
            realized_holding_days.extend(exit_holding_days)
            self._mark_positions(positions=positions, scored_df=scored_df)
            portfolio_summary = self._portfolio_summary(positions=positions, cash_balance=cash_balance, scored_df=scored_df)
            daily_curve.append(
                {
                    "date": trade_date.isoformat(),
                    "total_asset": round_money(portfolio_summary["total_asset"]),
                    "cash_balance": round_money(cash_balance),
                    "market_value": round_money(portfolio_summary["market_value"]),
                }
            )
            daily_decisions.append(
                {
                    "date": trade_date.isoformat(),
                    "market_regime": market_regime["market_regime"],
                    "items": items,
                    "candidate_summary": allocation["candidate_summary"],
                    "target_weights": allocation["target_weights"],
                    "quality_summary": quality_summary,
                }
            )
            for position in positions.values():
                if position["quantity"] > 0:
                    position["hold_days"] += 1

        metrics = self._build_metrics(
            initial_capital=request.initial_capital,
            daily_curve=daily_curve,
            trades=trades,
            annualization_days=annualization_days,
            realized_holding_days=realized_holding_days,
            replacement_days=replacement_days,
        )
        overview = {
            "one_line_conclusion": self._one_line_conclusion(metrics),
            "overall_performance": "positive" if metrics["total_return_pct"] > 0 else "flat_or_negative",
            "risk_level": "controlled" if metrics["max_drawdown_pct"] > -5 else "elevated",
        }
        return {
            "run_id": f"backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}",
            "run_type": "backtest",
            "created_at": datetime.now().isoformat(),
            "request": {
                "start_date": request.start_date.isoformat(),
                "end_date": request.end_date.isoformat(),
                "initial_capital": float(request.initial_capital),
                "risk_mode": request.risk_mode,
                "config_overrides": request.config_overrides or {},
            },
            "effective_preferences": {
                "risk_mode": getattr(preferences, "risk_mode", "balanced"),
                "allow_gold": bool(getattr(preferences, "allow_gold", True)),
                "allow_bond": bool(getattr(preferences, "allow_bond", True)),
                "allow_overseas": bool(getattr(preferences, "allow_overseas", True)),
                "target_holding_days": int(getattr(preferences, "target_holding_days", 30) or 30),
                "min_trade_amount": float(getattr(preferences, "min_trade_amount", self.settings.default_min_advice_amount)),
                "max_total_position_pct": float(getattr(preferences, "max_total_position_pct", 0.85)),
                "max_single_position_pct": float(getattr(preferences, "max_single_position_pct", 0.35)),
                "cash_reserve_pct": float(getattr(preferences, "cash_reserve_pct", 0.0)),
            },
            "metrics": metrics,
            "overview": overview,
            "daily_curve": daily_curve,
            "daily_decisions": daily_decisions,
            "trades": trades,
        }

    def _build_preferences(
        self,
        *,
        request: BacktestRunConfig,
        base_preferences: Any | None,
        default_target_holding_days: int,
    ) -> Any:
        overrides = request.config_overrides or {}
        target_holding_days = int(
            overrides.get(
                "target_holding_days",
                overrides.get(
                    "preferences.target_holding_days",
                    getattr(base_preferences, "target_holding_days", default_target_holding_days),
                ),
            )
            or default_target_holding_days
        )
        return SimpleNamespace(
            risk_level=str(getattr(base_preferences, "risk_level", "中性")),
            risk_mode=str(request.risk_mode or getattr(base_preferences, "risk_mode", "balanced")),
            allow_gold=bool(getattr(base_preferences, "allow_gold", True)),
            allow_bond=bool(getattr(base_preferences, "allow_bond", True)),
            allow_overseas=bool(getattr(base_preferences, "allow_overseas", True)),
            target_holding_days=max(1, target_holding_days),
            min_trade_amount=float(getattr(base_preferences, "min_trade_amount", self.settings.default_min_advice_amount)),
            max_total_position_pct=float(getattr(base_preferences, "max_total_position_pct", 0.85)),
            max_single_position_pct=float(getattr(base_preferences, "max_single_position_pct", 0.35)),
            cash_reserve_pct=float(getattr(base_preferences, "cash_reserve_pct", 0.0)),
        )

    def _build_daily_features(
        self,
        *,
        dataset: dict[str, Any],
        trade_date: date,
        data_quality_service: DataQualityService,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        rows = []
        quality_reports: list[dict[str, Any]] = []
        risk_rules = load_yaml_config(self.settings.config_dir / "risk_rules.yaml")
        anomaly_threshold = float(risk_rules.get("anomaly_pct_change_threshold", 9.0))
        for symbol, payload in dataset["history_by_symbol"].items():
            history = payload["history"]
            truncated = history[pd.to_datetime(history["date"]).dt.date <= trade_date].copy()
            etf = payload["etf"]
            quality_report = data_quality_service.assess_history(
                symbol=str(etf.symbol),
                name=str(etf.name),
                source=str(payload.get("source", "akshare")),
                history=truncated,
                requested_trade_date=trade_date,
                min_avg_amount=float(etf.min_avg_amount),
                anomaly_pct_change_threshold=anomaly_threshold,
            )
            quality_reports.append(quality_report.payload)
            if len(quality_report.clean_history) < 21:
                continue
            features = self.feature_engine.calculate(quality_report.clean_history)
            decision_meta = self.policy.classify(
                symbol=str(etf.symbol),
                universe_category=str(etf.category),
                asset_class=str(etf.asset_class),
                trade_mode=str(etf.trade_mode),
            )
            rows.append(
                {
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
            )
        df = pd.DataFrame(rows)
        quality_summary = data_quality_service.build_summary(
            quality_reports=quality_reports,
            expected_trade_date=trade_date,
            current_time=datetime.combine(trade_date, time(15, 0)),
            session_mode="backtest",
        )
        if df.empty:
            return df, quality_summary
        category_return = df.groupby("decision_category")["momentum_10d"].transform("mean")
        df["category_return_10d"] = category_return.fillna(0.0)
        df["relative_strength_10d"] = df["momentum_10d"] - df["category_return_10d"]
        return df, quality_summary

    def _current_holdings(self, positions: dict[str, dict[str, Any]], portfolio_summary: dict[str, Any]) -> list[dict[str, Any]]:
        rows = []
        for holding in portfolio_summary["holdings"]:
            position = positions[str(holding["symbol"])]
            rows.append(
                {
                    "symbol": holding["symbol"],
                    "name": holding["name"],
                    "category": position["category"],
                    "current_weight": float(holding["weight_pct"]),
                    "current_amount": float(holding["market_value"]),
                    "hold_days": int(position["hold_days"]),
                    "hold_days_known": True,
                    "acquired_at": position.get("acquired_at"),
                    "quantity": float(position["quantity"]),
                    "avg_cost": float(position["avg_cost"]),
                    "last_price": float(position["last_price"]),
                    "unrealized_pnl": float(holding["unrealized_pnl"]),
                }
            )
        return rows

    def _portfolio_summary(self, *, positions: dict[str, dict[str, Any]], cash_balance: float, scored_df: pd.DataFrame) -> dict[str, Any]:
        market_value = 0.0
        holdings = []
        price_lookup = {str(row["symbol"]): float(row["close_price"]) for _, row in scored_df.iterrows()}
        for symbol, position in positions.items():
            last_price = price_lookup.get(symbol, float(position.get("last_price", 0.0)))
            market_val = float(position["quantity"]) * last_price
            market_value += market_val
            holdings.append(
                {
                    "symbol": symbol,
                    "name": position["name"],
                    "quantity": position["quantity"],
                    "avg_cost": position["avg_cost"],
                    "last_price": last_price,
                    "market_value": market_val,
                    "unrealized_pnl": (last_price - position["avg_cost"]) * position["quantity"],
                    "weight_pct": 0.0,
                }
            )
        total_asset = cash_balance + market_value
        for holding in holdings:
            holding["weight_pct"] = holding["market_value"] / total_asset if total_asset else 0.0
        return {
            "cash_balance": cash_balance,
            "market_value": market_value,
            "total_asset": total_asset,
            "current_position_pct": market_value / total_asset if total_asset else 0.0,
            "holdings": holdings,
        }

    def _execute_items(
        self,
        *,
        items: list[dict[str, Any]],
        scored_df: pd.DataFrame,
        positions: dict[str, dict[str, Any]],
        cash_balance: float,
        trade_date: date,
        slippage_bps: float,
        execution_cost_service: ExecutionCostService,
        request: BacktestRunConfig,
    ) -> tuple[list[dict[str, Any]], float, list[int]]:
        trades: list[dict[str, Any]] = []
        realized_holding_days: list[int] = []
        universe_lookup = {
            str(payload["symbol"]): payload
            for payload in scored_df.to_dict(orient="records")
        }
        ordered_items = sorted(items, key=lambda item: 0 if item["action"] == "sell" else 1)
        for item in ordered_items:
            if item["action"] not in {"buy", "sell"}:
                continue
            symbol = str(item["symbol"])
            row = universe_lookup.get(symbol, {})
            price = float(row.get("close_price", 0.0))
            lot_size = float(row.get("lot_size", 100.0) or 100.0)
            fee_rate = float(row.get("fee_rate", self.settings.default_fee_rate) or self.settings.default_fee_rate)
            min_fee = float(row.get("min_fee", self.settings.default_min_fee) or self.settings.default_min_fee)
            slippage_rate = slippage_bps / 10000.0
            exec_price = price * (1 + slippage_rate) if item["action"] == "buy" else price * (1 - slippage_rate)
            position = positions.setdefault(
                symbol,
                {
                    "symbol": symbol,
                    "name": item["name"],
                    "quantity": 0.0,
                    "avg_cost": 0.0,
                    "last_price": price,
                    "hold_days": 0,
                    "acquired_at": None,
                    "category": item["category"],
                },
            )

            target_amount = float(item["target_amount"])
            current_quantity = float(position["quantity"])
            target_quantity = self._target_quantity(target_amount=target_amount, price=exec_price, lot_size=lot_size)
            if item["intent"] in {"reduce", "exit"}:
                quantity = max(current_quantity - target_quantity, 0.0)
                if quantity <= 0:
                    continue
                amount = quantity * exec_price
                fee = self._estimate_total_cost(amount, fee_rate, min_fee, execution_cost_service, request)
                realized_pnl = (exec_price - position["avg_cost"]) * quantity - fee
                cash_balance += amount - fee
                remaining_quantity = current_quantity - quantity
                if item["intent"] == "exit":
                    realized_holding_days.append(int(position["hold_days"]))
                position["quantity"] = remaining_quantity
                if remaining_quantity <= 0:
                    position["avg_cost"] = 0.0
                    position["hold_days"] = 0
                trades.append(
                    self._trade_row(
                        item=item,
                        trade_date=trade_date,
                        side="sell",
                        quantity=quantity,
                        price=exec_price,
                        amount=amount,
                        fee=fee,
                        realized_pnl=realized_pnl,
                    )
                )
            else:
                quantity = max(target_quantity - current_quantity, 0.0)
                if quantity <= 0:
                    continue
                affordable_quantity = self._affordable_quantity(
                    cash_balance=cash_balance,
                    quantity=quantity,
                    price=exec_price,
                    lot_size=lot_size,
                    fee_rate=fee_rate,
                    min_fee=min_fee,
                    execution_cost_service=execution_cost_service,
                    request=request,
                )
                if affordable_quantity <= 0:
                    continue
                amount = affordable_quantity * exec_price
                fee = self._estimate_total_cost(amount, fee_rate, min_fee, execution_cost_service, request)
                total_cost = position["avg_cost"] * current_quantity + amount + fee
                new_quantity = current_quantity + affordable_quantity
                cash_balance -= amount + fee
                position["quantity"] = new_quantity
                position["avg_cost"] = total_cost / new_quantity if new_quantity else 0.0
                if current_quantity <= 0 and new_quantity > 0:
                    position["hold_days"] = 0
                    position["acquired_at"] = trade_date.isoformat()
                trades.append(
                    self._trade_row(
                        item=item,
                        trade_date=trade_date,
                        side="buy",
                        quantity=affordable_quantity,
                        price=exec_price,
                        amount=amount,
                        fee=fee,
                        realized_pnl=0.0,
                    )
                )
        stale_symbols = [symbol for symbol, value in positions.items() if value["quantity"] <= 0]
        for symbol in stale_symbols:
            positions.pop(symbol, None)
        return trades, cash_balance, realized_holding_days

    def _target_quantity(self, *, target_amount: float, price: float, lot_size: float) -> float:
        if price <= 0 or lot_size <= 0:
            return 0.0
        raw = int(target_amount // price)
        return float((raw // int(lot_size)) * int(lot_size))

    def _affordable_quantity(
        self,
        *,
        cash_balance: float,
        quantity: float,
        price: float,
        lot_size: float,
        fee_rate: float,
        min_fee: float,
        execution_cost_service: ExecutionCostService,
        request: BacktestRunConfig,
    ) -> float:
        candidate = quantity
        while candidate > 0:
            amount = candidate * price
            fee = self._estimate_total_cost(amount, fee_rate, min_fee, execution_cost_service, request)
            if amount + fee <= cash_balance:
                return candidate
            candidate -= lot_size
        return 0.0

    def _estimate_total_cost(
        self,
        amount: float,
        fee_rate: float,
        min_fee: float,
        execution_cost_service: ExecutionCostService,
        request: BacktestRunConfig,
    ) -> float:
        broker_fee = max(amount * fee_rate, min_fee) if amount > 0 else 0.0
        impact_cost = execution_cost_service.estimate_execution_cost(
            amount,
            override_bps=request.execution_cost_bps_override,
        )
        return round_money(broker_fee + impact_cost)

    def _trade_row(
        self,
        *,
        item: dict[str, Any],
        trade_date: date,
        side: str,
        quantity: float,
        price: float,
        amount: float,
        fee: float,
        realized_pnl: float,
    ) -> dict[str, Any]:
        return {
            "executed_at": datetime.combine(trade_date, time(14, 50)).isoformat(),
            "symbol": item["symbol"],
            "name": item["name"],
            "side": side,
            "intent": item["intent"],
            "quantity": quantity,
            "price": round(price, 4),
            "amount": round_money(amount),
            "fee": fee,
            "realized_pnl": round_money(realized_pnl),
            "current_weight": float(item["current_weight"]),
            "target_weight": float(item["target_weight"]),
        }

    def _mark_positions(self, *, positions: dict[str, dict[str, Any]], scored_df: pd.DataFrame) -> None:
        price_lookup = {str(row["symbol"]): float(row["close_price"]) for _, row in scored_df.iterrows()}
        for symbol, position in positions.items():
            if symbol in price_lookup:
                position["last_price"] = price_lookup[symbol]

    def _build_metrics(
        self,
        *,
        initial_capital: float,
        daily_curve: list[dict[str, Any]],
        trades: list[dict[str, Any]],
        annualization_days: int,
        realized_holding_days: list[int],
        replacement_days: int,
    ) -> dict[str, Any]:
        final_asset = float(daily_curve[-1]["total_asset"]) if daily_curve else float(initial_capital)
        total_return_pct = ((final_asset / initial_capital) - 1.0) * 100.0 if initial_capital else 0.0
        periods = len(daily_curve)
        annualized_return_pct = 0.0
        if periods > 0 and initial_capital > 0:
            annualized_return_pct = ((final_asset / initial_capital) ** (annualization_days / max(periods, 1)) - 1.0) * 100.0

        sell_trades = [trade for trade in trades if trade["side"] == "sell"]
        win_rate_pct = (
            sum(1 for trade in sell_trades if trade["realized_pnl"] > 0) / len(sell_trades) * 100.0
            if sell_trades
            else 0.0
        )
        total_execution_cost = sum(float(trade["fee"]) for trade in trades)
        turnover_ratio = sum(float(trade["amount"]) for trade in trades) / initial_capital if initial_capital else 0.0
        intent_counts = {
            intent: sum(1 for trade in trades if trade["intent"] == intent)
            for intent in ["open", "add", "reduce", "exit"]
        }
        return {
            "total_return_pct": round(total_return_pct, 2),
            "annualized_return_pct": round(annualized_return_pct, 2),
            "max_drawdown_pct": round(max_drawdown([float(row["total_asset"]) for row in daily_curve]), 2),
            "win_rate_pct": round(win_rate_pct, 2),
            "trade_count": len(trades),
            "total_execution_cost": round_money(total_execution_cost),
            "turnover_ratio": round(turnover_ratio, 4),
            "final_asset": round_money(final_asset),
            "open_count": intent_counts["open"],
            "add_count": intent_counts["add"],
            "reduce_count": intent_counts["reduce"],
            "exit_count": intent_counts["exit"],
            "avg_holding_days": round(sum(realized_holding_days) / len(realized_holding_days), 2) if realized_holding_days else 0.0,
            "replacement_frequency": round(replacement_days / max(periods, 1), 4) if (periods := len(daily_curve)) else 0.0,
        }

    def _one_line_conclusion(self, metrics: dict[str, Any]) -> str:
        return (
            f"累计收益 {metrics['total_return_pct']:.2f}%，最大回撤 {metrics['max_drawdown_pct']:.2f}%，"
            f"共交易 {metrics['trade_count']} 次。"
        )

    def _apply_overrides(
        self,
        *,
        scoring_engine: ScoringEngine,
        allocator: PortfolioAllocator,
        decision_engine: DecisionEngine,
        overrides: dict[str, Any],
    ) -> None:
        if not overrides:
            return
        scorer = {
            "intra_score_weights": scoring_engine.intra_weights,
            "final_score_weights": scoring_engine.final_weights,
            "category_score_weights": scoring_engine.category_weights,
        }
        for path, value in overrides.items():
            if path.startswith("intra_score_weights."):
                scorer["intra_score_weights"][path.split(".", 1)[1]] = float(value)
            elif path.startswith("final_score_weights."):
                scorer["final_score_weights"][path.split(".", 1)[1]] = float(value)
            elif path == "selection.min_final_score_for_target":
                allocator.scoring_config.setdefault("selection", {})["min_final_score_for_target"] = float(value)
            elif path.startswith("selection."):
                allocator.constraints.setdefault("selection", {})[path.split(".", 1)[1]] = value
            elif path.startswith("budget."):
                allocator.constraints.setdefault("budget", {})[path.split(".", 1)[1]] = value
            elif path.startswith("execution_overlay."):
                self._apply_execution_overlay_override(decision_engine, path.split(".", 1)[1], value)
            elif path.startswith("category_heads."):
                self._apply_category_head_override(decision_engine, path.split(".", 1)[1], value)

    def _apply_execution_overlay_override(self, decision_engine: DecisionEngine, path: str, value: Any) -> None:
        overlay_service = decision_engine.execution_overlay_service
        config = overlay_service.config
        if path.startswith("internals."):
            path = path.split(".", 1)[1]
        if path in {"pullback_low_pct", "pullback_high_pct", "breakout_entry_threshold", "rebalance_band", "reduced_target_multiplier", "default_target_holding_days"}:
            replacement_kwargs = {}
            if path == "reduced_target_multiplier":
                replacement_kwargs["reduced_target_multiplier"] = float(value)
            elif path == "default_target_holding_days":
                replacement_kwargs["default_target_holding_days"] = max(1, int(value))
            else:
                replacement_kwargs[path] = float(value)
            overlay_service.config = replace(config, **replacement_kwargs)
            return
        if path.startswith("horizon_buckets."):
            parts = path.split(".")
            if len(parts) == 4:
                bucket_name, blend_key, weight_name = parts[1], parts[2], parts[3]
                overlay_service.config.horizon_buckets.setdefault(bucket_name, {}).setdefault(blend_key, {})[weight_name] = float(value)
            elif len(parts) == 3 and parts[2] == "max_days":
                bucket_name = parts[1]
                overlay_service.config.horizon_buckets.setdefault(bucket_name, {})["max_days"] = int(value)

    def _apply_category_head_override(self, decision_engine: DecisionEngine, path: str, value: Any) -> None:
        parts = path.split(".")
        if len(parts) != 3:
            return
        category, head_name, factor = parts
        decision_engine.execution_overlay_service.category_heads.setdefault(category, {}).setdefault(head_name, {})[factor] = float(value)

    def _quality_status_label(self, status: str) -> str:
        if status == "pass":
            return "ok"
        if status == "partial":
            return "weak"
        return "blocked"
