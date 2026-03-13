from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pandas as pd

from app.core.config import get_settings, load_yaml_config

POSITION_STATE_LABELS = {
    "HOLD": "继续持有",
    "REDUCE": "减仓观察",
    "EXIT": "退出",
    "NONE": "未持有",
}

ENTRY_CHANNEL_LABELS = {
    "none": "无",
    "A": "通道A：回撤后反弹",
    "B": "通道B：强趋势突破",
}


@dataclass(frozen=True)
class ExecutionOverlayConfig:
    pullback_low_pct: float
    pullback_high_pct: float
    breakout_entry_threshold: float
    rebalance_band: float
    reduced_target_multiplier: float
    default_target_holding_days: int
    horizon_buckets: dict[str, dict[str, Any]]


class ExecutionOverlayService:
    def __init__(self) -> None:
        settings = get_settings()
        overlay_cfg = load_yaml_config(settings.config_dir / "execution_overlay.yaml")
        self.category_profiles = load_yaml_config(settings.config_dir / "category_profiles.yaml")
        internals = overlay_cfg.get("internals", {})
        self.config = ExecutionOverlayConfig(
            pullback_low_pct=float(overlay_cfg.get("pullback_low_pct", -6.0)),
            pullback_high_pct=float(overlay_cfg.get("pullback_high_pct", -2.0)),
            breakout_entry_threshold=float(overlay_cfg.get("breakout_entry_threshold", 75.0)),
            rebalance_band=float(overlay_cfg.get("rebalance_band", 0.05)),
            reduced_target_multiplier=float(internals.get("reduced_target_multiplier", 0.5)),
            default_target_holding_days=max(1, int(internals.get("default_target_holding_days", 30))),
            horizon_buckets={
                str(name): dict(payload)
                for name, payload in overlay_cfg.get("horizon_buckets", {}).items()
            },
        )
        self.category_heads = {
            str(category): dict(payload)
            for category, payload in self.category_profiles.get("category_heads", {}).items()
        }

    def build_action_items(
        self,
        *,
        scored_df: pd.DataFrame,
        current_holdings: list[dict[str, Any]],
        allocation: dict[str, Any],
        portfolio_summary: dict[str, Any],
        preferences: Any,
        policy: Any,
        min_trade_amount: float,
    ) -> dict[str, Any]:
        if scored_df.empty:
            return {
                "items": [],
                "effective_target_weights": {},
                "overlay_rows": {},
                "overlay_traces": {},
            }

        current_by_symbol = {str(row["symbol"]): dict(row) for row in current_holdings}
        total_asset = float(portfolio_summary.get("total_asset", 0.0))
        prepared = self._prepare_overlay_frame(scored_df=scored_df, current_holdings=current_holdings, preferences=preferences)

        target_weights = {
            str(symbol): float(weight)
            for symbol, weight in allocation.get("target_weights", {}).items()
        }
        category_switch_context = self._build_switch_context(prepared, current_by_symbol, target_weights)

        items: list[dict[str, Any]] = []
        effective_target_weights: dict[str, float] = {}
        overlay_rows: dict[str, dict[str, Any]] = {
            str(row["symbol"]): row.to_dict()
            for _, row in prepared.iterrows()
        }
        overlay_traces: dict[str, dict[str, Any]] = {}
        symbols = list(prepared.sort_values(["global_rank", "symbol"], ascending=[True, True])["symbol"].astype(str))

        for symbol in symbols:
            row = dict(overlay_rows.get(symbol, {}))
            if not row:
                continue
            current = current_by_symbol.get(symbol, {})
            current_weight = float(current.get("current_weight", 0.0))
            normal_target_weight = float(target_weights.get(symbol, 0.0))
            is_held = current_weight > 0
            category = str(row["decision_category"])
            target_amount = 0.0
            suggested_amount = 0.0
            suggested_pct = 0.0

            category_context = category_switch_context.get(category, {})
            switch_in = category_context.get("switch_in_symbol") == symbol
            switch_out = category_context.get("switch_out_symbol") == symbol
            switch_partner = (
                str(category_context.get("switch_out_symbol", ""))
                if switch_in
                else str(category_context.get("switch_in_symbol", ""))
            )
            switch_blocked = (
                category_context.get("blocked_new_symbol") == symbol
                and str(category_context.get("blocked_old_symbol", "")) != symbol
            )
            if switch_blocked:
                switch_partner = str(category_context.get("blocked_old_symbol", ""))

            base_state = str(row["position_state"])
            entry_allowed = bool(row["entry_allowed"])
            reduced_target_weight = self._reduced_target_weight(
                normal_target_weight=normal_target_weight,
                current_weight=current_weight,
            )

            action = "no_trade"
            action_code = "no_trade"
            intent = "hold"
            effective_target_weight = normal_target_weight
            action_reason = ""

            if is_held:
                if switch_out:
                    effective_target_weight = 0.0
                    action = "sell"
                    action_code = "sell_exit"
                    intent = "exit"
                    action_reason = f"同类别新龙头 {switch_partner} 已满足入场条件，旧持仓让位退出。"
                elif base_state == "EXIT":
                    effective_target_weight = 0.0
                    action = "sell"
                    action_code = "sell_exit"
                    intent = "exit"
                    action_reason = "20日动量已转负且价格跌破20日均线，趋势已破坏，执行退出。"
                elif base_state == "REDUCE":
                    effective_target_weight = reduced_target_weight
                    if current_weight - effective_target_weight > self.config.rebalance_band:
                        action = "sell"
                        action_code = "sell_reduce"
                        intent = "reduce"
                        action_reason = "中期趋势走弱但未完全破坏，先把仓位降到正常目标的一半。"
                    else:
                        action = "hold"
                        action_code = "hold"
                        intent = "hold"
                        effective_target_weight = current_weight
                        action_reason = "趋势虽转弱，但当前仓位距离减仓目标不大，先继续观察。"
                else:
                    if entry_allowed and normal_target_weight - current_weight > self.config.rebalance_band:
                        action = "buy"
                        action_code = "buy_add"
                        intent = "add"
                        action_reason = f"持仓趋势仍健康，并且再次满足{self._entry_channel_label(str(row['entry_channel_used']))}，允许加仓。"
                    else:
                        action = "hold"
                        action_code = "hold"
                        intent = "hold"
                        effective_target_weight = max(current_weight, normal_target_weight)
                        action_reason = "当前持仓仍处于健康趋势状态，暂时继续持有。"
            else:
                if switch_blocked:
                    effective_target_weight = 0.0
                    action_reason = "同类别旧持仓还没有进入减仓/退出状态，暂不因为轻微领先差异直接换仓。"
                elif entry_allowed and normal_target_weight > 0 and normal_target_weight - current_weight > self.config.rebalance_band:
                    effective_target_weight = normal_target_weight
                    action = "buy"
                    action_code = "switch" if switch_in else "buy_open"
                    intent = "open"
                    action_reason = (
                        f"同类别旧持仓已转弱，新龙头满足{self._entry_channel_label(str(row['entry_channel_used']))}，执行换仓。"
                        if switch_in
                        else f"满足{self._entry_channel_label(str(row['entry_channel_used']))}，允许新开仓。"
                    )
                else:
                    effective_target_weight = 0.0
                    action_reason = self._blocked_entry_reason(row=row, normal_target_weight=normal_target_weight)

            target_amount = round(float(total_asset) * effective_target_weight, 2)
            current_amount = float(current.get("current_amount", 0.0))
            delta_weight = effective_target_weight - current_weight
            delta_amount = round(float(total_asset) * abs(delta_weight), 2)
            min_trade_blocked = False

            if action in {"buy", "sell"} and action_code != "sell_exit" and delta_amount < min_trade_amount:
                min_trade_blocked = True
                if is_held:
                    action = "hold"
                    action_code = "hold"
                    intent = "hold"
                    effective_target_weight = current_weight
                    target_amount = round(float(total_asset) * effective_target_weight, 2)
                    delta_weight = 0.0
                    delta_amount = 0.0
                    action_reason = f"信号存在，但本次调整金额低于最小交易金额 {min_trade_amount:.0f} 元，先不动。"
                else:
                    action = "no_trade"
                    action_code = "no_trade"
                    intent = "hold"
                    effective_target_weight = 0.0
                    target_amount = 0.0
                    delta_weight = 0.0
                    delta_amount = 0.0
                    action_reason = f"信号存在，但目标金额低于最小交易金额 {min_trade_amount:.0f} 元，暂不开仓。"

            if action != "no_trade":
                suggested_amount = round(abs(target_amount - current_amount), 2) if action in {"buy", "sell"} else 0.0
                suggested_pct = abs(delta_weight)
                effective_target_weights[symbol] = max(effective_target_weight, 0.0)

            rationale = {
                "trend_filter_pass": bool(row.get("trend_filter_pass", False)),
                "pullback_zone_pass": bool(row.get("pullback_zone_pass", False)),
                "rebound_confirmation_pass": bool(row.get("rebound_confirmation_pass", False)),
                "breakout_exception_pass": bool(row.get("breakout_exception_pass", False)),
                "entry_allowed": bool(row.get("entry_allowed", False)),
                "entry_channel_used": str(row.get("entry_channel_used", "none")),
                "entry_channel_label": self._entry_channel_label(str(row.get("entry_channel_used", "none"))),
                "position_state": base_state,
                "position_state_label": POSITION_STATE_LABELS.get(base_state, base_state),
                "action_reason": action_reason,
                "switch_blocked": switch_blocked,
                "switch_partner": switch_partner,
                "normal_target_weight": normal_target_weight,
                "reduced_target_weight": reduced_target_weight,
                "rebalance_band": self.config.rebalance_band,
                "pullback_low_pct": self.config.pullback_low_pct,
                "pullback_high_pct": self.config.pullback_high_pct,
                "breakout_entry_threshold": self.config.breakout_entry_threshold,
                "trend_snapshot": {
                    "momentum_3d": float(row.get("momentum_3d", 0.0)),
                    "momentum_5d": float(row.get("momentum_5d", 0.0)),
                    "momentum_20d": float(row.get("momentum_20d", 0.0)),
                    "close_price": float(row.get("close_price", 0.0)),
                    "ma5": float(row.get("ma5", 0.0)),
                    "ma20": float(row.get("ma20", 0.0)),
                    "drawdown_20d": float(row.get("drawdown_20d", 0.0)),
                    "volatility_20d": float(row.get("volatility_20d", 0.0)),
                    "category_median_volatility_20d": float(row.get("category_median_volatility_20d", 0.0)),
                },
            }
            execution_trace = self._build_execution_trace(
                row=row,
                current=current,
                category_context=category_context,
                base_state=base_state,
                normal_target_weight=normal_target_weight,
                reduced_target_weight=reduced_target_weight,
                effective_target_weight=max(effective_target_weight, 0.0),
                current_weight=current_weight,
                current_amount=current_amount,
                target_amount=target_amount,
                delta_weight=delta_weight,
                delta_amount=delta_amount,
                total_asset=total_asset,
                min_trade_amount=min_trade_amount,
                action=action,
                action_code=action_code,
                switch_in=switch_in,
                switch_out=switch_out,
                switch_blocked=switch_blocked,
                switch_partner=switch_partner,
                action_reason=action_reason,
                min_trade_blocked=min_trade_blocked,
            )

            item = {
                "symbol": symbol,
                "name": row["name"],
                "category": category,
                "category_label": policy.get_category_label(category),
                "rank": int(row.get("global_rank", 0) or 0),
                "global_rank": int(row.get("global_rank", 0) or 0),
                "category_rank": int(row.get("category_rank", 0) or 0),
                "action": action,
                "action_code": action_code,
                "intent": intent,
                "current_weight": current_weight,
                "target_weight": max(effective_target_weight, 0.0),
                "delta_weight": delta_weight,
                "current_amount": current_amount,
                "target_amount": target_amount,
                "suggested_amount": suggested_amount,
                "suggested_pct": suggested_pct,
                "score": float(row.get("decision_score", row.get("final_score", 0.0))),
                "score_gap": float(category_context.get("score_gap", 0.0)),
                "score_gap_vs_holding": float(category_context.get("score_gap", 0.0)),
                "replace_threshold_used": float(allocation.get("replace_threshold", 0.0)),
                "replacement_symbol": switch_partner if switch_in or switch_blocked else "",
                "final_score": float(row.get("final_score", 0.0)),
                "intra_score": float(row.get("intra_score", 0.0)),
                "category_score": float(row.get("category_score", 0.0)),
                "entry_score": float(row.get("entry_score", 0.0)),
                "hold_score": float(row.get("hold_score", 0.0)),
                "exit_score": float(row.get("exit_score", 0.0)),
                "decision_score": float(row.get("decision_score", 0.0)),
                "reason_short": action_reason,
                "action_reason": action_reason,
                "risk_level": str(row.get("risk_level", "")),
                "asset_class": str(row.get("asset_class", "")),
                "trade_mode": str(row.get("trade_mode", "")),
                "tradability_mode": str(row.get("tradability_mode", "")),
                "execution_note": self._execution_note(action_code=action_code, tradability_mode=str(row.get("tradability_mode", ""))),
                "is_new_position": bool(not is_held and effective_target_weight > 0),
                "hold_days": int(current.get("hold_days", 0) or 0),
                "hold_days_known": bool(current.get("hold_days_known", False)),
                "is_held": is_held,
                "latest_price": float(row.get("close_price", 0.0)),
                "scores": {
                    "entry_score": float(row.get("entry_score", 0.0)),
                    "hold_score": float(row.get("hold_score", 0.0)),
                    "exit_score": float(row.get("exit_score", 0.0)),
                    "decision_score": float(row.get("decision_score", 0.0)),
                    "intra_score": float(row.get("intra_score", 0.0)),
                    "category_score": float(row.get("category_score", 0.0)),
                    "final_score": float(row.get("final_score", 0.0)),
                },
                "score_breakdown": self._parse_json(row.get("score_breakdown_json", {})),
                "feature_snapshot": self._feature_snapshot(row),
                "rationale": rationale,
                "execution_trace": execution_trace,
            }
            overlay_traces[symbol] = {
                "symbol": symbol,
                "action": action,
                "action_code": action_code,
                "intent": intent,
                "current_weight": current_weight,
                "normal_target_weight": normal_target_weight,
                "effective_target_weight": max(effective_target_weight, 0.0),
                "reduced_target_weight": reduced_target_weight,
                "delta_weight": delta_weight,
                "current_amount": current_amount,
                "target_amount": target_amount,
                "delta_amount": delta_amount,
                "suggested_amount": suggested_amount,
                "suggested_pct": suggested_pct,
                "reason_short": action_reason,
                "scores": item["scores"],
                "rationale": rationale,
                "execution_trace": execution_trace,
                "feature_snapshot": item["feature_snapshot"],
                "score_breakdown": item["score_breakdown"],
                "is_held": is_held,
                "latest_price": float(row.get("close_price", 0.0)),
            }
            if action != "no_trade":
                items.append(item)

        items.sort(
            key=lambda row: (
                {"sell": 0, "buy": 1, "hold": 2, "no_trade": 3}.get(row["action"], 9),
                row["rank"],
                row["symbol"],
            )
        )
        return {
            "items": items,
            "effective_target_weights": effective_target_weights,
            "overlay_rows": overlay_rows,
            "overlay_traces": overlay_traces,
        }

    def _prepare_overlay_frame(
        self,
        *,
        scored_df: pd.DataFrame,
        current_holdings: list[dict[str, Any]],
        preferences: Any,
    ) -> pd.DataFrame:
        df = scored_df.copy()
        default_columns = {
            "momentum_3d": 0.0,
            "momentum_5d": 0.0,
            "momentum_10d": 0.0,
            "momentum_20d": 0.0,
            "ma5": 0.0,
            "ma20": 0.0,
            "close_price": 0.0,
            "volatility_10d": 0.0,
            "volatility_20d": 0.0,
            "drawdown_20d": 0.0,
            "relative_strength_10d": 0.0,
            "liquidity_score": 0.0,
            "decision_category": "",
            "symbol": "",
        }
        for column, default in default_columns.items():
            if column not in df.columns:
                df[column] = default
        current_by_symbol = {str(row["symbol"]): dict(row) for row in current_holdings}
        df["current_weight"] = df["symbol"].map(
            lambda symbol: float(current_by_symbol.get(str(symbol), {}).get("current_weight", 0.0) or 0.0)
        )
        df["hold_days"] = df["symbol"].map(lambda symbol: int(current_by_symbol.get(str(symbol), {}).get("hold_days", 0) or 0))
        df["hold_days_known"] = df["symbol"].map(
            lambda symbol: bool(current_by_symbol.get(str(symbol), {}).get("hold_days_known", False))
        )
        df["is_held"] = df["current_weight"] > 0
        df["abs_drawdown_20d"] = pd.to_numeric(df["drawdown_20d"], errors="coerce").fillna(0.0).abs()
        df["category_symbol_count"] = df.groupby("decision_category")["symbol"].transform("count")
        df["category_median_volatility_20d"] = df.groupby("decision_category")["volatility_20d"].transform("median").fillna(0.0)

        df["momentum_5d_rel"] = self._category_relative_percentile(df, "momentum_5d", higher_is_better=True)
        df["momentum_10d_rel"] = self._category_relative_percentile(df, "momentum_10d", higher_is_better=True)
        df["momentum_20d_rel"] = self._category_relative_percentile(df, "momentum_20d", higher_is_better=True)
        df["trend_strength_rel"] = self._category_relative_percentile(df, "trend_strength", higher_is_better=True)
        df["relative_strength_10d_rel"] = self._category_relative_percentile(df, "relative_strength_10d", higher_is_better=True)
        df["liquidity_rel"] = self._category_relative_percentile(df, "liquidity_score", higher_is_better=True)
        df["volatility_10d_goodness"] = self._category_relative_percentile(df, "volatility_10d", higher_is_better=False)
        df["volatility_20d_spike"] = self._category_relative_percentile(df, "volatility_20d", higher_is_better=True)
        df["drawdown_goodness"] = self._category_relative_percentile(df, "abs_drawdown_20d", higher_is_better=False)
        df["drawdown_severity"] = self._category_relative_percentile(df, "abs_drawdown_20d", higher_is_better=True)
        df["rank_drop_score"] = self._rank_drop_score(df)
        df["time_decay_score"] = self._time_decay_score(df, preferences)

        entry_scores: list[float] = []
        hold_scores: list[float] = []
        exit_scores: list[float] = []
        decision_scores: list[float] = []
        channels: list[str] = []
        states: list[str] = []
        trend_passes: list[bool] = []
        pullback_passes: list[bool] = []
        rebound_passes: list[bool] = []
        breakout_passes: list[bool] = []
        entry_allowed_flags: list[bool] = []

        for _, row in df.iterrows():
            category = str(row.get("decision_category", ""))
            is_held = bool(row.get("is_held", False))
            entry_score = self._head_score(category, "entry", row)
            hold_score = self._head_score(category, "hold", row)
            exit_score = self._head_score(category, "exit", row)
            decision_score = self._decision_score(
                entry_score,
                hold_score,
                exit_score,
                is_held=is_held,
                preferences=preferences,
            )

            trend_filter_pass = bool(float(row.get("momentum_20d", 0.0)) > 0 and float(row.get("close_price", 0.0)) > float(row.get("ma20", 0.0)))
            pullback_zone_pass = bool(self.config.pullback_low_pct <= float(row.get("drawdown_20d", 0.0)) <= self.config.pullback_high_pct)
            rebound_confirmation_pass = bool(float(row.get("close_price", 0.0)) > float(row.get("ma5", 0.0)) and float(row.get("momentum_3d", 0.0)) > 0)
            breakout_exception_pass = bool(
                trend_filter_pass
                and float(row.get("drawdown_20d", 0.0)) > self.config.pullback_high_pct
                and entry_score >= self.config.breakout_entry_threshold
                and float(row.get("momentum_5d", 0.0)) > 0
                and float(row.get("close_price", 0.0)) > float(row.get("ma5", 0.0))
                and float(row.get("volatility_20d", 0.0)) <= float(row.get("category_median_volatility_20d", 0.0))
            )
            if trend_filter_pass and pullback_zone_pass and rebound_confirmation_pass:
                entry_channel = "A"
            elif breakout_exception_pass:
                entry_channel = "B"
            else:
                entry_channel = "none"
            entry_allowed = entry_channel in {"A", "B"}
            position_state = self._position_state(
                momentum_20d=float(row.get("momentum_20d", 0.0)),
                close_price=float(row.get("close_price", 0.0)),
                ma20=float(row.get("ma20", 0.0)),
                is_held=is_held,
            )

            entry_scores.append(entry_score)
            hold_scores.append(hold_score)
            exit_scores.append(exit_score)
            decision_scores.append(decision_score)
            channels.append(entry_channel)
            states.append(position_state)
            trend_passes.append(trend_filter_pass)
            pullback_passes.append(pullback_zone_pass)
            rebound_passes.append(rebound_confirmation_pass)
            breakout_passes.append(breakout_exception_pass)
            entry_allowed_flags.append(entry_allowed)

        df["entry_score"] = entry_scores
        df["hold_score"] = hold_scores
        df["exit_score"] = exit_scores
        df["decision_score"] = decision_scores
        df["entry_channel_used"] = channels
        df["position_state"] = states
        df["trend_filter_pass"] = trend_passes
        df["pullback_zone_pass"] = pullback_passes
        df["rebound_confirmation_pass"] = rebound_passes
        df["breakout_exception_pass"] = breakout_passes
        df["entry_allowed"] = entry_allowed_flags
        return df

    def _build_switch_context(
        self,
        prepared: pd.DataFrame,
        current_by_symbol: dict[str, dict[str, Any]],
        target_weights: dict[str, float],
    ) -> dict[str, dict[str, Any]]:
        context: dict[str, dict[str, Any]] = {}
        grouped = prepared.groupby("decision_category", dropna=False, sort=False)
        for category, group in grouped:
            held_rows = [row for _, row in group.iterrows() if str(row["symbol"]) in current_by_symbol]
            target_rows = [row for _, row in group.iterrows() if float(target_weights.get(str(row["symbol"]), 0.0)) > 0]
            new_rows = [row for row in target_rows if str(row["symbol"]) not in current_by_symbol]
            if not held_rows or not new_rows:
                continue

            old_row = max(
                held_rows,
                key=lambda row: (
                    float(current_by_symbol[str(row["symbol"])].get("current_weight", 0.0)),
                    float(row.get("final_score", 0.0)),
                ),
            )
            new_row = max(
                new_rows,
                key=lambda row: (
                    float(row.get("entry_score", 0.0)),
                    float(row.get("decision_score", 0.0)),
                    float(row.get("final_score", 0.0)),
                ),
            )
            old_symbol = str(old_row["symbol"])
            new_symbol = str(new_row["symbol"])
            old_state = str(old_row.get("position_state", "NONE"))
            new_target_weight = float(target_weights.get(new_symbol, 0.0))
            score_gap = float(new_row.get("decision_score", 0.0)) - float(old_row.get("decision_score", 0.0))
            switch_allowed = bool(
                old_state in {"REDUCE", "EXIT"}
                and bool(new_row.get("entry_allowed", False))
                and new_target_weight > self.config.rebalance_band
            )
            context[str(category)] = {
                "switch_in_symbol": new_symbol if switch_allowed else "",
                "switch_out_symbol": old_symbol if switch_allowed else "",
                "blocked_new_symbol": "" if switch_allowed else new_symbol,
                "blocked_old_symbol": "" if switch_allowed else old_symbol,
                "score_gap": score_gap,
                "old_state": old_state,
                "new_entry_allowed": bool(new_row.get("entry_allowed", False)),
                "new_target_weight": new_target_weight,
                "rebalance_band": self.config.rebalance_band,
                "switch_allowed": switch_allowed,
            }
        return context

    def _head_score(self, category: str, head_name: str, row: pd.Series | dict[str, Any]) -> float:
        head_config = self.category_heads.get(category, {}).get(head_name, {})
        if not head_config:
            return 0.0

        component_values = {
            "momentum_5d": float(row.get("momentum_5d_rel", 0.0)),
            "momentum_10d": float(row.get("momentum_10d_rel", 0.0)),
            "momentum_20d": float(row.get("momentum_20d_rel", 0.0)),
            "trend_strength": float(row.get("trend_strength_rel", 0.0)),
            "relative_strength_10d": float(row.get("relative_strength_10d_rel", 0.0)),
            "liquidity_score": float(row.get("liquidity_rel", 0.0)),
            "volatility_10d": float(row.get("volatility_10d_goodness", 0.0)),
            "abs_drawdown_20d": float(row.get("drawdown_goodness", 0.0)),
            "volatility_spike": float(row.get("volatility_20d_spike", 0.0)),
            "rank_drop": float(row.get("rank_drop_score", 0.0)),
            "time_decay": float(row.get("time_decay_score", 0.0)),
        }
        total_weight = sum(abs(float(weight)) for weight in head_config.values())
        if total_weight <= 0:
            return 0.0
        weighted_sum = 0.0
        for key, weight in head_config.items():
            weighted_sum += component_values.get(str(key), 0.0) * abs(float(weight))
        return round(weighted_sum / total_weight, 2)

    def _decision_score(self, entry_score: float, hold_score: float, exit_score: float, *, is_held: bool, preferences: Any) -> float:
        bucket_key = self._resolve_horizon_bucket(preferences)
        bucket = self.config.horizon_buckets.get(bucket_key, {})
        blend_key = "held" if is_held else "non_held"
        blend = bucket.get(blend_key, {"entry": 0.7, "hold": 0.2, "exit_inverse": 0.1})
        stay_score = max(0.0, 100.0 - float(exit_score))
        decision = (
            float(entry_score) * float(blend.get("entry", 0.0))
            + float(hold_score) * float(blend.get("hold", 0.0))
            + stay_score * float(blend.get("exit_inverse", 0.0))
        )
        return round(decision, 2)

    def _resolve_horizon_bucket(self, preferences: Any) -> str:
        target_holding_days = max(
            1,
            int(getattr(preferences, "target_holding_days", self.config.default_target_holding_days) or self.config.default_target_holding_days),
        )
        for name, payload in self.config.horizon_buckets.items():
            if target_holding_days <= int(payload.get("max_days", 9999)):
                return str(name)
        return "long"

    def _position_state(self, *, momentum_20d: float, close_price: float, ma20: float, is_held: bool) -> str:
        if not is_held:
            return "NONE"
        if momentum_20d > 0 and close_price > ma20:
            return "HOLD"
        if momentum_20d <= 0 and close_price <= ma20:
            return "EXIT"
        return "REDUCE"

    def _reduced_target_weight(self, *, normal_target_weight: float, current_weight: float) -> float:
        if normal_target_weight > 0:
            return max(normal_target_weight * self.config.reduced_target_multiplier, 0.0)
        return max(current_weight * self.config.reduced_target_multiplier, 0.0)

    def _blocked_entry_reason(self, *, row: dict[str, Any], normal_target_weight: float) -> str:
        if normal_target_weight <= 0:
            return "这只 ETF 没进入本轮目标组合，因此暂不新开仓。"
        if not bool(row.get("trend_filter_pass", False)):
            return "20日动量或20日均线趋势不过关，先不做新的多头买入。"
        if bool(row.get("pullback_zone_pass", False)) and not bool(row.get("rebound_confirmation_pass", False)):
            return "虽然已进入合理回撤区，但反弹确认还不够，继续等待。"
        return "当前既不满足回撤后反弹，也不满足强趋势突破，因此暂不开仓。"

    def _execution_note(self, *, action_code: str, tradability_mode: str) -> str:
        if tradability_mode == "t0":
            mode_note = "该 ETF 按 T+0 口径展示执行提示。"
        else:
            mode_note = "该 ETF 按 T+1 口径展示执行提示。"
        if action_code == "switch":
            return f"{mode_note} 本次是同类别换仓，先确认旧仓减弱，再切到新龙头。"
        return mode_note

    def _feature_snapshot(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "close_price": float(row.get("close_price", 0.0)),
            "momentum_3d": float(row.get("momentum_3d", 0.0)),
            "momentum_5d": float(row.get("momentum_5d", 0.0)),
            "momentum_10d": float(row.get("momentum_10d", 0.0)),
            "momentum_20d": float(row.get("momentum_20d", 0.0)),
            "ma5": float(row.get("ma5", 0.0)),
            "ma10": float(row.get("ma10", 0.0)),
            "ma20": float(row.get("ma20", 0.0)),
            "trend_strength": float(row.get("trend_strength", 0.0)),
            "drawdown_20d": float(row.get("drawdown_20d", 0.0)),
            "volatility_20d": float(row.get("volatility_20d", 0.0)),
            "liquidity_score": float(row.get("liquidity_score", 0.0)),
            "decision_category": str(row.get("decision_category", "")),
            "tradability_mode": str(row.get("tradability_mode", "")),
        }

    def _build_execution_trace(
        self,
        *,
        row: dict[str, Any],
        current: dict[str, Any],
        category_context: dict[str, Any],
        base_state: str,
        normal_target_weight: float,
        reduced_target_weight: float,
        effective_target_weight: float,
        current_weight: float,
        current_amount: float,
        target_amount: float,
        delta_weight: float,
        delta_amount: float,
        total_asset: float,
        min_trade_amount: float,
        action: str,
        action_code: str,
        switch_in: bool,
        switch_out: bool,
        switch_blocked: bool,
        switch_partner: str,
        action_reason: str,
        min_trade_blocked: bool,
    ) -> dict[str, Any]:
        trend_filter_pass = bool(row.get("trend_filter_pass", False))
        pullback_zone_pass = bool(row.get("pullback_zone_pass", False))
        rebound_confirmation_pass = bool(row.get("rebound_confirmation_pass", False))
        breakout_exception_pass = bool(row.get("breakout_exception_pass", False))
        entry_allowed = bool(row.get("entry_allowed", False))
        entry_channel = str(row.get("entry_channel_used", "none"))
        current_weight_safe = float(current_weight)
        target_branch = "entry_not_allowed_zero"
        if switch_out or base_state == "EXIT":
            target_branch = "switch_out_or_exit"
        elif base_state == "REDUCE":
            target_branch = "reduce_to_half"
        elif action_code in {"buy_open", "buy_add", "switch"}:
            target_branch = "open_or_add_to_normal"
        elif base_state == "HOLD":
            target_branch = "hold_without_add"

        if base_state == "HOLD":
            position_state_reason = "20日动量仍为正且价格站在20日均线之上，趋势保持健康。"
        elif base_state == "REDUCE":
            position_state_reason = "20日动量仍为正，但价格已回到20日均线下方，趋势转弱。"
        elif base_state == "EXIT":
            position_state_reason = "20日动量已不再为正，且价格跌破20日均线，趋势破坏。"
        else:
            position_state_reason = "当前没有持仓，因此只进行入场可行性判断。"

        return {
            "entry_checks": {
                "trend_filter_pass": trend_filter_pass,
                "channel_a": {
                    "trend_filter_pass": trend_filter_pass,
                    "pullback_zone_pass": pullback_zone_pass,
                    "rebound_confirmation_pass": rebound_confirmation_pass,
                    "channel_a_pass": bool(trend_filter_pass and pullback_zone_pass and rebound_confirmation_pass),
                },
                "channel_b": {
                    "trend_filter_pass": trend_filter_pass,
                    "drawdown_near_high_pass": bool(float(row.get("drawdown_20d", 0.0)) > self.config.pullback_high_pct),
                    "entry_score_pass": bool(float(row.get("entry_score", 0.0)) >= self.config.breakout_entry_threshold),
                    "momentum_5d_pass": bool(float(row.get("momentum_5d", 0.0)) > 0),
                    "close_above_ma5_pass": bool(float(row.get("close_price", 0.0)) > float(row.get("ma5", 0.0))),
                    "volatility_guard_pass": bool(float(row.get("volatility_20d", 0.0)) <= float(row.get("category_median_volatility_20d", 0.0))),
                    "channel_b_pass": breakout_exception_pass,
                },
                "entry_channel": entry_channel,
                "entry_allowed": entry_allowed,
                "breakout_entry_threshold": self.config.breakout_entry_threshold,
                "pullback_low_pct": self.config.pullback_low_pct,
                "pullback_high_pct": self.config.pullback_high_pct,
            },
            "position_state": {
                "current_weight": current_weight_safe,
                "position_state": base_state,
                "position_state_label": POSITION_STATE_LABELS.get(base_state, base_state),
                "reason": position_state_reason,
                "hold_days": int(current.get("hold_days", 0) or 0),
                "hold_days_known": bool(current.get("hold_days_known", False)),
                "reduced_target_weight": reduced_target_weight if base_state == "REDUCE" else 0.0,
            },
            "switch_checks": {
                "old_state": str(category_context.get("old_state", "NONE")),
                "new_entry_allowed": bool(category_context.get("new_entry_allowed", False)),
                "new_target_weight": float(category_context.get("new_target_weight", 0.0)),
                "rebalance_band": float(category_context.get("rebalance_band", self.config.rebalance_band)),
                "switch_allowed": bool(category_context.get("switch_allowed", False)),
                "switch_in": switch_in,
                "switch_out": switch_out,
                "switch_blocked": switch_blocked,
                "switch_partner": switch_partner,
                "score_gap": float(category_context.get("score_gap", 0.0)),
            },
            "target_weight_adjustment": {
                "normal_target_weight": normal_target_weight,
                "current_weight": current_weight_safe,
                "reduced_target_weight": reduced_target_weight,
                "effective_target_weight": effective_target_weight,
                "branch": target_branch,
            },
            "final_action_calc": {
                "current_weight": current_weight_safe,
                "effective_target_weight": effective_target_weight,
                "delta_weight": delta_weight,
                "total_asset": float(total_asset),
                "current_amount": current_amount,
                "target_amount": target_amount,
                "delta_amount": delta_amount,
                "min_trade_amount": min_trade_amount,
                "rebalance_band": self.config.rebalance_band,
                "min_trade_blocked": min_trade_blocked,
                "action": action,
                "action_code": action_code,
                "action_reason": action_reason,
            },
        }

    def _entry_channel_label(self, value: str) -> str:
        return ENTRY_CHANNEL_LABELS.get(value, value)

    def _category_relative_percentile(self, df: pd.DataFrame, column: str, *, higher_is_better: bool) -> pd.Series:
        values = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
        global_percentile = self._percentile(values, higher_is_better=higher_is_better)
        result = global_percentile.copy()
        for _, index in df.groupby("decision_category", dropna=False).groups.items():
            category_values = values.loc[index]
            if len(category_values) > 1:
                result.loc[index] = self._percentile(category_values, higher_is_better=higher_is_better)
        return result.reindex(df.index).fillna(global_percentile)

    def _percentile(self, series: pd.Series, *, higher_is_better: bool) -> pd.Series:
        clean = pd.to_numeric(series, errors="coerce").fillna(0.0)
        count = len(clean)
        if count == 0:
            return pd.Series(dtype=float)
        if count == 1:
            return pd.Series([100.0], index=clean.index, dtype=float)
        ranks = clean.rank(method="average", ascending=True)
        denominator = max(count - 1, 1)
        if higher_is_better:
            percentile = (ranks - 1) / denominator
        else:
            percentile = (count - ranks) / denominator
        return percentile.clip(lower=0.0, upper=1.0) * 100.0

    def _rank_drop_score(self, df: pd.DataFrame) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=float)
        values = []
        for _, row in df.iterrows():
            size = int(row.get("category_symbol_count", 1) or 1)
            rank = int(row.get("category_rank", 1) or 1)
            if size <= 1:
                values.append(0.0)
                continue
            values.append(((rank - 1) / max(size - 1, 1)) * 100.0)
        return pd.Series(values, index=df.index, dtype=float)

    def _time_decay_score(self, df: pd.DataFrame, preferences: Any) -> pd.Series:
        target_holding_days = max(
            1,
            int(getattr(preferences, "target_holding_days", self.config.default_target_holding_days) or self.config.default_target_holding_days),
        )
        values = []
        for _, row in df.iterrows():
            hold_days = max(int(row.get("hold_days", 0) or 0), 0)
            values.append(min(hold_days / target_holding_days, 1.0) * 100.0)
        return pd.Series(values, index=df.index, dtype=float)

    def _parse_json(self, value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str) and value.strip():
            try:
                loaded = json.loads(value)
            except json.JSONDecodeError:
                return {}
            return loaded if isinstance(loaded, dict) else {}
        return {}
