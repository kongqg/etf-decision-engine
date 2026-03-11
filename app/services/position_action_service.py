from __future__ import annotations

from datetime import datetime
from typing import Any

from app.core.config import get_settings, load_yaml_config
from app.services.decision_policy_service import get_decision_policy_service


ORDER_ACTIONS = {"buy_open", "buy_add", "reduce", "sell_exit", "park_in_money_etf"}
BUY_ACTIONS = {"buy_open", "buy_add", "park_in_money_etf"}
SELL_ACTIONS = {"reduce", "sell_exit"}

POSITION_ACTION_BY_LEGACY = {
    "buy_open": "open_position",
    "buy_add": "add_position",
    "hold": "hold_position",
    "reduce": "reduce_position",
    "sell_exit": "exit_position",
    "no_trade": "no_trade",
    "park_in_money_etf": "park_in_money_etf",
}

POSITION_ACTION_LABELS = {
    "open_position": "开仓买入",
    "add_position": "继续加仓",
    "hold_position": "继续持有",
    "reduce_position": "减仓",
    "exit_position": "卖出退出",
    "no_trade": "暂不交易",
    "park_in_money_etf": "转入货币ETF",
}


class PositionActionService:
    def __init__(self) -> None:
        settings = get_settings()
        self.settings = settings
        self.rules = load_yaml_config(settings.config_dir / "action_thresholds.yaml")
        self.policy = get_decision_policy_service()

    def decide(
        self,
        *,
        row: dict[str, Any],
        position: dict[str, Any] | None,
        preferences,
        total_asset: float,
        available_cash: float,
        current_position_pct: float,
        target_position_pct: float,
        target_weight: float,
        selected_category: str,
        offensive_edge: bool,
        fallback_action: str,
        trade_context: dict[str, Any],
        current_time: datetime,
        thresholds: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        rules = thresholds or self.rules
        decision_thresholds = rules.get("decision_thresholds", {})
        position_rules = rules.get("position_rules", {})
        action_rules = rules.get("position_action_rules", {})

        reduce_fraction = float(position_rules.get("reduce_fraction", 0.5))
        full_exit_fraction = float(position_rules.get("full_exit_fraction", 1.0))
        rebalance_tolerance = float(position_rules.get("rebalance_tolerance_pct", 0.01))
        minimum_action_delta = max(
            rebalance_tolerance,
            float(action_rules.get("minimum_action_delta_pct", rebalance_tolerance)),
        )
        hold_exit_buffer = float(action_rules.get("hold_exit_score_buffer", 6.0))
        reduce_rank_drop = int(action_rules.get("rank_drop_to_reduce", 1))
        exit_rank_drop = int(action_rules.get("rank_drop_to_exit", 2))
        reopen_cooldown_days = int(action_rules.get("reopen_cooldown_days_after_exit", 2))
        reopen_score_buffer = float(action_rules.get("reopen_score_buffer", 4.0))
        minimum_holding_days_before_reduce = int(action_rules.get("minimum_holding_days_before_reduce", 1))

        open_threshold = float(decision_thresholds.get("open_threshold", 58.0))
        add_threshold = float(decision_thresholds.get("add_threshold", 64.0))
        hold_threshold = float(decision_thresholds.get("hold_threshold", 48.0))
        reduce_threshold = float(decision_thresholds.get("reduce_threshold", 58.0))
        full_exit_threshold = float(decision_thresholds.get("full_exit_threshold", 72.0))
        strong_entry_threshold = float(decision_thresholds.get("strong_entry_threshold", 62.0))
        strong_hold_threshold = float(decision_thresholds.get("strong_hold_threshold", 55.0))

        symbol = str(row["symbol"])
        decision_score = float(row["decision_score"])
        entry_score = float(row["entry_score"])
        hold_score = float(row["hold_score"])
        exit_score = float(row["exit_score"])
        category = str(row["decision_category"])
        min_order_amount = float(row["close_price"]) * float(row["lot_size"])
        rank_drop = int(row.get("rank_drop", 0) or 0)
        days_held = int(row.get("days_held", 0) or 0)

        current_weight = float(position.get("weight_pct", 0.0)) if position is not None else 0.0
        market_value = float(position.get("market_value", 0.0)) if position is not None else 0.0
        current_amount = market_value if position is not None else 0.0
        target_weight = max(float(target_weight), 0.0)
        target_amount = target_weight * total_asset
        delta_weight = target_weight - current_weight
        delta_amount = max(delta_weight, 0.0) * total_asset
        trim_amount = max(current_weight - target_weight, 0.0) * total_asset
        selected_for_target = target_weight > minimum_action_delta
        defensive_category = self.policy.defensive_category()

        last_trade = trade_context.get("last_trade_by_symbol", {}).get(symbol)
        recently_sold = False
        if position is None and last_trade is not None and str(getattr(last_trade, "side", "")) == "sell":
            days_since_sell = max((current_time.date() - last_trade.executed_at.date()).days, 0)
            recently_sold = days_since_sell < reopen_cooldown_days

        legacy_action_code = "no_trade"
        action_reason = "当前还没有形成足够清晰的新开仓优势。"
        suggested_amount = 0.0
        suggested_pct = 0.0

        if position is not None:
            if (
                exit_score >= full_exit_threshold
                and (
                    target_weight <= minimum_action_delta
                    or rank_drop >= exit_rank_drop
                    or decision_score < hold_threshold
                    or exit_score >= hold_score + hold_exit_buffer
                )
            ):
                legacy_action_code = "sell_exit"
                suggested_amount = market_value * full_exit_fraction
                suggested_pct = current_weight * full_exit_fraction
                if target_weight <= minimum_action_delta:
                    action_reason = "退出分显著抬升，且目标组合已不再保留这只持仓，建议清仓。"
                elif rank_drop >= exit_rank_drop:
                    action_reason = "退出分显著抬升，且同类排名明显下滑，建议清仓。"
                else:
                    action_reason = "退出分已经明显高于阈值，当前继续持有的优势不足，建议清仓。"
            elif delta_weight < -minimum_action_delta:
                if days_held < minimum_holding_days_before_reduce and exit_score < full_exit_threshold:
                    legacy_action_code = "hold"
                    action_reason = "这只持仓建仓时间还短，先观察一天，避免刚建仓就因小波动反复调仓。"
                elif (
                    exit_score >= reduce_threshold
                    or rank_drop >= reduce_rank_drop
                    or exit_score >= hold_score + hold_exit_buffer
                ):
                    legacy_action_code = "reduce"
                    suggested_amount = min(max(trim_amount, market_value * reduce_fraction), market_value)
                    suggested_pct = suggested_amount / total_asset if total_asset else 0.0
                    if target_weight <= minimum_action_delta:
                        action_reason = "当前目标仓位已经降到接近零，且退出压力抬升，先减仓退出这只持仓。"
                    else:
                        action_reason = "当前目标仓位下降，且退出分抬升，先减仓向目标权重靠拢。"
                else:
                    legacy_action_code = "hold"
                    action_reason = "目标仓位虽然下调，但退出信号还不够强，先继续持有观察。"
            elif delta_weight > minimum_action_delta:
                addable_amount = min(delta_amount, available_cash)
                if (
                    category == defensive_category
                    and not offensive_edge
                    and fallback_action == "park_in_money_etf"
                ):
                    legacy_action_code = "park_in_money_etf"
                    suggested_amount = addable_amount
                    suggested_pct = suggested_amount / total_asset if total_asset else 0.0
                    action_reason = "进攻边际不足，新增可用仓位先停泊到货币ETF。"
                elif (
                    offensive_edge
                    and category == selected_category
                    and decision_score >= add_threshold
                    and entry_score >= strong_entry_threshold
                    and hold_score >= strong_hold_threshold
                ):
                    legacy_action_code = "buy_add"
                    suggested_amount = addable_amount
                    suggested_pct = suggested_amount / total_asset if total_asset else 0.0
                    action_reason = "入场分和综合分都强，且目标仓位仍高于当前，建议加仓。"
                else:
                    legacy_action_code = "hold"
                    action_reason = "它仍在目标组合里，但今天的加仓信号还不够强，先继续持有。"
            elif exit_score >= reduce_threshold and exit_score >= hold_score + hold_exit_buffer:
                legacy_action_code = "reduce"
                suggested_amount = min(max(market_value * reduce_fraction, trim_amount), market_value)
                suggested_pct = suggested_amount / total_asset if total_asset else 0.0
                action_reason = "当前目标仓位变化不大，但退出分明显抬升，先主动减仓控制风险。"
            else:
                legacy_action_code = "hold"
                if abs(delta_weight) <= minimum_action_delta:
                    action_reason = "持有分仍强，退出分不高，当前仓位也已接近目标，继续持有。"
                else:
                    action_reason = "当前没有足够强的增减仓理由，继续持有等待更清晰的信号。"
        elif selected_for_target:
            open_amount = min(target_amount, available_cash)
            if category == defensive_category and not offensive_edge and fallback_action == "park_in_money_etf":
                legacy_action_code = "park_in_money_etf"
                suggested_amount = open_amount
                suggested_pct = suggested_amount / total_asset if total_asset else 0.0
                action_reason = "进攻边际不足，新增可用仓位先停泊到货币ETF。"
            elif recently_sold and decision_score < open_threshold + reopen_score_buffer:
                legacy_action_code = "no_trade"
                action_reason = "这只标的刚在近几天内卖出，当前信号虽有改善，但还不够强，不急着反手重开。"
            elif (
                offensive_edge
                and category == selected_category
                and decision_score >= open_threshold
                and entry_score >= strong_entry_threshold
                and current_position_pct < target_position_pct
            ):
                legacy_action_code = "buy_open"
                suggested_amount = open_amount
                suggested_pct = suggested_amount / total_asset if total_asset else 0.0
                action_reason = "类别优势明确，入场分和综合分都已过阈值，且组合还有仓位空间，建议开仓。"
            else:
                legacy_action_code = "no_trade"
                action_reason = "虽然它进入了目标组合候选，但当前分数还没有强到今天就开仓。"

        if legacy_action_code in BUY_ACTIONS and (
            suggested_amount < float(preferences.min_trade_amount) or suggested_amount < min_order_amount
        ):
            if legacy_action_code == "park_in_money_etf":
                action_reason = "防守切换成立，但当前可停泊的剩余资金还不足以覆盖最小可执行门槛。"
            else:
                action_reason = "开仓或加仓信号成立，但本次金额还不足以覆盖最小建议金额或一手门槛。"

        position_action = POSITION_ACTION_BY_LEGACY[legacy_action_code]
        position_action_label = POSITION_ACTION_LABELS[position_action]

        return {
            "action_code": legacy_action_code,
            "position_action": position_action,
            "position_action_label": position_action_label,
            "action_reason": action_reason,
            "suggested_amount": round(float(suggested_amount), 2),
            "suggested_pct": float(suggested_pct),
            "min_order_amount": min_order_amount,
            "current_weight": current_weight,
            "target_weight": target_weight,
            "delta_weight": delta_weight,
            "current_amount": current_amount,
            "target_amount": target_amount,
            "rank_drop": rank_drop,
            "days_held": days_held,
        }
