from __future__ import annotations

from typing import Any

import pandas as pd

from app.core.config import get_settings, load_yaml_config


class AllocationService:
    def __init__(self) -> None:
        settings = get_settings()
        self.settings = settings
        self.rules = load_yaml_config(settings.config_dir / "risk_rules.yaml")

    def _target_position_pct(self, preferences: Any, market_regime: str) -> float:
        base = float(self.rules["position_by_regime"].get(market_regime, 0.05))
        adjust = float(self.rules["risk_adjustment_by_preference"].get(preferences.risk_level, 0.0))
        value = max(0.0, min(base + adjust, float(preferences.max_total_position_pct)))
        reserve_cap = max(0.0, 1.0 - float(preferences.cash_reserve_pct))
        return round(min(value, reserve_cap), 4)

    def plan(
        self,
        scored_df: pd.DataFrame,
        positions_df: pd.DataFrame,
        total_asset: float,
        current_position_pct: float,
        preferences: Any,
        market_regime: str,
    ) -> dict:
        target_position_pct = self._target_position_pct(preferences, market_regime)
        min_score_to_buy = self.settings.min_score_to_buy
        min_score_gap_for_single = self.settings.min_score_gap_for_single
        candidate_count = int(len(scored_df))

        base_facts = {
            "candidate_count": candidate_count,
            "current_position_pct": round(current_position_pct, 4),
            "target_position_pct": round(target_position_pct, 4),
            "buy_score_threshold": round(min_score_to_buy, 2),
            "min_trade_amount": float(preferences.min_trade_amount),
        }

        if scored_df.empty:
            return {
                "action": "不操作",
                "target_position_pct": target_position_pct,
                "items": [],
                "summary": "今天没有 ETF 通过筛选，系统建议先观望。",
                "reason_code": "no_candidates",
                "facts": base_facts,
            }

        top_score = float(scored_df.iloc[0]["total_score"])
        base_facts["top_score"] = round(top_score, 2)
        base_facts["top_symbol"] = str(scored_df.iloc[0]["symbol"])
        base_facts["top_name"] = str(scored_df.iloc[0]["name"])

        if market_regime == "观望":
            return {
                "action": "不操作",
                "target_position_pct": target_position_pct,
                "items": [],
                "summary": "当前市场偏弱，系统建议先不出手。",
                "reason_code": "weak_market",
                "facts": base_facts,
            }

        if top_score < min_score_to_buy:
            return {
                "action": "不操作",
                "target_position_pct": target_position_pct,
                "items": [],
                "summary": "最强候选 ETF 的信号还不够强，系统建议先不操作。",
                "reason_code": "weak_score",
                "facts": base_facts,
            }

        if current_position_pct + 0.03 < target_position_pct:
            action = "买入"
        elif current_position_pct - 0.08 > target_position_pct:
            action = "卖出"
        else:
            action = "不操作"

        if action == "不操作":
            return {
                "action": action,
                "target_position_pct": target_position_pct,
                "items": [],
                "summary": "当前仓位已经接近目标仓位，系统建议暂时不动。",
                "reason_code": "near_target_position",
                "facts": base_facts,
            }

        if action == "买入":
            top_n = 1 if len(scored_df) == 1 else 2
            if len(scored_df) >= 3 and top_score - float(scored_df.iloc[1]["total_score"]) < min_score_gap_for_single:
                top_n = min(self.settings.top_n_default, len(scored_df))

            selected = scored_df.head(top_n).copy()
            desired_increment = max(target_position_pct - current_position_pct, 0.0)
            deploy_pct = desired_increment * self.settings.initial_build_ratio
            deploy_amount = total_asset * deploy_pct

            scores = selected["total_score"].clip(lower=1.0)
            selected["weight_raw"] = scores / scores.sum()
            selected["suggested_pct"] = (selected["weight_raw"] * deploy_pct).clip(
                upper=float(preferences.max_single_position_pct)
            )
            selected["suggested_amount"] = selected["suggested_pct"] * total_asset
            selected = selected[selected["suggested_amount"] >= float(preferences.min_trade_amount)].copy()

            facts = {
                **base_facts,
                "top_n": top_n,
                "desired_increment_pct": round(desired_increment, 4),
                "deploy_pct": round(deploy_pct, 4),
                "deploy_amount": round(deploy_amount, 2),
            }

            if selected.empty:
                return {
                    "action": "不操作",
                    "target_position_pct": target_position_pct,
                    "items": [],
                    "summary": "虽然有可关注的 ETF，但本次建议金额低于最小交易金额，系统建议先不操作。",
                    "reason_code": "amount_below_min_trade",
                    "facts": facts,
                }

            return {
                "action": action,
                "target_position_pct": target_position_pct,
                "items": selected.to_dict(orient="records"),
                "summary": f"当前市场偏{market_regime}，建议先分批买入，目标总仓位约 {target_position_pct:.0%}。",
                "reason_code": "buy_candidates",
                "facts": facts,
            }

        ranked_positions = positions_df.copy()
        if ranked_positions.empty:
            return {
                "action": "不操作",
                "target_position_pct": target_position_pct,
                "items": [],
                "summary": "虽然系统认为仓位偏高，但你当前没有持仓可减。",
                "reason_code": "no_positions_to_reduce",
                "facts": base_facts,
            }

        ranked_positions = ranked_positions.sort_values(["score", "market_value"], ascending=[True, False]).reset_index(drop=True)
        reduction_pct = max(current_position_pct - target_position_pct, 0.0) * 0.6
        reduction_amount = total_asset * reduction_pct
        remaining = reduction_amount
        sell_items = []

        for _, row in ranked_positions.iterrows():
            if remaining <= 0:
                break
            item_amount = min(float(row["market_value"]), remaining)
            if item_amount < float(preferences.min_trade_amount):
                continue
            payload = row.to_dict()
            payload["suggested_amount"] = item_amount
            payload["suggested_pct"] = item_amount / total_asset if total_asset else 0.0
            sell_items.append(payload)
            remaining -= item_amount

        facts = {
            **base_facts,
            "reduction_pct": round(reduction_pct, 4),
            "reduction_amount": round(reduction_amount, 2),
        }

        if not sell_items:
            return {
                "action": "不操作",
                "target_position_pct": target_position_pct,
                "items": [],
                "summary": "虽然仓位偏高，但这次可减仓金额低于最小交易金额，系统建议先不操作。",
                "reason_code": "trim_amount_below_min_trade",
                "facts": facts,
            }

        return {
            "action": "卖出",
            "target_position_pct": target_position_pct,
            "items": sell_items,
            "summary": f"当前仓位偏高，市场偏{market_regime}，建议先减一部分仓位，把总仓位向 {target_position_pct:.0%} 靠拢。",
            "reason_code": "trim_positions",
            "facts": facts,
        }
