from __future__ import annotations

from typing import Any

import pandas as pd

from app.core.config import get_settings, load_yaml_config
from app.utils.maths import round_money


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

    def _strategy_min_amount(self, preferences: Any) -> float:
        return float(getattr(preferences, "min_trade_amount", self.settings.default_min_advice_amount))

    def _lot_size_for_row(self, row: pd.Series) -> float:
        lot_size = float(row.get("lot_size", self.settings.default_lot_size))
        return lot_size if lot_size > 0 else float(self.settings.default_lot_size)

    def _annotate_buy_candidate(self, row: pd.Series, min_advice_amount: float, available_cash: float) -> dict[str, Any]:
        payload = row.to_dict()
        raw_price = float(payload.get("close_price", 0.0))
        latest_price = round_money(raw_price)
        lot_size = self._lot_size_for_row(row)
        min_order_amount = round_money(max(raw_price * lot_size, 0.0))
        suggested_amount = round_money(float(payload.get("suggested_amount", 0.0)))
        is_executable = not self.settings.budget_filter_enabled or suggested_amount >= min_order_amount
        budget_gap = round_money(max(min_order_amount - suggested_amount, 0.0))

        payload.update(
            {
                "latest_price": latest_price,
                "lot_size": lot_size,
                "min_advice_amount": round_money(min_advice_amount),
                "min_order_amount": min_order_amount,
                "available_cash": round_money(available_cash),
                "budget_gap_to_min_order": budget_gap,
                "is_executable": is_executable,
                "recommendation_bucket": (
                    "executable_recommendations" if is_executable else "watchlist_recommendations"
                ),
                "execution_status": "可执行买入" if is_executable else "关注标的",
                "not_executable_reason": (
                    ""
                    if is_executable
                    else f"当前建议金额 {suggested_amount:.2f} 元，还不够买入 1 手（约 {min_order_amount:.2f} 元）。"
                ),
                "execution_note": (
                    f"按当前预算，这只 ETF 的建议金额约 {suggested_amount:.2f} 元，已经覆盖 1 手所需金额，可执行。"
                    if is_executable
                    else f"这只 ETF 综合评分不差，但当前建议金额只有 {suggested_amount:.2f} 元，低于 1 手所需的 {min_order_amount:.2f} 元，先放入关注标的。"
                ),
            }
        )
        return payload

    def _candidate_order_metrics(self, scored_df: pd.DataFrame) -> pd.DataFrame:
        if scored_df.empty:
            return scored_df
        enriched = scored_df.copy()
        enriched["lot_size"] = float(self.settings.default_lot_size)
        enriched["min_order_amount"] = (enriched["close_price"] * enriched["lot_size"]).map(round_money)
        return enriched

    def _build_watchlist_items(
        self,
        scored_df: pd.DataFrame,
        min_advice_amount: float,
        available_cash: float,
        exclude_symbols: set[str] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        if scored_df.empty or not self.settings.show_watchlist_recommendations:
            return []

        exclude_symbols = exclude_symbols or set()
        ranked = self._candidate_order_metrics(scored_df)
        ranked = ranked[~ranked["symbol"].isin(exclude_symbols)].copy()
        ranked = ranked[ranked["min_order_amount"] > float(available_cash)].reset_index(drop=True)
        if limit is not None:
            ranked = ranked.head(limit)

        items = []
        for _, row in ranked.iterrows():
            payload = row.to_dict()
            payload["suggested_amount"] = round_money(min(float(available_cash), float(payload["min_order_amount"])))
            payload["suggested_pct"] = 0.0
            payload = self._annotate_buy_candidate(pd.Series(payload), min_advice_amount, available_cash)
            payload["is_executable"] = False
            payload["recommendation_bucket"] = "watchlist_recommendations"
            payload["execution_status"] = "关注标的"
            payload["not_executable_reason"] = (
                f"当前现金 {available_cash:.2f} 元，还不够买入 1 手（约 {payload['min_order_amount']:.2f} 元）。"
            )
            payload["execution_note"] = (
                f"这只 ETF 综合评分较高，但当前现金 {available_cash:.2f} 元不足以覆盖 1 手所需的 {payload['min_order_amount']:.2f} 元，先放入关注标的。"
            )
            items.append(payload)
        return items

    def _build_one_lot_fallback_item(
        self,
        scored_df: pd.DataFrame,
        min_advice_amount: float,
        available_cash: float,
        deploy_amount: float,
        total_asset: float,
    ) -> dict[str, Any] | None:
        ranked = self._candidate_order_metrics(scored_df)
        if ranked.empty:
            return None

        affordable = ranked[ranked["min_order_amount"] <= float(available_cash)].copy()
        if affordable.empty:
            return None
        affordable = affordable[affordable["total_score"] >= float(self.settings.min_score_to_buy)].copy()
        if affordable.empty:
            return None

        sort_columns = ["total_score"]
        ascending = [False]
        if "momentum_5d" in affordable.columns:
            sort_columns.append("momentum_5d")
            ascending.append(False)
        affordable = affordable.sort_values(sort_columns, ascending=ascending).reset_index(drop=True)
        payload = affordable.iloc[0].to_dict()
        payload["suggested_amount"] = round_money(float(payload["min_order_amount"]))
        payload["suggested_pct"] = round(payload["suggested_amount"] / total_asset, 4) if total_asset else 0.0
        annotated = self._annotate_buy_candidate(pd.Series(payload), min_advice_amount, available_cash)
        annotated["small_account_override"] = True
        annotated["execution_note"] = (
            f"常规分批预算约 {deploy_amount:.2f} 元，但小资金账户至少要能买 1 手。"
            f"当前现金足够买入 1 手 {annotated['name']}，所以系统优先给出这只当前买得起的高分 ETF。"
        )
        return annotated

    def plan(
        self,
        scored_df: pd.DataFrame,
        positions_df: pd.DataFrame,
        total_asset: float,
        available_cash: float,
        current_position_pct: float,
        preferences: Any,
        market_regime: str,
    ) -> dict:
        target_position_pct = self._target_position_pct(preferences, market_regime)
        min_score_to_buy = self.settings.min_score_to_buy
        min_score_gap_for_single = self.settings.min_score_gap_for_single
        min_advice_amount = self._strategy_min_amount(preferences)
        candidate_count = int(len(scored_df))

        base_facts = {
            "candidate_count": candidate_count,
            "current_position_pct": round(current_position_pct, 4),
            "target_position_pct": round(target_position_pct, 4),
            "buy_score_threshold": round(min_score_to_buy, 2),
            "min_advice_amount": round_money(min_advice_amount),
            "min_trade_amount": round_money(min_advice_amount),
            "available_cash": round_money(available_cash),
            "lot_size": float(self.settings.default_lot_size),
            "budget_filter_enabled": self.settings.budget_filter_enabled,
            "show_watchlist_recommendations": self.settings.show_watchlist_recommendations,
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
            selected["lot_size"] = float(self.settings.default_lot_size)

            total_suggested_amount = float(selected["suggested_amount"].sum())
            effective_budget = min(float(available_cash), total_suggested_amount) if total_suggested_amount > 0 else 0.0
            if total_suggested_amount > 0 and effective_budget < total_suggested_amount:
                scale = effective_budget / total_suggested_amount
                selected["suggested_amount"] = selected["suggested_amount"] * scale
                selected["suggested_pct"] = selected["suggested_pct"] * scale

            selected["suggested_amount"] = selected["suggested_amount"].map(round_money)
            selected["suggested_pct"] = selected["suggested_pct"].round(4)
            selected = selected[selected["suggested_amount"] >= min_advice_amount].copy()

            facts = {
                **base_facts,
                "top_n": top_n,
                "desired_increment_pct": round(desired_increment, 4),
                "deploy_pct": round(deploy_pct, 4),
                "deploy_amount": round_money(min(deploy_amount, available_cash)),
            }

            if selected.empty:
                fallback_item = self._build_one_lot_fallback_item(
                    scored_df=scored_df,
                    min_advice_amount=min_advice_amount,
                    available_cash=available_cash,
                    deploy_amount=facts["deploy_amount"],
                    total_asset=total_asset,
                )
                if fallback_item is not None:
                    watchlist_items = self._build_watchlist_items(
                        scored_df=scored_df,
                        min_advice_amount=min_advice_amount,
                        available_cash=available_cash,
                        exclude_symbols={fallback_item["symbol"]},
                        limit=self.settings.top_n_default,
                    )
                    facts.update(
                        {
                            "executable_count": 1,
                            "watchlist_count": len(watchlist_items),
                            "small_account_override": True,
                            "fallback_symbol": fallback_item["symbol"],
                            "fallback_min_order_amount": fallback_item["min_order_amount"],
                        }
                    )
                    return {
                        "action": "买入",
                        "target_position_pct": target_position_pct,
                        "items": [fallback_item],
                        "watchlist_items": watchlist_items,
                        "summary": (
                            f"常规分批预算偏小，但你的当前现金足够买入 1 手 {fallback_item['name']}，"
                            "系统已切换为小资金可执行方案。"
                        ),
                        "reason_code": "buy_candidates_one_lot_override",
                        "facts": facts,
                    }
                return {
                    "action": "不操作",
                    "target_position_pct": target_position_pct,
                    "items": [],
                    "watchlist_items": self._build_watchlist_items(
                        scored_df=scored_df,
                        min_advice_amount=min_advice_amount,
                        available_cash=available_cash,
                        limit=self.settings.top_n_default,
                    ),
                    "summary": "本次建议金额过小，未达到系统最小建议金额，系统建议先不操作。",
                    "reason_code": "amount_below_min_advice",
                    "facts": facts,
                }

            annotated_items = [
                self._annotate_buy_candidate(row, min_advice_amount, available_cash)
                for _, row in selected.iterrows()
            ]

            executable_items = [item for item in annotated_items if item["is_executable"]]
            watchlist_items = [item for item in annotated_items if not item["is_executable"]]
            if not self.settings.show_watchlist_recommendations:
                watchlist_items = []

            facts.update(
                {
                    "executable_count": len(executable_items),
                    "watchlist_count": len(watchlist_items),
                }
            )

            if not executable_items and watchlist_items:
                fallback_item = self._build_one_lot_fallback_item(
                    scored_df=scored_df,
                    min_advice_amount=min_advice_amount,
                    available_cash=available_cash,
                    deploy_amount=facts["deploy_amount"],
                    total_asset=total_asset,
                )
                if fallback_item is not None:
                    watchlist_items = self._build_watchlist_items(
                        scored_df=scored_df,
                        min_advice_amount=min_advice_amount,
                        available_cash=available_cash,
                        exclude_symbols={fallback_item["symbol"]},
                        limit=self.settings.top_n_default,
                    )
                    facts.update(
                        {
                            "executable_count": 1,
                            "watchlist_count": len(watchlist_items),
                            "small_account_override": True,
                            "fallback_symbol": fallback_item["symbol"],
                            "fallback_min_order_amount": fallback_item["min_order_amount"],
                        }
                    )
                    return {
                        "action": "买入",
                        "target_position_pct": target_position_pct,
                        "items": [fallback_item],
                        "watchlist_items": watchlist_items,
                        "summary": (
                            f"常规分批预算还不够覆盖 1 手，但你的当前现金足够买入 1 手 {fallback_item['name']}，"
                            "系统已切换为小资金可执行方案。"
                        ),
                        "reason_code": "buy_candidates_one_lot_override",
                        "facts": facts,
                    }
                return {
                    "action": "不操作",
                    "target_position_pct": target_position_pct,
                    "items": [],
                    "watchlist_items": watchlist_items,
                    "summary": "候选 ETF 里有高分标的，但按你当前预算还买不起 1 手，先放入关注标的，不建议立即执行。",
                    "reason_code": "watchlist_only_budget_limited",
                    "facts": facts,
                }

            return {
                "action": action,
                "target_position_pct": target_position_pct,
                "items": executable_items,
                "watchlist_items": watchlist_items,
                "summary": (
                    f"当前市场偏{market_regime}，建议优先买入 {len(executable_items)} 只当前买得起的 ETF，目标总仓位约 {target_position_pct:.0%}。"
                    if not watchlist_items
                    else f"当前市场偏{market_regime}，建议优先买入 {len(executable_items)} 只当前买得起的 ETF；另有 {len(watchlist_items)} 只高分 ETF 因预算不足转入关注标的。"
                ),
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
            if item_amount < min_advice_amount:
                continue
            payload = row.to_dict()
            payload["suggested_amount"] = round_money(item_amount)
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
                "watchlist_items": [],
                "summary": "虽然仓位偏高，但这次可减仓金额低于最小建议金额，系统建议先不操作。",
                "reason_code": "trim_amount_below_min_advice",
                "facts": facts,
            }

        return {
            "action": "卖出",
            "target_position_pct": target_position_pct,
            "items": sell_items,
            "watchlist_items": [],
            "summary": f"当前仓位偏高，市场偏{market_regime}，建议先减一部分仓位，把总仓位向 {target_position_pct:.0%} 靠拢。",
            "reason_code": "trim_positions",
            "facts": facts,
        }
