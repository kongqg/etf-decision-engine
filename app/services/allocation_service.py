from __future__ import annotations

from typing import Any

import pandas as pd

from app.core.config import get_settings, load_yaml_config
from app.utils.maths import round_money


ASSET_CLASS_ORDER = ["股票", "债券", "黄金", "货币", "跨境"]

CATEGORY_TO_ASSET_CLASS = {
    "宽基": "股票",
    "行业": "股票",
    "债券": "债券",
    "黄金": "黄金",
    "货币": "货币",
    "跨境": "跨境",
}

TRADE_MODE_BY_ASSET_CLASS = {
    "股票": "T+1",
    "债券": "T+0",
    "黄金": "T+0",
    "货币": "T+0",
    "跨境": "T+0",
}


class AllocationService:
    def __init__(self) -> None:
        settings = get_settings()
        self.settings = settings
        self.rules = load_yaml_config(settings.config_dir / "risk_rules.yaml")
        self.asset_class_weights = self.rules.get("asset_class_allocation_by_regime", {})
        self.asset_class_thresholds = self.rules.get("asset_class_signal_threshold_by_class", {})
        self.asset_class_positive_ratio = self.rules.get("asset_class_signal_min_positive_ratio", {})
        raw_trend_filter = self.rules.get("absolute_trend_filter", {})
        if "default" in raw_trend_filter or "by_regime" in raw_trend_filter:
            self.absolute_trend_filter_default = raw_trend_filter.get("default", {})
            self.absolute_trend_filter_by_regime = raw_trend_filter.get("by_regime", {})
        else:
            self.absolute_trend_filter_default = raw_trend_filter
            self.absolute_trend_filter_by_regime = {}

    def _target_position_pct(self, preferences: Any, market_regime: str) -> float:
        base = float(self.rules["position_by_regime"].get(market_regime, 0.05))
        adjust = float(self.rules["risk_adjustment_by_preference"].get(preferences.risk_level, 0.0))
        value = max(0.0, min(base + adjust, float(preferences.max_total_position_pct)))
        reserve_cap = max(0.0, 1.0 - float(preferences.cash_reserve_pct))
        return round(min(value, reserve_cap), 4)

    def _strategy_min_amount(self, preferences: Any) -> float:
        return float(getattr(preferences, "min_trade_amount", self.settings.default_min_advice_amount))

    def _ensure_candidate_columns(self, scored_df: pd.DataFrame) -> pd.DataFrame:
        if scored_df.empty:
            return scored_df.copy()

        df = scored_df.copy()
        defaults: dict[str, Any] = {
            "name": "",
            "category": "",
            "asset_class": None,
            "trade_mode": None,
            "risk_level": "中",
            "close_price": 0.0,
            "total_score": 0.0,
            "rank_in_pool": 1,
            "rank_in_asset_class": 1,
            "momentum_3d": 0.0,
            "momentum_5d": 0.0,
            "momentum_10d": 0.0,
            "trend_strength": 0.0,
            "ma_gap_5": 0.0,
            "ma_gap_10": 0.0,
            "volatility_10d": 0.0,
            "drawdown_20d": 0.0,
            "avg_amount_20d": 0.0,
            "lot_size": self.settings.default_lot_size,
            "fee_rate": self.settings.default_fee_rate,
            "min_fee": self.settings.default_min_fee,
            "asset_allocation_weight": 0.0,
            "asset_class_signal_score": 0.0,
            "asset_class_base_weight": 0.0,
        }
        for column, default in defaults.items():
            if column not in df.columns:
                df[column] = default

        numeric_columns = [
            "close_price",
            "total_score",
            "rank_in_pool",
            "rank_in_asset_class",
            "momentum_3d",
            "momentum_5d",
            "momentum_10d",
            "trend_strength",
            "ma_gap_5",
            "ma_gap_10",
            "volatility_10d",
            "drawdown_20d",
            "avg_amount_20d",
            "lot_size",
            "fee_rate",
            "min_fee",
            "asset_allocation_weight",
            "asset_class_signal_score",
            "asset_class_base_weight",
        ]
        for column in numeric_columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(defaults[column])

        df["category"] = df["category"].fillna("")
        asset_class_series = df["asset_class"].astype("object").replace("", pd.NA)
        mapped_asset_class = df["category"].map(CATEGORY_TO_ASSET_CLASS)
        df["asset_class"] = asset_class_series.combine_first(mapped_asset_class).fillna("股票")
        trade_mode_series = df["trade_mode"].astype("object").replace("", pd.NA)
        mapped_trade_mode = df["asset_class"].map(TRADE_MODE_BY_ASSET_CLASS)
        df["trade_mode"] = trade_mode_series.combine_first(mapped_trade_mode).fillna("T+1")
        if "rank_in_asset_class" not in scored_df.columns:
            df["rank_in_asset_class"] = (
                df.groupby("asset_class")["total_score"].rank(method="first", ascending=False).astype(int)
            )
        else:
            df["rank_in_asset_class"] = df["rank_in_asset_class"].astype(int)
        df["rank_in_pool"] = df["rank_in_pool"].astype(int)
        return df

    def _lot_size_for_row(self, row: pd.Series) -> float:
        lot_size = float(row.get("lot_size", self.settings.default_lot_size))
        return lot_size if lot_size > 0 else float(self.settings.default_lot_size)

    def _fee_rate_for_row(self, row: pd.Series) -> float:
        fee_rate = float(row.get("fee_rate", self.settings.default_fee_rate))
        return fee_rate if fee_rate >= 0 else float(self.settings.default_fee_rate)

    def _min_fee_for_row(self, row: pd.Series) -> float:
        min_fee = float(row.get("min_fee", self.settings.default_min_fee))
        return min_fee if min_fee >= 0 else float(self.settings.default_min_fee)

    def _trade_mode_for_row(self, row: pd.Series) -> str:
        trade_mode = str(row.get("trade_mode") or "").strip()
        if trade_mode:
            return trade_mode
        return TRADE_MODE_BY_ASSET_CLASS.get(self._asset_class_for_row(row), "T+1")

    def _asset_class_for_row(self, row: pd.Series) -> str:
        asset_class = str(row.get("asset_class") or "").strip()
        if asset_class:
            return asset_class
        category = str(row.get("category") or "").strip()
        return CATEGORY_TO_ASSET_CLASS.get(category, "股票")

    def _estimate_fee(self, amount: float, fee_rate: float, min_fee: float) -> tuple[float, float]:
        amount = float(amount)
        if amount <= 0:
            return 0.0, 0.0
        estimated_fee = max(amount * fee_rate, min_fee)
        return round_money(estimated_fee), float(estimated_fee / amount)

    def _trade_mode_note(self, asset_class: str, trade_mode: str) -> str:
        if trade_mode == "T+1":
            return "这只股票 ETF 按 T+1 节奏看待，更适合做趋势持有，不适合来回短打。"
        if asset_class == "跨境":
            return "这只跨境 ETF 按 T+0 节奏处理，但还要额外关注外盘和汇率波动。"
        return "这只 ETF 按 T+0 节奏处理，更适合分批跟踪，但也不建议频繁情绪化交易。"

    def _practical_buy_cap_amount(
        self,
        total_asset: float,
        available_cash: float,
        current_position_pct: float,
        target_position_pct: float,
        preferences: Any,
    ) -> float:
        remaining_target_amount = max(float(target_position_pct - current_position_pct), 0.0) * float(total_asset)
        single_cap_amount = float(total_asset) * float(preferences.max_single_position_pct)
        return round_money(min(float(available_cash), remaining_target_amount, single_cap_amount))

    def _candidate_order_metrics(self, scored_df: pd.DataFrame) -> pd.DataFrame:
        if scored_df.empty:
            return scored_df.copy()
        enriched = self._ensure_candidate_columns(scored_df)
        enriched["lot_size"] = enriched.apply(self._lot_size_for_row, axis=1)
        enriched["fee_rate"] = enriched.apply(self._fee_rate_for_row, axis=1)
        enriched["min_fee"] = enriched.apply(self._min_fee_for_row, axis=1)
        enriched["asset_class"] = enriched.apply(self._asset_class_for_row, axis=1)
        enriched["trade_mode"] = enriched.apply(self._trade_mode_for_row, axis=1)
        enriched["min_order_amount"] = (enriched["close_price"] * enriched["lot_size"]).map(round_money)
        return enriched

    def _trend_filter_for(self, asset_class: str, market_regime: str) -> dict[str, float]:
        base = {
            "min_momentum_5d": float(self.absolute_trend_filter_default.get("min_momentum_5d", 0.0)),
            "min_momentum_10d": float(self.absolute_trend_filter_default.get("min_momentum_10d", 0.0)),
            "min_ma_gap_10": float(self.absolute_trend_filter_default.get("min_ma_gap_10", 0.0)),
            "min_trend_strength": float(self.absolute_trend_filter_default.get("min_trend_strength", 0.0)),
        }
        regime_rules = self.absolute_trend_filter_by_regime.get(market_regime, {})
        asset_rules = regime_rules.get(asset_class, {})
        return {
            "min_momentum_5d": float(asset_rules.get("min_momentum_5d", base["min_momentum_5d"])),
            "min_momentum_10d": float(asset_rules.get("min_momentum_10d", base["min_momentum_10d"])),
            "min_ma_gap_10": float(asset_rules.get("min_ma_gap_10", base["min_ma_gap_10"])),
            "min_trend_strength": float(asset_rules.get("min_trend_strength", base["min_trend_strength"])),
        }

    def _absolute_trend_mask(self, scored_df: pd.DataFrame, market_regime: str) -> pd.Series:
        if scored_df.empty:
            return pd.Series(dtype=bool)
        mask: list[bool] = []
        for _, row in scored_df.iterrows():
            asset_class = self._asset_class_for_row(row)
            thresholds = self._trend_filter_for(asset_class, market_regime)
            mask.append(
                bool(
                    float(row["momentum_5d"]) >= thresholds["min_momentum_5d"]
                    and float(row["momentum_10d"]) >= thresholds["min_momentum_10d"]
                    and float(row["ma_gap_10"]) >= thresholds["min_ma_gap_10"]
                    and float(row["trend_strength"]) >= thresholds["min_trend_strength"]
                )
            )
        return pd.Series(mask, index=scored_df.index, dtype=bool)

    def _asset_class_signal(self, class_df: pd.DataFrame, asset_class: str, market_regime: str) -> dict[str, float]:
        if class_df.empty:
            return {
                "signal_score": 0.0,
                "avg_momentum_5d": 0.0,
                "avg_momentum_10d": 0.0,
                "avg_trend_strength": 0.0,
                "positive_ratio": 0.0,
            }
        thresholds = self._trend_filter_for(asset_class, market_regime)
        avg_momentum_5d = float(class_df["momentum_5d"].mean())
        avg_momentum_10d = float(class_df["momentum_10d"].mean())
        avg_trend_strength = float(class_df["trend_strength"].mean())
        positive_ratio = float((class_df["ma_gap_10"] >= thresholds["min_ma_gap_10"]).mean())
        signal_score = min(
            max(
                50
                + avg_momentum_5d * 4
                + avg_momentum_10d * 2
                + avg_trend_strength * 6
                + positive_ratio * 12,
                0,
            ),
            100,
        )
        return {
            "signal_score": round(signal_score, 2),
            "avg_momentum_5d": round(avg_momentum_5d, 2),
            "avg_momentum_10d": round(avg_momentum_10d, 2),
            "avg_trend_strength": round(avg_trend_strength, 2),
            "positive_ratio": positive_ratio,
        }

    def _sorted_candidates(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return frame.copy()
        sort_columns = []
        ascending = []
        for column, is_ascending in [
            ("asset_allocation_weight", False),
            ("asset_class_signal_score", False),
            ("total_score", False),
            ("momentum_5d", False),
            ("rank_in_asset_class", True),
        ]:
            if column in frame.columns:
                sort_columns.append(column)
                ascending.append(is_ascending)
        if not sort_columns:
            return frame.reset_index(drop=True)
        return frame.sort_values(sort_columns, ascending=ascending).reset_index(drop=True)

    def _build_asset_class_plan(
        self,
        scored_df: pd.DataFrame,
        market_regime: str,
        min_score_to_buy: float,
    ) -> tuple[pd.DataFrame, list[dict[str, Any]], pd.DataFrame]:
        candidates = self._candidate_order_metrics(scored_df)
        if candidates.empty:
            return candidates, [], candidates.copy()

        candidates["absolute_trend_pass"] = self._absolute_trend_mask(candidates, market_regime)
        base_weights = {
            key: float(value)
            for key, value in self.asset_class_weights.get(market_regime, {}).items()
        }

        signal_weights: dict[str, float] = {}
        signal_lookup: dict[str, float] = {}
        plan_rows: list[dict[str, Any]] = []
        selected_rows: list[dict[str, Any]] = []

        asset_classes = list(dict.fromkeys([*ASSET_CLASS_ORDER, *base_weights.keys(), *candidates["asset_class"].tolist()]))
        for asset_class in asset_classes:
            class_df = candidates[candidates["asset_class"] == asset_class].copy()
            if class_df.empty and asset_class not in base_weights:
                continue

            metrics = self._asset_class_signal(class_df, asset_class, market_regime)
            threshold = float(self.asset_class_thresholds.get(asset_class, self.settings.min_score_to_buy))
            min_positive_ratio = float(self.asset_class_positive_ratio.get(asset_class, 0.5))
            base_weight = float(base_weights.get(asset_class, 0.0))
            trend_df = class_df[class_df["absolute_trend_pass"]].copy()
            top_row = self._sorted_candidates(class_df).head(1)
            top_symbol = str(top_row.iloc[0]["symbol"]) if not top_row.empty else ""
            top_name = str(top_row.iloc[0]["name"]) if not top_row.empty else ""
            top_score = float(top_row.iloc[0]["total_score"]) if not top_row.empty else 0.0

            active = True
            inactive_reason = ""
            if base_weight <= 0:
                active = False
                inactive_reason = "当前市场状态下，这类资产没有被分配到本轮建仓权重。"
            elif class_df.empty:
                active = False
                inactive_reason = "这一类当前没有 ETF 通过基础筛选。"
            elif trend_df.empty:
                active = False
                inactive_reason = "这一类 ETF 还没通过绝对趋势过滤。"
            elif metrics["signal_score"] < threshold:
                active = False
                inactive_reason = f"类趋势分 {metrics['signal_score']:.1f} 还没达到 {threshold:.1f}。"
            elif metrics["positive_ratio"] < min_positive_ratio:
                active = False
                inactive_reason = (
                    f"类内站上趋势线的比例只有 {metrics['positive_ratio'] * 100:.0f}%，"
                    f"还没达到 {min_positive_ratio * 100:.0f}%。"
                )
            elif float(trend_df["total_score"].max()) < min_score_to_buy:
                active = False
                inactive_reason = f"类内最强 ETF 分数仍低于出手阈值 {min_score_to_buy:.1f}。"

            chosen = self._sorted_candidates(trend_df).head(1)
            if active and not chosen.empty:
                signal_weight = base_weight * (0.7 + metrics["signal_score"] / 100 * 0.3)
                signal_weights[asset_class] = signal_weight
                signal_lookup[asset_class] = metrics["signal_score"]
                selected_rows.append(chosen.iloc[0].to_dict())
                top_symbol = str(chosen.iloc[0]["symbol"])
                top_name = str(chosen.iloc[0]["name"])
                top_score = float(chosen.iloc[0]["total_score"])

            plan_rows.append(
                {
                    "asset_class": asset_class,
                    "base_weight_pct": round(base_weight * 100, 2),
                    "signal_score": round(metrics["signal_score"], 2),
                    "avg_momentum_5d": round(metrics["avg_momentum_5d"], 2),
                    "avg_momentum_10d": round(metrics["avg_momentum_10d"], 2),
                    "avg_trend_strength": round(metrics["avg_trend_strength"], 2),
                    "trend_positive_ratio_pct": round(metrics["positive_ratio"] * 100, 2),
                    "trend_pass_count": int(len(trend_df)),
                    "candidate_count": int(len(class_df)),
                    "is_active": active,
                    "inactive_reason": inactive_reason,
                    "candidate_symbol": top_symbol,
                    "candidate_name": top_name,
                    "candidate_score": round(top_score, 2),
                    "allocation_weight_pct": 0.0,
                }
            )

        selected_df = pd.DataFrame(selected_rows)
        total_signal_weight = sum(signal_weights.values())
        if not selected_df.empty and total_signal_weight > 0:
            selected_df["asset_allocation_weight"] = selected_df["asset_class"].map(
                lambda asset_class: signal_weights.get(asset_class, 0.0) / total_signal_weight
            )
            selected_df["asset_class_signal_score"] = selected_df["asset_class"].map(signal_lookup)
            selected_df["asset_class_base_weight"] = selected_df["asset_class"].map(base_weights).fillna(0.0)

        allocation_lookup = {
            asset_class: (signal_weights.get(asset_class, 0.0) / total_signal_weight) * 100
            for asset_class in signal_weights
        } if total_signal_weight > 0 else {}
        for row in plan_rows:
            row["allocation_weight_pct"] = round(allocation_lookup.get(row["asset_class"], 0.0), 2)

        return candidates, plan_rows, self._sorted_candidates(selected_df)

    def _annotate_buy_candidate(self, row: pd.Series, min_advice_amount: float, available_cash: float) -> dict[str, Any]:
        payload = row.to_dict()
        raw_price = float(payload.get("close_price", 0.0))
        latest_price = round_money(raw_price)
        lot_size = self._lot_size_for_row(row)
        fee_rate = self._fee_rate_for_row(row)
        min_fee = self._min_fee_for_row(row)
        asset_class = self._asset_class_for_row(row)
        trade_mode = self._trade_mode_for_row(row)
        min_order_amount = round_money(max(raw_price * lot_size, 0.0))
        suggested_amount = round_money(float(payload.get("suggested_amount", 0.0)))
        estimated_fee, estimated_cost_rate = self._estimate_fee(suggested_amount, fee_rate, min_fee)
        budget_gap = round_money(max(min_order_amount - suggested_amount, 0.0))
        passes_min_advice = suggested_amount >= round_money(min_advice_amount)
        is_budget_executable = not self.settings.budget_filter_enabled or suggested_amount >= min_order_amount
        is_cost_efficient = (
            not self.settings.fee_filter_enabled
            or suggested_amount <= 0
            or estimated_cost_rate <= self.settings.max_fee_rate_for_execution
        )

        recommendation_bucket = "executable_recommendations"
        execution_status = "可执行买入"
        not_executable_reason = ""
        cost_reason = ""
        execution_note = (
            f"这只 {asset_class} ETF 当前建议金额约 {suggested_amount:.2f} 元，"
            f"已经覆盖 1 手门槛 {min_order_amount:.2f} 元，预计手续费约 {estimated_fee:.2f} 元，可执行。"
        )
        if not is_budget_executable:
            recommendation_bucket = "watchlist_recommendations"
            execution_status = "关注标的"
            not_executable_reason = (
                f"当前建议金额 {suggested_amount:.2f} 元，还不够买入 1 手（约 {min_order_amount:.2f} 元）。"
            )
            execution_note = (
                f"这只 {asset_class} ETF 趋势和分数都不差，但当前分配到它的金额只有 {suggested_amount:.2f} 元，"
                f"还买不起 1 手，先放入关注标的。"
            )
        elif not is_cost_efficient:
            recommendation_bucket = "cost_inefficient_recommendations"
            execution_status = "手续费偏高"
            cost_reason = (
                f"预计手续费约 {estimated_fee:.2f} 元，占这笔建议金额的 {estimated_cost_rate * 100:.2f}%，"
                "当前执行不划算。"
            )
            execution_note = (
                f"这只 {asset_class} ETF 的预算和 1 手门槛都没问题，但按当前建议金额计算，"
                f"手续费占比约 {estimated_cost_rate * 100:.2f}%，暂不列入主推荐。"
            )
        elif not passes_min_advice:
            recommendation_bucket = "watchlist_recommendations"
            execution_status = "关注标的"
            not_executable_reason = (
                f"系统这次只给到 {suggested_amount:.2f} 元，低于最小建议金额 {min_advice_amount:.2f} 元。"
            )
            execution_note = (
                f"这只 {asset_class} ETF 可以继续观察，但当前建议金额还没达到系统的最小建议门槛。"
            )

        payload.update(
            {
                "asset_class": asset_class,
                "trade_mode": trade_mode,
                "trade_mode_note": self._trade_mode_note(asset_class, trade_mode),
                "latest_price": latest_price,
                "lot_size": lot_size,
                "fee_rate": fee_rate,
                "min_fee": min_fee,
                "estimated_fee": estimated_fee,
                "estimated_cost_rate": round(estimated_cost_rate, 6),
                "is_cost_efficient": is_cost_efficient,
                "cost_reason": cost_reason,
                "passes_min_advice": passes_min_advice,
                "min_advice_amount": round_money(min_advice_amount),
                "min_order_amount": min_order_amount,
                "available_cash": round_money(available_cash),
                "budget_gap_to_min_order": budget_gap,
                "is_budget_executable": is_budget_executable,
                "is_executable": recommendation_bucket == "executable_recommendations",
                "recommendation_bucket": recommendation_bucket,
                "execution_status": execution_status,
                "not_executable_reason": not_executable_reason,
                "execution_note": execution_note,
                "asset_allocation_weight": round(float(payload.get("asset_allocation_weight", 0.0)), 6),
                "asset_class_signal_score": round(float(payload.get("asset_class_signal_score", 0.0)), 2),
            }
        )
        return payload

    def _bucket_items(
        self,
        candidates_df: pd.DataFrame,
        min_advice_amount: float,
        available_cash: float,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        if candidates_df.empty:
            return [], [], []

        executable_items: list[dict[str, Any]] = []
        watchlist_items: list[dict[str, Any]] = []
        cost_inefficient_items: list[dict[str, Any]] = []
        for _, row in self._sorted_candidates(candidates_df).iterrows():
            payload = self._annotate_buy_candidate(row, min_advice_amount, available_cash)
            bucket = payload["recommendation_bucket"]
            if bucket == "executable_recommendations":
                executable_items.append(payload)
            elif bucket == "watchlist_recommendations":
                watchlist_items.append(payload)
            elif bucket == "cost_inefficient_recommendations":
                cost_inefficient_items.append(payload)
        return executable_items, watchlist_items, cost_inefficient_items

    def _build_one_lot_fallback_item(
        self,
        candidates_df: pd.DataFrame,
        min_advice_amount: float,
        available_cash: float,
        deploy_amount: float,
        total_asset: float,
        practical_buy_cap_amount: float,
    ) -> dict[str, Any] | None:
        ranked = self._sorted_candidates(self._candidate_order_metrics(candidates_df))
        if ranked.empty:
            return None

        affordable = ranked[
            (ranked["min_order_amount"] <= float(available_cash))
            & (ranked["min_order_amount"] <= float(practical_buy_cap_amount))
        ].copy()
        if affordable.empty:
            return None

        for _, row in affordable.iterrows():
            payload = row.to_dict()
            payload["suggested_amount"] = round_money(float(payload["min_order_amount"]))
            payload["suggested_pct"] = round(payload["suggested_amount"] / total_asset, 4) if total_asset else 0.0
            annotated = self._annotate_buy_candidate(pd.Series(payload), min_advice_amount, available_cash)
            if annotated["recommendation_bucket"] != "executable_recommendations":
                continue
            annotated["small_account_override"] = True
            annotated["execution_note"] = (
                f"按常规分批节奏，这次计划投入约 {deploy_amount:.2f} 元，直接拆开后不容易形成一笔可执行交易。"
                f"但你当前现金足够买入 1 手 {annotated['name']}，而且手续费占比仍在可接受范围内，"
                "所以系统优先给出这只当前买得起的高分 ETF。"
            )
            return annotated
        return None

    def _build_budget_substitute_items(
        self,
        candidates_df: pd.DataFrame,
        min_advice_amount: float,
        available_cash: float,
        deploy_amount: float,
        total_asset: float,
        practical_buy_cap_amount: float,
        primary_asset_classes: list[str],
    ) -> list[dict[str, Any]]:
        ranked = self._candidate_order_metrics(candidates_df)
        if ranked.empty:
            return []

        affordable = ranked[
            (ranked["min_order_amount"] <= float(available_cash))
            & (ranked["min_order_amount"] <= float(practical_buy_cap_amount))
        ].copy()
        if affordable.empty:
            return []
        affordable = affordable[affordable["total_score"] >= float(self.settings.min_score_to_buy)].copy()
        if affordable.empty:
            return []

        affordable["substitute_asset_match"] = affordable["asset_class"].isin(primary_asset_classes).astype(int)
        if "absolute_trend_pass" not in affordable.columns:
            affordable["absolute_trend_pass"] = False
        affordable["absolute_trend_pass"] = affordable["absolute_trend_pass"].astype(int)
        affordable = affordable.sort_values(
            ["substitute_asset_match", "absolute_trend_pass", "total_score", "momentum_5d"],
            ascending=[False, False, False, False],
        ).reset_index(drop=True)

        primary_label = "、".join(primary_asset_classes) if primary_asset_classes else "主配置资产"
        substitute_items: list[dict[str, Any]] = []
        for _, row in affordable.iterrows():
            payload = row.to_dict()
            payload["suggested_amount"] = round_money(float(payload["min_order_amount"]))
            payload["suggested_pct"] = round(payload["suggested_amount"] / total_asset, 4) if total_asset else 0.0
            annotated = self._annotate_buy_candidate(pd.Series(payload), min_advice_amount, available_cash)
            if annotated["recommendation_bucket"] != "executable_recommendations":
                continue
            annotated["is_budget_substitute"] = True
            annotated["primary_asset_class"] = primary_label
            annotated["execution_status"] = "预算内替代执行"
            annotated["execution_note"] = (
                f"按当前策略，主配置优先是 {primary_label}，但这类 ETF 现在都买不起 1 手。"
                f"如果你希望在当前预算内也能执行一笔，系统补充给出 {annotated['name']} 作为次优替代标的。"
            )
            annotated["trade_mode_note"] = (
                f"{annotated['trade_mode_note']} 这不是当前主配置首选，所以金额仍应控制在轻仓范围内。"
            )
            annotated["budget_substitute_reason"] = (
                f"常规配置优先看 {primary_label}，但按当前现金 {available_cash:.2f} 元和本次计划投入 {deploy_amount:.2f} 元，"
                "主配置标的无法覆盖 1 手门槛，所以提供预算内替代执行。"
            )
            substitute_items.append(annotated)
            if len(substitute_items) >= self.settings.budget_substitute_top_n:
                break
        return substitute_items

    def _mark_best_unaffordable_item(
        self,
        watchlist_items: list[dict[str, Any]],
        active_asset_classes: list[str],
    ) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
        if not watchlist_items:
            return watchlist_items, None

        for item in watchlist_items:
            item["is_best_unaffordable"] = False
            item["best_unaffordable_reason"] = ""

        ranked_items = sorted(
            watchlist_items,
            key=lambda item: (
                int(str(item.get("asset_class", "")) in active_asset_classes),
                float(item.get("asset_allocation_weight", 0.0)),
                float(item.get("asset_class_signal_score", 0.0)),
                float(item.get("total_score", 0.0)),
                float(item.get("momentum_5d", 0.0)),
            ),
            reverse=True,
        )
        best_item = ranked_items[0]
        best_item["is_best_unaffordable"] = True
        best_item["best_unaffordable_reason"] = (
            f"它仍是当前主配置里相对更优先的一只 {best_item.get('asset_class', 'ETF')} ETF，"
            f"但当前建议金额 {best_item.get('suggested_amount', 0.0):.2f} 元还不够覆盖"
            f" 1 手门槛 {best_item.get('min_order_amount', 0.0):.2f} 元。"
        )
        return watchlist_items, best_item

    def _build_affordable_but_weak_items(
        self,
        candidates_df: pd.DataFrame,
        available_cash: float,
        practical_buy_cap_amount: float,
        total_asset: float,
        min_advice_amount: float,
        min_score_to_buy: float,
        active_asset_classes: list[str],
        asset_class_plan: list[dict[str, Any]],
        excluded_symbols: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        ranked = self._candidate_order_metrics(candidates_df)
        if ranked.empty:
            return []

        excluded_symbols = excluded_symbols or set()
        asset_class_reason_lookup = {
            str(row.get("asset_class", "")): str(row.get("inactive_reason", "") or "")
            for row in asset_class_plan
        }
        affordable = ranked[
            (ranked["min_order_amount"] <= float(available_cash))
            & (ranked["min_order_amount"] <= float(practical_buy_cap_amount))
        ].copy()
        if affordable.empty:
            return []
        affordable = affordable[~affordable["symbol"].isin(excluded_symbols)].copy()
        if affordable.empty:
            return []

        affordable = affordable.sort_values(["total_score", "momentum_5d", "min_order_amount"], ascending=[False, False, True]).reset_index(drop=True)
        items: list[dict[str, Any]] = []
        for _, row in affordable.iterrows():
            score_pass = float(row.get("total_score", 0.0)) >= float(min_score_to_buy)
            trend_pass = bool(row.get("absolute_trend_pass", False))
            asset_class = str(row.get("asset_class", "股票"))
            asset_active = asset_class in active_asset_classes
            if score_pass and trend_pass and asset_active:
                continue

            payload = row.to_dict()
            payload["suggested_amount"] = round_money(float(payload["min_order_amount"]))
            payload["suggested_pct"] = round(payload["suggested_amount"] / total_asset, 4) if total_asset else 0.0
            annotated = self._annotate_buy_candidate(pd.Series(payload), min_advice_amount, available_cash)

            weak_reasons = []
            if not score_pass:
                weak_reasons.append(
                    f"综合分 {float(payload.get('total_score', 0.0)):.1f} 还没达到出手阈值 {float(min_score_to_buy):.1f}"
                )
            if not trend_pass:
                weak_reasons.append("当前绝对趋势过滤还没通过")
            if not asset_active:
                asset_class_reason = str(asset_class_reason_lookup.get(asset_class) or "当前这类资产不是本轮主配置").strip()
                asset_class_reason = asset_class_reason.rstrip("。")
                if not (not trend_pass and "绝对趋势过滤" in asset_class_reason):
                    weak_reasons.append(asset_class_reason)
            if not weak_reasons:
                weak_reasons.append("当前还不属于值得执行的主推荐")

            weak_reason = "；".join(dict.fromkeys(weak_reasons)) + "。"
            annotated.update(
                {
                    "is_executable": False,
                    "is_affordable_but_weak": True,
                    "weak_signal_reason": weak_reason,
                    "recommendation_bucket": "affordable_but_weak_recommendations",
                    "execution_status": "买得起但当前不建议买",
                    "not_executable_reason": weak_reason,
                    "execution_note": (
                        f"这只 {asset_class} ETF 当前在仓位内买得起 1 手，"
                        f"但{weak_reason[:-1]}，所以系统不建议为了凑交易硬买。"
                    ),
                }
            )
            items.append(annotated)
        return items

    def plan(
        self,
        scored_df: pd.DataFrame,
        positions_df: pd.DataFrame,
        total_asset: float,
        available_cash: float,
        current_position_pct: float,
        preferences: Any,
        market_regime: str,
    ) -> dict[str, Any]:
        scored_df = self._ensure_candidate_columns(scored_df)
        target_position_pct = self._target_position_pct(preferences, market_regime)
        min_score_to_buy = self.settings.min_score_to_buy
        min_advice_amount = self._strategy_min_amount(preferences)
        candidate_count = int(len(scored_df))

        candidates_with_metrics, asset_class_plan, asset_candidates_df = self._build_asset_class_plan(
            scored_df=scored_df,
            market_regime=market_regime,
            min_score_to_buy=min_score_to_buy,
        )
        active_asset_classes = [row["asset_class"] for row in asset_class_plan if row["is_active"]]

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
            "fee_filter_enabled": self.settings.fee_filter_enabled,
            "default_fee_rate": self.settings.default_fee_rate,
            "default_min_fee": self.settings.default_min_fee,
            "max_fee_rate_for_execution": self.settings.max_fee_rate_for_execution,
            "show_watchlist_recommendations": self.settings.show_watchlist_recommendations,
            "show_cost_inefficient_recommendations": self.settings.show_cost_inefficient_recommendations,
            "budget_substitute_enabled": self.settings.budget_substitute_enabled,
            "asset_class_plan": asset_class_plan,
            "active_asset_classes": active_asset_classes,
            "active_asset_class_count": len(active_asset_classes),
            "trend_filter_rule": (
                "绝对趋势过滤会按资产类别和市场状态区分。股票/跨境默认要求短期趋势为正；"
                "防守或观望阶段下，债券、黄金、货币允许轻微走弱，但不能明显转差。"
            ),
        }

        if candidates_with_metrics.empty:
            return {
                "action": "不操作",
                "target_position_pct": target_position_pct,
                "items": [],
                "watchlist_items": [],
                "cost_inefficient_items": [],
                "summary": "今天没有 ETF 通过筛选，系统建议先观望。",
                "reason_code": "no_candidates",
                "facts": base_facts,
            }

        top_score = float(candidates_with_metrics.iloc[0]["total_score"])
        base_facts["top_score"] = round(top_score, 2)
        base_facts["top_symbol"] = str(candidates_with_metrics.iloc[0]["symbol"])
        base_facts["top_name"] = str(candidates_with_metrics.iloc[0]["name"])

        if market_regime == "观望":
            return {
                "action": "不操作",
                "target_position_pct": target_position_pct,
                "items": [],
                "watchlist_items": [],
                "cost_inefficient_items": [],
                "summary": "当前市场偏弱，系统建议先不出手。",
                "reason_code": "weak_market",
                "facts": base_facts,
            }

        if top_score < min_score_to_buy:
            return {
                "action": "不操作",
                "target_position_pct": target_position_pct,
                "items": [],
                "watchlist_items": [],
                "cost_inefficient_items": [],
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
                "watchlist_items": [],
                "cost_inefficient_items": [],
                "summary": "当前仓位已经接近目标仓位，系统建议暂时不动。",
                "reason_code": "near_target_position",
                "facts": base_facts,
            }

        if action == "买入":
            desired_increment = max(target_position_pct - current_position_pct, 0.0)
            deploy_pct = desired_increment * self.settings.initial_build_ratio
            deploy_amount = min(total_asset * deploy_pct, float(available_cash))

            facts = {
                **base_facts,
                "desired_increment_pct": round(desired_increment, 4),
                "deploy_pct": round(deploy_pct, 4),
                "deploy_amount": round_money(deploy_amount),
            }
            practical_buy_cap_amount = self._practical_buy_cap_amount(
                total_asset=total_asset,
                available_cash=available_cash,
                current_position_pct=current_position_pct,
                target_position_pct=target_position_pct,
                preferences=preferences,
            )
            affordable_one_lot_df = candidates_with_metrics[
                candidates_with_metrics["min_order_amount"] <= float(available_cash)
            ].copy()
            affordable_but_over_cap_df = affordable_one_lot_df[
                affordable_one_lot_df["min_order_amount"] > float(practical_buy_cap_amount)
            ].copy()
            affordable_but_over_cap_df = self._sorted_candidates(affordable_but_over_cap_df)
            blocked_override_symbol = ""
            blocked_override_name = ""
            blocked_override_min_order_amount = 0.0
            if not affordable_but_over_cap_df.empty:
                blocked_override_symbol = str(affordable_but_over_cap_df.iloc[0]["symbol"])
                blocked_override_name = str(affordable_but_over_cap_df.iloc[0]["name"])
                blocked_override_min_order_amount = round_money(
                    float(affordable_but_over_cap_df.iloc[0]["min_order_amount"])
                )
            facts.update(
                {
                    "practical_buy_cap_amount": practical_buy_cap_amount,
                    "affordable_one_lot_count": int(len(affordable_one_lot_df)),
                    "affordable_but_over_cap_count": int(len(affordable_but_over_cap_df)),
                    "blocked_override_symbol": blocked_override_symbol,
                    "blocked_override_name": blocked_override_name,
                    "blocked_override_min_order_amount": blocked_override_min_order_amount,
                }
            )
            affordable_but_weak_items = self._build_affordable_but_weak_items(
                candidates_df=candidates_with_metrics,
                available_cash=available_cash,
                practical_buy_cap_amount=practical_buy_cap_amount,
                total_asset=total_asset,
                min_advice_amount=min_advice_amount,
                min_score_to_buy=min_score_to_buy,
                active_asset_classes=active_asset_classes,
                asset_class_plan=asset_class_plan,
            )
            facts["affordable_but_weak_count"] = len(affordable_but_weak_items)

            if asset_candidates_df.empty:
                return {
                    "action": "不操作",
                    "target_position_pct": target_position_pct,
                    "items": [],
                    "affordable_but_weak_items": affordable_but_weak_items[: self.settings.top_n_default],
                    "watchlist_items": [],
                    "cost_inefficient_items": [],
                    "summary": "当前虽然不是观望状态，但还没有资产类别同时满足配置权重和趋势过滤，先不急着出手。",
                    "reason_code": "no_active_asset_classes",
                    "facts": facts,
                }

            planned_df = asset_candidates_df.copy()
            planned_df["suggested_pct"] = (planned_df["asset_allocation_weight"] * deploy_pct).clip(
                upper=float(preferences.max_single_position_pct)
            )
            planned_df["suggested_amount"] = planned_df["suggested_pct"] * total_asset

            total_suggested_amount = float(planned_df["suggested_amount"].sum())
            effective_budget = min(float(available_cash), total_suggested_amount) if total_suggested_amount > 0 else 0.0
            if total_suggested_amount > 0 and effective_budget < total_suggested_amount:
                scale = effective_budget / total_suggested_amount
                planned_df["suggested_amount"] = planned_df["suggested_amount"] * scale
                planned_df["suggested_pct"] = planned_df["suggested_pct"] * scale

            planned_df["suggested_amount"] = planned_df["suggested_amount"].map(round_money)
            planned_df["suggested_pct"] = planned_df["suggested_pct"].round(4)

            layered_source_df = planned_df[planned_df["suggested_amount"] >= min_advice_amount].copy()
            executable_items, watchlist_items, cost_inefficient_items = self._bucket_items(
                layered_source_df,
                min_advice_amount=min_advice_amount,
                available_cash=available_cash,
            )
            watchlist_items, best_unaffordable_item = self._mark_best_unaffordable_item(
                watchlist_items,
                active_asset_classes=active_asset_classes,
            )
            facts["best_unaffordable_symbol"] = str(best_unaffordable_item["symbol"]) if best_unaffordable_item else ""

            if not executable_items:
                fallback_item = self._build_one_lot_fallback_item(
                    candidates_df=planned_df,
                    min_advice_amount=min_advice_amount,
                    available_cash=available_cash,
                    deploy_amount=facts["deploy_amount"],
                    total_asset=total_asset,
                    practical_buy_cap_amount=practical_buy_cap_amount,
                )
                if fallback_item is not None:
                    affordable_but_weak_items = [
                        item for item in affordable_but_weak_items if item["symbol"] != fallback_item["symbol"]
                    ]
                    executable_items = [fallback_item]
                    _, watchlist_items, extra_cost = self._bucket_items(
                        planned_df[planned_df["symbol"] != fallback_item["symbol"]].copy(),
                        min_advice_amount=min_advice_amount,
                        available_cash=available_cash,
                    )
                    watchlist_items, best_unaffordable_item = self._mark_best_unaffordable_item(
                        watchlist_items,
                        active_asset_classes=active_asset_classes,
                    )
                    cost_inefficient_items = cost_inefficient_items + extra_cost
                    facts.update(
                        {
                            "executable_count": 1,
                            "affordable_but_weak_count": len(affordable_but_weak_items[: self.settings.top_n_default]),
                            "watchlist_count": len(watchlist_items),
                            "cost_inefficient_count": len(cost_inefficient_items),
                            "small_account_override": True,
                            "fallback_symbol": fallback_item["symbol"],
                            "fallback_min_order_amount": fallback_item["min_order_amount"],
                            "best_unaffordable_symbol": str(best_unaffordable_item["symbol"]) if best_unaffordable_item else "",
                        }
                    )
                    return {
                        "action": "买入",
                        "target_position_pct": target_position_pct,
                        "items": executable_items,
                        "best_unaffordable_item": best_unaffordable_item,
                        "affordable_but_weak_items": affordable_but_weak_items[: self.settings.top_n_default],
                        "watchlist_items": watchlist_items[: self.settings.top_n_default],
                        "cost_inefficient_items": cost_inefficient_items[: self.settings.top_n_default],
                        "summary": (
                            f"常规分批预算还不够形成完整的类别配置，但你的当前现金足够先买入 1 手 {fallback_item['name']}，"
                            "系统已切换为小资金可执行方案。"
                        ),
                        "reason_code": "buy_candidates_one_lot_override",
                        "facts": facts,
                    }
                if not watchlist_items and not cost_inefficient_items:
                    _, watchlist_items, cost_inefficient_items = self._bucket_items(
                        planned_df,
                        min_advice_amount=min_advice_amount,
                        available_cash=available_cash,
                    )
                watchlist_items, best_unaffordable_item = self._mark_best_unaffordable_item(
                    watchlist_items,
                    active_asset_classes=active_asset_classes,
                )
                facts["best_unaffordable_symbol"] = str(best_unaffordable_item["symbol"]) if best_unaffordable_item else ""

                if self.settings.budget_substitute_enabled:
                    substitute_items = self._build_budget_substitute_items(
                        candidates_df=candidates_with_metrics,
                        min_advice_amount=min_advice_amount,
                        available_cash=available_cash,
                        deploy_amount=facts["deploy_amount"],
                        total_asset=total_asset,
                        practical_buy_cap_amount=practical_buy_cap_amount,
                        primary_asset_classes=active_asset_classes,
                    )
                    if substitute_items:
                        substitute_symbols = {item["symbol"] for item in substitute_items}
                        affordable_but_weak_items = [
                            item for item in affordable_but_weak_items if item["symbol"] not in substitute_symbols
                        ]
                        substitute_names = [item["name"] for item in substitute_items]
                        facts.update(
                            {
                                "executable_count": len(substitute_items),
                                "affordable_but_weak_count": len(affordable_but_weak_items[: self.settings.top_n_default]),
                                "watchlist_count": len(watchlist_items),
                                "cost_inefficient_count": len(cost_inefficient_items),
                                "budget_substitute_used": True,
                                "budget_substitute_count": len(substitute_items),
                                "budget_substitute_symbols": [item["symbol"] for item in substitute_items],
                                "budget_substitute_names": substitute_names,
                                "budget_substitute_primary_asset_class": substitute_items[0]["primary_asset_class"],
                            }
                        )
                        substitute_label = "、".join(substitute_names)
                        return {
                            "action": "买入",
                            "target_position_pct": target_position_pct,
                            "items": substitute_items,
                            "best_unaffordable_item": best_unaffordable_item,
                            "affordable_but_weak_items": affordable_but_weak_items[: self.settings.top_n_default],
                            "watchlist_items": watchlist_items[: self.settings.top_n_default],
                            "cost_inefficient_items": cost_inefficient_items[: self.settings.top_n_default],
                            "summary": (
                                f"当前主配置优先看 {substitute_items[0]['primary_asset_class']}，"
                                "但对应 ETF 按你现在的预算还买不起 1 手。"
                                f"系统补充给出 {len(substitute_items)} 只预算内可执行的次优标的：{substitute_label}。"
                            ),
                            "reason_code": "budget_substitute_buy_candidates",
                            "facts": facts,
                        }

            watchlist_items = watchlist_items[: self.settings.top_n_default] if self.settings.show_watchlist_recommendations else []
            cost_inefficient_items = (
                cost_inefficient_items[: self.settings.top_n_default]
                if self.settings.show_cost_inefficient_recommendations
                else []
            )

            facts.update(
                {
                    "executable_count": len(executable_items),
                    "affordable_but_weak_count": len(affordable_but_weak_items[: self.settings.top_n_default]),
                    "watchlist_count": len(watchlist_items),
                    "cost_inefficient_count": len(cost_inefficient_items),
                }
            )

            if executable_items:
                summary = (
                    f"当前市场偏{market_regime}，系统先配置 {len(active_asset_classes)} 个趋势占优的资产类别，"
                    f"并在类内各选出 {len(executable_items)} 只当前买得起、手续费也合理的 ETF。"
                )
                if watchlist_items:
                    summary += f" 另有 {len(watchlist_items)} 只高分 ETF 因预算不足转入关注标的。"
                if affordable_but_weak_items:
                    summary += (
                        f" 还有 {len(affordable_but_weak_items[: self.settings.top_n_default])} 只 ETF"
                        " 虽然仓位内买得起，但当前信号还不够强，系统没有把它们列入主推荐。"
                    )
                if cost_inefficient_items:
                    summary += f" 还有 {len(cost_inefficient_items)} 只因手续费占比偏高暂不执行。"
                return {
                    "action": action,
                    "target_position_pct": target_position_pct,
                    "items": executable_items,
                    "best_unaffordable_item": best_unaffordable_item,
                    "affordable_but_weak_items": affordable_but_weak_items,
                    "watchlist_items": watchlist_items,
                    "cost_inefficient_items": cost_inefficient_items,
                    "summary": summary,
                    "reason_code": "asset_class_buy_candidates",
                    "facts": facts,
                }

            if watchlist_items and cost_inefficient_items:
                return {
                    "action": "不操作",
                    "target_position_pct": target_position_pct,
                    "items": [],
                    "best_unaffordable_item": best_unaffordable_item,
                    "affordable_but_weak_items": affordable_but_weak_items[: self.settings.top_n_default],
                    "watchlist_items": watchlist_items,
                    "cost_inefficient_items": cost_inefficient_items,
                    "summary": "当前适合参与的资产类别已经筛出来了，但要么买不起 1 手，要么手续费占比过高，这次先不执行。",
                    "reason_code": "asset_class_constraints_only",
                    "facts": facts,
                }

            if watchlist_items:
                if facts.get("affordable_but_over_cap_count", 0):
                    watchlist_summary = (
                        "当前已经筛出值得配置的资产类别，但主配置标的按你现在的预算还买不起 1 手。"
                        "另外虽然有更便宜的 ETF 按现金买得起 1 手，但一买就会明显超过当前目标仓位或单笔仓位上限，所以这次仍先不执行。"
                    )
                else:
                    watchlist_summary = "当前已经筛出值得配置的资产类别，但按你现在的预算还买不起对应的 1 手，先放入关注标的。"
                if affordable_but_weak_items:
                    watchlist_summary += " 另外有一些更便宜的 ETF 虽然在当前仓位内买得起，但分数或趋势还不够强，系统会单独告诉你“买得起，但现在不建议买”。"
                return {
                    "action": "不操作",
                    "target_position_pct": target_position_pct,
                    "items": [],
                    "best_unaffordable_item": best_unaffordable_item,
                    "affordable_but_weak_items": affordable_but_weak_items[: self.settings.top_n_default],
                    "watchlist_items": watchlist_items,
                    "cost_inefficient_items": [],
                    "summary": watchlist_summary,
                    "reason_code": (
                        "watchlist_only_budget_limited"
                        if not facts.get("affordable_but_over_cap_count", 0)
                        else "watchlist_only_budget_and_position_limited"
                    ),
                    "facts": facts,
                }

            if cost_inefficient_items:
                return {
                    "action": "不操作",
                    "target_position_pct": target_position_pct,
                    "items": [],
                    "affordable_but_weak_items": affordable_but_weak_items[: self.settings.top_n_default],
                    "watchlist_items": [],
                    "cost_inefficient_items": cost_inefficient_items,
                    "summary": "当前虽然有趋势不错的 ETF，但按这次建议金额估算，手续费占比偏高，先不急着做这笔交易。",
                    "reason_code": "cost_inefficient_only",
                    "facts": facts,
                }

            if affordable_but_weak_items:
                return {
                    "action": "不操作",
                    "target_position_pct": target_position_pct,
                    "items": [],
                    "affordable_but_weak_items": affordable_but_weak_items[: self.settings.top_n_default],
                    "watchlist_items": [],
                    "cost_inefficient_items": [],
                    "summary": "当前有一些 ETF 在仓位内买得起 1 手，但分数或趋势还不够强，这次不建议为了凑交易硬买。",
                    "reason_code": "affordable_but_weak_only",
                    "facts": facts,
                }

            return {
                "action": "不操作",
                "target_position_pct": target_position_pct,
                "items": [],
                "affordable_but_weak_items": [],
                "watchlist_items": [],
                "cost_inefficient_items": [],
                "summary": "这次分配到各类资产的金额仍偏小，暂时还形成不了一笔值得执行的交易。",
                "reason_code": "amount_below_min_advice",
                "facts": facts,
            }

        ranked_positions = positions_df.copy()
        if ranked_positions.empty:
            return {
                "action": "不操作",
                "target_position_pct": target_position_pct,
                "items": [],
                "affordable_but_weak_items": [],
                "watchlist_items": [],
                "cost_inefficient_items": [],
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
            "reduction_amount": round_money(reduction_amount),
            "watchlist_count": 0,
            "cost_inefficient_count": 0,
        }

        if not sell_items:
            return {
                "action": "不操作",
                "target_position_pct": target_position_pct,
                "items": [],
                "affordable_but_weak_items": [],
                "watchlist_items": [],
                "cost_inefficient_items": [],
                "summary": "虽然仓位偏高，但这次可减仓金额低于最小建议金额，系统建议先不操作。",
                "reason_code": "trim_amount_below_min_advice",
                "facts": facts,
            }

        return {
            "action": "卖出",
            "target_position_pct": target_position_pct,
            "items": sell_items,
            "affordable_but_weak_items": [],
            "watchlist_items": [],
            "cost_inefficient_items": [],
            "summary": f"当前仓位偏高，市场偏{market_regime}，建议先减一部分仓位，把总仓位向 {target_position_pct:.0%} 靠拢。",
            "reason_code": "trim_positions",
            "facts": facts,
        }
