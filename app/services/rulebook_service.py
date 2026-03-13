from __future__ import annotations

from typing import Any

from app.core.config import get_settings, load_yaml_config


CATEGORY_LABELS = {
    "stock_etf": "股票ETF",
    "bond_etf": "债券ETF",
    "gold_etf": "黄金ETF",
    "cross_border_etf": "跨境ETF",
    "money_etf": "货币ETF",
}

FEATURE_LABELS = {
    "momentum_3d": "3日动量",
    "momentum_5d": "5日动量",
    "momentum_10d": "10日动量",
    "momentum_20d": "20日动量",
    "ma5": "MA5",
    "ma10": "MA10",
    "ma20": "MA20",
    "close_price": "收盘价",
    "trend_strength": "趋势强度",
    "drawdown_20d": "20日回撤",
    "volatility_20d": "20日波动",
    "liquidity_score": "流动性分",
    "relative_strength_10d": "10日相对强弱",
    "volatility_10d": "10日波动",
    "abs_drawdown_20d": "20日绝对回撤",
    "rank_drop": "类别排名回落分",
    "time_decay": "持有时间衰减分",
}

FEATURE_MEANINGS = {
    "momentum_3d": "衡量最近 3 个交易日的短线涨跌。",
    "momentum_5d": "衡量最近 5 个交易日的一周动量。",
    "momentum_10d": "衡量最近 10 个交易日的中短期动量。",
    "momentum_20d": "衡量最近 20 个交易日的大趋势方向。",
    "ma5": "最近 5 个交易日收盘价均值。",
    "ma10": "最近 10 个交易日收盘价均值。",
    "ma20": "最近 20 个交易日收盘价均值，用于判断中期趋势。",
    "close_price": "最新一个交易日的收盘价。",
    "trend_strength": "价格相对 MA20 的偏离强度，越高代表趋势越强。",
    "drawdown_20d": "当前价格距离近 20 日高点的跌幅。",
    "volatility_20d": "最近 20 个交易日日收益率的标准差，反映波动。",
    "liquidity_score": "用成交额的对数衡量流动性，越大越容易成交。",
    "relative_strength_10d": "本 ETF 的 10 日动量减去所在类别平均 10 日动量。",
    "volatility_10d": "最近 10 个交易日日收益率的标准差。",
    "abs_drawdown_20d": "20 日回撤取绝对值后用于执行 head。",
    "rank_drop": "类别内排名是否从前排滑落到后排。",
    "time_decay": "已经持有多久，相对目标持有周期的衰减程度。",
}

HEAD_COMPONENT_NOTES = {
    "momentum_5d": "越高说明短线延续越强。",
    "momentum_10d": "越高说明中短期上行动能越强。",
    "momentum_20d": "越高说明 20 日趋势越完整。",
    "trend_strength": "越高说明价格站上中期均线越明显。",
    "relative_strength_10d": "越高说明它比同类平均更强。",
    "liquidity_score": "越高说明流动性更好，成交摩擦更低。",
    "volatility_10d": "原始波动越低越好，进入 head 前会先转成友好度分位。",
    "abs_drawdown_20d": "原始回撤越小越好，进入 entry/hold 前会先转成友好度分位；进入 exit 时则用回撤恶化侧。",
    "volatility_spike": "越高代表近期波动异常放大，偏退出信号。",
    "rank_drop": "越高代表在类别中的相对位置变差，偏退出信号。",
    "time_decay": "越高代表已持有更久，越接近计划退出时点。",
}


class RulebookService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self._reload_configs()

    def _reload_configs(self) -> None:
        self.strategy_scoring = load_yaml_config(self.settings.config_dir / "strategy_scoring.yaml")
        self.execution_overlay = load_yaml_config(self.settings.config_dir / "execution_overlay.yaml")
        self.category_profiles = load_yaml_config(self.settings.config_dir / "category_profiles.yaml")
        self.portfolio_constraints = load_yaml_config(self.settings.config_dir / "portfolio_constraints.yaml")
        self.execution_costs = load_yaml_config(self.settings.config_dir / "execution_costs.yaml")

    def build(self, preferences: Any | None = None) -> dict[str, Any]:
        self._reload_configs()
        target_holding_days = max(
            1,
            int(
                getattr(
                    preferences,
                    "target_holding_days",
                    self.execution_overlay.get("internals", {}).get("default_target_holding_days", 30),
                )
                or self.execution_overlay.get("internals", {}).get("default_target_holding_days", 30)
            ),
        )
        active_bucket_name, active_bucket = self._resolve_horizon_bucket(target_holding_days)
        return {
            "current_profile": {
                "target_holding_days": target_holding_days,
                "active_bucket_name": active_bucket_name,
                "active_bucket_label": self._bucket_label(active_bucket_name),
                "active_bucket_max_days": int(active_bucket.get("max_days", 9999)),
                "non_held_weights": {
                    "entry": float(active_bucket.get("non_held", {}).get("entry", 0.7)),
                    "hold": float(active_bucket.get("non_held", {}).get("hold", 0.2)),
                    "exit_inverse": float(active_bucket.get("non_held", {}).get("exit_inverse", 0.1)),
                },
                "held_weights": {
                    "entry": float(active_bucket.get("held", {}).get("entry", 0.3)),
                    "hold": float(active_bucket.get("held", {}).get("hold", 0.45)),
                    "exit_inverse": float(active_bucket.get("held", {}).get("exit_inverse", 0.25)),
                },
            },
            "feature_rules": self._feature_rules(),
            "normalization_rules": self._normalization_rules(),
            "score_rules": {
                "intra_score": self._intra_score_rule(),
                "category_score": self._category_score_rule(),
                "final_score": self._final_score_rule(),
                "decision_score": self._decision_score_rule(target_holding_days),
            },
            "category_heads": self._category_heads_rule(),
            "execution_rules": self._execution_rules(),
            "allocation_rules": self._allocation_rules(),
            "execution_cost_rules": self._execution_cost_rules(),
            "quick_guide": self._quick_guide(target_holding_days),
        }

    def build_decision_score_breakdown(
        self,
        *,
        scores: dict[str, Any],
        is_held: bool,
        preferences: Any | None = None,
    ) -> dict[str, Any]:
        self._reload_configs()
        target_holding_days = max(
            1,
            int(
                getattr(
                    preferences,
                    "target_holding_days",
                    self.execution_overlay.get("internals", {}).get("default_target_holding_days", 30),
                )
                or self.execution_overlay.get("internals", {}).get("default_target_holding_days", 30)
            ),
        )
        bucket_name, bucket = self._resolve_horizon_bucket(target_holding_days)
        blend_key = "held" if is_held else "non_held"
        blend = bucket.get(blend_key, {})
        entry_score = float(scores.get("entry_score", 0.0) or 0.0)
        hold_score = float(scores.get("hold_score", 0.0) or 0.0)
        exit_score = float(scores.get("exit_score", 0.0) or 0.0)
        stay_score = max(0.0, 100.0 - exit_score)
        components = [
            {
                "key": "entry_score",
                "label": "entry_score",
                "meaning": "新开仓吸引力，越高说明越像一个舒服的进场点。",
                "value": entry_score,
                "weight": float(blend.get("entry", 0.0)),
                "contribution": round(entry_score * float(blend.get("entry", 0.0)), 4),
                "formula_text": f"{entry_score:.2f} × {float(blend.get('entry', 0.0)):.2f}",
            },
            {
                "key": "hold_score",
                "label": "hold_score",
                "meaning": "已持有舒适度，越高说明继续拿着更合理。",
                "value": hold_score,
                "weight": float(blend.get("hold", 0.0)),
                "contribution": round(hold_score * float(blend.get("hold", 0.0)), 4),
                "formula_text": f"{hold_score:.2f} × {float(blend.get('hold', 0.0)):.2f}",
            },
            {
                "key": "stay_score",
                "label": "100 - exit_score",
                "meaning": "退出压力的反向分。exit_score 越高越危险，所以这里先做 100-exit_score。",
                "value": stay_score,
                "weight": float(blend.get("exit_inverse", 0.0)),
                "contribution": round(stay_score * float(blend.get("exit_inverse", 0.0)), 4),
                "formula_text": f"({100:.0f} - {exit_score:.2f}) × {float(blend.get('exit_inverse', 0.0)):.2f}",
            },
        ]
        return {
            "available": True,
            "formula": "decision_score = entry_score × w_entry + hold_score × w_hold + (100 - exit_score) × w_exit_inverse",
            "decision_score": round(sum(component["contribution"] for component in components), 2),
            "entry_score": entry_score,
            "hold_score": hold_score,
            "exit_score": exit_score,
            "stay_score": stay_score,
            "is_held": is_held,
            "blend_key": blend_key,
            "bucket_name": bucket_name,
            "bucket_label": self._bucket_label(bucket_name),
            "target_holding_days": target_holding_days,
            "weights": {
                "entry": float(blend.get("entry", 0.0)),
                "hold": float(blend.get("hold", 0.0)),
                "exit_inverse": float(blend.get("exit_inverse", 0.0)),
            },
            "components": components,
        }

    def _feature_rules(self) -> list[dict[str, Any]]:
        return [
            {
                "key": "close_price",
                "label": "close_price",
                "formula": "close_price = 最新交易日收盘价",
                "meaning": FEATURE_MEANINGS["close_price"],
                "used_in": "趋势过滤、通道判断、持仓状态、执行金额",
            },
            {
                "key": "momentum_3d",
                "label": "momentum_3d",
                "formula": "momentum_3d = (close_t / close_t-3交易日 - 1) × 100",
                "meaning": FEATURE_MEANINGS["momentum_3d"],
                "used_in": "通道A 反弹确认",
            },
            {
                "key": "momentum_5d",
                "label": "momentum_5d",
                "formula": "momentum_5d = (close_t / close_t-5交易日 - 1) × 100",
                "meaning": FEATURE_MEANINGS["momentum_5d"],
                "used_in": "单票分、entry_score、通道B",
            },
            {
                "key": "momentum_10d",
                "label": "momentum_10d",
                "formula": "momentum_10d = (close_t / close_t-10交易日 - 1) × 100",
                "meaning": FEATURE_MEANINGS["momentum_10d"],
                "used_in": "单票分、entry_score",
            },
            {
                "key": "momentum_20d",
                "label": "momentum_20d",
                "formula": "momentum_20d = (close_t / close_t-20交易日 - 1) × 100",
                "meaning": FEATURE_MEANINGS["momentum_20d"],
                "used_in": "单票分、类别广度、趋势过滤、hold_score",
            },
            {
                "key": "ma5 / ma10 / ma20",
                "label": "ma5 / ma10 / ma20",
                "formula": "对应窗口收盘价均值",
                "meaning": "分别反映短、中、20日均线位置。",
                "used_in": "趋势过滤、通道A/B、trend_strength",
            },
            {
                "key": "trend_strength",
                "label": "trend_strength",
                "formula": "trend_strength = (close_t / ma20 - 1) × 100",
                "meaning": FEATURE_MEANINGS["trend_strength"],
                "used_in": "单票分、类别广度、entry_score、hold_score",
            },
            {
                "key": "drawdown_20d",
                "label": "drawdown_20d",
                "formula": "drawdown_20d = (close_t / rolling_max_20d - 1) × 100",
                "meaning": FEATURE_MEANINGS["drawdown_20d"],
                "used_in": "单票分、通道A/B、entry/hold/exit 派生变量",
            },
            {
                "key": "volatility_20d",
                "label": "volatility_20d",
                "formula": "volatility_20d = std(ret_1d_raw, 20) × 100",
                "meaning": FEATURE_MEANINGS["volatility_20d"],
                "used_in": "单票分、通道B 波动保护、退出派生变量",
            },
            {
                "key": "liquidity_score",
                "label": "liquidity_score",
                "formula": "liquidity_score = ln(avg_amount_20d + 1)",
                "meaning": FEATURE_MEANINGS["liquidity_score"],
                "used_in": "单票分、entry_score、money_etf head",
            },
            {
                "key": "category_return_10d",
                "label": "category_return_10d",
                "formula": "同 decision_category 内所有 ETF 的 momentum_10d 平均值",
                "meaning": "给相对强弱提供类别基准。",
                "used_in": "relative_strength_10d",
            },
            {
                "key": "relative_strength_10d",
                "label": "relative_strength_10d",
                "formula": "relative_strength_10d = momentum_10d - category_return_10d",
                "meaning": FEATURE_MEANINGS["relative_strength_10d"],
                "used_in": "entry_score、hold_score",
            },
        ]

    def _normalization_rules(self) -> dict[str, Any]:
        normalization = self.strategy_scoring.get("normalization", {})
        directions = normalization.get("directions", {})
        features = []
        for key, direction in directions.items():
            features.append(
                {
                    "key": key,
                    "label": FEATURE_LABELS.get(str(key), str(key)),
                    "direction": "越大越好" if str(direction) == "higher" else "越小越好",
                    "formula": (
                        "分位 = (rank - 1) / (n - 1) × 100"
                        if str(direction) == "higher"
                        else "分位 = (n - rank) / (n - 1) × 100"
                    ),
                }
            )
        return {
            "method": str(normalization.get("method", "percentile_rank")),
            "scale": float(normalization.get("scale", 100.0)),
            "formula_note": "同类别内做百分位排名；如果某个类别只有 1 只 ETF，则该特征分位直接记为 100。",
            "features": features,
            "derived_variables": [
                {
                    "key": "momentum_5d_rel / momentum_10d_rel / momentum_20d_rel",
                    "formula": "对对应动量做类别内分位转换",
                    "meaning": "让不同类别里的标的在统一 0-100 尺度下可比较。",
                },
                {
                    "key": "trend_strength_rel / relative_strength_10d_rel / liquidity_rel",
                    "formula": "对趋势强度、相对强弱、流动性做类别内分位转换",
                    "meaning": "作为 entry / hold head 的输入。",
                },
                {
                    "key": "volatility_10d_goodness",
                    "formula": "对 volatility_10d 做“越低越好”的类别分位",
                    "meaning": "波动越低越加分。",
                },
                {
                    "key": "drawdown_goodness",
                    "formula": "对 abs_drawdown_20d 做“越低越好”的类别分位",
                    "meaning": "回撤越小越加分。",
                },
                {
                    "key": "volatility_20d_spike",
                    "formula": "对 volatility_20d 做“越高越危险”的类别分位",
                    "meaning": "作为 exit_score 的风险抬头输入。",
                },
                {
                    "key": "rank_drop_score",
                    "formula": "rank_drop_score = ((category_rank - 1) / (category_symbol_count - 1)) × 100；若类别仅 1 只则记 0",
                    "meaning": "类别排名越往后，退出压力越高。",
                },
                {
                    "key": "time_decay_score",
                    "formula": "time_decay_score = min(hold_days / target_holding_days, 1.0) × 100",
                    "meaning": "持有越久，越接近计划退出时点。",
                },
            ],
        }

    def _intra_score_rule(self) -> dict[str, Any]:
        components = []
        for key, weight in self.strategy_scoring.get("intra_score_weights", {}).items():
            components.append(
                {
                    "key": str(key),
                    "weight": float(weight),
                    "label": FEATURE_LABELS.get(str(key).replace("_rank", ""), str(key)),
                    "formula_text": f"{float(weight):.3f} × {str(key)}",
                }
            )
        return {
            "formula": "intra_score = Σ(各特征分位 × 权重)",
            "display_formula": "单票分 = 0.30×20日动量分位 + 0.20×10日动量分位 + 0.10×5日动量分位 + 0.15×趋势分位 + 0.10×流动性分位 + 0.075×波动友好度分位 + 0.075×回撤友好度分位",
            "components": components,
        }

    def _category_score_rule(self) -> dict[str, Any]:
        payload = self.strategy_scoring.get("category_score", {})
        weights = payload.get("weights", {})
        return {
            "formula": "category_score = 头部平均单票分 × w1 + 类别广度分 × w2 + 类别动量分 × w3",
            "display_formula": (
                f"类别分 = {float(weights.get('top_mean_intrascore', 0.5)):.2f}×头部平均单票分 + "
                f"{float(weights.get('breadth_score', 0.3)):.2f}×类别广度分 + "
                f"{float(weights.get('category_momentum_score', 0.2)):.2f}×类别动量分"
            ),
            "top_n": int(payload.get("top_n", 3)),
            "breadth_thresholds": payload.get("breadth_positive_thresholds", {}),
            "components": [
                {"key": "top_mean_intrascore", "weight": float(weights.get("top_mean_intrascore", 0.5))},
                {"key": "breadth_score", "weight": float(weights.get("breadth_score", 0.3))},
                {"key": "category_momentum_score", "weight": float(weights.get("category_momentum_score", 0.2))},
            ],
        }

    def _final_score_rule(self) -> dict[str, Any]:
        weights = self.strategy_scoring.get("final_score_weights", {})
        selection = self.strategy_scoring.get("selection", {})
        return {
            "formula": "final_score = intra_score × w_intra + category_score × w_category",
            "display_formula": (
                f"最终分 = {float(weights.get('intra_score', 0.7)):.2f}×单票分 + "
                f"{float(weights.get('category_score', 0.3)):.2f}×类别分"
            ),
            "components": [
                {"key": "intra_score", "weight": float(weights.get("intra_score", 0.7))},
                {"key": "category_score", "weight": float(weights.get("category_score", 0.3))},
            ],
            "min_final_score_for_target": float(selection.get("min_final_score_for_target", 55.0)),
            "candidate_watchlist_size": int(selection.get("candidate_watchlist_size", 8)),
        }

    def _decision_score_rule(self, target_holding_days: int) -> dict[str, Any]:
        bucket_name, _bucket = self._resolve_horizon_bucket(target_holding_days)
        buckets = []
        for name, payload in self.execution_overlay.get("horizon_buckets", {}).items():
            buckets.append(
                {
                    "name": str(name),
                    "label": self._bucket_label(str(name)),
                    "max_days": int(payload.get("max_days", 9999)),
                    "non_held": {
                        "entry": float(payload.get("non_held", {}).get("entry", 0.0)),
                        "hold": float(payload.get("non_held", {}).get("hold", 0.0)),
                        "exit_inverse": float(payload.get("non_held", {}).get("exit_inverse", 0.0)),
                    },
                    "held": {
                        "entry": float(payload.get("held", {}).get("entry", 0.0)),
                        "hold": float(payload.get("held", {}).get("hold", 0.0)),
                        "exit_inverse": float(payload.get("held", {}).get("exit_inverse", 0.0)),
                    },
                }
            )
        return {
            "formula": "decision_score = entry_score × w_entry + hold_score × w_hold + (100 - exit_score) × w_exit_inverse",
            "display_formula": "执行决策分 = entry_score × w_entry + hold_score × w_hold + (100 - exit_score) × w_exit_inverse",
            "meaning": "entry_score 看新开仓吸引力，hold_score 看继续持有舒适度，exit_score 看退出压力，公式里用 100-exit_score 把退出压力翻成“还能继续待”的分。",
            "active_bucket_name": bucket_name,
            "active_bucket_label": self._bucket_label(bucket_name),
            "active_target_holding_days": target_holding_days,
            "buckets": buckets,
        }

    def _category_heads_rule(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for category, payload in self.category_profiles.get("category_heads", {}).items():
            category_entry: dict[str, Any] = {
                "label": CATEGORY_LABELS.get(str(category), str(category)),
                "heads": {},
            }
            for head_name, head_payload in payload.items():
                components = []
                total_weight = sum(abs(float(weight)) for weight in head_payload.values())
                for key, weight in head_payload.items():
                    components.append(
                        {
                            "key": str(key),
                            "label": FEATURE_LABELS.get(str(key), str(key)),
                            "raw_weight": float(weight),
                            "effective_weight": abs(float(weight)),
                            "meaning": HEAD_COMPONENT_NOTES.get(str(key), ""),
                        }
                    )
                category_entry["heads"][str(head_name)] = {
                    "formula": f"{head_name}_score = Σ(预处理特征 × |权重|) / Σ|权重|",
                    "meaning": self._head_meaning(str(head_name)),
                    "components": components,
                    "total_weight": total_weight,
                    "note": "波动、回撤这类风险项在进入 head 前已经先转成友好度或风险度分位，因此 YAML 里出现的负号主要是语义标记，实际聚合时使用权重绝对值。",
                }
            result[str(category)] = category_entry
        return result

    def _execution_rules(self) -> dict[str, Any]:
        return {
            "trend_filter": {
                "formula": "momentum_20d > 0 且 close_price > ma20",
                "meaning": "只有中期趋势仍向上，才允许新的多头开仓/加仓。",
            },
            "channel_a": {
                "formula": "趋势过滤通过 且 drawdown_20d ∈ [pullback_low_pct, pullback_high_pct] 且 close_price > ma5 且 momentum_3d > 0",
                "params": {
                    "pullback_low_pct": float(self.execution_overlay.get("pullback_low_pct", -6.0)),
                    "pullback_high_pct": float(self.execution_overlay.get("pullback_high_pct", -2.0)),
                },
                "meaning": "趋势没坏、先回撤到合理区，再出现反弹确认，才算舒服买点。",
            },
            "channel_b": {
                "formula": "趋势过滤通过 且 drawdown_20d > pullback_high_pct 且 entry_score ≥ breakout_entry_threshold 且 momentum_5d > 0 且 close_price > ma5 且 volatility_20d ≤ 同类别中位数",
                "params": {
                    "pullback_high_pct": float(self.execution_overlay.get("pullback_high_pct", -2.0)),
                    "breakout_entry_threshold": float(self.execution_overlay.get("breakout_entry_threshold", 75.0)),
                },
                "meaning": "给极强趋势一个例外入口，避免一路强势却始终等不到回调。",
            },
            "position_state": [
                {"state": "HOLD", "formula": "momentum_20d > 0 且 close_price > ma20", "meaning": "趋势仍健康。"},
                {"state": "REDUCE", "formula": "momentum_20d > 0 且 close_price ≤ ma20", "meaning": "趋势未完全坏，但要减仓观察。"},
                {"state": "EXIT", "formula": "momentum_20d ≤ 0 且 close_price ≤ ma20", "meaning": "趋势破坏，退出。"},
            ],
            "rebalance_band": float(self.execution_overlay.get("rebalance_band", 0.05)),
            "reduced_target_multiplier": float(self.execution_overlay.get("internals", {}).get("reduced_target_multiplier", 0.5)),
        }

    def _allocation_rules(self) -> dict[str, Any]:
        selection = self.portfolio_constraints.get("selection", {})
        budget = self.portfolio_constraints.get("budget", {})
        return {
            "selection": [
                {"path": "selection.max_selected_total", "value": int(selection.get("max_selected_total", 3)), "meaning": "单轮最多正式保留多少只 ETF。"},
                {"path": "selection.max_selected_per_category", "value": int(selection.get("max_selected_per_category", 2)), "meaning": "单个类别最多保留多少只。"},
                {"path": "selection.hold_guard_global_rank", "value": int(selection.get("hold_guard_global_rank", 5)), "meaning": "当前持仓若全市场排名足够靠前，会自动进入保护。"},
                {"path": "selection.hold_guard_category_rank", "value": int(selection.get("hold_guard_category_rank", 2)), "meaning": "当前持仓若类别排名靠前，也会自动进入保护。"},
                {"path": "selection.replace_threshold", "value": float(selection.get("replace_threshold", 8.0)), "meaning": "新候选要比旧持仓至少高出多少分，才值得替换。"},
                {"path": "selection.min_hold_days_before_replace", "value": int(selection.get("min_hold_days_before_replace", 2)), "meaning": "如果持有天数已知且小于这个值，会触发最短持有期保护。"},
            ],
            "budget": [
                {"path": "budget.max_total_weight", "value": float(budget.get("max_total_weight", 0.8)), "meaning": "总仓位上限。"},
                {"path": "budget.max_single_weight", "value": float(budget.get("max_single_weight", 0.35)), "meaning": "单只 ETF 的理论仓位上限。"},
                {"path": "budget.min_position_weight", "value": float(budget.get("min_position_weight", 0.08)), "meaning": "低于这个仓位，不形成正式目标仓位。"},
                {"path": "budget.min_trade_weight_delta", "value": float(budget.get("min_trade_weight_delta", 0.01)), "meaning": "过小的权重变化不值得单独调仓。"},
            ],
            "category_caps": [
                {"path": f"category_caps.{key}", "value": float(value), "label": CATEGORY_LABELS.get(str(key), str(key))}
                for key, value in self.portfolio_constraints.get("category_caps", {}).items()
            ],
        }

    def _execution_cost_rules(self) -> dict[str, Any]:
        return {
            "execution_cost_bps": float(self.execution_costs.get("execution_cost_bps", 5.0)),
            "min_trade_amount": float(self.execution_costs.get("min_trade_amount", 100.0)),
        }

    def _quick_guide(self, target_holding_days: int) -> list[dict[str, Any]]:
        decision_rule = self._decision_score_rule(target_holding_days)
        return [
            {
                "title": "单票分 intra_score",
                "formula": self._intra_score_rule()["display_formula"],
                "meaning": "回答“这只 ETF 自身体质强不强”。",
            },
            {
                "title": "类别分 category_score",
                "formula": self._category_score_rule()["display_formula"],
                "meaning": "回答“它所在赛道环境顺不顺”。",
            },
            {
                "title": "最终分 final_score",
                "formula": self._final_score_rule()["display_formula"],
                "meaning": "回答“它有没有资格进入正式目标组合”。",
            },
            {
                "title": "执行决策分 decision_score",
                "formula": decision_rule["display_formula"],
                "meaning": f"回答“当前是不是一个舒服的执行时点”。当前持有周期 {target_holding_days} 天，命中 {decision_rule['active_bucket_label']} 权重桶。",
            },
        ]

    def _resolve_horizon_bucket(self, target_holding_days: int) -> tuple[str, dict[str, Any]]:
        buckets = self.execution_overlay.get("horizon_buckets", {})
        for name, payload in buckets.items():
            if target_holding_days <= int(payload.get("max_days", 9999)):
                return str(name), dict(payload)
        return "long", dict(buckets.get("long", {}))

    def _bucket_label(self, name: str) -> str:
        return {
            "short": "短周期桶",
            "medium": "中周期桶",
            "long": "长周期桶",
        }.get(str(name), str(name))

    def _head_meaning(self, head_name: str) -> str:
        return {
            "entry": "看现在像不像一个适合切进去的新买点。",
            "hold": "看已经持有时，继续拿着舒不舒服。",
            "exit": "看退出压力有多大，越高越危险。",
        }.get(head_name, head_name)
