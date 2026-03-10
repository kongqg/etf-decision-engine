from __future__ import annotations

import json
from typing import Any

from sqlalchemy.orm import Session

from app.repositories.advice_repo import get_advice_by_id, get_latest_advice
from app.repositories.market_repo import get_features_by_trade_date


ETF_SCORE_FORMULA = (
    "先按类别算 category_score，再按持有周期和阶段计算 "
    "entry_score / hold_score / exit_score，最后用 phase 权重组合成 decision_score。"
)


class DataEvidenceService:
    def build(self, session: Session, advice_id: int | None = None) -> dict[str, Any]:
        advice = get_advice_by_id(session, advice_id) if advice_id is not None else get_latest_advice(session)
        if advice is None:
            raise ValueError("暂无建议记录，无法查看数据证据。")

        evidence = json.loads(advice.evidence_json or "{}")
        market_snapshot = evidence.get("market_snapshot", {})
        raw = market_snapshot.get("raw", {})
        source = raw.get("source", {})
        quality_summary = raw.get("quality_summary", {})
        request_summary = raw.get("request_summary", {})
        quality_checks = raw.get("quality_checks", [])
        series_samples = raw.get("series_samples", {})

        feature_rows = get_features_by_trade_date(session, advice.advice_date)
        feature_map = {row.symbol: row for row in feature_rows}

        recommended_symbols = [item.symbol for item in advice.items]
        if not recommended_symbols:
            recommended_symbols = [
                row.symbol for row in feature_rows if row.filter_pass and row.rank_in_pool is not None
            ][:3]

        benchmark_symbols = self._benchmark_symbols(series_samples)

        return {
            "advice_id": advice.id,
            "headline": advice.summary_text,
            "action": advice.action,
            "market_regime": advice.market_regime,
            "trade_date": advice.advice_date.isoformat(),
            "created_at": advice.created_at.isoformat(),
            "trust_summary": [
                {"label": "数据来源", "value": source.get("label", "-")},
                {"label": "数据类型", "value": quality_summary.get("data_type", source.get("data_type", "-"))},
                {"label": "数据可信等级", "value": quality_summary.get("reliability_level", "-")},
                {"label": "验证状态", "value": quality_summary.get("verification_status", "-")},
                {"label": "交叉验证", "value": quality_summary.get("cross_source_status", "-")},
                {"label": "是否支持实时建议", "value": "是" if quality_summary.get("supports_live_execution") else "否"},
                {"label": "最新可用日期", "value": quality_summary.get("latest_available_date", "-")},
                {"label": "抓取时间", "value": source.get("captured_at", "-").replace("T", " ")[:16]},
            ],
            "freshness_note": quality_summary.get("freshness_label", ""),
            "live_execution_note": quality_summary.get("live_execution_note", ""),
            "source_note": source.get("note", ""),
            "request_summary": {
                "provider": request_summary.get("provider", "-"),
                "api": request_summary.get("api", "-"),
                "requested_trade_date": request_summary.get("requested_trade_date", "-"),
                "captured_at": request_summary.get("captured_at", "-").replace("T", " ")[:16],
                "symbols_count": request_summary.get("symbols_count", 0),
                "source_counts": request_summary.get("source_counts", {}),
                "default_params": request_summary.get("default_params", {}),
            },
            "quality_overview": {
                "passed_checks": quality_summary.get("passed_checks", 0),
                "failed_checks": quality_summary.get("failed_checks", 0),
                "failed_symbols": quality_summary.get("failed_symbols", []),
            },
            "quality_checks": quality_checks,
            "benchmark_series": self._series_cards(benchmark_symbols, series_samples, feature_map, quality_checks),
            "candidate_series": self._series_cards(recommended_symbols, series_samples, feature_map, quality_checks),
        }

    def _benchmark_symbols(self, series_samples: dict[str, Any]) -> list[str]:
        by_category: dict[str, list[str]] = {"宽基": [], "黄金": [], "债券": [], "货币": [], "行业": [], "跨境": []}
        for symbol, payload in series_samples.items():
            category = payload.get("category", "")
            by_category.setdefault(category, []).append(symbol)

        selected = []
        selected.extend(by_category.get("宽基", [])[:3])
        selected.extend(by_category.get("黄金", [])[:1])
        selected.extend(by_category.get("债券", [])[:1])
        selected.extend(by_category.get("货币", [])[:1])
        if len(selected) < 6:
            leftovers = []
            for category in ["行业", "跨境"]:
                leftovers.extend(by_category.get(category, []))
            for symbol in leftovers:
                if symbol not in selected:
                    selected.append(symbol)
                if len(selected) >= 6:
                    break
        return selected

    def _series_cards(
        self,
        symbols: list[str],
        series_samples: dict[str, Any],
        feature_map: dict[str, Any],
        quality_checks: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        quality_map = {item["symbol"]: item for item in quality_checks}
        cards = []
        for symbol in symbols:
            payload = series_samples.get(symbol)
            if payload is None:
                continue
            feature = feature_map.get(symbol)
            breakdown = self._parse_breakdown(feature.breakdown_json if feature is not None else "{}")
            cards.append(
                {
                    "symbol": symbol,
                    "name": payload.get("name", symbol),
                    "category": payload.get("category", "-"),
                    "source": payload.get("source", "-"),
                    "latest_row_date": payload.get("latest_row_date", "-"),
                    "request_params": payload.get("request_params", {}),
                    "rows": payload.get("rows", []),
                    "quality": quality_map.get(symbol, {}),
                    "score": round(float(feature.total_score), 2) if feature is not None else 0.0,
                    "rank_in_pool": int(feature.rank_in_pool) if feature is not None and feature.rank_in_pool else None,
                    "filter_pass": bool(feature.filter_pass) if feature is not None else False,
                    "metrics": self._feature_metrics(feature),
                    "score_formula": ETF_SCORE_FORMULA,
                    "score_substitution": self._score_substitution(breakdown),
                }
            )
        return cards

    def _feature_metrics(self, feature) -> list[dict[str, str]]:
        if feature is None:
            return []
        return [
            {"label": "收盘价", "value": f"{feature.close_price:.4f}"},
            {"label": "当日涨跌幅", "value": f"{feature.pct_change:.2f}%"},
            {"label": "3日动量", "value": f"{feature.momentum_3d:.2f}%"},
            {"label": "5日动量", "value": f"{feature.momentum_5d:.2f}%"},
            {"label": "10日动量", "value": f"{feature.momentum_10d:.2f}%"},
            {"label": "10日波动率", "value": f"{feature.volatility_10d:.2f}%"},
            {"label": "20日回撤", "value": f"{feature.drawdown_20d:.2f}%"},
            {"label": "20日均成交额", "value": f"{feature.avg_amount_20d / 100000000:.2f} 亿元"},
        ]

    def _parse_breakdown(self, value: Any) -> dict[str, float]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                loaded = json.loads(value)
            except json.JSONDecodeError:
                return {}
            if isinstance(loaded, dict):
                return loaded
        return {}

    def _score_substitution(self, breakdown: dict[str, Any]) -> str:
        if not breakdown:
            return "当前没有可用分项数据。"
        if "decision_score" in breakdown:
            weights = breakdown.get("phase_weights", {})
            return (
                f"{weights.get('entry', 0):.2f} × {breakdown.get('entry_score', 0):.2f}"
                f" + {weights.get('hold', 0):.2f} × {breakdown.get('hold_score', 0):.2f}"
                f" - {weights.get('exit', 0):.2f} × {breakdown.get('exit_score', 0):.2f}"
                f" = {float(breakdown.get('decision_score', 0)):.2f}"
            )
        return "当前分项结构属于旧版本，无法按新公式回放。"

    def _term_formula(self, weight: float, score_value: float) -> str:
        sign = "+" if weight >= 0 else "-"
        factor = abs(weight)
        contribution = factor * score_value
        return f"{sign}{factor:.2f} × {score_value:.2f} = {contribution:.2f}"
