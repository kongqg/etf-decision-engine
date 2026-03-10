from __future__ import annotations

from typing import Any

import pandas as pd


class MarketRegimeService:
    def evaluate(self, features_df: pd.DataFrame) -> dict[str, Any]:
        if features_df.empty:
            return {
                "market_regime": "观望",
                "broad_index_score": 0.0,
                "risk_appetite_score": 0.0,
                "trend_score": 0.0,
                "recommended_position_pct": 0.05,
                "evidence": {},
                "formulas": {},
            }

        broad_df = features_df[features_df["category"] == "宽基"].copy()
        offense_df = features_df[features_df["category"].isin(["宽基", "行业", "跨境"])].copy()
        defense_df = features_df[features_df["category"].isin(["债券", "黄金", "货币"])].copy()

        broad_momentum = float(broad_df[["momentum_5d", "momentum_10d"]].mean().mean()) if not broad_df.empty else 0.0
        broad_ma_gap = float(broad_df["ma_gap_10"].mean()) if not broad_df.empty else 0.0
        offense_score = float(offense_df["momentum_5d"].mean()) if not offense_df.empty else 0.0
        defense_score = float(defense_df["momentum_5d"].mean()) if not defense_df.empty else 0.0
        trend_positive_ratio = float((broad_df["ma_gap_10"] > 0).mean() * 100) if not broad_df.empty else 0.0
        trend_strength = float(broad_df["trend_strength"].mean()) if not broad_df.empty else 0.0

        broad_index_score = min(max(50 + broad_momentum * 4 + broad_ma_gap * 3, 0), 100)
        risk_appetite_score = min(max(50 + (offense_score - defense_score) * 5, 0), 100)
        trend_score = min(max(trend_positive_ratio * 0.8 + trend_strength * 2.5, 0), 100)

        if broad_index_score >= 60 and risk_appetite_score >= 55 and trend_score >= 55:
            regime = "进攻"
            position_pct = 0.80
        elif broad_index_score >= 50 and trend_score >= 45:
            regime = "中性"
            position_pct = 0.55
        elif broad_index_score >= 40 or trend_score >= 35:
            regime = "防守"
            position_pct = 0.25
        else:
            regime = "观望"
            position_pct = 0.05

        return {
            "market_regime": regime,
            "broad_index_score": round(broad_index_score, 2),
            "risk_appetite_score": round(risk_appetite_score, 2),
            "trend_score": round(trend_score, 2),
            "recommended_position_pct": position_pct,
            "evidence": {
                "broad_momentum": round(broad_momentum, 2),
                "broad_ma_gap": round(broad_ma_gap, 2),
                "offense_score": round(offense_score, 2),
                "defense_score": round(defense_score, 2),
                "trend_positive_ratio": round(trend_positive_ratio, 2),
                "trend_strength": round(trend_strength, 2),
            },
            "formulas": {
                "broad_index_score": "min(max(50 + broad_momentum * 4 + broad_ma_gap * 3, 0), 100)",
                "risk_appetite_score": "min(max(50 + (offense_score - defense_score) * 5, 0), 100)",
                "trend_score": "min(max(trend_positive_ratio * 0.8 + trend_strength * 2.5, 0), 100)",
            },
        }
