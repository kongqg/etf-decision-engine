from __future__ import annotations

import json

import pandas as pd

from app.utils.maths import pct_rank


class ScoringService:
    def score(self, candidates_df: pd.DataFrame) -> pd.DataFrame:
        if candidates_df.empty:
            return candidates_df

        df = candidates_df.copy()
        df["score_m3"] = pct_rank(df["momentum_3d"], ascending=True)
        df["score_m5"] = pct_rank(df["momentum_5d"], ascending=True)
        df["score_m10"] = pct_rank(df["momentum_10d"], ascending=True)
        df["score_trend"] = pct_rank(df["trend_strength"], ascending=True)
        df["score_ma"] = pct_rank(df["ma_gap_5"] + df["ma_gap_10"], ascending=True)
        df["score_vol_penalty"] = pct_rank(df["volatility_10d"], ascending=True)
        df["score_dd_penalty"] = pct_rank(df["drawdown_20d"].abs(), ascending=True)
        df["score_liquidity"] = pct_rank(df["avg_amount_20d"], ascending=True)

        df["total_score"] = (
            0.20 * df["score_m3"]
            + 0.25 * df["score_m5"]
            + 0.25 * df["score_m10"]
            + 0.15 * df["score_trend"]
            + 0.10 * df["score_ma"]
            - 0.03 * df["score_vol_penalty"]
            - 0.02 * df["score_dd_penalty"]
            + 0.10 * df["score_liquidity"]
        )
        df = df.sort_values(["total_score", "momentum_5d"], ascending=[False, False]).reset_index(drop=True)
        df["rank_in_pool"] = df.index + 1
        if "asset_class" in df.columns:
            group_values = df["asset_class"].fillna("全部")
        elif "category" in df.columns:
            group_values = df["category"].fillna("全部")
        else:
            group_values = pd.Series(["全部"] * len(df), index=df.index)
        df["rank_in_asset_class"] = df.groupby(group_values)["total_score"].rank(method="first", ascending=False).astype(int)

        breakdowns = []
        for _, row in df.iterrows():
            breakdowns.append(
                json.dumps(
                    {
                        "momentum_3d_score": round(float(row["score_m3"]), 2),
                        "momentum_5d_score": round(float(row["score_m5"]), 2),
                        "momentum_10d_score": round(float(row["score_m10"]), 2),
                        "trend_score": round(float(row["score_trend"]), 2),
                        "ma_score": round(float(row["score_ma"]), 2),
                        "volatility_penalty": round(float(row["score_vol_penalty"]), 2),
                        "drawdown_penalty": round(float(row["score_dd_penalty"]), 2),
                        "liquidity_score": round(float(row["score_liquidity"]), 2),
                        "formula_score": round(float(row["total_score"]), 2),
                        "rank_in_asset_class": int(row["rank_in_asset_class"]),
                    },
                    ensure_ascii=False,
                )
            )

        df["breakdown_json"] = breakdowns
        return df
