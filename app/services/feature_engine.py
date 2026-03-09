from __future__ import annotations

import pandas as pd

from app.utils.maths import annualized_volatility, safe_pct_change


class FeatureEngine:
    def calculate(self, history: pd.DataFrame) -> dict:
        frame = history.copy().sort_values("date").reset_index(drop=True)
        frame["return"] = frame["close"].pct_change()

        latest_close = float(frame["close"].iloc[-1])
        prev_close = float(frame["close"].iloc[-2]) if len(frame) >= 2 else latest_close
        ma5 = float(frame["close"].tail(5).mean())
        ma10 = float(frame["close"].tail(10).mean())
        max20 = float(frame["close"].tail(20).max())

        momentum_3d = safe_pct_change(latest_close, float(frame["close"].iloc[-4])) if len(frame) >= 4 else 0.0
        momentum_5d = safe_pct_change(latest_close, float(frame["close"].iloc[-6])) if len(frame) >= 6 else 0.0
        momentum_10d = safe_pct_change(latest_close, float(frame["close"].iloc[-11])) if len(frame) >= 11 else 0.0

        ma_gap_5 = safe_pct_change(latest_close, ma5)
        ma_gap_10 = safe_pct_change(latest_close, ma10)
        trend_strength = (ma_gap_5 * 0.4) + (ma_gap_10 * 0.6)
        drawdown_20d = safe_pct_change(latest_close, max20)
        pct_change = safe_pct_change(latest_close, prev_close)
        avg_amount_20d = float(frame["amount"].tail(20).mean())
        latest_amount = float(frame["amount"].iloc[-1])

        return {
            "close_price": latest_close,
            "pct_change": pct_change,
            "latest_amount": latest_amount,
            "avg_amount_20d": avg_amount_20d,
            "momentum_3d": momentum_3d,
            "momentum_5d": momentum_5d,
            "momentum_10d": momentum_10d,
            "ma_gap_5": ma_gap_5,
            "ma_gap_10": ma_gap_10,
            "trend_strength": trend_strength,
            "volatility_10d": annualized_volatility(frame["return"], window=10),
            "drawdown_20d": drawdown_20d,
        }
