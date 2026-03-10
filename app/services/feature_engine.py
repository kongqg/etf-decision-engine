from __future__ import annotations

import math

import pandas as pd

from app.utils.maths import safe_pct_change


class FeatureEngine:
    def calculate(self, history: pd.DataFrame) -> dict:
        frame = history.copy().sort_values("date").reset_index(drop=True)
        frame["ret_1d_raw"] = frame["close"].pct_change()

        latest_close = float(frame["close"].iloc[-1])
        prev_close = float(frame["close"].iloc[-2]) if len(frame) >= 2 else latest_close
        ma5 = float(frame["close"].tail(5).mean())
        ma10 = float(frame["close"].tail(10).mean())
        ma20 = float(frame["close"].tail(20).mean())
        max20 = float(frame["close"].tail(20).max())

        momentum_3d = safe_pct_change(latest_close, float(frame["close"].iloc[-4])) if len(frame) >= 4 else 0.0
        momentum_5d = safe_pct_change(latest_close, float(frame["close"].iloc[-6])) if len(frame) >= 6 else 0.0
        momentum_10d = safe_pct_change(latest_close, float(frame["close"].iloc[-11])) if len(frame) >= 11 else 0.0
        momentum_20d = safe_pct_change(latest_close, float(frame["close"].iloc[-21])) if len(frame) >= 21 else 0.0

        ma_gap_5 = safe_pct_change(latest_close, ma5)
        ma_gap_10 = safe_pct_change(latest_close, ma10)
        trend_strength = safe_pct_change(latest_close, ma20) if ma20 else 0.0
        drawdown_20d = safe_pct_change(latest_close, max20)
        pct_change = safe_pct_change(latest_close, prev_close)
        avg_amount_20d = float(frame["amount"].tail(20).mean())
        latest_amount = float(frame["amount"].iloc[-1])
        liquidity_score = math.log(avg_amount_20d + 1.0)
        ret_1d = pct_change
        volatility_5d = self._rolling_std_percent(frame["ret_1d_raw"], 5)
        volatility_10d = self._rolling_std_percent(frame["ret_1d_raw"], 10)
        volatility_20d = self._rolling_std_percent(frame["ret_1d_raw"], 20)

        return {
            "close_price": latest_close,
            "pct_change": pct_change,
            "ret_1d": ret_1d,
            "latest_amount": latest_amount,
            "avg_amount_20d": avg_amount_20d,
            "avg_turnover_20d": avg_amount_20d,
            "momentum_3d": momentum_3d,
            "momentum_5d": momentum_5d,
            "momentum_10d": momentum_10d,
            "momentum_20d": momentum_20d,
            "ma5": ma5,
            "ma10": ma10,
            "ma20": ma20,
            "ma_gap_5": ma_gap_5,
            "ma_gap_10": ma_gap_10,
            "trend_strength": trend_strength,
            "volatility_5d": volatility_5d,
            "volatility_10d": volatility_10d,
            "volatility_20d": volatility_20d,
            "rolling_max_20d": max20,
            "drawdown_20d": drawdown_20d,
            "liquidity_score": liquidity_score,
            "above_ma20_flag": latest_close > ma20 if ma20 else False,
        }

    def _rolling_std_percent(self, returns: pd.Series, window: int) -> float:
        clean = returns.dropna().tail(window)
        if clean.empty:
            return 0.0
        return float(clean.std(ddof=0) * 100.0)
