from __future__ import annotations

from typing import Any

import pandas as pd

from app.core.config import get_settings, load_yaml_config


RISK_ORDER = {
    "低": 1,
    "中": 2,
    "中高": 3,
    "高": 4,
}

RISK_LIMIT_BY_PREFERENCE = {
    "保守": 2,
    "中性": 3,
    "激进": 4,
}


class UniverseFilterService:
    def __init__(self) -> None:
        settings = get_settings()
        risk_rules = load_yaml_config(settings.config_dir / "risk_rules.yaml")
        self.max_volatility = risk_rules["max_volatility_by_preference"]

    def apply(self, features_df: pd.DataFrame, preferences: Any) -> pd.DataFrame:
        if features_df.empty:
            return features_df

        allowed_risk_level = RISK_LIMIT_BY_PREFERENCE.get(preferences.risk_level, 3)
        max_vol = float(self.max_volatility.get(preferences.risk_level, 5.0))

        df = features_df.copy()
        reasons: list[list[str]] = []
        passes: list[bool] = []

        for _, row in df.iterrows():
            row_reasons: list[str] = []
            if row["category"] == "黄金" and not preferences.allow_gold:
                row_reasons.append("用户未开启黄金 ETF")
            if row["category"] == "债券" and not preferences.allow_bond:
                row_reasons.append("用户未开启债券 ETF")
            if row["category"] == "跨境" and not preferences.allow_overseas:
                row_reasons.append("用户未开启跨境 ETF")
            if float(row["avg_amount_20d"]) < float(row["min_avg_amount"]):
                row_reasons.append("近 20 日成交额不达标")
            if bool(row["anomaly_flag"]):
                row_reasons.append("近期波动或涨跌异常")
            if float(row["volatility_10d"]) > max_vol:
                row_reasons.append("波动率超出当前风险偏好")
            if RISK_ORDER.get(str(row["risk_level"]), 4) > allowed_risk_level:
                row_reasons.append("风险等级高于当前偏好")

            reasons.append(row_reasons)
            passes.append(not row_reasons)

        df["filter_reasons"] = reasons
        df["filter_pass"] = passes
        return df
