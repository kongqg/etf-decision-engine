from __future__ import annotations

from typing import Any

from app.core.config import get_settings, load_yaml_config


class RiskService:
    def __init__(self) -> None:
        settings = get_settings()
        self.rules = load_yaml_config(settings.config_dir / "risk_rules.yaml")

    def get_stop_loss_pct(self, category: str) -> float:
        return float(self.rules["stop_loss_by_category"].get(category, 0.06))

    def get_take_profit_pct(self, category: str) -> float:
        return float(self.rules["take_profit_by_category"].get(category, 0.10))

    def build_global_risk_note(self, session_mode: str, market_regime: str) -> str:
        mode_note = {
            "intraday": "当前在交易时段，建议仍以分批执行为主，不要一次满仓。",
            "preopen": "当前还没正式连续交易，开盘后如果明显高开，不要机械追价。",
            "after_close": "当前已收盘，今晚输出的是明日预案，不是现在立刻成交建议。",
            "closed": "当前为休市阶段，系统仅给下一交易日预案。",
        }[session_mode]
        return f"{mode_note} 当前市场状态为{market_regime}，若后续走势明显转弱，应优先减少执行。"

    def position_action_hint(self, row: Any, market_regime: str) -> str:
        if market_regime == "观望":
            return "等待更强信号"
        if row.get("rank_in_pool", 99) <= 2:
            return "继续持有"
        if row.get("rank_in_pool", 99) <= 5:
            return "观察后续强弱"
        return "考虑减仓"
