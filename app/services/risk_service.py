from __future__ import annotations

from app.core.config import get_settings, load_yaml_config


class RiskService:
    def __init__(self) -> None:
        settings = get_settings()
        self.rules = load_yaml_config(settings.config_dir / "risk_rules.yaml")

    def get_stop_loss_pct(self, category: str) -> float:
        return float(self.rules["stop_loss_by_category"].get(self._risk_category_key(category), 0.06))

    def get_take_profit_pct(self, category: str) -> float:
        return float(self.rules["take_profit_by_category"].get(self._risk_category_key(category), 0.10))

    def build_global_risk_note(self, session_mode: str, market_regime: str) -> str:
        mode_note = {
            "intraday": "当前处于交易时段，执行上优先分批，不把日内节奏当成策略信号。",
            "preopen": "当前尚未连续竞价，开盘前建议把注意力放在预算和成交约束，而不是追单。",
            "after_close": "当前已收盘，这是一份下一交易日的计划，不是即时成交指令。",
            "closed": "当前为休市阶段，系统只输出下一交易日的候选组合和预算。",
        }.get(session_mode, "当前为非连续交易时段，系统输出的是计划而非即时成交指令。")
        return f"{mode_note} 当前市场状态为 {market_regime}，仓位预算会随状态调整，但不会直接替代 ETF 打分。"

    def _risk_category_key(self, category: str) -> str:
        mapping = {
            "stock_etf": "宽基",
            "bond_etf": "债券",
            "gold_etf": "黄金",
            "cross_border_etf": "跨境",
            "money_etf": "货币",
        }
        return mapping.get(category, category)
