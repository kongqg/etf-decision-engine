from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from app.core.config import get_settings, load_yaml_config


FILE_LABELS = {
    "strategy_scoring.yaml": "统一评分参数",
    "execution_overlay.yaml": "执行层参数",
    "category_profiles.yaml": "分类 head 参数",
    "portfolio_constraints.yaml": "仓位与替换约束",
    "execution_costs.yaml": "统一交易成本参数",
}

PATH_HELP = {
    "selection.max_selected_total": "单轮最多保留多少只正式目标 ETF。",
    "selection.max_selected_per_category": "单个类别最多保留多少只 ETF。",
    "selection.replace_threshold": "新候选分数至少高出多少，才值得替换旧仓。",
    "selection.min_hold_days_before_replace": "已知买入日期时，最少持有几天后才允许替换。",
    "budget.min_position_weight": "理论仓位低于这个值时，不形成正式目标仓位。",
    "budget.max_single_weight": "系统层面的单票仓位上限。",
    "budget.max_total_weight": "系统层面的总预算仓位上限。",
    "pullback_low_pct": "通道 A 回撤区间下沿。",
    "pullback_high_pct": "通道 A 回撤区间上沿，同时也是通道 B 的近高点判定线。",
    "breakout_entry_threshold": "通道 B 所需的 entry_score 最低阈值。",
    "rebalance_band": "权重差异低于这个值时，不做调仓。",
    "internals.default_target_holding_days": "当前用户没有单独持有周期时，执行层默认使用的天数。",
    "execution_cost_bps": "统一交易成本，单位 bps。",
    "min_trade_amount": "金额低于这个值时，不建议执行。",
    "selection.min_final_score_for_target": "最终分低于这个值时，不进入正式目标组合。",
}


class ConfigEditorService:
    def __init__(self, config_dir: Path | None = None) -> None:
        self.config_dir = config_dir or get_settings().config_dir
        self.file_order = [
            "strategy_scoring.yaml",
            "execution_overlay.yaml",
            "category_profiles.yaml",
            "portfolio_constraints.yaml",
            "execution_costs.yaml",
        ]

    def build_sections(self) -> list[dict[str, Any]]:
        sections: list[dict[str, Any]] = []
        for file_name in self.file_order:
            payload = load_yaml_config(self.config_dir / file_name)
            fields = self._flatten_fields(payload)
            sections.append(
                {
                    "file_name": file_name,
                    "file_label": FILE_LABELS.get(file_name, file_name),
                    "fields": fields,
                }
            )
        return sections

    def update_file(self, file_name: str, form_data: Mapping[str, Any]) -> None:
        if file_name not in self.file_order:
            raise ValueError("不支持更新这个配置文件。")
        path = self.config_dir / file_name
        payload = load_yaml_config(path)
        fields = self._flatten_fields(payload)
        field_lookup = {field["form_key"]: field for field in fields}
        for form_key, field in field_lookup.items():
            if form_key not in form_data:
                continue
            raw_value = form_data.get(form_key)
            parsed = self._parse_value(raw_value, field["value_type"])
            self._set_nested_value(payload, field["path"], parsed)
        with path.open("w", encoding="utf-8") as file_obj:
            yaml.safe_dump(payload, file_obj, allow_unicode=True, sort_keys=False)

    def _flatten_fields(self, payload: dict[str, Any], prefix: str = "") -> list[dict[str, Any]]:
        fields: list[dict[str, Any]] = []
        for key, value in payload.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(value, dict):
                fields.extend(self._flatten_fields(value, path))
                continue
            fields.append(
                {
                    "path": path,
                    "label": path,
                    "form_key": self._form_key(path),
                    "value": value,
                    "display_value": self._display_value(value),
                    "value_type": self._value_type(value),
                    "input_type": self._input_type(value),
                    "step": self._step(value),
                    "help_text": PATH_HELP.get(path, ""),
                }
            )
        return fields

    def _form_key(self, path: str) -> str:
        return path.replace(".", "__")

    def _display_value(self, value: Any) -> str:
        if isinstance(value, list):
            return ", ".join(str(item) for item in value)
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    def _value_type(self, value: Any) -> str:
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, int) and not isinstance(value, bool):
            return "int"
        if isinstance(value, float):
            return "float"
        if isinstance(value, list):
            return "list"
        return "str"

    def _input_type(self, value: Any) -> str:
        value_type = self._value_type(value)
        if value_type in {"int", "float"}:
            return "number"
        if value_type == "bool":
            return "select"
        return "text"

    def _step(self, value: Any) -> str:
        value_type = self._value_type(value)
        if value_type == "int":
            return "1"
        if value_type == "float":
            return "0.001"
        return "1"

    def _parse_value(self, raw_value: Any, value_type: str) -> Any:
        text = str(raw_value or "").strip()
        if value_type == "bool":
            return text.lower() in {"1", "true", "yes", "on"}
        if value_type == "int":
            return int(float(text or "0"))
        if value_type == "float":
            return float(text or "0")
        if value_type == "list":
            if not text:
                return []
            return [item.strip() for item in text.split(",") if item.strip()]
        return text

    def _set_nested_value(self, payload: dict[str, Any], path: str, value: Any) -> None:
        parts = path.split(".")
        current = payload
        for key in parts[:-1]:
            current = current.setdefault(key, {})
        current[parts[-1]] = value
