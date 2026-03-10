from __future__ import annotations

from datetime import datetime, time
from typing import Any

from app.core.config import get_settings, load_yaml_config


class ExecutionTimingService:
    def __init__(self) -> None:
        settings = get_settings()
        self.settings = settings
        rules = load_yaml_config(settings.config_dir / "risk_rules.yaml")
        self.rules = rules.get("execution_timing", {})
        self.asset_class_modes = self.rules.get("asset_class_modes", {})
        self.display_by_asset_class = self.rules.get("display_by_asset_class", {})
        self.asset_class_rules = self.rules.get("asset_classes", {})

    def annotate_items(
        self,
        items: list[dict[str, Any]],
        session_mode: str,
        now: datetime,
    ) -> list[dict[str, Any]]:
        return [self.annotate_item(item, session_mode, now) for item in items]

    def annotate_item(
        self,
        item: dict[str, Any] | None,
        session_mode: str,
        now: datetime,
    ) -> dict[str, Any] | None:
        if item is None:
            return None

        asset_class = str(item.get("asset_class") or "股票")
        mode = str(self.asset_class_modes.get(asset_class, "generic"))
        class_rules = self.asset_class_rules.get(asset_class, {})
        show_timing = bool(self.display_by_asset_class.get(asset_class, True)) and self.settings.show_timing_suggestions
        timing_rule_applied = bool(class_rules.get("timing_rule_applied", False)) and self.settings.timing_optimization_enabled
        current_phase = self._current_phase(session_mode=session_mode, now=now)

        recommended_windows = list(class_rules.get("recommended_execution_windows", [])) if show_timing else []
        avoid_windows = list(class_rules.get("avoid_execution_windows", [])) if show_timing else []
        timing_note = self._timing_note(class_rules=class_rules, current_phase=current_phase, session_mode=session_mode)
        if not show_timing:
            timing_note = ""
            recommended_windows = []
            avoid_windows = []
            timing_rule_applied = False

        payload = dict(item)
        payload.update(
            {
                "execution_timing_mode": mode,
                "execution_timing_label": str(class_rules.get("label", "执行提示")),
                "recommended_execution_windows": recommended_windows,
                "avoid_execution_windows": avoid_windows,
                "timing_note": timing_note,
                "timing_rule_applied": timing_rule_applied,
                "timing_display_enabled": show_timing,
                "current_execution_phase": current_phase,
            }
        )
        return payload

    def _timing_note(self, class_rules: dict[str, Any], current_phase: str, session_mode: str) -> str:
        notes = class_rules.get("notes", {})
        return str(
            notes.get(current_phase)
            or notes.get(session_mode)
            or notes.get("default")
            or ""
        )

    def _current_phase(self, session_mode: str, now: datetime) -> str:
        current_time = now.time()

        if session_mode == "closed":
            return "closed"
        if session_mode == "after_close":
            return "after_close"
        if session_mode == "preopen":
            if time(11, 30) <= current_time < time(13, 0):
                return "midday_break"
            return "preopen"
        if time(9, 30) <= current_time < time(9, 35):
            return "early_open"
        if time(9, 35) <= current_time <= time(10, 30):
            return "morning_window"
        if time(10, 30) < current_time < time(11, 30):
            return "late_morning"
        if time(11, 30) <= current_time < time(13, 0):
            return "midday_break"
        if time(13, 0) <= current_time < time(13, 30):
            return "midday_break"
        if time(13, 30) <= current_time <= time(14, 30):
            return "afternoon_window"
        if time(14, 30) < current_time <= time(15, 0):
            return "late_session"
        return session_mode
