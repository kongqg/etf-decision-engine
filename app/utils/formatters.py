from __future__ import annotations

from datetime import datetime


def money(value: float) -> str:
    return f"¥{value:,.2f}"


def pct(value: float) -> str:
    return f"{value:.2f}%"


def dt_string(value: datetime | None) -> str:
    if value is None:
        return "-"
    return value.strftime("%Y-%m-%d %H:%M")
