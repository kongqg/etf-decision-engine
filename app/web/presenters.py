from __future__ import annotations

import json
from typing import Any

from app.core.session_mode import SESSION_MODE_HINTS, SESSION_MODE_LABELS
from app.utils.formatters import dt_string, money, pct


def serialize_advice_record(advice) -> dict[str, Any] | None:
    if advice is None:
        return None
    return {
        "id": advice.id,
        "advice_date": advice.advice_date.isoformat(),
        "created_at": advice.created_at.isoformat(),
        "session_mode": advice.session_mode,
        "action": advice.action,
        "market_regime": advice.market_regime,
        "target_position_pct": advice.target_position_pct,
        "current_position_pct": advice.current_position_pct,
        "summary_text": advice.summary_text,
        "risk_text": advice.risk_text,
        "evidence": json.loads(advice.evidence_json or "{}"),
        "items": [
            {
                "symbol": item.symbol,
                "name": item.name,
                "rank": item.rank,
                "action": item.action,
                "suggested_amount": item.suggested_amount,
                "suggested_pct": item.suggested_pct,
                "trigger_price_low": item.trigger_price_low,
                "trigger_price_high": item.trigger_price_high,
                "stop_loss_pct": item.stop_loss_pct,
                "take_profit_pct": item.take_profit_pct,
                "score": item.score,
                "score_gap": item.score_gap,
                "reason_short": item.reason_short,
                "risk_level": item.risk_level,
            }
            for item in advice.items
        ],
    }


def serialize_explanations(records) -> dict[str, Any]:
    overall = {}
    items = []
    for record in records:
        payload = json.loads(record.explanation_json or "{}")
        if record.scope == "overall":
            overall = payload
        else:
            items.append(payload)
    return {"overall": overall, "items": items}


def page_context(title: str, session_mode: str, status_message: str | None = None) -> dict[str, Any]:
    return {
        "page_title": title,
        "session_mode": session_mode,
        "session_button_label": SESSION_MODE_LABELS[session_mode],
        "session_hint": SESSION_MODE_HINTS[session_mode],
        "status_message": status_message,
        "fmt_money": money,
        "fmt_pct": pct,
        "fmt_dt": dt_string,
    }
