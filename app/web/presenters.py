from __future__ import annotations

import json
from typing import Any

from app.core.config import get_settings
from app.core.session_mode import SESSION_MODE_HINTS, SESSION_MODE_LABELS
from app.utils.formatters import dt_string, money, pct


def serialize_advice_record(advice) -> dict[str, Any] | None:
    if advice is None:
        return None
    evidence = json.loads(advice.evidence_json or "{}")
    recommendation_groups = evidence.get("recommendation_groups", {})
    legacy_items = [
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
            "asset_class": "",
            "trade_mode": "",
            "trade_mode_note": "",
            "execution_timing_mode": "",
            "execution_timing_label": "",
            "recommended_execution_windows": [],
            "avoid_execution_windows": [],
            "timing_note": "",
            "timing_rule_applied": False,
            "timing_display_enabled": True,
            "current_execution_phase": "",
            "estimated_fee": 0.0,
            "estimated_cost_rate": 0.0,
            "cost_reason": "",
            "is_executable": True,
            "execution_status": "可执行",
            "recommendation_bucket": "executable_recommendations",
            "is_budget_substitute": False,
            "primary_asset_class": "",
            "budget_substitute_reason": "",
            "is_best_unaffordable": False,
            "best_unaffordable_reason": "",
            "is_affordable_but_weak": False,
            "weak_signal_reason": "",
        }
        for item in advice.items
    ]
    executable_items = recommendation_groups.get("executable_recommendations") or legacy_items
    best_unaffordable_recommendation = recommendation_groups.get("best_unaffordable_recommendation")
    affordable_but_weak_items = recommendation_groups.get("affordable_but_weak_recommendations") or []
    watchlist_items = recommendation_groups.get("watchlist_recommendations") or []
    cost_inefficient_items = recommendation_groups.get("cost_inefficient_recommendations") or []
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
        "evidence": evidence,
        "items": executable_items,
        "executable_recommendations": executable_items,
        "best_unaffordable_recommendation": best_unaffordable_recommendation,
        "affordable_but_weak_recommendations": affordable_but_weak_items,
        "watchlist_recommendations": watchlist_items,
        "cost_inefficient_recommendations": cost_inefficient_items,
        "show_watchlist_recommendations": recommendation_groups.get("show_watchlist_recommendations", True),
        "show_cost_inefficient_recommendations": recommendation_groups.get("show_cost_inefficient_recommendations", True),
        "budget_filter_enabled": recommendation_groups.get("budget_filter_enabled", True),
        "fee_filter_enabled": recommendation_groups.get("fee_filter_enabled", True),
        "recommendation_counts": {
            "executable": len(executable_items),
            "affordable_but_weak": len(affordable_but_weak_items),
            "watchlist": len(watchlist_items),
            "cost_inefficient": len(cost_inefficient_items),
        },
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
    settings = get_settings()
    return {
        "page_title": title,
        "session_mode": session_mode,
        "session_button_label": SESSION_MODE_LABELS[session_mode],
        "session_hint": SESSION_MODE_HINTS[session_mode],
        "status_message": status_message,
        "default_min_advice_amount": settings.default_min_advice_amount,
        "default_lot_size": settings.default_lot_size,
        "show_watchlist_recommendations": settings.show_watchlist_recommendations,
        "show_cost_inefficient_recommendations": settings.show_cost_inefficient_recommendations,
        "default_fee_rate": settings.default_fee_rate,
        "default_min_fee": settings.default_min_fee,
        "max_fee_rate_for_execution": settings.max_fee_rate_for_execution,
        "fmt_money": money,
        "fmt_pct": pct,
        "fmt_dt": dt_string,
    }
