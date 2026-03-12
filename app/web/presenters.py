from __future__ import annotations

import json
from typing import Any

from app.core.config import get_settings
from app.core.session_mode import SESSION_MODE_HINTS, SESSION_MODE_LABELS
from app.services.risk_mode_service import RISK_MODE_LABELS
from app.utils.formatters import dt_string, money, pct


def serialize_advice_record(advice) -> dict[str, Any] | None:
    if advice is None:
        return None
    evidence = _parse_json(advice.evidence_json)
    target_portfolio = _parse_json(getattr(advice, "target_portfolio_json", "{}"))
    budget_context = _parse_json(getattr(advice, "budget_context_json", "{}"))
    candidate_summary = _parse_json(getattr(advice, "candidate_summary_json", "{}"))
    items = []
    for item in sorted(advice.items, key=lambda row: (row.rank, row.symbol)):
        items.append(
            {
                "symbol": item.symbol,
                "name": item.name,
                "rank": item.rank,
                "action": item.action,
                "intent": getattr(item, "intent", ""),
                "category": getattr(item, "category", ""),
                "current_weight": float(getattr(item, "current_weight", 0.0)),
                "target_weight": float(getattr(item, "target_weight", 0.0)),
                "delta_weight": float(getattr(item, "delta_weight", 0.0)),
                "current_amount": float(getattr(item, "current_amount", 0.0)),
                "target_amount": float(getattr(item, "target_amount", 0.0)),
                "suggested_amount": float(item.suggested_amount),
                "suggested_pct": float(item.suggested_pct),
                "final_score": float(getattr(item, "final_score", item.score)),
                "intra_score": float(getattr(item, "intra_score", 0.0)),
                "category_score": float(getattr(item, "category_score", 0.0)),
                "global_rank": int(getattr(item, "global_rank", item.rank) or item.rank),
                "category_rank": int(getattr(item, "category_rank", 0) or 0),
                "score_gap_vs_holding": float(getattr(item, "score_gap_vs_holding", 0.0)),
                "replace_threshold_used": float(getattr(item, "replace_threshold_used", 0.0)),
                "replacement_symbol": str(getattr(item, "replacement_symbol", "")),
                "hold_days": int(getattr(item, "hold_days", 0) or 0),
                "reason_short": item.reason_short,
                "risk_level": item.risk_level,
                "score_breakdown": _parse_json(getattr(item, "score_breakdown_json", "{}")),
                "rationale": _parse_json(getattr(item, "rationale_json", "{}")),
            }
        )

    action_counts = {
        action: sum(1 for item in items if item["action"] == action)
        for action in ["buy", "sell", "hold", "no_trade"]
    }
    intent_counts = {
        intent: sum(1 for item in items if item["intent"] == intent)
        for intent in ["open", "add", "hold", "reduce", "exit"]
    }
    return {
        "id": advice.id,
        "advice_date": advice.advice_date.isoformat(),
        "created_at": advice.created_at.isoformat(),
        "session_mode": advice.session_mode,
        "action": advice.action,
        "display_action": getattr(advice, "display_action", advice.action),
        "action_code": getattr(advice, "action_code", advice.action),
        "reason_code": getattr(advice, "reason_code", ""),
        "market_regime": advice.market_regime,
        "target_position_pct": advice.target_position_pct,
        "current_position_pct": advice.current_position_pct,
        "summary_text": advice.summary_text,
        "risk_text": advice.risk_text,
        "evidence": evidence,
        "items": items,
        "action_counts": action_counts,
        "intent_counts": intent_counts,
        "target_portfolio": target_portfolio,
        "budget_context": budget_context,
        "candidate_summary": candidate_summary if isinstance(candidate_summary, list) else evidence.get("candidate_summary", []),
        "recommendation_counts": {
            "tradable": len(items),
            "buy": action_counts["buy"],
            "sell": action_counts["sell"],
            "hold": action_counts["hold"],
        },
    }


def merge_portfolio_with_advice(portfolio: dict[str, Any], advice: dict[str, Any] | None) -> dict[str, Any]:
    if not advice:
        return portfolio
    lookup = {item["symbol"]: item for item in advice.get("items", [])}
    merged = dict(portfolio)
    merged_holdings = []
    for row in portfolio.get("holdings", []):
        item = lookup.get(str(row["symbol"]))
        merged_row = dict(row)
        if item is not None:
            merged_row.update(
                {
                    "formal_action": item["action"],
                    "intent": item["intent"],
                    "target_weight": item["target_weight"],
                    "delta_weight": item["delta_weight"],
                    "target_amount": item["target_amount"],
                    "plan_note": item["reason_short"],
                    "advice_id": advice.get("id"),
                }
            )
        merged_holdings.append(merged_row)
    merged["holdings"] = merged_holdings
    return merged


def serialize_explanations(records) -> dict[str, Any]:
    overall = {}
    items = []
    for record in records:
        payload = _parse_json(record.explanation_json)
        if record.scope == "overall":
            overall = payload
        else:
            items.append(payload)
    return {"overall": overall, "items": items}


def serialize_advice_history(records, trade_stats: dict[int, dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    stats_lookup = trade_stats or {}
    rows = []
    for advice in records:
        payload = serialize_advice_record(advice)
        if payload is None:
            continue
        quality_summary = payload["evidence"].get("data_quality_gate", {}).get("summary", {})
        row = dict(payload)
        row.update(stats_lookup.get(advice.id, {}))
        row["source_label"] = payload["evidence"].get("market_snapshot", {}).get("source", {}).get("label", "-")
        row["verification_status"] = quality_summary.get("verification_status", "-")
        row["latest_available_date"] = quality_summary.get("latest_available_date", "-")
        rows.append(row)
    return rows


def build_data_status(snapshot=None, advice=None) -> dict[str, Any] | None:
    if snapshot is None:
        return None
    raw = _parse_json(getattr(snapshot, "raw_json", "{}"))
    quality = raw.get("quality_summary", {})
    source = raw.get("source", {})
    tone = "ok" if quality.get("quality_status") == "ok" else "warn" if quality.get("quality_status") == "weak" else "risk"
    return {
        "tone": tone,
        "summary": quality.get("verification_status", "No data status"),
        "badge_label": quality.get("quality_status", "-"),
        "source_label": source.get("label", "-"),
        "verification_status": quality.get("verification_status", "-"),
        "data_type": quality.get("data_type", source.get("data_type", "-")),
        "latest_available_date": quality.get("latest_available_date", "-"),
        "captured_at": source.get("captured_at", "-").replace("T", " ")[:16],
        "supports_live_execution": quality.get("supports_live_execution", False),
        "freshness_note": quality.get("freshness_label", ""),
        "execution_note": quality.get("live_execution_note", ""),
        "advice_captured_at": advice.created_at.isoformat().replace("T", " ")[:16] if advice else None,
        "advice_note": advice.summary_text if advice else "",
        "evidence_href": f"/evidence/{advice.id}" if advice else "/evidence",
    }


def page_context(title: str, session_mode: str, status_message: str | None = None) -> dict[str, Any]:
    settings = get_settings()
    return {
        "page_title": title,
        "status_message": status_message,
        "session_mode": session_mode,
        "session_button_label": SESSION_MODE_LABELS.get(session_mode, session_mode),
        "session_hint": SESSION_MODE_HINTS.get(session_mode, ""),
        "fmt_money": money,
        "fmt_pct": pct,
        "fmt_dt": dt_string,
        "risk_mode_labels": RISK_MODE_LABELS,
        "max_fee_rate_for_execution": settings.max_fee_rate_for_execution,
    }


def _parse_json(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return {}

