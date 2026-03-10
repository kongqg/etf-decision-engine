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
    portfolio_review_items = _normalize_recommendation_items(evidence.get("portfolio_review_items"))
    transition_plan = _normalize_recommendation_items(evidence.get("transition_plan"))
    daily_action_plan = _normalize_recommendation_items(evidence.get("daily_action_plan")) or transition_plan
    target_portfolio = evidence.get("target_portfolio", {}) if isinstance(evidence.get("target_portfolio"), dict) else {}
    action_counts = evidence.get("action_counts", {}) if isinstance(evidence.get("action_counts"), dict) else {}
    has_structured_groups = bool(recommendation_groups)
    legacy_items = [
        _normalize_recommendation_item(
            {
            "symbol": item.symbol,
            "name": item.name,
            "rank": item.rank,
            "action": item.action,
            "action_code": getattr(item, "action_code", ""),
            "position_action": "",
            "position_action_label": item.action,
            "action_reason": item.reason_short,
            "suggested_amount": item.suggested_amount,
            "suggested_pct": item.suggested_pct,
            "trigger_price_low": item.trigger_price_low,
            "trigger_price_high": item.trigger_price_high,
            "stop_loss_pct": item.stop_loss_pct,
            "take_profit_pct": item.take_profit_pct,
            "score": item.score,
            "decision_score": getattr(item, "decision_score", item.score),
            "score_gap": item.score_gap,
            "reason_short": item.reason_short,
            "risk_level": item.risk_level,
            "category": getattr(item, "category", ""),
            "asset_class": "",
            "trade_mode": getattr(item, "tradability_mode", ""),
            "tradability_mode": getattr(item, "tradability_mode", ""),
            "trade_mode_note": "",
            "execution_timing_mode": "",
            "execution_timing_label": "",
            "recommended_execution_windows": [],
            "avoid_execution_windows": [],
            "timing_note": "",
            "timing_rule_applied": False,
            "timing_display_enabled": True,
            "current_execution_phase": "",
            "entry_score": getattr(item, "entry_score", 0.0),
            "hold_score": getattr(item, "hold_score", 0.0),
            "exit_score": getattr(item, "exit_score", 0.0),
            "category_score": getattr(item, "category_score", 0.0),
            "target_holding_days": getattr(item, "target_holding_days", 5),
            "mapped_horizon_profile": getattr(item, "mapped_horizon_profile", "swing"),
            "lifecycle_phase": getattr(item, "lifecycle_phase", "build_phase"),
            "estimated_fee": 0.0,
            "estimated_cost_rate": 0.0,
            "cost_reason": "",
            "is_executable": bool(getattr(item, "executable_now", True)),
            "executable_now": bool(getattr(item, "executable_now", True)),
            "blocked_reason": getattr(item, "blocked_reason", ""),
            "planned_exit_days": getattr(item, "planned_exit_days", None),
            "planned_exit_rule_summary": getattr(item, "planned_exit_rule_summary", ""),
            "execution_status": "可执行" if getattr(item, "executable_now", True) else "等待执行",
            "recommendation_bucket": "executable_recommendations",
            "is_budget_substitute": False,
            "primary_asset_class": "",
            "budget_substitute_reason": "",
            "is_best_unaffordable": False,
            "best_unaffordable_reason": "",
            "is_affordable_but_weak": False,
            "weak_signal_reason": "",
            "scores": {
                "entry_score": getattr(item, "entry_score", 0.0),
                "hold_score": getattr(item, "hold_score", 0.0),
                "exit_score": getattr(item, "exit_score", 0.0),
                "decision_score": getattr(item, "decision_score", item.score),
                "category_score": getattr(item, "category_score", 0.0),
            },
        }
        )
        for item in advice.items
    ]
    executable_items = (
        _normalize_recommendation_items(recommendation_groups.get("executable_recommendations"))
        if has_structured_groups
        else legacy_items
    )
    best_unaffordable_recommendation = _normalize_recommendation_item(
        recommendation_groups.get("best_unaffordable_recommendation")
    )
    affordable_but_weak_items = _normalize_recommendation_items(
        recommendation_groups.get("affordable_but_weak_recommendations")
    )
    watchlist_items = _normalize_recommendation_items(recommendation_groups.get("watchlist_recommendations"))
    cost_inefficient_items = _normalize_recommendation_items(
        recommendation_groups.get("cost_inefficient_recommendations")
    )
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
        "action_code": getattr(advice, "action_code", ""),
        "mapped_horizon_profile": getattr(advice, "mapped_horizon_profile", ""),
        "lifecycle_phase": getattr(advice, "lifecycle_phase", ""),
        "category_score": getattr(advice, "category_score", 0.0),
        "executable_now": bool(getattr(advice, "executable_now", False)),
        "blocked_reason": getattr(advice, "blocked_reason", ""),
        "planned_exit_days": getattr(advice, "planned_exit_days", None),
        "planned_exit_rule_summary": getattr(advice, "planned_exit_rule_summary", ""),
        "evidence": evidence,
        "items": legacy_items if legacy_items else executable_items,
        "executable_recommendations": executable_items,
        "best_unaffordable_recommendation": best_unaffordable_recommendation,
        "affordable_but_weak_recommendations": affordable_but_weak_items,
        "watchlist_recommendations": watchlist_items,
        "cost_inefficient_recommendations": cost_inefficient_items,
        "portfolio_review_items": portfolio_review_items,
        "transition_plan": transition_plan,
        "daily_action_plan": daily_action_plan,
        "target_portfolio": target_portfolio,
        "action_counts": action_counts,
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


def _normalize_recommendation_items(items: Any) -> list[dict[str, Any]]:
    if not isinstance(items, list):
        return []
    return [normalized for item in items if (normalized := _normalize_recommendation_item(item)) is not None]


def _normalize_recommendation_item(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None

    normalized = dict(item)
    decision_score = normalized.get("decision_score", normalized.get("score", 0.0))
    normalized.setdefault("score", decision_score)
    normalized.setdefault("decision_score", decision_score)
    normalized.setdefault("symbol", "")
    normalized.setdefault("name", "")
    normalized.setdefault("rank", 0)
    normalized.setdefault("action", "")
    normalized.setdefault("action_code", "")
    normalized.setdefault("position_action", "")
    normalized.setdefault("position_action_label", normalized.get("action", ""))
    normalized.setdefault("action_reason", normalized.get("reason_short", ""))
    normalized.setdefault("suggested_amount", 0.0)
    normalized.setdefault("suggested_pct", 0.0)
    normalized.setdefault("trigger_price_low", None)
    normalized.setdefault("trigger_price_high", None)
    normalized.setdefault("stop_loss_pct", 0.0)
    normalized.setdefault("take_profit_pct", 0.0)
    normalized.setdefault("score_gap", 0.0)
    normalized.setdefault("reason_short", "")
    normalized.setdefault("risk_level", "-")
    normalized.setdefault("category", "")
    normalized.setdefault("asset_class", normalized.get("category_label", ""))
    normalized.setdefault("trade_mode", normalized.get("tradability_mode", ""))
    normalized.setdefault("tradability_mode", normalized.get("trade_mode", ""))
    normalized.setdefault("trade_mode_note", "")
    normalized.setdefault("execution_timing_mode", "")
    normalized.setdefault("execution_timing_label", "")
    normalized.setdefault("recommended_execution_windows", [])
    normalized.setdefault("avoid_execution_windows", [])
    normalized.setdefault("timing_note", "")
    normalized.setdefault("timing_rule_applied", False)
    normalized.setdefault("timing_display_enabled", True)
    normalized.setdefault("current_execution_phase", "")
    normalized.setdefault("latest_price", None)
    normalized.setdefault("lot_size", None)
    normalized.setdefault("fee_rate", 0.0)
    normalized.setdefault("min_fee", 0.0)
    normalized.setdefault("estimated_fee", 0.0)
    normalized.setdefault("estimated_cost_rate", 0.0)
    normalized.setdefault("is_cost_efficient", True)
    normalized.setdefault("cost_reason", "")
    normalized.setdefault("min_advice_amount", 0.0)
    normalized.setdefault("min_order_amount", 0.0)
    normalized.setdefault("available_cash", 0.0)
    normalized.setdefault(
        "budget_gap_to_min_order",
        max(float(normalized.get("min_order_amount", 0.0)) - float(normalized.get("suggested_amount", 0.0)), 0.0),
    )
    normalized.setdefault("is_budget_executable", bool(normalized.get("is_executable", False)))
    normalized.setdefault("passes_min_advice", True)
    normalized.setdefault("is_executable", bool(normalized.get("executable_now", False)))
    normalized.setdefault("execution_status", "可执行" if normalized["is_executable"] else "等待执行")
    normalized.setdefault("recommendation_bucket", "")
    normalized.setdefault("not_executable_reason", "")
    normalized.setdefault("execution_note", "")
    normalized.setdefault("small_account_override", False)
    normalized.setdefault("is_budget_substitute", False)
    normalized.setdefault("primary_asset_class", "")
    normalized.setdefault("budget_substitute_reason", "")
    normalized.setdefault("is_best_unaffordable", False)
    normalized.setdefault("best_unaffordable_reason", "")
    normalized.setdefault("is_affordable_but_weak", False)
    normalized.setdefault("weak_signal_reason", "")
    normalized.setdefault("asset_allocation_weight", None)
    normalized.setdefault("asset_class_signal_score", None)
    normalized.setdefault("entry_score", 0.0)
    normalized.setdefault("hold_score", 0.0)
    normalized.setdefault("exit_score", 0.0)
    normalized.setdefault("category_score", 0.0)
    normalized.setdefault("target_holding_days", 0)
    normalized.setdefault("mapped_horizon_profile", "")
    normalized.setdefault("horizon_profile_label", "")
    normalized.setdefault("lifecycle_phase", "")
    normalized.setdefault("executable_now", bool(normalized.get("is_executable", False)))
    normalized.setdefault("blocked_reason", "")
    normalized.setdefault("planned_exit_days", None)
    normalized.setdefault("planned_exit_rule_summary", "")
    normalized.setdefault("transition_label", "")
    normalized.setdefault("is_current_holding", False)
    normalized.setdefault("is_held", bool(normalized.get("is_current_holding", False)))
    normalized.setdefault("current_weight", 0.0)
    normalized.setdefault("target_weight", 0.0)
    normalized.setdefault("delta_weight", 0.0)
    normalized.setdefault("current_amount", 0.0)
    normalized.setdefault("target_amount", 0.0)
    normalized.setdefault("current_return_pct", 0.0)
    normalized.setdefault("rank_in_category", normalized.get("rank", 0))
    normalized.setdefault("previous_rank_in_category", normalized.get("rank", 0))
    normalized.setdefault("rank_drop", 0)
    normalized.setdefault("days_held", 0)
    normalized.setdefault("entry_eligible", True)
    normalized.setdefault("filter_pass", True)
    normalized.setdefault("filter_reasons", [])
    normalized.setdefault(
        "scores",
        {
            "entry_score": normalized.get("entry_score", 0.0),
            "hold_score": normalized.get("hold_score", 0.0),
            "exit_score": normalized.get("exit_score", 0.0),
            "decision_score": normalized.get("decision_score", normalized.get("score", 0.0)),
            "category_score": normalized.get("category_score", 0.0),
        },
    )
    normalized.setdefault("score_breakdown", None)
    return normalized


def merge_portfolio_with_advice(portfolio: dict[str, Any], advice: dict[str, Any] | None) -> dict[str, Any]:
    if not advice or not portfolio.get("holdings"):
        return portfolio

    review_lookup = {str(item["symbol"]): item for item in advice.get("portfolio_review_items", [])}
    merged = dict(portfolio)
    merged_holdings = []
    for row in portfolio["holdings"]:
        review_item = review_lookup.get(str(row["symbol"]))
        merged_row = dict(row)
        if review_item is not None:
            merged_row.update(
                {
                    "formal_action": review_item.get("action", row.get("last_action_suggestion", "-")),
                    "execution_status": review_item.get("execution_status", row.get("last_action_suggestion", "-")),
                    "transition_label": review_item.get("transition_label", ""),
                    "target_weight": review_item.get("target_weight", 0.0),
                    "delta_weight": review_item.get("delta_weight", 0.0),
                    "target_amount": review_item.get("target_amount", 0.0),
                    "blocked_reason": review_item.get("blocked_reason", ""),
                    "plan_note": review_item.get("execution_note", ""),
                    "advice_id": advice.get("id"),
                }
            )
        else:
            merged_row.setdefault("formal_action", row.get("last_action_suggestion", "-"))
            merged_row.setdefault("execution_status", row.get("last_action_suggestion", "-"))
            merged_row.setdefault("transition_label", "")
            merged_row.setdefault("target_weight", 0.0)
            merged_row.setdefault("delta_weight", 0.0)
            merged_row.setdefault("target_amount", 0.0)
            merged_row.setdefault("blocked_reason", "")
            merged_row.setdefault("plan_note", "")
            merged_row.setdefault("advice_id", advice.get("id"))
        merged_holdings.append(merged_row)
    merged["holdings"] = merged_holdings
    return merged


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


def serialize_advice_history(records, trade_stats: dict[int, dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    trade_stats = trade_stats or {}
    rows = []
    for advice in records:
        evidence = json.loads(advice.evidence_json or "{}")
        recommendation_groups = evidence.get("recommendation_groups", {})
        market_snapshot = evidence.get("market_snapshot", {})
        raw = market_snapshot.get("raw", {})
        source = raw.get("source", {})
        quality_summary = raw.get("quality_summary", {})
        stats = trade_stats.get(advice.id, {})
        executable_items = recommendation_groups.get("executable_recommendations") or list(advice.items)
        affordable_but_weak_items = recommendation_groups.get("affordable_but_weak_recommendations") or []
        watchlist_items = recommendation_groups.get("watchlist_recommendations") or []
        cost_inefficient_items = recommendation_groups.get("cost_inefficient_recommendations") or []
        rows.append(
            {
                "id": advice.id,
                "advice_date": advice.advice_date.isoformat(),
                "created_at": advice.created_at.isoformat(),
                "session_mode": advice.session_mode,
                "action": advice.action,
                "market_regime": advice.market_regime,
                "summary_text": advice.summary_text,
                "status": advice.status,
                "source_label": source.get("label", "-"),
                "verification_status": quality_summary.get("verification_status", "-"),
                "latest_available_date": quality_summary.get(
                    "latest_available_date",
                    source.get("trade_date", advice.advice_date.isoformat()),
                ),
                "is_demo": source.get("code") == "fallback"
                or quality_summary.get("verification_status") == "模拟数据",
                "recommendation_counts": {
                    "executable": len(executable_items),
                    "affordable_but_weak": len(affordable_but_weak_items),
                    "watchlist": len(watchlist_items),
                    "cost_inefficient": len(cost_inefficient_items),
                },
                "linked_trade_count": int(stats.get("linked_trade_count", 0)),
                "last_trade_at": stats.get("last_trade_at"),
            }
        )
    return rows


def build_data_status(snapshot=None, advice=None) -> dict[str, Any] | None:
    snapshot_raw: dict[str, Any] = {}
    advice_raw: dict[str, Any] = {}
    evidence_href: str | None = None

    if snapshot is not None:
        snapshot_raw = json.loads(snapshot.raw_json or "{}")
    if advice is not None:
        evidence = json.loads(advice.evidence_json or "{}")
        advice_raw = evidence.get("market_snapshot", {}).get("raw", {})
        evidence_href = f"/evidence/{advice.id}"

    primary_raw = snapshot_raw or advice_raw
    primary_status = _status_from_market_raw(primary_raw)
    if primary_status is None:
        return None

    payload = {
        "badge_label": primary_status["badge_label"],
        "tone": primary_status["tone"],
        "summary": primary_status["summary"],
        "source_label": primary_status["source_label"],
        "verification_status": primary_status["verification_status"],
        "data_type": primary_status["data_type"],
        "latest_available_date": primary_status["latest_available_date"],
        "captured_at": primary_status["captured_at"],
        "supports_live_execution": primary_status["supports_live_execution"],
        "freshness_note": primary_status["freshness_note"],
        "execution_note": primary_status["execution_note"],
        "evidence_href": evidence_href,
        "advice_captured_at": "",
        "advice_latest_available_date": "",
        "advice_note": "",
    }

    advice_status = _status_from_market_raw(advice_raw)
    if advice_status is not None:
        snapshot_is_newer = bool(snapshot_raw)
        advice_matches_latest = (
            advice_status["captured_at"] == primary_status["captured_at"]
            and advice_status["latest_available_date"] == primary_status["latest_available_date"]
            and advice_status["source_label"] == primary_status["source_label"]
        )
        if snapshot_is_newer and not advice_matches_latest:
            payload["advice_captured_at"] = advice_status["captured_at"]
            payload["advice_latest_available_date"] = advice_status["latest_available_date"]
            payload["advice_note"] = (
                f"当前页面里的建议仍基于 {advice_status['captured_at']} 抓取的数据；"
                "如果要让建议与最新数据同步，需要重新生成建议。"
            )

    return payload


def _status_from_market_raw(raw: dict[str, Any]) -> dict[str, Any] | None:
    source = raw.get("source", {})
    quality_summary = raw.get("quality_summary", {})
    if not source and not quality_summary:
        return None

    verification_status = quality_summary.get("verification_status", "-")
    source_code = str(source.get("code") or "").strip()
    is_demo = source_code == "fallback" or verification_status == "模拟数据"
    is_mixed = source_code == "mixed" or verification_status == "部分真实、部分回退"
    latest_available_date = quality_summary.get("latest_available_date", source.get("trade_date", "-"))
    return {
        "badge_label": "演示数据" if is_demo else "混合数据" if is_mixed else "真实日线",
        "tone": "warning" if is_demo or is_mixed else "neutral",
        "summary": (
            "当前页面基于模拟数据演示，不代表真实市场。"
            if is_demo
            else "当前页面基于部分真实、部分补全的数据。"
            if is_mixed
            else f"当前页面基于截至 {latest_available_date} 的日线数据。"
        ),
        "source_label": source.get("label", "-"),
        "verification_status": verification_status,
        "data_type": source.get("data_type", quality_summary.get("data_type", "-")),
        "latest_available_date": latest_available_date,
        "captured_at": str(source.get("captured_at", "-")).replace("T", " ")[:16],
        "supports_live_execution": bool(quality_summary.get("supports_live_execution", False)),
        "freshness_note": quality_summary.get("freshness_label") or source.get("note", ""),
        "execution_note": quality_summary.get("live_execution_note", ""),
    }


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
        "data_status": None,
        "fmt_money": money,
        "fmt_pct": pct,
        "fmt_dt": dt_string,
    }
