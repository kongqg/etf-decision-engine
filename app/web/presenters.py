from __future__ import annotations

import json
from typing import Any

from app.core.config import get_settings
from app.core.session_mode import SESSION_MODE_HINTS, SESSION_MODE_LABELS
from app.services.risk_mode_service import RISK_MODE_LABELS
from app.utils.formatters import dt_string, money, pct

DATA_STATUS_LABELS = {
    "ok": "正常",
    "weak": "需谨慎",
    "blocked": "已拦截",
}

ACTION_LABELS = {
    "buy": "买入",
    "sell": "卖出",
    "hold": "持有",
    "no_trade": "暂不交易",
}

ACTION_CODE_LABELS = {
    "buy_open": "开仓买入",
    "buy_add": "继续加仓",
    "hold": "继续持有",
    "sell_reduce": "减仓卖出",
    "sell_exit": "卖出退出",
    "switch": "同类换仓",
    "no_trade": "暂不交易",
}

INTENT_LABELS = {
    "open": "开仓",
    "add": "加仓",
    "hold": "持有",
    "reduce": "减仓",
    "exit": "退出",
}

MARKET_REGIME_LABELS = {
    "risk_on": "偏进攻",
    "neutral": "中性",
    "risk_off": "偏防守",
}

CATEGORY_LABELS = {
    "stock_etf": "股票ETF",
    "bond_etf": "债券ETF",
    "gold_etf": "黄金ETF",
    "cross_border_etf": "跨境ETF",
    "money_etf": "货币ETF",
}

REASON_CODE_LABELS = {
    "no_target": "暂无目标",
    "portfolio_hold": "继续持有",
    "rebalance": "调仓换仓",
    "new_entry_or_add": "开仓或加仓",
    "reduce_or_exit": "减仓或退出",
    "data_quality_not_ready": "数据未就绪",
}

POSITION_STATE_LABELS = {
    "HOLD": "继续持有",
    "REDUCE": "减仓观察",
    "EXIT": "退出",
    "NONE": "未持有",
}

ENTRY_CHANNEL_LABELS = {
    "none": "无",
    "A": "通道A：回撤后反弹",
    "B": "通道B：强趋势突破",
}


def serialize_advice_record(advice) -> dict[str, Any] | None:
    if advice is None:
        return None
    evidence = _parse_json(advice.evidence_json)
    target_portfolio = _parse_json(getattr(advice, "target_portfolio_json", "{}"))
    budget_context = _parse_json(getattr(advice, "budget_context_json", "{}"))
    candidate_summary = _parse_json(getattr(advice, "candidate_summary_json", "{}"))
    items = []
    for item in sorted(advice.items, key=lambda row: (row.rank, row.symbol)):
        raw_action = str(item.action)
        raw_action_code = str(getattr(item, "action_code", raw_action))
        raw_intent = str(getattr(item, "intent", ""))
        rationale = _parse_json(getattr(item, "rationale_json", "{}"))
        overlay = rationale.get("execution_overlay", {}) if isinstance(rationale, dict) else {}
        items.append(
            {
                "symbol": item.symbol,
                "name": item.name,
                "rank": item.rank,
                "action": ACTION_LABELS.get(raw_action, raw_action),
                "action_bucket": raw_action,
                "action_code": raw_action_code,
                "action_code_label": ACTION_CODE_LABELS.get(raw_action_code, raw_action_code),
                "intent": INTENT_LABELS.get(raw_intent, raw_intent),
                "intent_code": raw_intent,
                "category": CATEGORY_LABELS.get(str(getattr(item, "category", "")), getattr(item, "category", "")),
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
                "rationale": rationale,
                "execution_overlay": {
                    **overlay,
                    "position_state_label": POSITION_STATE_LABELS.get(str(overlay.get("position_state", "")), overlay.get("position_state", "")),
                    "entry_channel_label": ENTRY_CHANNEL_LABELS.get(str(overlay.get("entry_channel_used", "none")), overlay.get("entry_channel_used", "none")),
                },
            }
        )

    action_counts = {
        action: sum(1 for item in items if item["action_bucket"] == action)
        for action in ["buy", "sell", "hold", "no_trade"]
    }
    intent_counts = {
        intent: sum(1 for item in items if item["intent_code"] == intent)
        for intent in ["open", "add", "hold", "reduce", "exit"]
    }
    raw_action = str(advice.action)
    raw_display_action = str(getattr(advice, "display_action", raw_action))
    raw_reason_code = str(getattr(advice, "reason_code", ""))
    raw_market_regime = str(advice.market_regime)
    return {
        "id": advice.id,
        "advice_date": advice.advice_date.isoformat(),
        "created_at": advice.created_at.isoformat(),
        "session_mode": advice.session_mode,
        "action": ACTION_LABELS.get(raw_action, raw_action),
        "display_action": ACTION_LABELS.get(raw_display_action, raw_display_action),
        "action_code": getattr(advice, "action_code", advice.action),
        "reason_code": REASON_CODE_LABELS.get(raw_reason_code, raw_reason_code),
        "market_regime": MARKET_REGIME_LABELS.get(raw_market_regime, raw_market_regime),
        "market_regime_code": raw_market_regime,
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
        "candidate_summary": _serialize_candidate_summary(
            candidate_summary if isinstance(candidate_summary, list) else evidence.get("candidate_summary", [])
        ),
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
            market_regime = str(payload.get("market_regime", ""))
            if market_regime:
                payload["market_regime"] = MARKET_REGIME_LABELS.get(market_regime, market_regime)
            payload["candidate_summary"] = _serialize_candidate_summary(payload.get("candidate_summary", []))
            overall = payload
        else:
            action = str(payload.get("action", ""))
            action_code = str(payload.get("action_code", ""))
            intent = str(payload.get("intent", ""))
            if action:
                payload["action"] = ACTION_LABELS.get(action, action)
            if action_code:
                payload["action_code"] = ACTION_CODE_LABELS.get(action_code, action_code)
            if intent:
                payload["intent"] = INTENT_LABELS.get(intent, intent)
            category = str(payload.get("category", ""))
            if category:
                payload["category"] = CATEGORY_LABELS.get(category, category)
            scores = payload.get("scores", {})
            if not isinstance(scores, dict):
                scores = {}
            payload["scores"] = {
                "entry_score": float(scores.get("entry_score", 0.0) or 0.0),
                "hold_score": float(scores.get("hold_score", 0.0) or 0.0),
                "exit_score": float(scores.get("exit_score", 0.0) or 0.0),
                "decision_score": float(scores.get("decision_score", scores.get("final_score", 0.0)) or 0.0),
                "intra_score": float(scores.get("intra_score", 0.0) or 0.0),
                "category_score": float(scores.get("category_score", 0.0) or 0.0),
                "final_score": float(scores.get("final_score", 0.0) or 0.0),
            }
            overlay = payload.get("execution_overlay", {})
            if isinstance(overlay, dict):
                state = str(overlay.get("position_state", ""))
                channel = str(overlay.get("entry_channel_used", "none"))
                if state:
                    overlay["position_state"] = POSITION_STATE_LABELS.get(state, state)
                if channel:
                    overlay["entry_channel_used"] = ENTRY_CHANNEL_LABELS.get(channel, channel)
                payload["execution_overlay"] = overlay
            else:
                payload["execution_overlay"] = {}
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
    quality_status = str(quality.get("quality_status", "")).strip().lower()
    tone = "ok" if quality_status == "ok" else "warn" if quality_status == "weak" else "risk"
    return {
        "tone": tone,
        "summary": quality.get("verification_status", "暂无数据状态"),
        "badge_label": DATA_STATUS_LABELS.get(quality_status, quality.get("quality_status", "-")),
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


def _serialize_candidate_summary(rows: Any) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        return []
    payload = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        item = dict(row)
        category = str(item.get("category", ""))
        if category:
            item["category"] = CATEGORY_LABELS.get(category, category)
        payload.append(item)
    return payload
