from __future__ import annotations

import json
from typing import Any

from sqlalchemy.orm import Session

from app.repositories.advice_repo import get_advice_by_id, get_latest_advice
from app.repositories.market_repo import get_features_by_trade_date

QUALITY_STATUS_LABELS = {
    "ok": "正常",
    "weak": "需谨慎",
    "blocked": "已拦截",
}


class DataEvidenceService:
    def build(self, session: Session, advice_id: int | None = None) -> dict[str, Any]:
        advice = get_advice_by_id(session, advice_id) if advice_id is not None else get_latest_advice(session)
        if advice is None:
            raise ValueError("当前还没有可用的建议记录。")

        evidence = self._parse_json(advice.evidence_json)
        market_snapshot = evidence.get("market_snapshot", {})
        quality_summary = dict(evidence.get("data_quality_gate", {}).get("summary", {}))
        quality_status = str(quality_summary.get("quality_status", ""))
        if quality_status:
            quality_summary["quality_status_label"] = QUALITY_STATUS_LABELS.get(quality_status, quality_status)
        feature_rows = get_features_by_trade_date(session, advice.advice_date)

        top_features = []
        for row in feature_rows[:8]:
            breakdown = self._parse_json(getattr(row, "score_breakdown_json", "{}"))
            top_features.append(
                {
                    "symbol": row.symbol,
                    "final_score": float(getattr(row, "final_score", 0.0)),
                    "intra_score": float(getattr(row, "intra_score", 0.0)),
                    "category_score": float(getattr(row, "category_score", 0.0)),
                    "global_rank": int(getattr(row, "global_rank", 0) or 0),
                    "category_rank": int(getattr(row, "category_rank", 0) or 0),
                    "features": breakdown.get("features", {}),
                    "ranks": breakdown.get("ranks", {}),
                    "basic_filter_pass": bool(getattr(row, "basic_filter_pass", False)),
                    "basic_filter_reason": str(getattr(row, "basic_filter_reason", "")),
                }
            )

        return {
            "advice_id": advice.id,
            "headline": advice.summary_text,
            "action": advice.action,
            "market_regime": advice.market_regime,
            "trade_date": advice.advice_date.isoformat(),
            "created_at": advice.created_at.isoformat(),
            "quality_summary": quality_summary,
            "budget_context": self._parse_json(getattr(advice, "budget_context_json", "{}")),
            "target_portfolio": self._parse_json(getattr(advice, "target_portfolio_json", "{}")),
            "candidate_summary": evidence.get("candidate_summary", []),
            "category_scores": evidence.get("category_scores", []),
            "top_features": top_features,
            "market_snapshot": market_snapshot,
        }

    def _parse_json(self, value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str) and value.strip():
            try:
                loaded = json.loads(value)
            except json.JSONDecodeError:
                return {}
            return loaded if isinstance(loaded, dict) else {}
        return {}
