from __future__ import annotations

from sqlalchemy import desc, select
from sqlalchemy.orm import Session, selectinload

from app.db.models import AdviceRecord, ExplanationRecord


def get_latest_advice(session: Session) -> AdviceRecord | None:
    return session.scalar(
        select(AdviceRecord)
        .options(selectinload(AdviceRecord.items), selectinload(AdviceRecord.explanations))
        .order_by(desc(AdviceRecord.created_at))
    )


def list_advices(session: Session, limit: int = 50) -> list[AdviceRecord]:
    return list(
        session.scalars(
            select(AdviceRecord)
            .options(selectinload(AdviceRecord.items), selectinload(AdviceRecord.explanations))
            .order_by(desc(AdviceRecord.created_at))
            .limit(limit)
        )
    )


def get_advice_by_id(session: Session, advice_id: int) -> AdviceRecord | None:
    return session.scalar(
        select(AdviceRecord)
        .options(selectinload(AdviceRecord.items), selectinload(AdviceRecord.explanations))
        .where(AdviceRecord.id == advice_id)
    )


def get_explanations_by_advice(session: Session, advice_id: int) -> list[ExplanationRecord]:
    return list(
        session.scalars(
            select(ExplanationRecord)
            .where(ExplanationRecord.advice_id == advice_id)
            .order_by(ExplanationRecord.scope, ExplanationRecord.symbol)
        )
    )
