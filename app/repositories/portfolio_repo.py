from __future__ import annotations

from sqlalchemy import desc, func, or_, select
from sqlalchemy.orm import Session

from app.db.models import CapitalFlow, PerformanceSnapshot, Position, Trade


def list_positions(session: Session) -> list[Position]:
    return list(session.scalars(select(Position).where(Position.quantity > 0).order_by(Position.market_value.desc())))


def get_position_by_symbol(session: Session, symbol: str) -> Position | None:
    normalized_symbol = symbol.strip()
    return session.scalar(
        select(Position).where(
            or_(
                Position.symbol == normalized_symbol,
                func.trim(Position.symbol) == normalized_symbol,
            )
        )
    )


def list_trades(session: Session, limit: int = 100) -> list[Trade]:
    return list(session.scalars(select(Trade).order_by(desc(Trade.executed_at)).limit(limit)))


def list_snapshots(session: Session, limit: int = 180) -> list[PerformanceSnapshot]:
    return list(session.scalars(select(PerformanceSnapshot).order_by(PerformanceSnapshot.snapshot_date).limit(limit)))


def list_capital_flows(session: Session, limit: int = 100) -> list[CapitalFlow]:
    return list(session.scalars(select(CapitalFlow).order_by(desc(CapitalFlow.executed_at)).limit(limit)))


def trade_stats_by_advice(session: Session) -> dict[int, dict[str, object]]:
    rows = session.execute(
        select(Trade.related_advice_id, func.count(Trade.id), func.max(Trade.executed_at))
        .where(Trade.related_advice_id.is_not(None))
        .group_by(Trade.related_advice_id)
    ).all()
    return {
        int(advice_id): {
            "linked_trade_count": int(count),
            "last_trade_at": executed_at.isoformat() if executed_at is not None else None,
        }
        for advice_id, count, executed_at in rows
        if advice_id is not None
    }
