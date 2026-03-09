from __future__ import annotations

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.db.models import PerformanceSnapshot, Position, Trade


def list_positions(session: Session) -> list[Position]:
    return list(session.scalars(select(Position).where(Position.quantity > 0).order_by(Position.market_value.desc())))


def get_position_by_symbol(session: Session, symbol: str) -> Position | None:
    return session.scalar(select(Position).where(Position.symbol == symbol))


def list_trades(session: Session, limit: int = 100) -> list[Trade]:
    return list(session.scalars(select(Trade).order_by(desc(Trade.executed_at)).limit(limit)))


def list_snapshots(session: Session, limit: int = 180) -> list[PerformanceSnapshot]:
    return list(session.scalars(select(PerformanceSnapshot).order_by(PerformanceSnapshot.snapshot_date).limit(limit)))
