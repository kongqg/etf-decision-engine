from __future__ import annotations

from datetime import date

from sqlalchemy import delete, desc, select
from sqlalchemy.orm import Session

from app.db.models import ETFFeature, ETFUniverse, MarketSnapshot


def list_universe(session: Session) -> list[ETFUniverse]:
    return list(session.scalars(select(ETFUniverse).where(ETFUniverse.enabled.is_(True)).order_by(ETFUniverse.symbol)))


def replace_features_for_date(session: Session, trade_date: date, feature_rows: list[ETFFeature]) -> None:
    session.execute(delete(ETFFeature).where(ETFFeature.trade_date == trade_date))
    session.add_all(feature_rows)


def add_market_snapshot(session: Session, snapshot: MarketSnapshot) -> None:
    session.add(snapshot)


def get_latest_market_snapshot(session: Session) -> MarketSnapshot | None:
    return session.scalar(select(MarketSnapshot).order_by(desc(MarketSnapshot.captured_at)))


def get_features_by_trade_date(session: Session, trade_date: date) -> list[ETFFeature]:
    return list(
        session.scalars(
            select(ETFFeature)
            .where(ETFFeature.trade_date == trade_date)
            .order_by(ETFFeature.total_score.desc(), ETFFeature.symbol)
        )
    )


def get_latest_trade_date(session: Session) -> date | None:
    return session.scalar(select(ETFFeature.trade_date).order_by(ETFFeature.trade_date.desc()))
