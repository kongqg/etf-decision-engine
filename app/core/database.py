from __future__ import annotations

from functools import lru_cache

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.db.base import Base


@lru_cache(maxsize=1)
def get_engine():
    settings = get_settings()
    return create_engine(
        settings.database_url,
        connect_args={"check_same_thread": False} if settings.database_url.startswith("sqlite") else {},
    )


def get_session_local():
    return sessionmaker(bind=get_engine(), autoflush=False, autocommit=False, class_=Session)


def init_db() -> None:
    from app.db.seed import seed_universe

    engine = get_engine()
    Base.metadata.create_all(bind=engine)
    _run_schema_compatibility_migrations(engine)
    with get_session_local()() as session:
        seed_universe(session)


def get_db():
    db = get_session_local()()
    try:
        yield db
    finally:
        db.close()


def _run_schema_compatibility_migrations(engine) -> None:
    inspector = inspect(engine)
    if not inspector.has_table("etf_universe"):
        return

    existing_columns = {column["name"] for column in inspector.get_columns("etf_universe")}
    required_columns = {
        "asset_class": "ALTER TABLE etf_universe ADD COLUMN asset_class VARCHAR(20) NOT NULL DEFAULT '股票'",
        "trade_mode": "ALTER TABLE etf_universe ADD COLUMN trade_mode VARCHAR(10) NOT NULL DEFAULT 'T+1'",
        "lot_size": "ALTER TABLE etf_universe ADD COLUMN lot_size FLOAT NOT NULL DEFAULT 100",
        "fee_rate": "ALTER TABLE etf_universe ADD COLUMN fee_rate FLOAT NOT NULL DEFAULT 0.0003",
        "min_fee": "ALTER TABLE etf_universe ADD COLUMN min_fee FLOAT NOT NULL DEFAULT 1.0",
    }
    pending = [sql for name, sql in required_columns.items() if name not in existing_columns]
    if not pending:
        return

    with engine.begin() as connection:
        for sql in pending:
            connection.execute(text(sql))
