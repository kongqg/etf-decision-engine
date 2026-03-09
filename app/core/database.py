from __future__ import annotations

from functools import lru_cache

from sqlalchemy import create_engine
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

    Base.metadata.create_all(bind=get_engine())
    with get_session_local()() as session:
        seed_universe(session)


def get_db():
    db = get_session_local()()
    try:
        yield db
    finally:
        db.close()
