from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.models import UserPreferences, UserProfile


def get_user(session: Session) -> UserProfile | None:
    settings = get_settings()
    return session.get(UserProfile, settings.default_user_id)


def get_preferences(session: Session) -> UserPreferences | None:
    user = get_user(session)
    if user is None:
        return None
    return session.scalar(select(UserPreferences).where(UserPreferences.user_id == user.id))
