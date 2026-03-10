from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings, load_yaml_config
from app.db.models import ETFUniverse


def seed_universe(session: Session) -> None:
    settings = get_settings()
    config = load_yaml_config(settings.config_dir / "etf_universe.yaml")
    existing_rows = {
        row.symbol: row for row in session.scalars(select(ETFUniverse))
    }

    for item in config.get("etfs", []):
        existing = existing_rows.get(item["symbol"])
        if existing is None:
            session.add(ETFUniverse(**item))
            continue
        for key, value in item.items():
            setattr(existing, key, value)

    session.commit()
