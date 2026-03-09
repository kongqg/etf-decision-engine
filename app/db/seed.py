from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings, load_yaml_config
from app.db.models import ETFUniverse


def seed_universe(session: Session) -> None:
    settings = get_settings()
    config = load_yaml_config(settings.config_dir / "etf_universe.yaml")
    existing_symbols = {
        row[0] for row in session.execute(select(ETFUniverse.symbol)).all()
    }

    for item in config.get("etfs", []):
        if item["symbol"] in existing_symbols:
            continue
        session.add(ETFUniverse(**item))

    session.commit()
