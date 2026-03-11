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
    required_columns_by_table = {
        "etf_universe": {
            "asset_class": "ALTER TABLE etf_universe ADD COLUMN asset_class VARCHAR(20) NOT NULL DEFAULT '股票'",
            "trade_mode": "ALTER TABLE etf_universe ADD COLUMN trade_mode VARCHAR(10) NOT NULL DEFAULT 'T+1'",
            "lot_size": "ALTER TABLE etf_universe ADD COLUMN lot_size FLOAT NOT NULL DEFAULT 100",
            "fee_rate": "ALTER TABLE etf_universe ADD COLUMN fee_rate FLOAT NOT NULL DEFAULT 0.0003",
            "min_fee": "ALTER TABLE etf_universe ADD COLUMN min_fee FLOAT NOT NULL DEFAULT 1.0",
        },
        "user_preferences": {
            "risk_mode": "ALTER TABLE user_preferences ADD COLUMN risk_mode VARCHAR(20) NOT NULL DEFAULT 'balanced'",
            "target_holding_days": "ALTER TABLE user_preferences ADD COLUMN target_holding_days INTEGER NOT NULL DEFAULT 5",
            "max_total_position_pct": "ALTER TABLE user_preferences ADD COLUMN max_total_position_pct FLOAT NOT NULL DEFAULT 0.7",
            "max_single_position_pct": "ALTER TABLE user_preferences ADD COLUMN max_single_position_pct FLOAT NOT NULL DEFAULT 0.35",
            "cash_reserve_pct": "ALTER TABLE user_preferences ADD COLUMN cash_reserve_pct FLOAT NOT NULL DEFAULT 0.2",
        },
        "etf_features": {
            "momentum_20d": "ALTER TABLE etf_features ADD COLUMN momentum_20d FLOAT NOT NULL DEFAULT 0.0",
            "ma5": "ALTER TABLE etf_features ADD COLUMN ma5 FLOAT NOT NULL DEFAULT 0.0",
            "ma10": "ALTER TABLE etf_features ADD COLUMN ma10 FLOAT NOT NULL DEFAULT 0.0",
            "ma20": "ALTER TABLE etf_features ADD COLUMN ma20 FLOAT NOT NULL DEFAULT 0.0",
            "ret_1d": "ALTER TABLE etf_features ADD COLUMN ret_1d FLOAT NOT NULL DEFAULT 0.0",
            "volatility_5d": "ALTER TABLE etf_features ADD COLUMN volatility_5d FLOAT NOT NULL DEFAULT 0.0",
            "volatility_20d": "ALTER TABLE etf_features ADD COLUMN volatility_20d FLOAT NOT NULL DEFAULT 0.0",
            "rolling_max_20d": "ALTER TABLE etf_features ADD COLUMN rolling_max_20d FLOAT NOT NULL DEFAULT 0.0",
            "avg_turnover_20d": "ALTER TABLE etf_features ADD COLUMN avg_turnover_20d FLOAT NOT NULL DEFAULT 0.0",
            "category_return_10d": "ALTER TABLE etf_features ADD COLUMN category_return_10d FLOAT NOT NULL DEFAULT 0.0",
            "relative_strength_10d": "ALTER TABLE etf_features ADD COLUMN relative_strength_10d FLOAT NOT NULL DEFAULT 0.0",
            "above_ma20_flag": "ALTER TABLE etf_features ADD COLUMN above_ma20_flag BOOLEAN NOT NULL DEFAULT 0",
            "decision_category": "ALTER TABLE etf_features ADD COLUMN decision_category VARCHAR(30) NOT NULL DEFAULT ''",
            "tradability_mode": "ALTER TABLE etf_features ADD COLUMN tradability_mode VARCHAR(10) NOT NULL DEFAULT ''",
        },
        "advice_records": {
            "action_code": "ALTER TABLE advice_records ADD COLUMN action_code VARCHAR(30) NOT NULL DEFAULT ''",
            "winning_category": "ALTER TABLE advice_records ADD COLUMN winning_category VARCHAR(30) NOT NULL DEFAULT ''",
            "target_holding_days": "ALTER TABLE advice_records ADD COLUMN target_holding_days INTEGER NOT NULL DEFAULT 5",
            "mapped_horizon_profile": "ALTER TABLE advice_records ADD COLUMN mapped_horizon_profile VARCHAR(30) NOT NULL DEFAULT 'swing'",
            "lifecycle_phase": "ALTER TABLE advice_records ADD COLUMN lifecycle_phase VARCHAR(30) NOT NULL DEFAULT 'build_phase'",
            "category_score": "ALTER TABLE advice_records ADD COLUMN category_score FLOAT NOT NULL DEFAULT 0.0",
            "executable_now": "ALTER TABLE advice_records ADD COLUMN executable_now BOOLEAN NOT NULL DEFAULT 1",
            "blocked_reason": "ALTER TABLE advice_records ADD COLUMN blocked_reason TEXT NOT NULL DEFAULT ''",
            "planned_exit_days": "ALTER TABLE advice_records ADD COLUMN planned_exit_days INTEGER",
            "planned_exit_rule_summary": "ALTER TABLE advice_records ADD COLUMN planned_exit_rule_summary TEXT NOT NULL DEFAULT ''",
        },
        "advice_items": {
            "action_code": "ALTER TABLE advice_items ADD COLUMN action_code VARCHAR(30) NOT NULL DEFAULT ''",
            "category": "ALTER TABLE advice_items ADD COLUMN category VARCHAR(30) NOT NULL DEFAULT ''",
            "tradability_mode": "ALTER TABLE advice_items ADD COLUMN tradability_mode VARCHAR(10) NOT NULL DEFAULT ''",
            "target_holding_days": "ALTER TABLE advice_items ADD COLUMN target_holding_days INTEGER NOT NULL DEFAULT 5",
            "mapped_horizon_profile": "ALTER TABLE advice_items ADD COLUMN mapped_horizon_profile VARCHAR(30) NOT NULL DEFAULT 'swing'",
            "lifecycle_phase": "ALTER TABLE advice_items ADD COLUMN lifecycle_phase VARCHAR(30) NOT NULL DEFAULT 'build_phase'",
            "entry_score": "ALTER TABLE advice_items ADD COLUMN entry_score FLOAT NOT NULL DEFAULT 0.0",
            "hold_score": "ALTER TABLE advice_items ADD COLUMN hold_score FLOAT NOT NULL DEFAULT 0.0",
            "exit_score": "ALTER TABLE advice_items ADD COLUMN exit_score FLOAT NOT NULL DEFAULT 0.0",
            "category_score": "ALTER TABLE advice_items ADD COLUMN category_score FLOAT NOT NULL DEFAULT 0.0",
            "decision_score": "ALTER TABLE advice_items ADD COLUMN decision_score FLOAT NOT NULL DEFAULT 0.0",
            "executable_now": "ALTER TABLE advice_items ADD COLUMN executable_now BOOLEAN NOT NULL DEFAULT 1",
            "blocked_reason": "ALTER TABLE advice_items ADD COLUMN blocked_reason TEXT NOT NULL DEFAULT ''",
            "planned_exit_days": "ALTER TABLE advice_items ADD COLUMN planned_exit_days INTEGER",
            "planned_exit_rule_summary": "ALTER TABLE advice_items ADD COLUMN planned_exit_rule_summary TEXT NOT NULL DEFAULT ''",
        },
    }

    inspector = inspect(engine)
    with engine.begin() as connection:
        for table_name, required_columns in required_columns_by_table.items():
            if not inspector.has_table(table_name):
                continue
            existing_columns = {column["name"] for column in inspector.get_columns(table_name)}
            for column_name, sql in required_columns.items():
                if column_name not in existing_columns:
                    connection.execute(text(sql))
