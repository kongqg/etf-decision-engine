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
            "max_total_position_pct": "ALTER TABLE user_preferences ADD COLUMN max_total_position_pct FLOAT NOT NULL DEFAULT 0.7",
            "max_single_position_pct": "ALTER TABLE user_preferences ADD COLUMN max_single_position_pct FLOAT NOT NULL DEFAULT 0.35",
            "cash_reserve_pct": "ALTER TABLE user_preferences ADD COLUMN cash_reserve_pct FLOAT NOT NULL DEFAULT 0.2",
        },
        "etf_features": {
            "latest_row_date": "ALTER TABLE etf_features ADD COLUMN latest_row_date DATE",
            "source_code": "ALTER TABLE etf_features ADD COLUMN source_code VARCHAR(20) NOT NULL DEFAULT ''",
            "stale_data_flag": "ALTER TABLE etf_features ADD COLUMN stale_data_flag BOOLEAN NOT NULL DEFAULT 0",
            "quality_status": "ALTER TABLE etf_features ADD COLUMN quality_status VARCHAR(20) NOT NULL DEFAULT ''",
            "formal_eligible": "ALTER TABLE etf_features ADD COLUMN formal_eligible BOOLEAN NOT NULL DEFAULT 1",
            "source_request_json": "ALTER TABLE etf_features ADD COLUMN source_request_json TEXT NOT NULL DEFAULT '{}'",
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
            "momentum_5d_rank": "ALTER TABLE etf_features ADD COLUMN momentum_5d_rank FLOAT NOT NULL DEFAULT 0.0",
            "momentum_10d_rank": "ALTER TABLE etf_features ADD COLUMN momentum_10d_rank FLOAT NOT NULL DEFAULT 0.0",
            "momentum_20d_rank": "ALTER TABLE etf_features ADD COLUMN momentum_20d_rank FLOAT NOT NULL DEFAULT 0.0",
            "trend_rank": "ALTER TABLE etf_features ADD COLUMN trend_rank FLOAT NOT NULL DEFAULT 0.0",
            "volatility_rank": "ALTER TABLE etf_features ADD COLUMN volatility_rank FLOAT NOT NULL DEFAULT 0.0",
            "drawdown_rank": "ALTER TABLE etf_features ADD COLUMN drawdown_rank FLOAT NOT NULL DEFAULT 0.0",
            "liquidity_rank": "ALTER TABLE etf_features ADD COLUMN liquidity_rank FLOAT NOT NULL DEFAULT 0.0",
            "intra_score": "ALTER TABLE etf_features ADD COLUMN intra_score FLOAT NOT NULL DEFAULT 0.0",
            "category_score": "ALTER TABLE etf_features ADD COLUMN category_score FLOAT NOT NULL DEFAULT 0.0",
            "final_score": "ALTER TABLE etf_features ADD COLUMN final_score FLOAT NOT NULL DEFAULT 0.0",
            "global_rank": "ALTER TABLE etf_features ADD COLUMN global_rank INTEGER",
            "category_rank": "ALTER TABLE etf_features ADD COLUMN category_rank INTEGER",
            "basic_filter_pass": "ALTER TABLE etf_features ADD COLUMN basic_filter_pass BOOLEAN NOT NULL DEFAULT 0",
            "basic_filter_reason": "ALTER TABLE etf_features ADD COLUMN basic_filter_reason TEXT NOT NULL DEFAULT ''",
            "score_breakdown_json": "ALTER TABLE etf_features ADD COLUMN score_breakdown_json TEXT NOT NULL DEFAULT '{}'",
        },
        "advice_records": {
            "action_code": "ALTER TABLE advice_records ADD COLUMN action_code VARCHAR(30) NOT NULL DEFAULT ''",
            "reason_code": "ALTER TABLE advice_records ADD COLUMN reason_code VARCHAR(40) NOT NULL DEFAULT ''",
            "display_action": "ALTER TABLE advice_records ADD COLUMN display_action VARCHAR(20) NOT NULL DEFAULT ''",
            "strategy_version": "ALTER TABLE advice_records ADD COLUMN strategy_version VARCHAR(40) NOT NULL DEFAULT 'score_v2'",
            "target_portfolio_json": "ALTER TABLE advice_records ADD COLUMN target_portfolio_json TEXT NOT NULL DEFAULT '{}'",
            "budget_context_json": "ALTER TABLE advice_records ADD COLUMN budget_context_json TEXT NOT NULL DEFAULT '{}'",
            "candidate_summary_json": "ALTER TABLE advice_records ADD COLUMN candidate_summary_json TEXT NOT NULL DEFAULT '{}'",
        },
        "market_snapshots": {
            "data_source": "ALTER TABLE market_snapshots ADD COLUMN data_source VARCHAR(20) NOT NULL DEFAULT ''",
            "quality_status": "ALTER TABLE market_snapshots ADD COLUMN quality_status VARCHAR(20) NOT NULL DEFAULT ''",
            "formal_decision_ready": "ALTER TABLE market_snapshots ADD COLUMN formal_decision_ready BOOLEAN NOT NULL DEFAULT 1",
            "latest_available_date": "ALTER TABLE market_snapshots ADD COLUMN latest_available_date DATE",
            "budget_total_pct": "ALTER TABLE market_snapshots ADD COLUMN budget_total_pct FLOAT NOT NULL DEFAULT 0.0",
            "budget_by_category_json": "ALTER TABLE market_snapshots ADD COLUMN budget_by_category_json TEXT NOT NULL DEFAULT '{}'",
        },
        "advice_items": {
            "action_code": "ALTER TABLE advice_items ADD COLUMN action_code VARCHAR(30) NOT NULL DEFAULT ''",
            "intent": "ALTER TABLE advice_items ADD COLUMN intent VARCHAR(20) NOT NULL DEFAULT ''",
            "category": "ALTER TABLE advice_items ADD COLUMN category VARCHAR(30) NOT NULL DEFAULT ''",
            "tradability_mode": "ALTER TABLE advice_items ADD COLUMN tradability_mode VARCHAR(10) NOT NULL DEFAULT ''",
            "category_score": "ALTER TABLE advice_items ADD COLUMN category_score FLOAT NOT NULL DEFAULT 0.0",
            "current_weight": "ALTER TABLE advice_items ADD COLUMN current_weight FLOAT NOT NULL DEFAULT 0.0",
            "target_weight": "ALTER TABLE advice_items ADD COLUMN target_weight FLOAT NOT NULL DEFAULT 0.0",
            "delta_weight": "ALTER TABLE advice_items ADD COLUMN delta_weight FLOAT NOT NULL DEFAULT 0.0",
            "is_new_position": "ALTER TABLE advice_items ADD COLUMN is_new_position BOOLEAN NOT NULL DEFAULT 0",
            "replace_threshold_used": "ALTER TABLE advice_items ADD COLUMN replace_threshold_used FLOAT NOT NULL DEFAULT 0.0",
            "final_score": "ALTER TABLE advice_items ADD COLUMN final_score FLOAT NOT NULL DEFAULT 0.0",
            "intra_score": "ALTER TABLE advice_items ADD COLUMN intra_score FLOAT NOT NULL DEFAULT 0.0",
            "global_rank": "ALTER TABLE advice_items ADD COLUMN global_rank INTEGER",
            "category_rank": "ALTER TABLE advice_items ADD COLUMN category_rank INTEGER",
            "hold_days": "ALTER TABLE advice_items ADD COLUMN hold_days INTEGER NOT NULL DEFAULT 0",
            "score_gap_vs_holding": "ALTER TABLE advice_items ADD COLUMN score_gap_vs_holding FLOAT NOT NULL DEFAULT 0.0",
            "replacement_symbol": "ALTER TABLE advice_items ADD COLUMN replacement_symbol VARCHAR(10) NOT NULL DEFAULT ''",
            "current_amount": "ALTER TABLE advice_items ADD COLUMN current_amount FLOAT NOT NULL DEFAULT 0.0",
            "target_amount": "ALTER TABLE advice_items ADD COLUMN target_amount FLOAT NOT NULL DEFAULT 0.0",
            "rationale_json": "ALTER TABLE advice_items ADD COLUMN rationale_json TEXT NOT NULL DEFAULT '{}'",
            "score_breakdown_json": "ALTER TABLE advice_items ADD COLUMN score_breakdown_json TEXT NOT NULL DEFAULT '{}'",
        },
        "trades": {
            "advice_item_id": "ALTER TABLE trades ADD COLUMN advice_item_id INTEGER",
            "intent": "ALTER TABLE trades ADD COLUMN intent VARCHAR(20) NOT NULL DEFAULT ''",
            "weight_before": "ALTER TABLE trades ADD COLUMN weight_before FLOAT NOT NULL DEFAULT 0.0",
            "weight_after": "ALTER TABLE trades ADD COLUMN weight_after FLOAT NOT NULL DEFAULT 0.0",
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
