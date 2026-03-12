from __future__ import annotations

import math
from datetime import date, timedelta
from pathlib import Path
from uuid import uuid4

import pandas as pd
from sqlalchemy import select


DEFAULT_SYMBOLS = ["510300", "510500", "159915", "518880", "511010", "511990", "513100"]


def setup_test_db(monkeypatch):
    test_db = Path("data") / f"test_backtest_{uuid4().hex}.db"
    database_url = f"sqlite:///{test_db.resolve().as_posix()}"
    monkeypatch.setenv("ETF_ASSISTANT_DATABASE_URL", database_url)

    from app.core.config import get_settings
    from app.core.database import get_engine, get_session_local, init_db

    get_settings.cache_clear()
    get_engine.cache_clear()
    init_db()
    return get_session_local


def seed_user(session):
    from app.services.user_service import UserService

    UserService().init_user(
        session,
        initial_capital=100000,
        risk_level="激进",
        risk_mode="balanced",
        allow_gold=True,
        allow_bond=True,
        allow_overseas=True,
        min_trade_amount=1000,
    )


def build_dataset(
    session,
    *,
    start_date: date,
    end_date: date,
    symbols: list[str] | None = None,
    variant: str = "normal",
    cutoff: date | None = None,
):
    from app.db.models import ETFUniverse

    selected_symbols = symbols or DEFAULT_SYMBOLS
    universe_rows = list(
        session.scalars(select(ETFUniverse).where(ETFUniverse.symbol.in_(selected_symbols)).order_by(ETFUniverse.symbol))
    )
    dates = pd.bdate_range(start=start_date - timedelta(days=70), end=end_date)
    history_by_symbol = {}
    trading_dates = [timestamp.date() for timestamp in dates if start_date <= timestamp.date() <= end_date]
    for etf in universe_rows:
        history_by_symbol[etf.symbol] = {
            "etf": etf,
            "history": _build_history(etf.symbol, dates, variant=variant, cutoff=cutoff),
            "source": "akshare",
            "request_params": {
                "symbol": etf.symbol,
                "period": "daily",
                "adjust": "qfq",
                "start_date": dates[0].strftime("%Y%m%d"),
                "end_date": dates[-1].strftime("%Y%m%d"),
            },
        }
    return {
        "start_date": start_date,
        "end_date": end_date,
        "warmup_start": dates[0].date(),
        "universe": {row.symbol: row for row in universe_rows},
        "history_by_symbol": history_by_symbol,
        "trading_dates": trading_dates,
    }


def _build_history(symbol: str, dates: pd.DatetimeIndex, *, variant: str, cutoff: date | None):
    base_price_map = {
        "510300": 1.0,
        "510500": 0.9,
        "159915": 1.2,
        "518880": 4.0,
        "511010": 100.0,
        "511990": 100.0,
        "513100": 1.1,
    }
    slope_map = {
        "510300": 0.012,
        "510500": 0.011,
        "159915": 0.015,
        "518880": 0.007,
        "511010": 0.004,
        "511990": 0.003,
        "513100": 0.013,
    }
    amplitude_map = {
        "510300": 0.03,
        "510500": 0.028,
        "159915": 0.04,
        "518880": 0.02,
        "511010": 0.01,
        "511990": 0.005,
        "513100": 0.035,
    }
    base_price = base_price_map.get(symbol, 1.0)
    slope = slope_map.get(symbol, 0.01)
    amplitude = amplitude_map.get(symbol, 0.02)
    seed = int(symbol[-2:])
    rows = []
    cutoff_index = next((idx for idx, value in enumerate(dates) if value.date() == cutoff), None)
    for idx, current_date in enumerate(dates):
        close = base_price + slope * idx + amplitude * math.sin((idx + seed) / 4)
        if variant == "future_crash" and cutoff_index is not None and idx > cutoff_index and symbol in {"510300", "510500"}:
            close -= 0.05 * (idx - cutoff_index)
        amount = 20_000_000 * (1.0 + 0.05 * math.sin((idx + seed) / 6))
        rows.append(
            {
                "date": current_date,
                "close": round(max(close, 0.2), 4),
                "amount": round(amount, 2),
            }
        )
    return pd.DataFrame(rows)
