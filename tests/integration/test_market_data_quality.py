import json
from datetime import date, datetime
from pathlib import Path
from uuid import uuid4

import pandas as pd
from sqlalchemy import select


def setup_test_db(monkeypatch):
    test_db = Path("data") / f"test_{uuid4().hex}.db"
    database_url = f"sqlite:///{test_db.resolve().as_posix()}"
    monkeypatch.setenv("ETF_ASSISTANT_DATABASE_URL", database_url)

    from app.core.config import get_settings
    from app.core.database import get_engine, get_session_local, init_db

    get_settings.cache_clear()
    get_engine.cache_clear()
    init_db()
    return get_session_local


def build_history(
    end_date: date,
    *,
    periods: int = 40,
    stale_shift: int = 0,
    amount: float = 100000000.0,
) -> pd.DataFrame:
    dates = pd.bdate_range(end=pd.Timestamp(end_date) - pd.offsets.BDay(stale_shift), periods=periods)
    return pd.DataFrame(
        {
            "date": dates,
            "close": [100 + index * 0.5 for index in range(len(dates))],
            "amount": [amount for _ in range(len(dates))],
        }
    )


def init_user(session):
    from app.services.user_service import UserService

    UserService().init_user(
        session,
        initial_capital=100000,
        risk_level="中性",
        allow_gold=True,
        allow_bond=True,
        allow_overseas=True,
        min_trade_amount=100,
    )


def test_refresh_data_persists_real_latest_date_and_stale_flag(monkeypatch):
    session_local = setup_test_db(monkeypatch)

    from app.db.models import ETFFeature
    from app.repositories.market_repo import get_latest_market_snapshot
    from app.services.market_data_service import MarketDataService

    service = MarketDataService()

    def fake_akshare(symbol: str, trade_date: date):
        stale_shift = 1 if symbol == "510300" else 0
        return build_history(trade_date, stale_shift=stale_shift)

    service.source_loader_map["akshare"] = fake_akshare
    service.source_loader_map["fallback"] = lambda symbol, category, min_avg_amount, trade_date: build_history(
        trade_date,
        amount=max(float(min_avg_amount), 5000000.0),
    )

    with session_local()() as session:
        result = service.refresh_data(session, now=datetime(2026, 3, 11, 10, 0, 0))
        feature = session.scalar(select(ETFFeature).where(ETFFeature.symbol == "510300"))
        snapshot = get_latest_market_snapshot(session)
        raw = json.loads(snapshot.raw_json or "{}")

        assert feature is not None
        assert feature.trade_date == date(2026, 3, 11)
        assert feature.latest_row_date == date(2026, 3, 10)
        assert feature.stale_data_flag is True
        assert feature.source_code == "akshare"
        assert json.loads(feature.source_request_json)["symbol"] == "510300"
        assert result["formal_decision_ready"] is False
        assert raw["quality_summary"]["formal_decision_ready"] is False
        assert "510300" in raw["quality_summary"]["stale_symbols"]


def test_decision_blocks_when_refresh_uses_fallback_data(monkeypatch):
    session_local = setup_test_db(monkeypatch)

    from app.services.decision_engine import DecisionEngine
    from app.services.market_data_service import MarketDataService

    service = MarketDataService()
    service.source_loader_map["akshare"] = lambda symbol, trade_date: None
    service.source_loader_map["fallback"] = lambda symbol, category, min_avg_amount, trade_date: build_history(
        trade_date,
        amount=max(float(min_avg_amount), 5000000.0),
    )

    with session_local()() as session:
        init_user(session)
        service.refresh_data(session, now=datetime(2026, 3, 11, 10, 0, 0))

        advice = DecisionEngine().decide(session, now=datetime(2026, 3, 11, 10, 5, 0))

        assert advice["action_code"] == "no_trade"
        assert advice["reason_code"] == "data_quality_not_ready"
        assert "数据质量不足" in advice["summary_text"]
        assert advice["evidence"]["data_quality_gate"]["summary"]["quality_status"] == "blocked"


def test_partial_stale_non_core_data_marks_weak_but_keeps_formal_decision(monkeypatch):
    session_local = setup_test_db(monkeypatch)

    from app.repositories.market_repo import get_latest_market_snapshot
    from app.services.decision_engine import DecisionEngine
    from app.services.market_data_service import MarketDataService

    service = MarketDataService()

    def fake_akshare(symbol: str, trade_date: date):
        stale_shift = 1 if symbol == "512660" else 0
        return build_history(trade_date, stale_shift=stale_shift)

    service.source_loader_map["akshare"] = fake_akshare
    service.source_loader_map["fallback"] = lambda symbol, category, min_avg_amount, trade_date: build_history(
        trade_date,
        amount=max(float(min_avg_amount), 5000000.0),
    )

    with session_local()() as session:
        init_user(session)
        service.refresh_data(session, now=datetime(2026, 3, 11, 10, 0, 0))
        snapshot = get_latest_market_snapshot(session)
        raw = json.loads(snapshot.raw_json or "{}")

        advice = DecisionEngine().decide(session, now=datetime(2026, 3, 11, 10, 5, 0))

        assert raw["quality_summary"]["quality_status"] == "weak"
        assert raw["quality_summary"]["formal_decision_ready"] is True
        assert advice["reason_code"] != "data_quality_not_ready"


def test_fresh_real_data_keeps_normal_decision_flow(monkeypatch):
    session_local = setup_test_db(monkeypatch)

    from app.repositories.market_repo import get_latest_market_snapshot
    from app.services.decision_engine import DecisionEngine
    from app.services.market_data_service import MarketDataService

    service = MarketDataService()
    service.source_loader_map["akshare"] = lambda symbol, trade_date: build_history(trade_date)
    service.source_loader_map["fallback"] = lambda symbol, category, min_avg_amount, trade_date: build_history(
        trade_date,
        amount=max(float(min_avg_amount), 5000000.0),
    )

    with session_local()() as session:
        init_user(session)
        service.refresh_data(session, now=datetime(2026, 3, 11, 10, 0, 0))
        snapshot = get_latest_market_snapshot(session)
        raw = json.loads(snapshot.raw_json or "{}")

        advice = DecisionEngine().decide(session, now=datetime(2026, 3, 11, 10, 5, 0))

        assert raw["quality_summary"]["quality_status"] == "ok"
        assert raw["quality_summary"]["formal_decision_ready"] is True
        assert advice["reason_code"] != "data_quality_not_ready"
