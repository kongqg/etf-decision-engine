from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient


def create_test_client(monkeypatch):
    test_db = Path("data") / f"test_{uuid4().hex}.db"
    database_url = f"sqlite:///{test_db.resolve().as_posix()}"
    monkeypatch.setenv("ETF_ASSISTANT_DATABASE_URL", database_url)

    from app.core.config import get_settings
    from app.core.database import get_engine, init_db

    get_settings.cache_clear()
    get_engine.cache_clear()

    from app.main import create_app

    app = create_app()
    init_db()
    return TestClient(app)


def test_core_flow(monkeypatch):
    client = create_test_client(monkeypatch)

    init_response = client.post(
        "/api/init-user",
        json={
            "initial_capital": 100000,
            "risk_level": "中性",
            "allow_gold": True,
            "allow_bond": True,
            "allow_overseas": True,
            "min_trade_amount": 1000,
        },
    )
    assert init_response.status_code == 200

    refresh_response = client.post("/api/refresh-data")
    assert refresh_response.status_code == 200

    decide_response = client.post("/api/decide-now")
    assert decide_response.status_code == 200
    advice = decide_response.json()
    assert advice["action"] in {"买入", "卖出", "不操作"}

    explanation_response = client.get(f"/api/explanation/{advice['id']}")
    assert explanation_response.status_code == 200
    explanation = explanation_response.json()
    assert explanation["overall"]["reasons"]
    assert explanation["overall"]["evidence"]
    assert explanation["overall"]["source_info"]
    assert explanation["overall"]["market_score_details"]
    assert explanation["overall"]["execution_rule"]
    assert explanation["overall"]["etf_input_formulas"]

    evidence_response = client.get(f"/api/evidence/{advice['id']}")
    assert evidence_response.status_code == 200
    data_evidence = evidence_response.json()
    assert data_evidence["trust_summary"]
    assert data_evidence["request_summary"]["api"]
    assert data_evidence["quality_checks"]

    trade_response = client.post(
        "/api/record-trade",
        json={
            "executed_at": "2026-03-09T10:00:00",
            "symbol": "510300",
            "name": "沪深300ETF",
            "side": "buy",
            "price": 4.0,
            "amount": 4000,
            "fee": 0,
            "related_advice_id": advice["id"],
            "note": "test",
        },
    )
    assert trade_response.status_code == 200

    portfolio_response = client.get("/api/portfolio")
    assert portfolio_response.status_code == 200
    assert "holdings" in portfolio_response.json()

    performance_response = client.get("/api/performance")
    assert performance_response.status_code == 200
    assert "curve" in performance_response.json()
