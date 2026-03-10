import json
from datetime import date, datetime
from pathlib import Path
from uuid import uuid4

from starlette.requests import Request


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


def seed_decision_inputs(session_local):
    from app.db.models import ETFFeature, MarketSnapshot

    trade_date = date(2026, 3, 10)
    captured_at = datetime(2026, 3, 10, 9, 35, 0)
    raw_json = {
        "source": {
            "label": "test-seed",
            "api": "seed",
            "data_type": "mock",
            "trade_date": trade_date.isoformat(),
            "captured_at": captured_at.isoformat(),
            "note": "integration test seed",
        },
        "formulas": {
            "broad_index_score": "test",
            "risk_appetite_score": "test",
            "trend_score": "test",
        },
        "request_summary": {"api": "seed"},
        "quality_summary": {
            "verification_status": "ok",
            "latest_available_date": trade_date.isoformat(),
            "supports_live_execution": True,
        },
        "quality_checks": [],
        "series_samples": {},
        "evidence": {
            "broad_momentum": 2.0,
            "broad_ma_gap": 1.0,
            "offense_score": 1.6,
            "defense_score": 0.4,
            "trend_positive_ratio": 70.0,
            "trend_strength": 1.2,
        },
    }
    feature_rows = [
        {
            "symbol": "510300",
            "close_price": 1.2,
            "pct_change": 0.8,
            "latest_amount": 120000000.0,
            "avg_amount_20d": 150000000.0,
            "momentum_3d": 1.8,
            "momentum_5d": 2.6,
            "momentum_10d": 3.4,
            "ma_gap_5": 1.2,
            "ma_gap_10": 1.6,
            "trend_strength": 1.8,
            "volatility_10d": 2.2,
            "drawdown_20d": -2.5,
            "liquidity_score": 80.0,
            "anomaly_flag": False,
        },
        {
            "symbol": "510500",
            "close_price": 0.95,
            "pct_change": 0.5,
            "latest_amount": 110000000.0,
            "avg_amount_20d": 130000000.0,
            "momentum_3d": 1.2,
            "momentum_5d": 2.0,
            "momentum_10d": 2.8,
            "ma_gap_5": 0.9,
            "ma_gap_10": 1.1,
            "trend_strength": 1.3,
            "volatility_10d": 2.4,
            "drawdown_20d": -3.0,
            "liquidity_score": 75.0,
            "anomaly_flag": False,
        },
        {
            "symbol": "512100",
            "close_price": 5.0,
            "pct_change": 1.1,
            "latest_amount": 90000000.0,
            "avg_amount_20d": 120000000.0,
            "momentum_3d": 2.1,
            "momentum_5d": 3.0,
            "momentum_10d": 4.0,
            "ma_gap_5": 1.5,
            "ma_gap_10": 1.8,
            "trend_strength": 2.0,
            "volatility_10d": 3.1,
            "drawdown_20d": -4.5,
            "liquidity_score": 70.0,
            "anomaly_flag": False,
        },
    ]

    with session_local()() as session:
        session.add(
            MarketSnapshot(
                trade_date=trade_date,
                captured_at=captured_at,
                session_mode="intraday",
                market_regime="中性",
                broad_index_score=70.0,
                risk_appetite_score=68.0,
                trend_score=72.0,
                recommended_position_pct=0.55,
                raw_json=json.dumps(raw_json, ensure_ascii=False),
            )
        )
        for row in feature_rows:
            session.add(ETFFeature(trade_date=trade_date, captured_at=captured_at, **row))
        session.commit()


def test_core_flow(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    seed_decision_inputs(session_local)

    from app.repositories.advice_repo import get_explanations_by_advice
    from app.services.data_evidence_service import DataEvidenceService
    from app.services.decision_engine import DecisionEngine
    from app.services.performance_service import PerformanceService
    from app.services.portfolio_service import PortfolioService
    from app.services.trade_service import TradeService
    from app.services.user_service import UserService
    from app.web.presenters import serialize_explanations

    with session_local()() as session:
        user_service = UserService()
        user_service.init_user(
            session,
            initial_capital=100000,
            risk_level="中性",
            allow_gold=True,
            allow_bond=True,
            allow_overseas=True,
            min_trade_amount=100,
        )

        advice = DecisionEngine().decide(session, now=datetime(2026, 3, 10, 10, 0, 0))
        assert advice["action"] in {"买入", "卖出", "不操作"}
        assert "executable_recommendations" in advice
        assert "watchlist_recommendations" in advice
        assert "recommendation_counts" in advice

        explanation = serialize_explanations(get_explanations_by_advice(session, advice["id"]))
        assert explanation["overall"]["reasons"]
        assert explanation["overall"]["evidence"]
        assert explanation["overall"]["source_info"]
        assert explanation["overall"]["market_score_details"]
        assert explanation["overall"]["execution_rule"]
        assert "budget_passed" in explanation["overall"]["execution_rule"]
        assert explanation["overall"]["etf_input_formulas"]

        data_evidence = DataEvidenceService().build(session, advice_id=advice["id"])
        assert data_evidence["trust_summary"]
        assert data_evidence["request_summary"]["api"]
        assert data_evidence["quality_checks"] is not None

        TradeService().record_trade(
            session,
            {
                "executed_at": datetime(2026, 3, 10, 10, 0, 0),
                "symbol": "510300",
                "name": "沪深300ETF",
                "side": "buy",
                "price": 4.0,
                "amount": 4000.0,
                "fee": 0.0,
                "related_advice_id": advice["id"],
                "note": "test",
            },
        )

        portfolio = PortfolioService().get_portfolio_summary(session)
        assert "holdings" in portfolio
        performance = PerformanceService().get_summary(session)
        assert "curve" in performance


def test_advice_api_and_page_show_recommendation_layers(monkeypatch):
    session_local = setup_test_db(monkeypatch)

    from app.api.routes.advice import get_advice as get_advice_route
    from app.db.models import AdviceRecord
    from app.main import create_app
    from app.web.pages import advice_detail_page

    executable_item = {
        "symbol": "510300",
        "name": "沪深300ETF",
        "rank": 1,
        "action": "买入",
        "suggested_amount": 240.0,
        "suggested_pct": 0.24,
        "trigger_price_low": 1.198,
        "trigger_price_high": 1.212,
        "stop_loss_pct": 0.05,
        "take_profit_pct": 0.1,
        "score": 88.0,
        "score_gap": 0.0,
        "reason_short": "综合得分高，且当前预算可执行。",
        "risk_level": "中",
        "latest_price": 1.2,
        "lot_size": 100.0,
        "min_advice_amount": 100.0,
        "min_order_amount": 120.0,
        "available_cash": 1000.0,
        "budget_gap_to_min_order": 0.0,
        "is_executable": True,
        "execution_status": "可执行买入",
        "recommendation_bucket": "executable_recommendations",
        "not_executable_reason": "",
        "execution_note": "当前建议金额已经覆盖 1 手门槛，可执行。",
    }
    watchlist_item = {
        "symbol": "512100",
        "name": "中证1000ETF",
        "rank": 2,
        "action": "关注",
        "suggested_amount": 180.0,
        "suggested_pct": 0.18,
        "trigger_price_low": 4.975,
        "trigger_price_high": 5.05,
        "stop_loss_pct": 0.07,
        "take_profit_pct": 0.12,
        "score": 86.0,
        "score_gap": 2.0,
        "reason_short": "综合得分高，但当前预算不足。",
        "risk_level": "中高",
        "latest_price": 5.0,
        "lot_size": 100.0,
        "min_advice_amount": 100.0,
        "min_order_amount": 500.0,
        "available_cash": 1000.0,
        "budget_gap_to_min_order": 320.0,
        "is_executable": False,
        "execution_status": "关注标的",
        "recommendation_bucket": "watchlist_recommendations",
        "not_executable_reason": "当前预算不足以买入 1 手。",
        "execution_note": "综合评分不错，但当前建议金额还买不起 1 手。",
    }

    with session_local()() as session:
        advice = AdviceRecord(
            advice_date=date(2026, 3, 10),
            created_at=datetime(2026, 3, 10, 10, 0, 0),
            session_mode="intraday",
            action="买入",
            market_regime="中性",
            target_position_pct=0.55,
            current_position_pct=0.0,
            summary_text="优先买入当前可执行的 ETF，同时保留高分关注标的。",
            risk_text="注意分批建仓。",
            status="active",
            evidence_json=json.dumps(
                {
                    "recommendation_groups": {
                        "executable_recommendations": [executable_item],
                        "watchlist_recommendations": [watchlist_item],
                        "show_watchlist_recommendations": True,
                        "budget_filter_enabled": True,
                    }
                },
                ensure_ascii=False,
            ),
        )
        session.add(advice)
        session.commit()
        session.refresh(advice)
        advice_id = advice.id

        payload = get_advice_route(advice_id, db=session)
        assert payload["executable_recommendations"][0]["symbol"] == "510300"
        assert payload["watchlist_recommendations"][0]["symbol"] == "512100"
        assert payload["watchlist_recommendations"][0]["is_executable"] is False

        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": f"/advice/{advice_id}",
                "headers": [],
                "query_string": b"",
                "app": create_app(),
            }
        )
        page_response = advice_detail_page(advice_id, request=request, db=session)
        body = page_response.body.decode("utf-8")
        assert "可执行推荐" in body
        assert "关注标的" in body
        assert "当前预算还买不起 1 手" in body
