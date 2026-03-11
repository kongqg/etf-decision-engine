import json
import re
from datetime import date, datetime
from pathlib import Path
from uuid import uuid4

import pytest
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
            "ret_1d": 0.8,
            "latest_amount": 120000000.0,
            "avg_amount_20d": 150000000.0,
            "avg_turnover_20d": 150000000.0,
            "momentum_3d": 1.8,
            "momentum_5d": 3.6,
            "momentum_10d": 5.2,
            "momentum_20d": 8.0,
            "ma5": 1.16,
            "ma10": 1.12,
            "ma20": 1.08,
            "ma_gap_5": 1.2,
            "ma_gap_10": 1.6,
            "trend_strength": 2.8,
            "volatility_5d": 1.6,
            "volatility_10d": 2.2,
            "volatility_20d": 2.8,
            "rolling_max_20d": 1.23,
            "drawdown_20d": -2.5,
            "liquidity_score": 18.8,
            "category_return_10d": 4.6,
            "relative_strength_10d": 0.6,
            "above_ma20_flag": True,
            "anomaly_flag": False,
        },
        {
            "symbol": "510500",
            "close_price": 0.95,
            "pct_change": 0.5,
            "ret_1d": 0.5,
            "latest_amount": 110000000.0,
            "avg_amount_20d": 130000000.0,
            "avg_turnover_20d": 130000000.0,
            "momentum_3d": 1.2,
            "momentum_5d": 2.0,
            "momentum_10d": 4.0,
            "momentum_20d": 6.0,
            "ma5": 0.93,
            "ma10": 0.91,
            "ma20": 0.89,
            "ma_gap_5": 0.9,
            "ma_gap_10": 1.1,
            "trend_strength": 1.9,
            "volatility_5d": 1.8,
            "volatility_10d": 2.4,
            "volatility_20d": 3.0,
            "rolling_max_20d": 0.97,
            "drawdown_20d": -3.0,
            "liquidity_score": 18.6,
            "category_return_10d": 4.6,
            "relative_strength_10d": -0.6,
            "above_ma20_flag": True,
            "anomaly_flag": False,
        },
        {
            "symbol": "511010",
            "close_price": 100.5,
            "pct_change": 0.1,
            "ret_1d": 0.1,
            "latest_amount": 6000000.0,
            "avg_amount_20d": 10000000.0,
            "avg_turnover_20d": 10000000.0,
            "momentum_3d": 0.2,
            "momentum_5d": 0.3,
            "momentum_10d": 0.6,
            "momentum_20d": 1.2,
            "ma5": 100.2,
            "ma10": 100.0,
            "ma20": 99.8,
            "ma_gap_5": 0.3,
            "ma_gap_10": 0.5,
            "trend_strength": 0.7,
            "volatility_5d": 0.2,
            "volatility_10d": 0.4,
            "volatility_20d": 0.6,
            "rolling_max_20d": 100.8,
            "drawdown_20d": -0.3,
            "liquidity_score": 16.1,
            "category_return_10d": 0.6,
            "relative_strength_10d": 0.0,
            "above_ma20_flag": True,
            "anomaly_flag": False,
        },
        {
            "symbol": "518880",
            "close_price": 4.2,
            "pct_change": 0.2,
            "ret_1d": 0.2,
            "latest_amount": 40000000.0,
            "avg_amount_20d": 50000000.0,
            "avg_turnover_20d": 50000000.0,
            "momentum_3d": 0.5,
            "momentum_5d": 0.8,
            "momentum_10d": 1.0,
            "momentum_20d": 1.6,
            "ma5": 4.15,
            "ma10": 4.10,
            "ma20": 4.05,
            "ma_gap_5": 0.6,
            "ma_gap_10": 0.9,
            "trend_strength": 1.2,
            "volatility_5d": 0.7,
            "volatility_10d": 1.0,
            "volatility_20d": 1.2,
            "rolling_max_20d": 4.25,
            "drawdown_20d": -1.2,
            "liquidity_score": 17.4,
            "category_return_10d": 1.0,
            "relative_strength_10d": 0.0,
            "above_ma20_flag": True,
            "anomaly_flag": False,
        },
        {
            "symbol": "513100",
            "close_price": 1.1,
            "pct_change": 0.3,
            "ret_1d": 0.3,
            "latest_amount": 35000000.0,
            "avg_amount_20d": 38000000.0,
            "avg_turnover_20d": 38000000.0,
            "momentum_3d": 0.6,
            "momentum_5d": 1.0,
            "momentum_10d": 1.4,
            "momentum_20d": 2.2,
            "ma5": 1.08,
            "ma10": 1.07,
            "ma20": 1.05,
            "ma_gap_5": 0.8,
            "ma_gap_10": 1.1,
            "trend_strength": 1.4,
            "volatility_5d": 1.3,
            "volatility_10d": 1.8,
            "volatility_20d": 2.4,
            "rolling_max_20d": 1.12,
            "drawdown_20d": -1.6,
            "liquidity_score": 17.0,
            "category_return_10d": 1.4,
            "relative_strength_10d": 0.0,
            "above_ma20_flag": True,
            "anomaly_flag": False,
        },
        {
            "symbol": "511990",
            "close_price": 100.0,
            "pct_change": 0.02,
            "ret_1d": 0.02,
            "latest_amount": 5000000.0,
            "avg_amount_20d": 6000000.0,
            "avg_turnover_20d": 6000000.0,
            "momentum_3d": 0.02,
            "momentum_5d": 0.05,
            "momentum_10d": 0.08,
            "momentum_20d": 0.15,
            "ma5": 99.98,
            "ma10": 99.96,
            "ma20": 99.95,
            "ma_gap_5": 0.02,
            "ma_gap_10": 0.04,
            "trend_strength": 0.05,
            "volatility_5d": 0.03,
            "volatility_10d": 0.05,
            "volatility_20d": 0.08,
            "rolling_max_20d": 100.02,
            "drawdown_20d": -0.02,
            "liquidity_score": 15.4,
            "category_return_10d": 0.08,
            "relative_strength_10d": 0.0,
            "above_ma20_flag": True,
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
        assert advice["action_code"] in {
            "buy_open",
            "buy_add",
            "hold",
            "reduce",
            "sell_exit",
            "no_trade",
            "park_in_money_etf",
        }
        assert "executable_recommendations" in advice
        assert "watchlist_recommendations" in advice
        assert "recommendation_counts" in advice
        assert "mapped_horizon_profile" in advice
        assert "transition_plan" in advice
        assert "daily_action_plan" in advice
        assert "action_counts" in advice
        assert "target_portfolio" in advice
        assert advice["daily_action_plan"]
        assert "position_action" in advice["daily_action_plan"][0]
        assert "current_weight" in advice["daily_action_plan"][0]
        assert "target_weight" in advice["daily_action_plan"][0]
        assert "delta_weight" in advice["daily_action_plan"][0]

        explanation = serialize_explanations(get_explanations_by_advice(session, advice["id"]))
        assert explanation["overall"]["reasons"]
        assert explanation["overall"]["evidence"]
        assert explanation["overall"]["source_info"]
        assert explanation["overall"]["execution_rule"]
        assert explanation["overall"]["category_scores"]
        assert explanation["overall"]["action_counts"] == advice["action_counts"]
        assert explanation["overall"]["portfolio_transition"]["rows"] is not None
        if explanation["items"]:
            assert explanation["items"][0]["score_breakdown"]["entry_details"]
            assert explanation["items"][0]["category_breakdown"] is not None

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


def test_existing_holding_stays_in_formal_review_even_when_not_entry_eligible(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    seed_decision_inputs(session_local)

    from sqlalchemy import select

    from app.db.models import ETFFeature
    from app.services.decision_engine import DecisionEngine
    from app.services.trade_service import TradeService
    from app.services.user_service import UserService

    with session_local()() as session:
        UserService().init_user(
            session,
            initial_capital=100000,
            risk_level="中性",
            allow_gold=True,
            allow_bond=True,
            allow_overseas=True,
            min_trade_amount=100,
        )
        TradeService().record_trade(
            session,
            {
                "executed_at": datetime(2026, 3, 10, 9, 50, 0),
                "symbol": "510300",
                "name": "沪深300ETF",
                "side": "buy",
                "price": 4.0,
                "amount": 4000.0,
                "fee": 0.0,
                "note": "test",
            },
        )
        feature = session.scalar(select(ETFFeature).where(ETFFeature.symbol == "510300"))
        feature.avg_amount_20d = 1000.0
        session.commit()

        advice = DecisionEngine().decide(session, now=datetime(2026, 3, 10, 10, 0, 0))
        holding_item = next(item for item in advice["portfolio_review_items"] if item["symbol"] == "510300")

        assert holding_item["is_current_holding"] is True
        assert holding_item["is_held"] is True
        assert holding_item["entry_eligible"] is False
        assert holding_item["action_code"] in {"hold", "reduce", "sell_exit", "buy_add"}
        assert holding_item["position_action"] in {"hold_position", "reduce_position", "exit_position", "add_position"}
        assert "filter_reasons" in holding_item


def test_decide_now_action_redirects_to_clean_advice_url(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    seed_decision_inputs(session_local)

    from app.services.user_service import UserService
    from app.web.pages import decide_now_action

    with session_local()() as session:
        UserService().init_user(
            session,
            initial_capital=5000,
            risk_level="中性",
            allow_gold=True,
            allow_bond=True,
            allow_overseas=True,
            min_trade_amount=100,
        )

        response = decide_now_action(db=session)

        assert response.status_code == 303
        assert re.fullmatch(r"/advice/\d+", response.headers["location"])


def test_capital_adjustment_updates_portfolio_and_history(monkeypatch):
    session_local = setup_test_db(monkeypatch)

    from app.api.routes.portfolio import adjust_capital, get_portfolio
    from app.main import create_app
    from app.schemas.capital_flow import CapitalAdjustmentRequest
    from app.services.user_service import UserService
    from app.web.pages import portfolio_page

    with session_local()() as session:
        UserService().init_user(
            session,
            initial_capital=1000,
            risk_level="中性",
            allow_gold=True,
            allow_bond=True,
            allow_overseas=True,
            min_trade_amount=100,
        )

        deposit_result = adjust_capital(
            CapitalAdjustmentRequest(
                executed_at=datetime(2026, 3, 10, 10, 5, 0),
                flow_type="deposit",
                amount=500,
                note="追加资金",
            ),
            db=session,
        )
        assert deposit_result["portfolio"]["cash_balance"] == 1500.0
        assert deposit_result["portfolio"]["total_asset"] == 1500.0

        withdraw_result = adjust_capital(
            CapitalAdjustmentRequest(
                executed_at=datetime(2026, 3, 10, 10, 10, 0),
                flow_type="withdraw",
                amount=200,
                note="转出部分现金",
            ),
            db=session,
        )
        assert withdraw_result["portfolio"]["cash_balance"] == 1300.0
        assert withdraw_result["portfolio"]["total_asset"] == 1300.0

        portfolio = get_portfolio(db=session)
        assert portfolio["net_capital_flow_amount"] == 300.0
        assert portfolio["current_capital_base"] == 1300.0
        assert len(portfolio["capital_flows"]) == 2
        assert portfolio["capital_flows"][0]["flow_type"] == "withdraw"
        assert portfolio["capital_flows"][1]["flow_type"] == "deposit"

        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/portfolio",
                "headers": [],
                "query_string": b"",
                "app": create_app(),
            }
        )
        page_response = portfolio_page(request=request, db=session)
        body = page_response.body.decode("utf-8")
        assert "调整资金" in body
        assert "记录资金调整" in body
        assert "资金变动记录" in body
        assert "增加资金" in body
        assert "减少资金" in body


def test_portfolio_page_amount_inputs_allow_decimal(monkeypatch):
    session_local = setup_test_db(monkeypatch)

    from app.main import create_app
    from app.services.user_service import UserService
    from app.web.pages import portfolio_page

    with session_local()() as session:
        UserService().init_user(
            session,
            initial_capital=1000,
            risk_level="中性",
            allow_gold=True,
            allow_bond=True,
            allow_overseas=True,
            min_trade_amount=100,
        )

        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/portfolio",
                "headers": [],
                "query_string": b"",
                "app": create_app(),
            }
        )
        page_response = portfolio_page(request=request, db=session)
        body = page_response.body.decode("utf-8")

        matches = re.findall(r'<input type="number" name="amount" min="0\.01" step="0\.01" required>', body)
        assert len(matches) == 2


def test_capital_adjustment_rejects_withdraw_above_cash(monkeypatch):
    session_local = setup_test_db(monkeypatch)

    from fastapi import HTTPException

    from app.api.routes.portfolio import adjust_capital
    from app.schemas.capital_flow import CapitalAdjustmentRequest
    from app.services.user_service import UserService

    with session_local()() as session:
        UserService().init_user(
            session,
            initial_capital=1000,
            risk_level="中性",
            allow_gold=True,
            allow_bond=True,
            allow_overseas=True,
            min_trade_amount=100,
        )

        with pytest.raises(HTTPException) as exc_info:
            adjust_capital(
                CapitalAdjustmentRequest(
                    executed_at=datetime(2026, 3, 10, 10, 15, 0),
                    flow_type="withdraw",
                    amount=1200,
                    note="超额出金",
                ),
                db=session,
            )
        assert exc_info.value.status_code == 400
        assert "当前可用现金不足" in str(exc_info.value.detail)


def test_update_preferences_keeps_existing_records(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    seed_decision_inputs(session_local)

    from app.api.routes.user import update_preferences
    from app.repositories.advice_repo import get_latest_advice
    from app.repositories.portfolio_repo import list_trades
    from app.schemas.user import UpdatePreferencesRequest
    from app.services.decision_engine import DecisionEngine
    from app.services.trade_service import TradeService
    from app.services.user_service import UserService

    with session_local()() as session:
        UserService().init_user(
            session,
            initial_capital=5000,
            risk_level="中性",
            allow_gold=True,
            allow_bond=True,
            allow_overseas=True,
            min_trade_amount=100,
        )
        advice = DecisionEngine().decide(session, now=datetime(2026, 3, 10, 10, 0, 0))
        TradeService().record_trade(
            session,
            {
                "executed_at": datetime(2026, 3, 10, 10, 5, 0),
                "symbol": "510300",
                "name": "沪深300ETF",
                "side": "buy",
                "price": 4.0,
                "amount": 400.0,
                "fee": 1.0,
                "related_advice_id": advice["id"],
                "note": "existing record",
            },
        )

        payload = update_preferences(
            UpdatePreferencesRequest(
                risk_level="保守",
                risk_mode="conservative",
                allow_gold=False,
                allow_bond=True,
                allow_overseas=False,
                min_trade_amount=200,
                max_total_position_pct=0.5,
                max_single_position_pct=0.2,
                cash_reserve_pct=0.3,
            ),
            db=session,
        )

        latest_advice = get_latest_advice(session)
        trades = list_trades(session, limit=20)
        assert payload["risk_level"] == "保守"
        assert payload["risk_mode"] == "conservative"
        assert payload["allow_gold"] is False
        assert payload["max_total_position_pct"] == 0.5
        assert latest_advice is not None
        assert latest_advice.id == advice["id"]
        assert len(trades) == 1
        assert trades[0].related_advice_id == advice["id"]


def test_init_user_defaults_risk_mode_to_balanced(monkeypatch):
    session_local = setup_test_db(monkeypatch)

    from app.repositories.user_repo import get_preferences
    from app.services.user_service import UserService

    with session_local()() as session:
        UserService().init_user(
            session,
            initial_capital=5000,
            risk_level="中性",
            allow_gold=True,
            allow_bond=True,
            allow_overseas=True,
            min_trade_amount=100,
        )

        preferences = get_preferences(session)
        assert preferences is not None
        assert preferences.risk_mode == "balanced"


def test_decision_engine_uses_effective_risk_mode_values(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    seed_decision_inputs(session_local)

    from app.repositories.advice_repo import get_latest_advice
    from app.services.decision_engine import DecisionEngine
    from app.services.user_service import UserService

    with session_local()() as session:
        service = UserService()
        service.init_user(
            session,
            initial_capital=5000,
            risk_level="中性",
            allow_gold=True,
            allow_bond=True,
            allow_overseas=True,
            min_trade_amount=100,
        )
        service.update_preferences(
            session,
            risk_level="中性",
            risk_mode="aggressive",
            allow_gold=True,
            allow_bond=True,
            allow_overseas=True,
            min_trade_amount=100,
            target_holding_days=2,
            max_total_position_pct=0.4,
            max_single_position_pct=0.2,
            cash_reserve_pct=0.2,
        )

        advice = DecisionEngine().decide(session, now=datetime(2026, 3, 10, 10, 0, 0))
        latest_advice = get_latest_advice(session)
        plan_facts = advice["evidence"]["plan_facts"]

        assert advice["target_position_pct"] == 0.55
        assert latest_advice is not None
        assert latest_advice.target_holding_days == 8
        assert plan_facts["risk_mode"] == "aggressive"
        assert plan_facts["effective_max_total_position_pct"] == 0.8
        assert plan_facts["effective_target_holding_days"] == 8


def test_decision_engine_recalculates_after_capital_adjustment(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    seed_decision_inputs(session_local)

    from app.schemas.capital_flow import CapitalAdjustmentRequest
    from app.services.capital_flow_service import CapitalFlowService
    from app.services.decision_engine import DecisionEngine
    from app.services.user_service import UserService

    with session_local()() as session:
        UserService().init_user(
            session,
            initial_capital=1000,
            risk_level="中性",
            allow_gold=True,
            allow_bond=True,
            allow_overseas=True,
            min_trade_amount=100,
        )

        engine = DecisionEngine()
        before = engine.decide(session, now=datetime(2026, 3, 10, 10, 0, 0))
        before_facts = before["evidence"]["plan_facts"]

        CapitalFlowService().record_adjustment(
            session,
            CapitalAdjustmentRequest(
                executed_at=datetime(2026, 3, 10, 10, 30, 0),
                flow_type="deposit",
                amount=1000,
                note="追加资金",
            ).model_dump(),
        )
        after = engine.decide(session, now=datetime(2026, 3, 10, 10, 35, 0))
        after_facts = after["evidence"]["plan_facts"]

        assert before_facts["available_cash"] == 1000.0
        assert after_facts["available_cash"] == 2000.0
        assert after_facts["total_asset"] > before_facts["total_asset"]


def test_history_api_and_page_show_saved_advice(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    seed_decision_inputs(session_local)

    from app.api.routes.advice import advice_history
    from app.main import create_app
    from app.services.decision_engine import DecisionEngine
    from app.services.trade_service import TradeService
    from app.services.user_service import UserService
    from app.web.pages import history_page

    with session_local()() as session:
        UserService().init_user(
            session,
            initial_capital=5000,
            risk_level="中性",
            allow_gold=True,
            allow_bond=True,
            allow_overseas=True,
            min_trade_amount=100,
        )
        advice = DecisionEngine().decide(session, now=datetime(2026, 3, 10, 10, 0, 0))
        TradeService().record_trade(
            session,
            {
                "executed_at": datetime(2026, 3, 10, 10, 6, 0),
                "symbol": "510300",
                "name": "沪深300ETF",
                "side": "buy",
                "price": 4.0,
                "amount": 400.0,
                "fee": 1.0,
                "related_advice_id": advice["id"],
                "note": "history link",
            },
        )

        payload = advice_history(db=session)
        assert len(payload) == 1
        assert payload[0]["id"] == advice["id"]
        assert payload[0]["source_label"] == "test-seed"
        assert payload[0]["linked_trade_count"] == 1

        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/history",
                "headers": [],
                "query_string": b"",
                "app": create_app(),
            }
        )
        page_response = history_page(request=request, db=session)
        body = page_response.body.decode("utf-8")
        assert "建议历史" in body
        assert "test-seed" in body
        assert "1 笔" in body


def test_portfolio_uses_latest_feature_price_even_if_trade_symbol_has_spaces(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    seed_decision_inputs(session_local)

    from app.services.performance_service import PerformanceService
    from app.services.portfolio_service import PortfolioService
    from app.services.trade_service import TradeService
    from app.services.user_service import UserService

    with session_local()() as session:
        UserService().init_user(
            session,
            initial_capital=1000,
            risk_level="中性",
            allow_gold=True,
            allow_bond=True,
            allow_overseas=True,
            min_trade_amount=100,
        )
        TradeService().record_trade(
            session,
            {
                "executed_at": datetime(2026, 3, 10, 10, 5, 0),
                "symbol": "  510300 ",
                "name": " 沪深300ETF ",
                "side": " buy ",
                "price": 4.0,
                "amount": 400.0,
                "fee": 0.0,
                "note": " spaced symbol ",
            },
        )

        portfolio = PortfolioService().get_portfolio_summary(session)
        holding = portfolio["holdings"][0]
        performance = PerformanceService().get_summary(session)

        assert holding["symbol"] == "510300"
        assert holding["name"] == "沪深300ETF"
        assert holding["last_price"] == pytest.approx(1.2)
        assert holding["market_value"] == pytest.approx(120.0)
        assert portfolio["total_asset"] == pytest.approx(720.0)
        assert performance["cumulative_return_pct"] == pytest.approx(-28.0)


def test_dashboard_page_shows_data_status_banner(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    seed_decision_inputs(session_local)

    from app.main import create_app
    from app.services.decision_engine import DecisionEngine
    from app.services.user_service import UserService
    from app.web.pages import home

    with session_local()() as session:
        UserService().init_user(
            session,
            initial_capital=5000,
            risk_level="中性",
            allow_gold=True,
            allow_bond=True,
            allow_overseas=True,
            min_trade_amount=100,
        )
        DecisionEngine().decide(session, now=datetime(2026, 3, 10, 10, 0, 0))

        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/",
                "headers": [],
                "query_string": b"",
                "app": create_app(),
            }
        )
        page_response = home(request=request, db=session)
        body = page_response.body.decode("utf-8")
        assert "当前数据状态" in body
        assert "test-seed" in body
        assert "截至 2026-03-10 的日线数据" in body
        assert "最新数据抓取时间" in body


def test_dashboard_page_shows_latest_snapshot_time_and_stale_advice_note(monkeypatch):
    session_local = setup_test_db(monkeypatch)
    seed_decision_inputs(session_local)

    from app.db.models import MarketSnapshot
    from app.main import create_app
    from app.services.decision_engine import DecisionEngine
    from app.services.user_service import UserService
    from app.web.pages import home

    with session_local()() as session:
        UserService().init_user(
            session,
            initial_capital=5000,
            risk_level="中性",
            allow_gold=True,
            allow_bond=True,
            allow_overseas=True,
            min_trade_amount=100,
        )
        DecisionEngine().decide(session, now=datetime(2026, 3, 10, 10, 0, 0))
        session.add(
            MarketSnapshot(
                trade_date=date(2026, 3, 10),
                captured_at=datetime(2026, 3, 10, 16, 44, 36),
                session_mode="after_close",
                market_regime="中性",
                broad_index_score=72.0,
                risk_appetite_score=66.0,
                trend_score=70.0,
                recommended_position_pct=0.5,
                raw_json=json.dumps(
                    {
                        "source": {
                            "code": "akshare",
                            "label": "latest-seed",
                            "api": "seed",
                            "data_type": "日线历史",
                            "trade_date": "2026-03-10",
                            "captured_at": "2026-03-10T16:44:36",
                            "note": "latest snapshot for banner test",
                        },
                        "quality_summary": {
                            "verification_status": "ok",
                            "latest_available_date": "2026-03-10",
                            "supports_live_execution": False,
                        },
                    },
                    ensure_ascii=False,
                ),
            )
        )
        session.commit()

        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/",
                "headers": [],
                "query_string": b"",
                "app": create_app(),
            }
        )
        page_response = home(request=request, db=session)
        body = page_response.body.decode("utf-8")
        assert "latest-seed" in body
        assert "2026-03-10 16:44" in body
        assert "当前建议数据时间" in body
        assert "2026-03-10 09:35" in body
        assert "如果要让建议与最新数据同步，需要重新生成建议" in body


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
        "category": "宽基",
        "asset_class": "股票",
        "trade_mode": "T+1",
        "trade_mode_note": "股票 ETF 按 T+1 节奏看待。",
        "execution_timing_mode": "stock_windowed",
        "execution_timing_label": "时间段优化",
        "recommended_execution_windows": ["09:35-10:30", "13:30-14:30"],
        "avoid_execution_windows": ["09:15-09:30"],
        "timing_note": "股票类 ETF 更适合等开盘后 09:35-10:30 或 13:30-14:30，再看盘面是否稳定，不建议机械追价。",
        "timing_rule_applied": True,
        "timing_display_enabled": True,
        "current_execution_phase": "preopen",
        "latest_price": 1.2,
        "lot_size": 100.0,
        "fee_rate": 0.0003,
        "min_fee": 1.0,
        "estimated_fee": 1.0,
        "estimated_cost_rate": 0.004166,
        "is_cost_efficient": True,
        "cost_reason": "",
        "min_advice_amount": 100.0,
        "min_order_amount": 120.0,
        "available_cash": 1000.0,
        "budget_gap_to_min_order": 0.0,
        "is_budget_executable": True,
        "passes_min_advice": True,
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
        "category": "宽基",
        "asset_class": "股票",
        "trade_mode": "T+1",
        "trade_mode_note": "股票 ETF 按 T+1 节奏看待。",
        "execution_timing_mode": "stock_windowed",
        "execution_timing_label": "时间段优化",
        "recommended_execution_windows": ["09:35-10:30", "13:30-14:30"],
        "avoid_execution_windows": ["09:15-09:30"],
        "timing_note": "股票类 ETF 不建议在开盘混乱时段机械追价。",
        "timing_rule_applied": True,
        "timing_display_enabled": True,
        "current_execution_phase": "preopen",
        "latest_price": 5.0,
        "lot_size": 100.0,
        "fee_rate": 0.0003,
        "min_fee": 1.0,
        "estimated_fee": 1.0,
        "estimated_cost_rate": 0.005556,
        "is_cost_efficient": True,
        "cost_reason": "",
        "min_advice_amount": 100.0,
        "min_order_amount": 500.0,
        "available_cash": 1000.0,
        "budget_gap_to_min_order": 320.0,
        "is_budget_executable": False,
        "passes_min_advice": True,
        "is_executable": False,
        "execution_status": "关注标的",
        "recommendation_bucket": "watchlist_recommendations",
        "not_executable_reason": "当前预算不足以买入 1 手。",
        "execution_note": "综合评分不错，但当前建议金额还买不起 1 手。",
    }
    affordable_but_weak_item = {
        "symbol": "513500",
        "name": "标普500ETF",
        "rank": 2,
        "action": "不建议执行",
        "suggested_amount": 230.5,
        "suggested_pct": 0.2305,
        "trigger_price_low": 2.29,
        "trigger_price_high": 2.33,
        "stop_loss_pct": 0.07,
        "take_profit_pct": 0.12,
        "score": 18.0,
        "score_gap": 68.0,
        "reason_short": "这只 ETF 虽然买得起，但当前信号还不够强。",
        "risk_level": "中",
        "category": "跨境",
        "asset_class": "跨境",
        "trade_mode": "T+0",
        "trade_mode_note": "跨境 ETF 按 T+0 节奏处理，但还要额外关注外盘和汇率波动。",
        "execution_timing_mode": "cross_border_observe",
        "execution_timing_label": "单独观察",
        "recommended_execution_windows": [],
        "avoid_execution_windows": [],
        "timing_note": "跨境 ETF 暂不直接套用股票类的固定时间窗口，第一版先保守处理。",
        "timing_rule_applied": False,
        "timing_display_enabled": True,
        "current_execution_phase": "preopen",
        "latest_price": 2.305,
        "lot_size": 100.0,
        "fee_rate": 0.0003,
        "min_fee": 1.0,
        "estimated_fee": 1.0,
        "estimated_cost_rate": 0.004338,
        "is_cost_efficient": True,
        "cost_reason": "",
        "min_advice_amount": 100.0,
        "min_order_amount": 230.5,
        "available_cash": 1000.0,
        "budget_gap_to_min_order": 0.0,
        "is_budget_executable": True,
        "passes_min_advice": True,
        "is_executable": False,
        "execution_status": "买得起但当前不建议买",
        "recommendation_bucket": "affordable_but_weak_recommendations",
        "not_executable_reason": "综合分 18.0 还没达到出手阈值 55.0；当前绝对趋势过滤还没通过。",
        "execution_note": "这只 ETF 当前在仓位内买得起 1 手，但当前信号还不够强，所以系统不建议为了凑交易硬买。",
        "is_affordable_but_weak": True,
        "weak_signal_reason": "综合分 18.0 还没达到出手阈值 55.0；当前绝对趋势过滤还没通过。",
    }
    cost_inefficient_item = {
        "symbol": "518880",
        "name": "黄金ETF",
        "rank": 3,
        "action": "不建议执行",
        "suggested_amount": 120.0,
        "suggested_pct": 0.12,
        "trigger_price_low": 5.97,
        "trigger_price_high": 6.06,
        "stop_loss_pct": 0.05,
        "take_profit_pct": 0.08,
        "score": 81.0,
        "score_gap": 7.0,
        "reason_short": "趋势不错，但当前手续费占比偏高。",
        "risk_level": "中",
        "category": "黄金",
        "asset_class": "黄金",
        "trade_mode": "T+0",
        "trade_mode_note": "黄金 ETF 按 T+0 节奏处理。",
        "execution_timing_mode": "defensive_allocation",
        "execution_timing_label": "避险配置",
        "recommended_execution_windows": [],
        "avoid_execution_windows": [],
        "timing_note": "黄金 ETF 更偏避险配置，时间优化弱于股票类。",
        "timing_rule_applied": False,
        "timing_display_enabled": True,
        "current_execution_phase": "preopen",
        "latest_price": 6.0,
        "lot_size": 100.0,
        "fee_rate": 0.0003,
        "min_fee": 10.0,
        "estimated_fee": 10.0,
        "estimated_cost_rate": 0.083333,
        "is_cost_efficient": False,
        "cost_reason": "预计手续费约 10 元，占建议金额 8.33%，当前执行不划算。",
        "min_advice_amount": 100.0,
        "min_order_amount": 100.0,
        "available_cash": 1000.0,
        "budget_gap_to_min_order": 0.0,
        "is_budget_executable": True,
        "passes_min_advice": True,
        "is_executable": False,
        "execution_status": "手续费偏高",
        "recommendation_bucket": "cost_inefficient_recommendations",
        "not_executable_reason": "",
        "execution_note": "趋势不错，但按这次建议金额估算手续费偏高。",
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
                        "affordable_but_weak_recommendations": [affordable_but_weak_item],
                        "watchlist_recommendations": [watchlist_item],
                        "cost_inefficient_recommendations": [cost_inefficient_item],
                        "show_watchlist_recommendations": True,
                        "show_cost_inefficient_recommendations": True,
                        "budget_filter_enabled": True,
                        "fee_filter_enabled": True,
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
        assert payload["affordable_but_weak_recommendations"][0]["symbol"] == "513500"
        assert payload["watchlist_recommendations"][0]["symbol"] == "512100"
        assert payload["watchlist_recommendations"][0]["is_executable"] is False
        assert payload["cost_inefficient_recommendations"][0]["symbol"] == "518880"

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
        assert "买得起但当前不建议买" in body
        assert "关注标的" in body
        assert "手续费暂不划算" in body
        assert "当前预算还买不起 1 手" in body
        assert "仓位内买得起 1 手" in body
        assert "信号还不够强" in body
        assert "T+1" in body
        assert "09:35-10:30" in body
        assert "09:15-09:30" in body
        assert "机械追价" in body


def test_advice_page_marks_budget_substitute_recommendation(monkeypatch):
    session_local = setup_test_db(monkeypatch)

    from app.db.models import AdviceRecord
    from app.main import create_app
    from app.web.pages import advice_detail_page

    substitute_item = {
        "symbol": "510300",
        "name": "沪深300ETF",
        "rank": 1,
        "action": "买入",
        "suggested_amount": 470.0,
        "suggested_pct": 0.47,
        "trigger_price_low": 4.65,
        "trigger_price_high": 4.75,
        "stop_loss_pct": 0.05,
        "take_profit_pct": 0.1,
        "score": 76.5,
        "score_gap": 0.0,
        "reason_short": "当前主配置标的买不起，这只是预算内替代执行。",
        "risk_level": "中",
        "category": "宽基",
        "asset_class": "股票",
        "trade_mode": "T+1",
        "trade_mode_note": "股票 ETF 按 T+1 节奏看待。 这不是当前主配置首选，所以金额仍应控制在轻仓范围内。",
        "latest_price": 4.7,
        "lot_size": 100.0,
        "fee_rate": 0.0003,
        "min_fee": 1.0,
        "estimated_fee": 1.0,
        "estimated_cost_rate": 0.002128,
        "is_cost_efficient": True,
        "cost_reason": "",
        "min_advice_amount": 100.0,
        "min_order_amount": 470.0,
        "available_cash": 1000.0,
        "budget_gap_to_min_order": 0.0,
        "is_budget_executable": True,
        "passes_min_advice": True,
        "is_executable": True,
        "execution_status": "预算内替代执行",
        "recommendation_bucket": "executable_recommendations",
        "not_executable_reason": "",
        "execution_note": "按当前策略，主配置优先是债券，但你现在买不起对应的 1 手，所以给出预算内替代执行。",
        "is_budget_substitute": True,
        "primary_asset_class": "债券",
        "budget_substitute_reason": "主配置优先看债券，但当前预算下先给出可执行的替代标的。",
    }
    watchlist_item = {
        "symbol": "511010",
        "name": "国债ETF",
        "rank": 1,
        "action": "关注",
        "suggested_amount": 150.0,
        "suggested_pct": 0.15,
        "trigger_price_low": 139.5,
        "trigger_price_high": 140.5,
        "stop_loss_pct": 0.03,
        "take_profit_pct": 0.05,
        "score": 79.2,
        "score_gap": 0.0,
        "reason_short": "主配置首选，但当前预算还买不起。",
        "risk_level": "低",
        "category": "债券",
        "asset_class": "债券",
        "trade_mode": "T+0",
        "trade_mode_note": "债券 ETF 按 T+0 节奏处理。",
        "latest_price": 140.0,
        "lot_size": 100.0,
        "fee_rate": 0.0003,
        "min_fee": 1.0,
        "estimated_fee": 1.0,
        "estimated_cost_rate": 0.006667,
        "is_cost_efficient": True,
        "cost_reason": "",
        "min_advice_amount": 100.0,
        "min_order_amount": 14000.0,
        "available_cash": 1000.0,
        "budget_gap_to_min_order": 13850.0,
        "is_budget_executable": False,
        "passes_min_advice": True,
        "is_executable": False,
        "execution_status": "关注标的",
        "recommendation_bucket": "watchlist_recommendations",
        "not_executable_reason": "当前预算不足以买入 1 手。",
        "execution_note": "当前资产类别方向没问题，但你现在买不起 1 手。",
    }

    with session_local()() as session:
        advice = AdviceRecord(
            advice_date=date(2026, 3, 10),
            created_at=datetime(2026, 3, 10, 12, 45, 0),
            session_mode="pre_open",
            action="买入",
            market_regime="防守",
            target_position_pct=0.25,
            current_position_pct=0.0,
            summary_text="主配置标的暂时买不起，系统补充给出预算内替代执行。",
            risk_text="注意轻仓执行。",
            status="active",
            evidence_json=json.dumps(
                {
                    "recommendation_groups": {
                        "executable_recommendations": [substitute_item],
                        "watchlist_recommendations": [watchlist_item],
                        "cost_inefficient_recommendations": [],
                        "show_watchlist_recommendations": True,
                        "show_cost_inefficient_recommendations": True,
                        "budget_filter_enabled": True,
                        "fee_filter_enabled": True,
                    }
                },
                ensure_ascii=False,
            ),
        )
        session.add(advice)
        session.commit()
        session.refresh(advice)

        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": f"/advice/{advice.id}",
                "headers": [],
                "query_string": b"",
                "app": create_app(),
            }
        )
        page_response = advice_detail_page(advice.id, request=request, db=session)
        body = page_response.body.decode("utf-8")
        assert "预算内替代执行" in body
        assert "主配置首选：债券" in body
        assert "这只是预算内替代执行，不是当前主配置首选" in body


def test_advice_page_shows_best_unaffordable_recommendation(monkeypatch):
    session_local = setup_test_db(monkeypatch)

    from app.db.models import AdviceRecord
    from app.main import create_app
    from app.web.pages import advice_detail_page

    best_unaffordable_item = {
        "symbol": "511010",
        "name": "国债ETF",
        "rank": 1,
        "action": "关注",
        "suggested_amount": 150.0,
        "suggested_pct": 0.15,
        "trigger_price_low": 139.5,
        "trigger_price_high": 140.5,
        "stop_loss_pct": 0.03,
        "take_profit_pct": 0.05,
        "score": 79.2,
        "score_gap": 0.0,
        "reason_short": "当前最优，但这次买不起 1 手。",
        "risk_level": "低",
        "category": "债券",
        "asset_class": "债券",
        "trade_mode": "T+0",
        "trade_mode_note": "债券 ETF 按 T+0 节奏处理。",
        "latest_price": 140.0,
        "lot_size": 100.0,
        "fee_rate": 0.0003,
        "min_fee": 1.0,
        "estimated_fee": 1.0,
        "estimated_cost_rate": 0.006667,
        "is_cost_efficient": True,
        "cost_reason": "",
        "min_advice_amount": 100.0,
        "min_order_amount": 14000.0,
        "available_cash": 1000.0,
        "budget_gap_to_min_order": 13850.0,
        "is_budget_executable": False,
        "passes_min_advice": True,
        "is_executable": False,
        "execution_status": "关注标的",
        "recommendation_bucket": "watchlist_recommendations",
        "not_executable_reason": "当前预算不足以买入 1 手。",
        "execution_note": "它仍是当前更优先的一只 ETF，但你这次还买不起 1 手。",
        "is_best_unaffordable": True,
        "best_unaffordable_reason": "它仍是当前更优先的一只 ETF，但你这次还买不起 1 手。",
    }

    with session_local()() as session:
        advice = AdviceRecord(
            advice_date=date(2026, 3, 10),
            created_at=datetime(2026, 3, 10, 13, 5, 0),
            session_mode="pre_open",
            action="不操作",
            market_regime="防守",
            target_position_pct=0.25,
            current_position_pct=0.0,
            summary_text="主配置里最优先的 ETF 这次仍买不起 1 手，先不执行。",
            risk_text="注意控制仓位。",
            status="active",
            evidence_json=json.dumps(
                {
                    "recommendation_groups": {
                        "executable_recommendations": [],
                        "best_unaffordable_recommendation": best_unaffordable_item,
                        "watchlist_recommendations": [best_unaffordable_item],
                        "cost_inefficient_recommendations": [],
                        "show_watchlist_recommendations": True,
                        "show_cost_inefficient_recommendations": True,
                        "budget_filter_enabled": True,
                        "fee_filter_enabled": True,
                    }
                },
                ensure_ascii=False,
            ),
        )
        session.add(advice)
        session.commit()
        session.refresh(advice)

        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": f"/advice/{advice.id}",
                "headers": [],
                "query_string": b"",
                "app": create_app(),
            }
        )
        page_response = advice_detail_page(advice.id, request=request, db=session)
        body = page_response.body.decode("utf-8")
        assert "当前最优但暂时买不起" in body
        assert "它仍是当前更优先的一只 ETF" in body
        assert "国债ETF" in body


def test_advice_page_handles_sparse_watchlist_item_fields(monkeypatch):
    session_local = setup_test_db(monkeypatch)

    from app.db.models import AdviceRecord
    from app.main import create_app
    from app.web.pages import advice_page

    sparse_watchlist_item = {
        "symbol": "511990",
        "name": "华宝添益",
        "rank": 1,
        "action": "转入货币ETF",
        "action_code": "park_in_money_etf",
        "suggested_amount": 1128.02,
        "reason_short": "防守切换成立，但当前剩余可停车资金还不到最小可执行门槛。",
        "risk_level": "低",
        "category": "money_etf",
        "asset_class": "货币ETF",
        "trade_mode": "T+0",
        "execution_status": "等待转入货币ETF",
        "recommendation_bucket": "watchlist_recommendations",
        "execution_note": "当前建议金额还不到最小可执行门槛。",
        "score": 40.0,
        "decision_score": 40.0,
    }

    with session_local()() as session:
        advice = AdviceRecord(
            advice_date=date(2026, 3, 10),
            created_at=datetime(2026, 3, 10, 15, 0, 0),
            session_mode="after_close",
            action="不操作",
            market_regime="防守",
            target_position_pct=0.0,
            current_position_pct=0.2,
            summary_text="当前更偏防守，先观察。",
            risk_text="暂不追单。",
            status="active",
            evidence_json=json.dumps(
                {
                    "recommendation_groups": {
                        "executable_recommendations": [],
                        "watchlist_recommendations": [sparse_watchlist_item],
                        "show_watchlist_recommendations": True,
                        "show_cost_inefficient_recommendations": False,
                        "budget_filter_enabled": True,
                        "fee_filter_enabled": False,
                    }
                },
                ensure_ascii=False,
            ),
        )
        session.add(advice)
        session.commit()
        session.refresh(advice)

        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/advice",
                "headers": [],
                "query_string": b"",
                "app": create_app(),
            }
        )
        page_response = advice_page(request=request, db=session)
        body = page_response.body.decode("utf-8")
        assert "关注标的" in body
        assert "华宝添益" in body
        assert "还差金额" in body


def test_structured_recommendation_groups_do_not_fallback_watchlist_into_executable(monkeypatch):
    session_local = setup_test_db(monkeypatch)

    from app.db.models import AdviceItem, AdviceRecord
    from app.web.presenters import serialize_advice_record

    watchlist_item = {
        "symbol": "511990",
        "name": "华宝添益",
        "rank": 1,
        "action": "转入货币ETF",
        "action_code": "park_in_money_etf",
        "suggested_amount": 1128.02,
        "score": 40.0,
        "decision_score": 40.0,
        "risk_level": "低",
        "category": "money_etf",
        "asset_class": "货币ETF",
        "trade_mode": "T+0",
        "min_order_amount": 9999.5,
        "execution_status": "等待转入货币ETF",
        "recommendation_bucket": "watchlist_recommendations",
        "is_executable": False,
        "executable_now": False,
        "execution_note": "当前剩余可停车资金还不到最小可执行门槛。",
    }

    with session_local()() as session:
        advice = AdviceRecord(
            advice_date=date(2026, 3, 10),
            created_at=datetime(2026, 3, 10, 15, 30, 0),
            session_mode="after_close",
            action="转入货币ETF",
            market_regime="防守",
            target_position_pct=0.0,
            current_position_pct=0.2,
            summary_text="进攻边不足，先防守。",
            risk_text="暂不追单。",
            status="active",
            evidence_json=json.dumps(
                {
                    "recommendation_groups": {
                        "executable_recommendations": [],
                        "watchlist_recommendations": [watchlist_item],
                        "show_watchlist_recommendations": True,
                        "show_cost_inefficient_recommendations": False,
                        "budget_filter_enabled": True,
                        "fee_filter_enabled": False,
                    }
                },
                ensure_ascii=False,
            ),
        )
        session.add(advice)
        session.flush()
        session.add(
            AdviceItem(
                advice_id=advice.id,
                rank=1,
                symbol="511990",
                name="华宝添益",
                action="转入货币ETF",
                suggested_amount=1128.02,
                suggested_pct=0.56,
                trigger_price_low=99.49,
                trigger_price_high=100.99,
                stop_loss_pct=0.01,
                take_profit_pct=0.02,
                score=40.0,
                score_gap=0.0,
                reason_short="防守切换成立。",
                risk_level="低",
                action_code="park_in_money_etf",
                category="money_etf",
                tradability_mode="t0",
                target_holding_days=5,
                mapped_horizon_profile="defensive_cash",
                lifecycle_phase="build_phase",
                entry_score=50.0,
                hold_score=50.0,
                exit_score=50.0,
                category_score=46.0,
                decision_score=40.0,
                executable_now=False,
                blocked_reason="当前建议金额不足最小可执行门槛。",
                planned_exit_days=1,
                planned_exit_rule_summary="当前时段只生成预案，下一交易时段再执行。",
            )
        )
        session.commit()
        session.refresh(advice)

        payload = serialize_advice_record(advice)
        assert payload["executable_recommendations"] == []
        assert payload["watchlist_recommendations"][0]["symbol"] == "511990"
        assert payload["watchlist_recommendations"][0]["min_order_amount"] == 9999.5
