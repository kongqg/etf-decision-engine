import json
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
        assert "affordable_but_weak_recommendations" in advice
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
        assert after_facts["practical_buy_cap_amount"] > before_facts["practical_buy_cap_amount"]


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
