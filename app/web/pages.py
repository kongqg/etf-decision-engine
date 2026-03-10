from __future__ import annotations

from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.database import get_db
from app.repositories.advice_repo import get_advice_by_id, get_explanations_by_advice, get_latest_advice, list_advices
from app.repositories.market_repo import get_latest_market_snapshot
from app.repositories.portfolio_repo import trade_stats_by_advice
from app.repositories.user_repo import get_preferences, get_user
from app.services.decision_engine import DecisionEngine
from app.services.data_evidence_service import DataEvidenceService
from app.services.capital_flow_service import CapitalFlowService
from app.services.market_data_service import MarketDataService
from app.services.performance_service import PerformanceService
from app.services.portfolio_service import PortfolioService
from app.services.trade_service import TradeService
from app.services.user_service import UserService
from app.utils.dates import detect_session_mode, get_now
from app.web.presenters import (
    build_data_status,
    merge_portfolio_with_advice,
    page_context,
    serialize_advice_history,
    serialize_advice_record,
    serialize_explanations,
)


settings = get_settings()
templates = Jinja2Templates(directory=str(Path(settings.base_dir) / "templates"))
router = APIRouter(tags=["pages"])

user_service = UserService()
market_data_service = MarketDataService()
decision_engine = DecisionEngine()
data_evidence_service = DataEvidenceService()
portfolio_service = PortfolioService()
trade_service = TradeService()
capital_flow_service = CapitalFlowService()
performance_service = PerformanceService()


def _data_status_context(db: Session, advice=None) -> dict | None:
    latest_snapshot = get_latest_market_snapshot(db)
    reference_advice = advice if advice is not None else get_latest_advice(db)
    return build_data_status(snapshot=latest_snapshot, advice=reference_advice)


@router.get("/")
def home(request: Request, status: str | None = Query(default=None), db: Session = Depends(get_db)):
    session_mode = detect_session_mode()
    user = get_user(db)
    if user is None:
        context = page_context("初始化", session_mode, status)
        context["data_status"] = _data_status_context(db)
        return templates.TemplateResponse(request=request, name="onboarding.html", context=context)

    preferences = get_preferences(db)
    portfolio = portfolio_service.get_portfolio_summary(db)
    latest_advice = get_latest_advice(db)
    advice = serialize_advice_record(latest_advice)
    performance = performance_service.get_summary(db)
    context = page_context("仪表盘", session_mode, status)
    context.update(
        {
            "user": user,
            "preferences": preferences,
            "portfolio": portfolio,
            "advice": advice,
            "performance": performance,
            "data_status": _data_status_context(db, latest_advice),
        }
    )
    return templates.TemplateResponse(request=request, name="dashboard.html", context=context)


@router.get("/advice")
def advice_page(request: Request, status: str | None = Query(default=None), db: Session = Depends(get_db)):
    advice = get_latest_advice(db)
    session_mode = detect_session_mode()
    context = page_context("今日建议", session_mode, status)
    context["data_status"] = _data_status_context(db, advice)
    if advice is None:
        return templates.TemplateResponse(request=request, name="advice.html", context=context)
    context["advice"] = serialize_advice_record(advice)
    context["explanation"] = serialize_explanations(get_explanations_by_advice(db, advice.id))
    return templates.TemplateResponse(request=request, name="advice.html", context=context)


@router.get("/advice/{advice_id}")
def advice_detail_page(advice_id: int, request: Request, db: Session = Depends(get_db)):
    advice = get_advice_by_id(db, advice_id)
    session_mode = detect_session_mode()
    context = page_context("建议详情", session_mode)
    context["data_status"] = _data_status_context(db, advice)
    context["advice"] = serialize_advice_record(advice) if advice else None
    context["explanation"] = serialize_explanations(get_explanations_by_advice(db, advice_id)) if advice else None
    return templates.TemplateResponse(request=request, name="advice.html", context=context)


@router.get("/explanation/{advice_id}")
def explanation_page(advice_id: int, request: Request, db: Session = Depends(get_db)):
    advice = get_advice_by_id(db, advice_id)
    session_mode = detect_session_mode()
    context = page_context("解释详情", session_mode)
    context["data_status"] = _data_status_context(db, advice)
    context["advice"] = serialize_advice_record(advice) if advice else None
    context["explanation"] = serialize_explanations(get_explanations_by_advice(db, advice_id)) if advice else None
    return templates.TemplateResponse(request=request, name="explanation.html", context=context)


@router.get("/evidence")
def evidence_page(request: Request, status: str | None = Query(default=None), db: Session = Depends(get_db)):
    session_mode = detect_session_mode()
    context = page_context("数据证据", session_mode, status)
    try:
        evidence = data_evidence_service.build(db)
        context["evidence"] = evidence
        advice = get_advice_by_id(db, evidence["advice_id"])
        context["advice"] = serialize_advice_record(advice)
        context["data_status"] = _data_status_context(db, advice)
    except ValueError:
        context["evidence"] = None
        context["advice"] = None
        context["data_status"] = _data_status_context(db)
    return templates.TemplateResponse(request=request, name="evidence.html", context=context)


@router.get("/evidence/{advice_id}")
def evidence_detail_page(advice_id: int, request: Request, db: Session = Depends(get_db)):
    session_mode = detect_session_mode()
    context = page_context("数据证据", session_mode)
    try:
        context["evidence"] = data_evidence_service.build(db, advice_id=advice_id)
    except ValueError:
        context["evidence"] = None
    advice = get_advice_by_id(db, advice_id)
    context["data_status"] = _data_status_context(db, advice)
    context["advice"] = serialize_advice_record(advice) if advice else None
    return templates.TemplateResponse(request=request, name="evidence.html", context=context)


@router.get("/portfolio")
def portfolio_page(request: Request, status: str | None = Query(default=None), db: Session = Depends(get_db)):
    session_mode = detect_session_mode()
    context = page_context("持仓", session_mode, status)
    latest_advice = get_latest_advice(db)
    serialized_advice = serialize_advice_record(latest_advice)
    context["portfolio"] = merge_portfolio_with_advice(portfolio_service.get_portfolio_summary(db), serialized_advice)
    context["advice"] = serialized_advice
    context["data_status"] = _data_status_context(db, latest_advice)
    context["now_iso"] = get_now().strftime("%Y-%m-%dT%H:%M")
    return templates.TemplateResponse(request=request, name="portfolio.html", context=context)


@router.get("/performance")
def performance_page(request: Request, status: str | None = Query(default=None), db: Session = Depends(get_db)):
    session_mode = detect_session_mode()
    context = page_context("绩效", session_mode, status)
    context["performance"] = performance_service.get_summary(db)
    context["portfolio"] = portfolio_service.get_portfolio_summary(db)
    context["data_status"] = _data_status_context(db)
    return templates.TemplateResponse(request=request, name="performance.html", context=context)


@router.get("/settings")
def settings_page(request: Request, status: str | None = Query(default=None), db: Session = Depends(get_db)):
    session_mode = detect_session_mode()
    preferences = get_preferences(db)
    context = page_context("设置", session_mode, status)
    context["preferences"] = preferences
    context["user"] = get_user(db)
    context["data_status"] = _data_status_context(db)
    return templates.TemplateResponse(request=request, name="settings.html", context=context)


@router.get("/history")
def history_page(request: Request, status: str | None = Query(default=None), db: Session = Depends(get_db)):
    session_mode = detect_session_mode()
    context = page_context("建议历史", session_mode, status)
    advice_rows = list_advices(db, limit=80)
    context["history_rows"] = serialize_advice_history(advice_rows, trade_stats_by_advice(db))
    context["data_status"] = _data_status_context(db)
    return templates.TemplateResponse(request=request, name="history.html", context=context)


@router.post("/actions/init-user")
def init_user_action(
    initial_capital: float = Form(...),
    risk_level: str = Form(...),
    allow_gold: bool = Form(default=False),
    allow_bond: bool = Form(default=False),
    allow_overseas: bool = Form(default=False),
    min_trade_amount: float = Form(default=settings.default_min_advice_amount),
    target_holding_days: int = Form(default=5),
    db: Session = Depends(get_db),
):
    user_service.init_user(
        db,
        initial_capital=initial_capital,
        risk_level=risk_level,
        allow_gold=allow_gold,
        allow_bond=allow_bond,
        allow_overseas=allow_overseas,
        min_trade_amount=min_trade_amount,
        target_holding_days=target_holding_days,
    )
    return RedirectResponse(url="/?status=用户已初始化", status_code=303)


@router.post("/actions/update-preferences")
def update_preferences_action(
    risk_level: str = Form(...),
    allow_gold: bool = Form(default=False),
    allow_bond: bool = Form(default=False),
    allow_overseas: bool = Form(default=False),
    min_trade_amount: float = Form(default=settings.default_min_advice_amount),
    target_holding_days: int = Form(default=5),
    max_total_position_pct: float = Form(...),
    max_single_position_pct: float = Form(...),
    cash_reserve_pct: float = Form(...),
    db: Session = Depends(get_db),
):
    try:
        user_service.update_preferences(
            db,
            risk_level=risk_level,
            allow_gold=allow_gold,
            allow_bond=allow_bond,
            allow_overseas=allow_overseas,
            min_trade_amount=min_trade_amount,
            target_holding_days=target_holding_days,
            max_total_position_pct=max_total_position_pct / 100,
            max_single_position_pct=max_single_position_pct / 100,
            cash_reserve_pct=cash_reserve_pct / 100,
        )
        return RedirectResponse(url="/settings?status=偏好已更新", status_code=303)
    except ValueError as exc:
        return RedirectResponse(url=f"/settings?status={exc}", status_code=303)


@router.post("/actions/refresh-data")
def refresh_data_action(db: Session = Depends(get_db)):
    result = market_data_service.refresh_data(db)
    return RedirectResponse(url=f"/?status=数据已刷新（{result['data_source']}）", status_code=303)


@router.post("/actions/decide-now")
def decide_now_action(db: Session = Depends(get_db)):
    try:
        advice = decision_engine.decide(db)
        return RedirectResponse(url=f"/advice/{advice['id']}", status_code=303)
    except ValueError as exc:
        return RedirectResponse(url=f"/?status={exc}", status_code=303)


@router.post("/actions/record-trade")
def record_trade_action(
    symbol: str = Form(...),
    name: str = Form(...),
    side: str = Form(...),
    price: float = Form(...),
    amount: float = Form(...),
    quantity: float | None = Form(default=None),
    fee: float = Form(default=0.0),
    related_advice_id: int | None = Form(default=None),
    note: str = Form(default=""),
    executed_at: str = Form(...),
    db: Session = Depends(get_db),
):
    try:
        payload = {
            "symbol": symbol,
            "name": name,
            "side": side,
            "price": price,
            "amount": amount,
            "quantity": quantity,
            "fee": fee,
            "related_advice_id": related_advice_id,
            "note": note,
            "executed_at": datetime.fromisoformat(executed_at),
        }
        trade = trade_service.record_trade(db, payload)
        portfolio_service.update_market_prices(db)
        performance_service.capture_snapshot(db, snapshot_date=trade.executed_at.date())
        return RedirectResponse(url="/portfolio?status=成交已记录", status_code=303)
    except ValueError as exc:
        return RedirectResponse(url=f"/portfolio?status={exc}", status_code=303)


@router.post("/actions/adjust-capital")
def adjust_capital_action(
    flow_type: str = Form(...),
    amount: float = Form(...),
    note: str = Form(default=""),
    executed_at: str = Form(...),
    db: Session = Depends(get_db),
):
    try:
        flow = capital_flow_service.record_adjustment(
            db,
            {
                "flow_type": flow_type,
                "amount": amount,
                "note": note,
                "executed_at": datetime.fromisoformat(executed_at),
            },
        )
        portfolio_service.update_market_prices(db)
        performance_service.capture_snapshot(db, snapshot_date=flow.executed_at.date())
        action_label = "入金" if flow.flow_type == "deposit" else "出金"
        return RedirectResponse(url=f"/portfolio?status={action_label}已记录", status_code=303)
    except ValueError as exc:
        return RedirectResponse(url=f"/portfolio?status={exc}", status_code=303)
