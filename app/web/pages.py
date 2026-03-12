from __future__ import annotations

from datetime import date, datetime, timedelta
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
from app.services.backtest_service import BacktestRequest, BacktestService
from app.services.capital_flow_service import CapitalFlowService
from app.services.data_evidence_service import DataEvidenceService
from app.services.decision_engine import DecisionEngine
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
backtest_service = BacktestService()


def _data_status_context(db: Session, advice=None) -> dict | None:
    latest_snapshot = get_latest_market_snapshot(db)
    reference_advice = advice if advice is not None else get_latest_advice(db)
    return build_data_status(snapshot=latest_snapshot, advice=reference_advice)


@router.get("/")
def home(request: Request, status: str | None = Query(default=None), db: Session = Depends(get_db)):
    session_mode = detect_session_mode()
    user = get_user(db)
    context = page_context("Dashboard", session_mode, status)
    context["data_status"] = _data_status_context(db)
    if user is None:
        return templates.TemplateResponse(request=request, name="onboarding.html", context=context)

    preferences = get_preferences(db)
    portfolio = portfolio_service.get_portfolio_summary(db)
    latest_advice = get_latest_advice(db)
    context.update(
        {
            "user": user,
            "preferences": preferences,
            "portfolio": portfolio,
            "advice": serialize_advice_record(latest_advice),
            "performance": performance_service.get_summary(db),
        }
    )
    return templates.TemplateResponse(request=request, name="dashboard.html", context=context)


@router.get("/advice")
def advice_page(request: Request, status: str | None = Query(default=None), db: Session = Depends(get_db)):
    advice = get_latest_advice(db)
    session_mode = detect_session_mode()
    context = page_context("Advice", session_mode, status)
    context["data_status"] = _data_status_context(db, advice)
    context["advice"] = serialize_advice_record(advice) if advice else None
    context["explanation"] = serialize_explanations(get_explanations_by_advice(db, advice.id)) if advice else None
    return templates.TemplateResponse(request=request, name="advice.html", context=context)


@router.get("/advice/{advice_id}")
def advice_detail_page(advice_id: int, request: Request, db: Session = Depends(get_db)):
    advice = get_advice_by_id(db, advice_id)
    session_mode = detect_session_mode()
    context = page_context("Advice", session_mode)
    context["data_status"] = _data_status_context(db, advice)
    context["advice"] = serialize_advice_record(advice) if advice else None
    context["explanation"] = serialize_explanations(get_explanations_by_advice(db, advice_id)) if advice else None
    return templates.TemplateResponse(request=request, name="advice.html", context=context)


@router.get("/explanation/{advice_id}")
def explanation_page(advice_id: int, request: Request, db: Session = Depends(get_db)):
    advice = get_advice_by_id(db, advice_id)
    session_mode = detect_session_mode()
    context = page_context("Explanation", session_mode)
    context["data_status"] = _data_status_context(db, advice)
    context["advice"] = serialize_advice_record(advice) if advice else None
    context["explanation"] = serialize_explanations(get_explanations_by_advice(db, advice_id)) if advice else None
    return templates.TemplateResponse(request=request, name="explanation.html", context=context)


@router.get("/evidence")
def evidence_page(request: Request, status: str | None = Query(default=None), db: Session = Depends(get_db)):
    session_mode = detect_session_mode()
    context = page_context("Evidence", session_mode, status)
    latest_advice = get_latest_advice(db)
    context["data_status"] = _data_status_context(db, latest_advice)
    context["advice"] = serialize_advice_record(latest_advice) if latest_advice else None
    try:
        context["evidence"] = data_evidence_service.build(db)
    except ValueError:
        context["evidence"] = None
    return templates.TemplateResponse(request=request, name="evidence.html", context=context)


@router.get("/evidence/{advice_id}")
def evidence_detail_page(advice_id: int, request: Request, db: Session = Depends(get_db)):
    session_mode = detect_session_mode()
    advice = get_advice_by_id(db, advice_id)
    context = page_context("Evidence", session_mode)
    context["data_status"] = _data_status_context(db, advice)
    context["advice"] = serialize_advice_record(advice) if advice else None
    try:
        context["evidence"] = data_evidence_service.build(db, advice_id=advice_id)
    except ValueError:
        context["evidence"] = None
    return templates.TemplateResponse(request=request, name="evidence.html", context=context)


@router.get("/portfolio")
def portfolio_page(request: Request, status: str | None = Query(default=None), db: Session = Depends(get_db)):
    session_mode = detect_session_mode()
    latest_advice = get_latest_advice(db)
    serialized_advice = serialize_advice_record(latest_advice)
    context = page_context("Portfolio", session_mode, status)
    context["portfolio"] = merge_portfolio_with_advice(portfolio_service.get_portfolio_summary(db), serialized_advice)
    context["advice"] = serialized_advice
    context["data_status"] = _data_status_context(db, latest_advice)
    context["now_iso"] = get_now().strftime("%Y-%m-%dT%H:%M")
    return templates.TemplateResponse(request=request, name="portfolio.html", context=context)


@router.get("/performance")
def performance_page(request: Request, status: str | None = Query(default=None), db: Session = Depends(get_db)):
    session_mode = detect_session_mode()
    context = page_context("Performance", session_mode, status)
    context["performance"] = performance_service.get_summary(db)
    context["portfolio"] = portfolio_service.get_portfolio_summary(db)
    context["data_status"] = _data_status_context(db)
    return templates.TemplateResponse(request=request, name="performance.html", context=context)


@router.get("/backtest")
def backtest_page(
    request: Request,
    run_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    session_mode = detect_session_mode()
    preferences = get_preferences(db)
    user = get_user(db)
    now = get_now().date()
    context = page_context("Backtest", session_mode, status)
    context["preferences"] = preferences
    context["user"] = user
    context["backtest_result"] = backtest_service.load_saved_run(run_id) if run_id else None
    context["latest_backtest_runs"] = backtest_service.list_saved_runs(limit=12)
    context["default_backtest_form"] = {
        "start_date": (now - timedelta(days=120)).isoformat(),
        "end_date": now.isoformat(),
        "initial_capital": float(user.initial_capital) if user is not None else 100000.0,
        "risk_mode": getattr(preferences, "risk_mode", "balanced") if preferences is not None else "balanced",
        "slippage_bps": float(backtest_service.config.get("execution", {}).get("default_slippage_bps", 3.0)),
        "execution_cost_bps": float(backtest_service.execution_cost_service.execution_cost_bps()),
        "replace_threshold": 8.0,
        "max_selected_total": 3,
        "max_selected_per_category": 2,
        "min_final_score_for_target": 55.0,
    }
    context["data_status"] = _data_status_context(db)
    return templates.TemplateResponse(request=request, name="backtest.html", context=context)


@router.get("/settings")
def settings_page(request: Request, status: str | None = Query(default=None), db: Session = Depends(get_db)):
    session_mode = detect_session_mode()
    context = page_context("Settings", session_mode, status)
    context["preferences"] = get_preferences(db)
    context["user"] = get_user(db)
    context["data_status"] = _data_status_context(db)
    return templates.TemplateResponse(request=request, name="settings.html", context=context)


@router.get("/history")
def history_page(request: Request, status: str | None = Query(default=None), db: Session = Depends(get_db)):
    session_mode = detect_session_mode()
    context = page_context("History", session_mode, status)
    context["history_rows"] = serialize_advice_history(list_advices(db, limit=80), trade_stats_by_advice(db))
    context["data_status"] = _data_status_context(db)
    return templates.TemplateResponse(request=request, name="history.html", context=context)


@router.post("/actions/run-backtest")
def run_backtest_action(
    start_date: str = Form(...),
    end_date: str = Form(...),
    initial_capital: float = Form(...),
    risk_mode: str = Form(default="balanced"),
    slippage_bps: float = Form(default=3.0),
    execution_cost_bps: float = Form(default=5.0),
    use_live_trades: bool = Form(default=False),
    allow_weak_data: bool = Form(default=False),
    replace_threshold: float = Form(default=8.0),
    max_selected_total: int = Form(default=3),
    max_selected_per_category: int = Form(default=2),
    min_final_score_for_target: float = Form(default=55.0),
    db: Session = Depends(get_db),
):
    try:
        result = backtest_service.run(
            db,
            BacktestRequest(
                start_date=date.fromisoformat(start_date),
                end_date=date.fromisoformat(end_date),
                initial_capital=initial_capital,
                use_live_trades=use_live_trades,
                risk_mode=risk_mode,
                slippage_bps=slippage_bps,
                execution_cost_bps_override=execution_cost_bps,
                strict_data_quality=not allow_weak_data,
                config_overrides={
                    "selection.replace_threshold": replace_threshold,
                    "selection.max_selected_total": max_selected_total,
                    "selection.max_selected_per_category": max_selected_per_category,
                    "selection.min_final_score_for_target": min_final_score_for_target,
                },
            ),
        )
        return RedirectResponse(url=f"/backtest?run_id={result['run_id']}&status=Backtest completed", status_code=303)
    except ValueError as exc:
        return RedirectResponse(url=f"/backtest?status={exc}", status_code=303)


@router.post("/actions/init-user")
def init_user_action(
    initial_capital: float = Form(...),
    risk_level: str = Form(...),
    risk_mode: str = Form(default="balanced"),
    allow_gold: bool = Form(default=False),
    allow_bond: bool = Form(default=False),
    allow_overseas: bool = Form(default=False),
    min_trade_amount: float = Form(default=settings.default_min_advice_amount),
    db: Session = Depends(get_db),
):
    user_service.init_user(
        db,
        initial_capital=initial_capital,
        risk_level=risk_level,
        risk_mode=risk_mode,
        allow_gold=allow_gold,
        allow_bond=allow_bond,
        allow_overseas=allow_overseas,
        min_trade_amount=min_trade_amount,
    )
    return RedirectResponse(url="/?status=User initialized", status_code=303)


@router.post("/actions/update-preferences")
def update_preferences_action(
    risk_level: str = Form(...),
    risk_mode: str = Form(default="balanced"),
    allow_gold: bool = Form(default=False),
    allow_bond: bool = Form(default=False),
    allow_overseas: bool = Form(default=False),
    min_trade_amount: float = Form(default=settings.default_min_advice_amount),
    max_total_position_pct: float = Form(...),
    max_single_position_pct: float = Form(...),
    cash_reserve_pct: float = Form(...),
    db: Session = Depends(get_db),
):
    try:
        user_service.update_preferences(
            db,
            risk_level=risk_level,
            risk_mode=risk_mode,
            allow_gold=allow_gold,
            allow_bond=allow_bond,
            allow_overseas=allow_overseas,
            min_trade_amount=min_trade_amount,
            max_total_position_pct=max_total_position_pct / 100,
            max_single_position_pct=max_single_position_pct / 100,
            cash_reserve_pct=cash_reserve_pct / 100,
        )
        return RedirectResponse(url="/settings?status=Preferences updated", status_code=303)
    except ValueError as exc:
        return RedirectResponse(url=f"/settings?status={exc}", status_code=303)


@router.post("/actions/refresh-data")
def refresh_data_action(db: Session = Depends(get_db)):
    result = market_data_service.refresh_data(db)
    return RedirectResponse(url=f"/?status=Data refreshed ({result['data_source']})", status_code=303)


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
    advice_item_id: int | None = Form(default=None),
    intent: str = Form(default=""),
    weight_before: float = Form(default=0.0),
    weight_after: float = Form(default=0.0),
    note: str = Form(default=""),
    executed_at: str = Form(...),
    db: Session = Depends(get_db),
):
    try:
        trade = trade_service.record_trade(
            db,
            {
                "symbol": symbol,
                "name": name,
                "side": side,
                "price": price,
                "amount": amount,
                "quantity": quantity,
                "fee": fee,
                "related_advice_id": related_advice_id,
                "advice_item_id": advice_item_id,
                "intent": intent,
                "weight_before": weight_before,
                "weight_after": weight_after,
                "note": note,
                "executed_at": datetime.fromisoformat(executed_at),
            },
        )
        portfolio_service.update_market_prices(db)
        performance_service.capture_snapshot(db, snapshot_date=trade.executed_at.date())
        return RedirectResponse(url="/portfolio?status=Trade recorded", status_code=303)
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
        performance_service.capture_snapshot(db, snapshot_date=flow.executed_at.date())
        return RedirectResponse(url="/portfolio?status=Capital adjusted", status_code=303)
    except ValueError as exc:
        return RedirectResponse(url=f"/portfolio?status={exc}", status_code=303)
