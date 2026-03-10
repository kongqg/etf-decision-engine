from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.schemas.capital_flow import CapitalAdjustmentRequest
from app.schemas.trade import RecordTradeRequest
from app.services.capital_flow_service import CapitalFlowService
from app.services.performance_service import PerformanceService
from app.services.portfolio_service import PortfolioService
from app.services.trade_service import TradeService


router = APIRouter(prefix="/api", tags=["portfolio"])
portfolio_service = PortfolioService()
trade_service = TradeService()
capital_flow_service = CapitalFlowService()
performance_service = PerformanceService()


@router.post("/record-trade")
def record_trade(payload: RecordTradeRequest, db: Session = Depends(get_db)):
    try:
        trade = trade_service.record_trade(db, payload.model_dump())
        portfolio_service.update_market_prices(db)
        performance_service.capture_snapshot(db, snapshot_date=trade.executed_at.date())
        return jsonable_encoder(
            {
                "trade_id": trade.id,
                "executed_at": trade.executed_at,
                "symbol": trade.symbol,
                "side": trade.side,
                "quantity": trade.quantity,
                "amount": trade.amount,
            }
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/portfolio")
def get_portfolio(db: Session = Depends(get_db)):
    return jsonable_encoder(portfolio_service.get_portfolio_summary(db))


@router.post("/adjust-capital")
def adjust_capital(payload: CapitalAdjustmentRequest, db: Session = Depends(get_db)):
    try:
        flow = capital_flow_service.record_adjustment(db, payload.model_dump())
        portfolio = portfolio_service.get_portfolio_summary(db)
        performance_service.capture_snapshot(db, snapshot_date=flow.executed_at.date())
        return jsonable_encoder(
            {
                "flow_id": flow.id,
                "executed_at": flow.executed_at,
                "flow_type": flow.flow_type,
                "amount": flow.amount,
                "portfolio": portfolio,
            }
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
