from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.repositories.advice_repo import get_latest_advice
from app.repositories.user_repo import get_preferences, get_user
from app.services.performance_service import PerformanceService
from app.services.portfolio_service import PortfolioService
from app.utils.dates import detect_session_mode
from app.web.presenters import serialize_advice_record


router = APIRouter(prefix="/api", tags=["performance"])
performance_service = PerformanceService()
portfolio_service = PortfolioService()


@router.get("/performance")
def get_performance(db: Session = Depends(get_db)):
    return jsonable_encoder(performance_service.get_summary(db))


@router.get("/dashboard")
def get_dashboard(db: Session = Depends(get_db)):
    user = get_user(db)
    preferences = get_preferences(db)
    advice = get_latest_advice(db)
    return jsonable_encoder(
        {
            "session_mode": detect_session_mode(),
            "user": {
                "initial_capital": user.initial_capital if user else 0.0,
                "risk_level": preferences.risk_level if preferences else None,
            },
            "portfolio": portfolio_service.get_portfolio_summary(db) if user else None,
            "last_advice": serialize_advice_record(advice) if advice else None,
            "performance": performance_service.get_summary(db) if user else None,
        }
    )
