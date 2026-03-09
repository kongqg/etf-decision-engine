from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.repositories.user_repo import get_preferences, get_user
from app.schemas.user import InitUserRequest
from app.services.user_service import UserService


router = APIRouter(prefix="/api", tags=["user"])
user_service = UserService()


@router.post("/init-user")
def init_user(payload: InitUserRequest, db: Session = Depends(get_db)):
    user_service.init_user(
        db,
        initial_capital=payload.initial_capital,
        risk_level=payload.risk_level,
        allow_gold=payload.allow_gold,
        allow_bond=payload.allow_bond,
        allow_overseas=payload.allow_overseas,
        min_trade_amount=payload.min_trade_amount,
    )
    user = get_user(db)
    preferences = get_preferences(db)
    return jsonable_encoder(
        {
            "user_id": user.id,
            "initial_capital": user.initial_capital,
            "cash_balance": user.cash_balance,
            "total_asset": user.total_asset,
            "risk_level": preferences.risk_level,
            "allow_gold": preferences.allow_gold,
            "allow_bond": preferences.allow_bond,
            "allow_overseas": preferences.allow_overseas,
        }
    )
