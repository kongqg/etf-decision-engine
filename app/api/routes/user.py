from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.repositories.user_repo import get_preferences, get_user
from app.schemas.user import InitUserRequest, UpdatePreferencesRequest
from app.services.user_service import UserService


router = APIRouter(prefix="/api", tags=["user"])
user_service = UserService()


def _preferences_payload(preferences) -> dict:
    return {
        "risk_level": preferences.risk_level,
        "allow_gold": preferences.allow_gold,
        "allow_bond": preferences.allow_bond,
        "allow_overseas": preferences.allow_overseas,
        "min_trade_amount": preferences.min_trade_amount,
        "target_holding_days": preferences.target_holding_days,
        "max_total_position_pct": preferences.max_total_position_pct,
        "max_single_position_pct": preferences.max_single_position_pct,
        "cash_reserve_pct": preferences.cash_reserve_pct,
    }


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
        target_holding_days=payload.target_holding_days,
    )
    user = get_user(db)
    preferences = get_preferences(db)
    return jsonable_encoder(
        {
            "user_id": user.id,
            "initial_capital": user.initial_capital,
            "cash_balance": user.cash_balance,
            "total_asset": user.total_asset,
            **_preferences_payload(preferences),
        }
    )


@router.get("/preferences")
def get_user_preferences(db: Session = Depends(get_db)):
    preferences = get_preferences(db)
    if preferences is None:
        raise HTTPException(status_code=404, detail="暂无用户偏好，请先初始化用户。")
    return jsonable_encoder(_preferences_payload(preferences))


@router.put("/preferences")
def update_preferences(payload: UpdatePreferencesRequest, db: Session = Depends(get_db)):
    try:
        preferences = user_service.update_preferences(db, **payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return jsonable_encoder(_preferences_payload(preferences))
