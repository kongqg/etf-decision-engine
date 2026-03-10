from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.repositories.advice_repo import get_advice_by_id, get_explanations_by_advice, get_latest_advice, list_advices
from app.repositories.portfolio_repo import trade_stats_by_advice
from app.services.decision_engine import DecisionEngine
from app.web.presenters import serialize_advice_history, serialize_advice_record, serialize_explanations


router = APIRouter(prefix="/api", tags=["advice"])
decision_engine = DecisionEngine()


@router.post("/decide-now")
def decide_now(db: Session = Depends(get_db)):
    try:
        return jsonable_encoder(decision_engine.decide(db))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/last-advice")
def last_advice(db: Session = Depends(get_db)):
    advice = get_latest_advice(db)
    if advice is None:
        raise HTTPException(status_code=404, detail="暂无建议记录。")
    return jsonable_encoder(serialize_advice_record(advice))


@router.get("/advices")
def advice_history(db: Session = Depends(get_db)):
    return jsonable_encoder(serialize_advice_history(list_advices(db), trade_stats_by_advice(db)))


@router.get("/advice/{advice_id}")
def get_advice(advice_id: int, db: Session = Depends(get_db)):
    advice = get_advice_by_id(db, advice_id)
    if advice is None:
        raise HTTPException(status_code=404, detail="未找到建议记录。")
    return jsonable_encoder(serialize_advice_record(advice))


@router.get("/explanation/{advice_id}")
def get_explanation(advice_id: int, db: Session = Depends(get_db)):
    advice = get_advice_by_id(db, advice_id)
    if advice is None:
        raise HTTPException(status_code=404, detail="未找到建议记录。")
    return jsonable_encoder(serialize_explanations(get_explanations_by_advice(db, advice_id)))
