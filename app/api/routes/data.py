from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.services.data_evidence_service import DataEvidenceService
from app.services.market_data_service import MarketDataService


router = APIRouter(prefix="/api", tags=["data"])
market_data_service = MarketDataService()
data_evidence_service = DataEvidenceService()


@router.post("/refresh-data")
def refresh_data(db: Session = Depends(get_db)):
    return jsonable_encoder(market_data_service.refresh_data(db))


@router.get("/evidence/latest")
def latest_evidence(db: Session = Depends(get_db)):
    try:
        return jsonable_encoder(data_evidence_service.build(db))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/evidence/{advice_id}")
def evidence_by_advice(advice_id: int, db: Session = Depends(get_db)):
    try:
        return jsonable_encoder(data_evidence_service.build(db, advice_id=advice_id))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
