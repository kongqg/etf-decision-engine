from __future__ import annotations

from sqlalchemy import delete
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.models import (
    AdviceItem,
    AdviceRecord,
    ExplanationRecord,
    MarketSnapshot,
    PerformanceSnapshot,
    Position,
    Trade,
    UserPreferences,
    UserProfile,
)


class UserService:
    def init_user(
        self,
        session: Session,
        initial_capital: float,
        risk_level: str,
        allow_gold: bool,
        allow_bond: bool,
        allow_overseas: bool,
        min_trade_amount: float,
    ) -> UserProfile:
        settings = get_settings()
        max_total_position_pct = {
            "保守": 0.55,
            "中性": 0.70,
            "激进": 0.85,
        }.get(risk_level, 0.70)
        max_single_position_pct = {
            "保守": 0.25,
            "中性": 0.35,
            "激进": 0.45,
        }.get(risk_level, 0.35)
        cash_reserve_pct = {
            "保守": 0.35,
            "中性": 0.20,
            "激进": 0.10,
        }.get(risk_level, 0.20)

        user = session.get(UserProfile, settings.default_user_id)
        if user is None:
            user = UserProfile(
                id=settings.default_user_id,
                initial_capital=initial_capital,
                cash_balance=initial_capital,
                total_asset=initial_capital,
            )
            session.add(user)
            session.flush()
        else:
            session.execute(delete(ExplanationRecord))
            session.execute(delete(AdviceItem))
            session.execute(delete(AdviceRecord))
            session.execute(delete(Trade))
            session.execute(delete(Position))
            session.execute(delete(PerformanceSnapshot))
            session.execute(delete(MarketSnapshot))
            user.initial_capital = initial_capital
            user.cash_balance = initial_capital
            user.total_asset = initial_capital

        preferences = session.query(UserPreferences).filter(UserPreferences.user_id == user.id).one_or_none()
        if preferences is None:
            preferences = UserPreferences(
                user_id=user.id,
                risk_level=risk_level,
                allow_gold=allow_gold,
                allow_bond=allow_bond,
                allow_overseas=allow_overseas,
                min_trade_amount=min_trade_amount,
                max_total_position_pct=max_total_position_pct,
                max_single_position_pct=max_single_position_pct,
                cash_reserve_pct=cash_reserve_pct,
            )
            session.add(preferences)
        else:
            preferences.risk_level = risk_level
            preferences.allow_gold = allow_gold
            preferences.allow_bond = allow_bond
            preferences.allow_overseas = allow_overseas
            preferences.min_trade_amount = min_trade_amount
            preferences.max_total_position_pct = max_total_position_pct
            preferences.max_single_position_pct = max_single_position_pct
            preferences.cash_reserve_pct = cash_reserve_pct

        session.commit()
        session.refresh(user)
        return user
