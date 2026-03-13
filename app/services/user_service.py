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
    def _risk_profile_defaults(self, risk_level: str) -> dict[str, float]:
        normalized = str(risk_level or "中性").strip()
        return {
            "max_total_position_pct": {
                "保守": 0.85,
                "中性": 1.00,
                "激进": 1.00,
            }.get(normalized, 1.00),
            "max_single_position_pct": {
                "保守": 0.35,
                "中性": 0.50,
                "激进": 0.60,
            }.get(normalized, 0.50),
            "cash_reserve_pct": {
                "保守": 0.05,
                "中性": 0.00,
                "激进": 0.00,
            }.get(normalized, 0.00),
        }

    def _validate_preferences(
        self,
        max_total_position_pct: float,
        max_single_position_pct: float,
        cash_reserve_pct: float,
    ) -> None:
        if max_single_position_pct > max_total_position_pct:
            raise ValueError("单只仓位上限不能高于总仓位上限。")
        if max_total_position_pct + cash_reserve_pct > 1:
            raise ValueError("总仓位上限加现金保留比例不能超过 100%。")

    def init_user(
        self,
        session: Session,
        initial_capital: float,
        risk_level: str,
        allow_gold: bool,
        allow_bond: bool,
        allow_overseas: bool,
        min_trade_amount: float,
        risk_mode: str = "balanced",
    ) -> UserProfile:
        settings = get_settings()
        defaults = self._risk_profile_defaults(risk_level)
        max_total_position_pct = defaults["max_total_position_pct"]
        max_single_position_pct = defaults["max_single_position_pct"]
        cash_reserve_pct = defaults["cash_reserve_pct"]
        self._validate_preferences(max_total_position_pct, max_single_position_pct, cash_reserve_pct)

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
            preferences = UserPreferences(user_id=user.id)
            session.add(preferences)

        preferences.risk_level = risk_level
        preferences.risk_mode = risk_mode
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

    def update_preferences(
        self,
        session: Session,
        risk_level: str,
        allow_gold: bool,
        allow_bond: bool,
        allow_overseas: bool,
        min_trade_amount: float,
        max_total_position_pct: float,
        max_single_position_pct: float,
        cash_reserve_pct: float,
        risk_mode: str = "balanced",
    ) -> UserPreferences:
        user = session.get(UserProfile, get_settings().default_user_id)
        if user is None:
            raise ValueError("请先初始化用户。")

        self._validate_preferences(max_total_position_pct, max_single_position_pct, cash_reserve_pct)
        preferences = session.query(UserPreferences).filter(UserPreferences.user_id == user.id).one_or_none()
        if preferences is None:
            preferences = UserPreferences(user_id=user.id)
            session.add(preferences)

        preferences.risk_level = risk_level
        preferences.risk_mode = risk_mode
        preferences.allow_gold = allow_gold
        preferences.allow_bond = allow_bond
        preferences.allow_overseas = allow_overseas
        preferences.min_trade_amount = min_trade_amount
        preferences.max_total_position_pct = max_total_position_pct
        preferences.max_single_position_pct = max_single_position_pct
        preferences.cash_reserve_pct = cash_reserve_pct

        session.commit()
        session.refresh(preferences)
        return preferences
