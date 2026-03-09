from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class UserProfile(Base):
    __tablename__ = "user_profile"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    initial_capital: Mapped[float] = mapped_column(Float, nullable=False)
    cash_balance: Mapped[float] = mapped_column(Float, nullable=False)
    total_asset: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    preferences: Mapped["UserPreferences"] = relationship(back_populates="user", uselist=False)


class UserPreferences(Base):
    __tablename__ = "user_preferences"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("user_profile.id"), unique=True, nullable=False)
    risk_level: Mapped[str] = mapped_column(String(20), nullable=False, default="中性")
    allow_gold: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    allow_bond: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    allow_overseas: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    min_trade_amount: Mapped[float] = mapped_column(Float, nullable=False, default=1000.0)
    max_total_position_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.7)
    max_single_position_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.35)
    cash_reserve_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.2)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    user: Mapped["UserProfile"] = relationship(back_populates="preferences")


class ETFUniverse(Base):
    __tablename__ = "etf_universe"

    symbol: Mapped[str] = mapped_column(String(10), primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    category: Mapped[str] = mapped_column(String(20), nullable=False)
    market: Mapped[str] = mapped_column(String(10), nullable=False)
    benchmark: Mapped[str] = mapped_column(String(50), nullable=False)
    risk_level: Mapped[str] = mapped_column(String(20), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    allow_gold: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    allow_bond: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    allow_overseas: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    min_avg_amount: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    settlement_note: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )


class MarketSnapshot(Base):
    __tablename__ = "market_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    session_mode: Mapped[str] = mapped_column(String(30), nullable=False)
    market_regime: Mapped[str] = mapped_column(String(20), nullable=False)
    broad_index_score: Mapped[float] = mapped_column(Float, nullable=False)
    risk_appetite_score: Mapped[float] = mapped_column(Float, nullable=False)
    trend_score: Mapped[float] = mapped_column(Float, nullable=False)
    recommended_position_pct: Mapped[float] = mapped_column(Float, nullable=False)
    raw_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")


class ETFFeature(Base):
    __tablename__ = "etf_features"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    symbol: Mapped[str] = mapped_column(ForeignKey("etf_universe.symbol"), nullable=False)
    close_price: Mapped[float] = mapped_column(Float, nullable=False)
    pct_change: Mapped[float] = mapped_column(Float, nullable=False)
    latest_amount: Mapped[float] = mapped_column(Float, nullable=False)
    avg_amount_20d: Mapped[float] = mapped_column(Float, nullable=False)
    momentum_3d: Mapped[float] = mapped_column(Float, nullable=False)
    momentum_5d: Mapped[float] = mapped_column(Float, nullable=False)
    momentum_10d: Mapped[float] = mapped_column(Float, nullable=False)
    ma_gap_5: Mapped[float] = mapped_column(Float, nullable=False)
    ma_gap_10: Mapped[float] = mapped_column(Float, nullable=False)
    trend_strength: Mapped[float] = mapped_column(Float, nullable=False)
    volatility_10d: Mapped[float] = mapped_column(Float, nullable=False)
    drawdown_20d: Mapped[float] = mapped_column(Float, nullable=False)
    liquidity_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    anomaly_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    filter_pass: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    total_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    rank_in_pool: Mapped[int | None] = mapped_column(Integer, nullable=True)
    breakdown_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")


class AdviceRecord(Base):
    __tablename__ = "advice_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    advice_date: Mapped[date] = mapped_column(Date, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    session_mode: Mapped[str] = mapped_column(String(30), nullable=False)
    action: Mapped[str] = mapped_column(String(20), nullable=False)
    market_regime: Mapped[str] = mapped_column(String(20), nullable=False)
    target_position_pct: Mapped[float] = mapped_column(Float, nullable=False)
    current_position_pct: Mapped[float] = mapped_column(Float, nullable=False)
    summary_text: Mapped[str] = mapped_column(Text, nullable=False)
    risk_text: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="active")
    evidence_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")

    items: Mapped[list["AdviceItem"]] = relationship(back_populates="advice", cascade="all, delete-orphan")
    explanations: Mapped[list["ExplanationRecord"]] = relationship(
        back_populates="advice",
        cascade="all, delete-orphan",
    )


class AdviceItem(Base):
    __tablename__ = "advice_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    advice_id: Mapped[int] = mapped_column(ForeignKey("advice_records.id"), nullable=False)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    action: Mapped[str] = mapped_column(String(20), nullable=False)
    suggested_amount: Mapped[float] = mapped_column(Float, nullable=False)
    suggested_pct: Mapped[float] = mapped_column(Float, nullable=False)
    trigger_price_low: Mapped[float | None] = mapped_column(Float, nullable=True)
    trigger_price_high: Mapped[float | None] = mapped_column(Float, nullable=True)
    stop_loss_pct: Mapped[float] = mapped_column(Float, nullable=False)
    take_profit_pct: Mapped[float] = mapped_column(Float, nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    score_gap: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    reason_short: Mapped[str] = mapped_column(Text, nullable=False)
    risk_level: Mapped[str] = mapped_column(String(20), nullable=False)

    advice: Mapped["AdviceRecord"] = relationship(back_populates="items")


class ExplanationRecord(Base):
    __tablename__ = "explanation_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    advice_id: Mapped[int] = mapped_column(ForeignKey("advice_records.id"), nullable=False)
    scope: Mapped[str] = mapped_column(String(20), nullable=False)
    symbol: Mapped[str | None] = mapped_column(String(10), nullable=True)
    title: Mapped[str] = mapped_column(String(100), nullable=False)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    explanation_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    advice: Mapped["AdviceRecord"] = relationship(back_populates="explanations")


class Position(Base):
    __tablename__ = "positions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(10), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    avg_cost: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    last_price: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    market_value: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    unrealized_pnl: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    realized_pnl: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    weight_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    last_action_suggestion: Mapped[str] = mapped_column(String(20), nullable=False, default="继续持有")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )


class Trade(Base):
    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    executed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    side: Mapped[str] = mapped_column(String(10), nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    amount: Mapped[float] = mapped_column(Float, nullable=False)
    fee: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    realized_pnl: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    related_advice_id: Mapped[int | None] = mapped_column(ForeignKey("advice_records.id"), nullable=True)
    note: Mapped[str] = mapped_column(Text, nullable=False, default="")


class PerformanceSnapshot(Base):
    __tablename__ = "performance_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False)
    total_asset: Mapped[float] = mapped_column(Float, nullable=False)
    cash_balance: Mapped[float] = mapped_column(Float, nullable=False)
    market_value: Mapped[float] = mapped_column(Float, nullable=False)
    daily_return_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    cumulative_return_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    win_rate: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    max_drawdown_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    advice_hit_rate: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    benchmark_return_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
