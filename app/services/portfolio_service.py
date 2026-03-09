from __future__ import annotations

from typing import Any

import pandas as pd
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.db.models import ETFFeature
from app.repositories.portfolio_repo import list_positions
from app.repositories.user_repo import get_user


class PortfolioService:
    def update_market_prices(self, session: Session) -> None:
        positions = list_positions(session)
        user = get_user(session)

        if not positions:
            if user is not None:
                user.total_asset = user.cash_balance
                session.commit()
            return

        latest_features = {}
        for feature in session.scalars(
            select(ETFFeature).order_by(desc(ETFFeature.trade_date), desc(ETFFeature.captured_at))
        ):
            if feature.symbol not in latest_features:
                latest_features[feature.symbol] = feature

        total_market_value = 0.0
        for position in positions:
            feature = latest_features.get(position.symbol)
            if feature is not None:
                position.last_price = feature.close_price
            position.market_value = position.quantity * position.last_price
            position.unrealized_pnl = (position.last_price - position.avg_cost) * position.quantity
            total_market_value += position.market_value

        if user is not None:
            total_asset = user.cash_balance + total_market_value
            user.total_asset = total_asset
            for position in positions:
                position.weight_pct = position.market_value / total_asset if total_asset else 0.0

        session.commit()

    def get_portfolio_summary(self, session: Session) -> dict[str, Any]:
        self.update_market_prices(session)
        user = get_user(session)
        positions = list_positions(session)
        market_value = sum(item.market_value for item in positions)
        total_asset = user.total_asset if user is not None else market_value
        current_position_pct = market_value / total_asset if total_asset else 0.0

        holdings = [
            {
                "symbol": row.symbol,
                "name": row.name,
                "quantity": row.quantity,
                "avg_cost": row.avg_cost,
                "last_price": row.last_price,
                "market_value": row.market_value,
                "unrealized_pnl": row.unrealized_pnl,
                "weight_pct": row.weight_pct,
                "last_action_suggestion": row.last_action_suggestion,
            }
            for row in positions
        ]

        return {
            "cash_balance": user.cash_balance if user is not None else 0.0,
            "market_value": market_value,
            "total_asset": total_asset,
            "current_position_pct": current_position_pct,
            "holdings": holdings,
        }

    def positions_dataframe(self, session: Session) -> pd.DataFrame:
        summary = self.get_portfolio_summary(session)
        if not summary["holdings"]:
            return pd.DataFrame()
        return pd.DataFrame(summary["holdings"])
