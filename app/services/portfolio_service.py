from __future__ import annotations

from collections import Counter
from typing import Any

import pandas as pd
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.db.models import ETFFeature
from app.repositories.portfolio_repo import list_capital_flows, list_positions
from app.repositories.user_repo import get_user


class PortfolioService:
    def capital_flow_summary(self, session: Session, limit: int = 20) -> dict[str, Any]:
        user = get_user(session)
        flows = list_capital_flows(session, limit=limit)
        cumulative_flows = list_capital_flows(session, limit=10000)
        cumulative_deposit_amount = sum(flow.amount for flow in cumulative_flows if flow.flow_type == "deposit")
        cumulative_withdraw_amount = sum(flow.amount for flow in cumulative_flows if flow.flow_type == "withdraw")
        net_capital_flow_amount = cumulative_deposit_amount - cumulative_withdraw_amount
        current_capital_base = (user.initial_capital + net_capital_flow_amount) if user is not None else net_capital_flow_amount
        return {
            "cumulative_deposit_amount": cumulative_deposit_amount,
            "cumulative_withdraw_amount": cumulative_withdraw_amount,
            "net_capital_flow_amount": net_capital_flow_amount,
            "current_capital_base": current_capital_base,
            "capital_flows": [
                {
                    "id": flow.id,
                    "executed_at": flow.executed_at.isoformat(),
                    "flow_type": flow.flow_type,
                    "flow_label": "入金" if flow.flow_type == "deposit" else "出金",
                    "amount": flow.amount,
                    "note": flow.note,
                    "cash_balance_after": flow.cash_balance_after,
                    "total_asset_after": flow.total_asset_after,
                }
                for flow in flows
            ],
        }

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
            normalized_symbol = feature.symbol.strip()
            if normalized_symbol not in latest_features:
                latest_features[normalized_symbol] = feature

        normalized_symbols = [position.symbol.strip() for position in positions]
        normalized_counts = Counter(normalized_symbols)

        total_market_value = 0.0
        for position in positions:
            normalized_symbol = position.symbol.strip()
            feature = latest_features.get(normalized_symbol)
            if feature is not None:
                position.last_price = feature.close_price
            if position.symbol != normalized_symbol and normalized_counts[normalized_symbol] == 1:
                position.symbol = normalized_symbol
            position.name = position.name.strip()
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
            **self.capital_flow_summary(session),
        }

    def positions_dataframe(self, session: Session) -> pd.DataFrame:
        summary = self.get_portfolio_summary(session)
        if not summary["holdings"]:
            return pd.DataFrame()
        return pd.DataFrame(summary["holdings"])
