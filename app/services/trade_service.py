from __future__ import annotations

import math
from typing import Any

from sqlalchemy.orm import Session

from app.db.models import Position, Trade
from app.repositories.portfolio_repo import get_position_by_symbol
from app.repositories.user_repo import get_user


class TradeService:
    def record_trade(self, session: Session, payload: dict[str, Any]) -> Trade:
        user = get_user(session)
        if user is None:
            raise ValueError("请先初始化用户资金。")

        quantity = payload.get("quantity")
        if quantity is None:
            raw_quantity = payload["amount"] / payload["price"]
            quantity = math.floor(raw_quantity / 100) * 100
            if quantity <= 0:
                raise ValueError("按当前价格和金额计算后，可成交份额不足 100。")

        amount = quantity * payload["price"]
        fee = payload.get("fee", 0.0)
        side = payload["side"]
        position = get_position_by_symbol(session, payload["symbol"])

        if position is None:
            position = Position(
                symbol=payload["symbol"],
                name=payload["name"],
                quantity=0.0,
                avg_cost=0.0,
                last_price=payload["price"],
                market_value=0.0,
                unrealized_pnl=0.0,
                realized_pnl=0.0,
                weight_pct=0.0,
            )
            session.add(position)

        if side == "buy":
            total_cost = position.avg_cost * position.quantity + amount + fee
            position.quantity += quantity
            position.avg_cost = total_cost / position.quantity if position.quantity else 0.0
            user.cash_balance -= amount + fee
            realized_pnl = 0.0
        elif side == "sell":
            if quantity > position.quantity:
                raise ValueError("卖出数量超过当前持仓。")
            realized_pnl = (payload["price"] - position.avg_cost) * quantity - fee
            position.realized_pnl += realized_pnl
            position.quantity -= quantity
            user.cash_balance += amount - fee
            if position.quantity <= 0:
                position.quantity = 0.0
                position.avg_cost = 0.0
        else:
            raise ValueError("side 只支持 buy 或 sell。")

        trade = Trade(
            executed_at=payload["executed_at"],
            symbol=payload["symbol"],
            name=payload["name"],
            side=side,
            quantity=quantity,
            price=payload["price"],
            amount=amount,
            fee=fee,
            realized_pnl=realized_pnl,
            related_advice_id=payload.get("related_advice_id"),
            note=payload.get("note", ""),
        )
        session.add(trade)
        session.commit()
        return trade
