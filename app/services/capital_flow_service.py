from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.db.models import CapitalFlow
from app.repositories.user_repo import get_user


class CapitalFlowService:
    VALID_FLOW_TYPES = {"deposit", "withdraw"}

    def record_adjustment(self, session: Session, payload: dict[str, Any]) -> CapitalFlow:
        user = get_user(session)
        if user is None:
            raise ValueError("请先初始化用户资金。")

        flow_type = str(payload.get("flow_type", "")).strip().lower()
        if flow_type not in self.VALID_FLOW_TYPES:
            raise ValueError("flow_type 只支持 deposit 或 withdraw。")

        amount = float(payload.get("amount", 0.0))
        if amount <= 0:
            raise ValueError("资金变动金额必须大于 0。")

        if flow_type == "withdraw" and amount > float(user.cash_balance):
            raise ValueError("当前可用现金不足，不能直接出金这么多。")

        if flow_type == "deposit":
            user.cash_balance += amount
            user.total_asset += amount
        else:
            user.cash_balance -= amount
            user.total_asset -= amount

        flow = CapitalFlow(
            executed_at=payload["executed_at"],
            flow_type=flow_type,
            amount=amount,
            note=str(payload.get("note", "")),
            cash_balance_after=user.cash_balance,
            total_asset_after=user.total_asset,
        )
        session.add(flow)
        session.commit()
        return flow

