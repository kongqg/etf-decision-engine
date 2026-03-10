from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import PerformanceSnapshot
from app.repositories.portfolio_repo import list_capital_flows, list_snapshots, list_trades
from app.repositories.user_repo import get_user
from app.utils.maths import max_drawdown


class PerformanceService:
    def _capital_flow_stats(self, session: Session) -> dict[str, Any]:
        flows = list_capital_flows(session, limit=10000)
        cumulative_deposit_amount = sum(flow.amount for flow in flows if flow.flow_type == "deposit")
        cumulative_withdraw_amount = sum(flow.amount for flow in flows if flow.flow_type == "withdraw")
        flow_stats_by_date: dict[date, float] = {}
        for flow in flows:
            day = flow.executed_at.date()
            signed_amount = flow.amount if flow.flow_type == "deposit" else -flow.amount
            flow_stats_by_date[day] = flow_stats_by_date.get(day, 0.0) + signed_amount
        return {
            "cumulative_deposit_amount": cumulative_deposit_amount,
            "cumulative_withdraw_amount": cumulative_withdraw_amount,
            "net_capital_flow_amount": cumulative_deposit_amount - cumulative_withdraw_amount,
            "flow_stats_by_date": flow_stats_by_date,
            "capital_flows": [
                {
                    "executed_at": flow.executed_at.isoformat(),
                    "flow_type": flow.flow_type,
                    "flow_label": "入金" if flow.flow_type == "deposit" else "出金",
                    "amount": flow.amount,
                    "note": flow.note,
                }
                for flow in flows[:50]
            ],
        }

    def capture_snapshot(self, session: Session, snapshot_date: date | None = None) -> PerformanceSnapshot | None:
        user = get_user(session)
        if user is None:
            return None

        target_date = snapshot_date or date.today()
        trades = list_trades(session, limit=1000)
        sell_trades = [trade for trade in trades if trade.side == "sell"]
        win_trades = [trade for trade in sell_trades if trade.realized_pnl > 0]
        win_rate = len(win_trades) / len(sell_trades) if sell_trades else 0.0

        snapshots = list_snapshots(session, limit=1000)
        previous_snapshots = [item for item in snapshots if item.snapshot_date < target_date]
        capital_flow_stats = self._capital_flow_stats(session)
        previous_total_asset = previous_snapshots[-1].total_asset if previous_snapshots else user.initial_capital
        market_value = user.total_asset - user.cash_balance
        effective_total_asset = (
            user.total_asset
            + capital_flow_stats["cumulative_withdraw_amount"]
            - capital_flow_stats["cumulative_deposit_amount"]
        )
        flow_on_target_date = capital_flow_stats["flow_stats_by_date"].get(target_date, 0.0)
        adjusted_daily_asset = user.total_asset - flow_on_target_date
        cumulative_return_pct = (effective_total_asset / user.initial_capital - 1.0) * 100.0 if user.initial_capital else 0.0
        daily_return_pct = (adjusted_daily_asset / previous_total_asset - 1.0) * 100.0 if previous_total_asset else 0.0
        curve = [item.total_asset for item in previous_snapshots] + [user.total_asset]

        advice_trade_count = len([trade for trade in trades if trade.related_advice_id is not None])
        advice_sell_count = len([trade for trade in sell_trades if trade.related_advice_id is not None])
        advice_hit_rate = advice_sell_count / advice_trade_count if advice_trade_count else 0.0

        exists = session.scalar(select(PerformanceSnapshot).where(PerformanceSnapshot.snapshot_date == target_date))
        if exists is not None:
            exists.total_asset = user.total_asset
            exists.cash_balance = user.cash_balance
            exists.market_value = market_value
            exists.daily_return_pct = daily_return_pct
            exists.cumulative_return_pct = cumulative_return_pct
            exists.win_rate = win_rate
            exists.max_drawdown_pct = max_drawdown(curve)
            exists.advice_hit_rate = advice_hit_rate * 100.0
            session.commit()
            return exists

        snapshot = PerformanceSnapshot(
            snapshot_date=target_date,
            total_asset=user.total_asset,
            cash_balance=user.cash_balance,
            market_value=market_value,
            daily_return_pct=daily_return_pct,
            cumulative_return_pct=cumulative_return_pct,
            win_rate=win_rate,
            max_drawdown_pct=max_drawdown(curve),
            advice_hit_rate=advice_hit_rate * 100.0,
            benchmark_return_pct=0.0,
        )
        session.add(snapshot)
        session.commit()
        return snapshot

    def get_summary(self, session: Session) -> dict[str, Any]:
        snapshots = list_snapshots(session, limit=365)
        trades = list_trades(session, limit=200)
        capital_flow_stats = self._capital_flow_stats(session)
        user = get_user(session)
        latest = snapshots[-1] if snapshots else None
        return {
            "cumulative_return_pct": latest.cumulative_return_pct if latest else 0.0,
            "win_rate": latest.win_rate * 100.0 if latest else 0.0,
            "max_drawdown_pct": latest.max_drawdown_pct if latest else 0.0,
            "advice_hit_rate": latest.advice_hit_rate if latest else 0.0,
            "cumulative_deposit_amount": capital_flow_stats["cumulative_deposit_amount"],
            "cumulative_withdraw_amount": capital_flow_stats["cumulative_withdraw_amount"],
            "net_capital_flow_amount": capital_flow_stats["net_capital_flow_amount"],
            "current_capital_base": (
                user.initial_capital + capital_flow_stats["net_capital_flow_amount"] if user is not None else 0.0
            ),
            "return_basis_note": "累计收益率已按入金和出金做简单口径校正，不再把外部加钱误算成投资收益。",
            "curve": [
                {
                    "date": item.snapshot_date.isoformat(),
                    "total_asset": item.total_asset,
                    "cumulative_return_pct": item.cumulative_return_pct,
                }
                for item in snapshots
            ],
            "capital_flows": capital_flow_stats["capital_flows"],
            "trades": [
                {
                    "executed_at": trade.executed_at.isoformat(),
                    "symbol": trade.symbol,
                    "name": trade.name,
                    "side": "买入" if trade.side == "buy" else "卖出",
                    "quantity": trade.quantity,
                    "price": trade.price,
                    "amount": trade.amount,
                    "fee": trade.fee,
                }
                for trade in trades
            ],
        }
