from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import AdviceItem, AdviceRecord, ETFFeature, ETFUniverse, ExplanationRecord, MarketSnapshot, Trade
from app.repositories.market_repo import get_features_by_trade_date, get_latest_market_snapshot, get_latest_trade_date
from app.repositories.user_repo import get_preferences, get_user
from app.services.decision_policy_service import get_decision_policy_service
from app.services.execution_cost_service import get_execution_cost_service
from app.services.explanation_engine import ExplanationEngine
from app.services.market_data_service import MarketDataService
from app.services.portfolio_allocator import PortfolioAllocator
from app.services.portfolio_service import PortfolioService
from app.services.risk_service import RiskService
from app.services.scoring_engine import ScoringEngine
from app.services.universe_filter_service import UniverseFilterService
from app.utils.dates import detect_session_mode, get_now
from app.utils.maths import round_money

ACTION_DISPLAY_LABELS = {
    "buy": "买入",
    "sell": "卖出",
    "hold": "持有",
    "no_trade": "暂不交易",
}

MARKET_REGIME_LABELS = {
    "risk_on": "偏进攻",
    "neutral": "中性",
    "risk_off": "偏防守",
}


class DecisionEngine:
    def __init__(self) -> None:
        self.market_data_service = MarketDataService()
        self.portfolio_service = PortfolioService()
        self.filter_service = UniverseFilterService()
        self.scoring_engine = ScoringEngine()
        self.allocator = PortfolioAllocator()
        self.policy = get_decision_policy_service()
        self.execution_cost_service = get_execution_cost_service()
        self.risk_service = RiskService()
        self.explanation_engine = ExplanationEngine()

    def decide(self, session: Session, now: datetime | None = None) -> dict[str, Any]:
        user = get_user(session)
        preferences = get_preferences(session)
        if user is None or preferences is None:
            raise ValueError("请先初始化用户，再生成建议。")

        current_time = now or get_now()
        latest_trade_date = get_latest_trade_date(session)
        if latest_trade_date is None:
            self.market_data_service.refresh_data(session, now=current_time)
            latest_trade_date = get_latest_trade_date(session)
        if latest_trade_date is None:
            raise ValueError("当前没有可用的 ETF 特征数据。")

        feature_rows = get_features_by_trade_date(session, latest_trade_date)
        if not feature_rows:
            self.market_data_service.refresh_data(session, now=current_time)
            feature_rows = get_features_by_trade_date(session, latest_trade_date)
        if not feature_rows:
            raise ValueError("当前没有可用的 ETF 特征数据。")

        market_snapshot = get_latest_market_snapshot(session)
        plan = self.build_plan_from_features(
            session,
            feature_rows=feature_rows,
            market_snapshot=market_snapshot,
            preferences=preferences,
            now=current_time,
            allow_weak_data=False,
        )
        return self._persist_plan(session, plan)

    def build_plan_from_features(
        self,
        session: Session,
        *,
        feature_rows: list[ETFFeature],
        market_snapshot: MarketSnapshot | None,
        preferences: Any,
        now: datetime,
        allow_weak_data: bool,
    ) -> dict[str, Any]:
        session_mode = detect_session_mode(now)
        features_df = self._features_to_frame(session, feature_rows)
        raw_snapshot = self._snapshot_payload(market_snapshot)
        quality_summary = raw_snapshot.get("quality_summary", {})
        if not allow_weak_data and not bool(quality_summary.get("formal_decision_ready", True)):
            return self._blocked_plan(
                session=session,
                features_df=features_df,
                raw_snapshot=raw_snapshot,
                now=now,
                session_mode=session_mode,
            )

        filtered_df = self.filter_service.apply(features_df, preferences)
        scoring_result = self.scoring_engine.score(filtered_df)
        scored_df = scoring_result["scored_df"]
        portfolio_summary = self.portfolio_service.get_portfolio_summary(session)
        current_holdings = self._build_current_holdings(session, scored_df, portfolio_summary, now)
        market_regime = self._resolve_market_regime(scored_df, raw_snapshot)
        allocation = self.allocator.build_target_portfolio(
            scored_df,
            current_holdings=current_holdings,
            preferences=preferences,
            market_regime=market_regime,
            risk_mode=getattr(preferences, "risk_mode", "balanced"),
        )
        items = self._build_action_items(
            scored_df=scored_df,
            current_holdings=current_holdings,
            allocation=allocation,
            portfolio_summary=portfolio_summary,
            preferences=preferences,
        )
        summary_action = self._summary_action(items)
        reason_code = self._reason_code(items)
        explanation = self.explanation_engine.build(
            market_regime=market_regime,
            allocation=allocation,
            items=items,
            candidate_summary=allocation["candidate_summary"],
            portfolio_summary=portfolio_summary,
            quality_summary=quality_summary,
        )
        evidence = {
            "market_snapshot": raw_snapshot,
            "market_regime": market_regime,
            "data_quality_gate": {"summary": quality_summary},
            "target_portfolio": {
                "symbols": list(allocation["target_weights"].keys()),
                "weights": allocation["target_weights"],
            },
            "budget_context": {
                "total_budget_pct": allocation["total_budget_pct"],
                "single_weight_cap": allocation["single_weight_cap"],
                "category_budget_caps": allocation["category_budget_caps"],
                "replace_threshold": allocation["replace_threshold"],
            },
            "category_scores": scoring_result["category_scores"],
            "candidate_summary": allocation["candidate_summary"],
            "action_items": items,
            "explanation": explanation,
        }
        summary_text = self._summary_text(summary_action, items, market_regime, allocation)
        risk_text = self.risk_service.build_global_risk_note(session_mode, market_regime["market_regime"])
        return {
            "advice_date": scored_df["trade_date"].iloc[0] if not scored_df.empty else now.date(),
            "created_at": now,
            "session_mode": session_mode,
            "action": summary_action,
            "display_action": self._display_action_label(summary_action),
            "action_code": summary_action,
            "reason_code": reason_code,
            "market_regime": market_regime["market_regime"],
            "target_position_pct": allocation["total_budget_pct"],
            "current_position_pct": portfolio_summary.get("current_position_pct", 0.0),
            "summary_text": summary_text,
            "risk_text": risk_text,
            "items": items,
            "evidence": evidence,
            "explanation": explanation,
            "category_scores": scoring_result["category_scores"],
            "candidate_summary": allocation["candidate_summary"],
            "budget_context": evidence["budget_context"],
            "target_portfolio": evidence["target_portfolio"],
        }

    def _blocked_plan(
        self,
        *,
        session: Session,
        features_df: pd.DataFrame,
        raw_snapshot: dict[str, Any],
        now: datetime,
        session_mode: str,
    ) -> dict[str, Any]:
        portfolio_summary = self.portfolio_service.get_portfolio_summary(session)
        quality_summary = raw_snapshot.get("quality_summary", {})
        market_regime = self._resolve_market_regime(features_df, raw_snapshot)
        explanation = self.explanation_engine.build(
            market_regime=market_regime,
            allocation={
                "target_weights": {},
                "total_budget_pct": 0.0,
                "single_weight_cap": 0.0,
                "category_budget_caps": {},
                "replace_threshold": 0.0,
                "candidate_summary": [],
            },
            items=[],
            candidate_summary=[],
            portfolio_summary=portfolio_summary,
            quality_summary=quality_summary,
        )
        evidence = {
            "market_snapshot": raw_snapshot,
            "market_regime": market_regime,
            "data_quality_gate": {"summary": quality_summary},
            "target_portfolio": {"symbols": [], "weights": {}},
            "budget_context": {
                "total_budget_pct": 0.0,
                "single_weight_cap": 0.0,
                "category_budget_caps": {},
                "replace_threshold": 0.0,
            },
            "category_scores": [],
            "candidate_summary": [],
            "action_items": [],
            "explanation": explanation,
        }
        return {
            "advice_date": features_df["trade_date"].iloc[0] if not features_df.empty else now.date(),
            "created_at": now,
            "session_mode": session_mode,
            "action": "no_trade",
            "display_action": self._display_action_label("no_trade"),
            "action_code": "no_trade",
            "reason_code": "data_quality_not_ready",
            "market_regime": market_regime["market_regime"],
            "target_position_pct": 0.0,
            "current_position_pct": portfolio_summary.get("current_position_pct", 0.0),
            "summary_text": "今天的数据质量还不足以支持正式建议，因此系统保持暂不交易。",
            "risk_text": self.risk_service.build_global_risk_note(session_mode, market_regime["market_regime"]),
            "items": [],
            "evidence": evidence,
            "explanation": explanation,
            "category_scores": [],
            "candidate_summary": [],
            "budget_context": evidence["budget_context"],
            "target_portfolio": evidence["target_portfolio"],
        }

    def _build_current_holdings(
        self,
        session: Session,
        scored_df: pd.DataFrame,
        portfolio_summary: dict[str, Any],
        now: datetime,
    ) -> list[dict[str, Any]]:
        holding_rows = portfolio_summary.get("holdings", [])
        if not holding_rows:
            return []

        trades = list(session.scalars(select(Trade).order_by(Trade.executed_at.desc())))
        last_buy_by_symbol: dict[str, datetime] = {}
        for trade in trades:
            if trade.side != "buy":
                continue
            symbol = str(trade.symbol)
            if symbol not in last_buy_by_symbol:
                last_buy_by_symbol[symbol] = trade.executed_at

        current_holdings: list[dict[str, Any]] = []
        for row in holding_rows:
            symbol = str(row["symbol"])
            score_row = scored_df[scored_df["symbol"] == symbol]
            category = str(score_row.iloc[0]["decision_category"]) if not score_row.empty else ""
            hold_days = 0
            if symbol in last_buy_by_symbol:
                hold_days = max((now.date() - last_buy_by_symbol[symbol].date()).days, 0)
            current_holdings.append(
                {
                    "symbol": symbol,
                    "name": row["name"],
                    "category": category,
                    "current_weight": float(row.get("weight_pct", 0.0)),
                    "current_amount": float(row.get("market_value", 0.0)),
                    "hold_days": hold_days,
                    "quantity": float(row.get("quantity", 0.0)),
                    "avg_cost": float(row.get("avg_cost", 0.0)),
                    "last_price": float(row.get("last_price", 0.0)),
                    "unrealized_pnl": float(row.get("unrealized_pnl", 0.0)),
                }
            )
        return current_holdings

    def _build_action_items(
        self,
        *,
        scored_df: pd.DataFrame,
        current_holdings: list[dict[str, Any]],
        allocation: dict[str, Any],
        portfolio_summary: dict[str, Any],
        preferences: Any,
    ) -> list[dict[str, Any]]:
        total_asset = float(portfolio_summary.get("total_asset", 0.0))
        target_weights = allocation["target_weights"]
        current_by_symbol = {str(row["symbol"]): row for row in current_holdings}
        min_trade_amount = self.execution_cost_service.effective_min_trade_amount(getattr(preferences, "min_trade_amount", 0.0))
        tolerance = float(self.allocator.constraints.get("selection", {}).get("rebalance_weight_tolerance", 0.015))
        min_trade_weight_delta = float(self.allocator.constraints.get("budget", {}).get("min_trade_weight_delta", 0.01))

        symbols = set(target_weights.keys()) | set(current_by_symbol.keys())
        items: list[dict[str, Any]] = []
        for symbol in symbols:
            row = scored_df[scored_df["symbol"] == symbol]
            if row.empty:
                continue
            row_dict = row.iloc[0].to_dict()
            current = current_by_symbol.get(symbol, {})
            current_weight = float(current.get("current_weight", 0.0))
            target_weight = float(target_weights.get(symbol, 0.0))
            delta_weight = target_weight - current_weight
            current_amount = float(current.get("current_amount", 0.0))
            target_amount = round_money(total_asset * target_weight)
            delta_amount = round_money(total_asset * abs(delta_weight))
            intent, action = self._resolve_intent(
                current_weight=current_weight,
                target_weight=target_weight,
                delta_weight=delta_weight,
                delta_amount=delta_amount,
                tolerance=tolerance,
                min_trade_amount=min_trade_amount,
                min_trade_weight_delta=min_trade_weight_delta,
            )
            replacement_symbol, score_gap_vs_holding = self._replacement_context(
                symbol=symbol,
                category=str(row_dict["decision_category"]),
                current_holdings=current_holdings,
                scored_df=scored_df,
            )
            item = {
                "symbol": symbol,
                "name": row_dict["name"],
                "category": str(row_dict["decision_category"]),
                "category_label": self.policy.get_category_label(str(row_dict["decision_category"])),
                "rank": int(row_dict.get("global_rank", 0) or 0),
                "global_rank": int(row_dict.get("global_rank", 0) or 0),
                "category_rank": int(row_dict.get("category_rank", 0) or 0),
                "action": action,
                "action_code": action,
                "intent": intent,
                "current_weight": current_weight,
                "target_weight": target_weight,
                "delta_weight": delta_weight,
                "current_amount": current_amount,
                "target_amount": target_amount,
                "suggested_amount": round_money(abs(target_amount - current_amount)) if action in {"buy", "sell"} else 0.0,
                "suggested_pct": abs(delta_weight),
                "score": float(row_dict.get("final_score", 0.0)),
                "score_gap": float(score_gap_vs_holding),
                "score_gap_vs_holding": float(score_gap_vs_holding),
                "replace_threshold_used": float(allocation.get("replace_threshold", 0.0)),
                "replacement_symbol": replacement_symbol,
                "final_score": float(row_dict.get("final_score", 0.0)),
                "intra_score": float(row_dict.get("intra_score", 0.0)),
                "category_score": float(row_dict.get("category_score", 0.0)),
                "reason_short": self._reason_short(
                    action=action,
                    intent=intent,
                    row=row_dict,
                    current_weight=current_weight,
                    target_weight=target_weight,
                    replacement_symbol=replacement_symbol,
                    score_gap_vs_holding=score_gap_vs_holding,
                ),
                "risk_level": str(row_dict.get("risk_level", "")),
                "trade_mode": str(row_dict.get("trade_mode", "")),
                "tradability_mode": str(row_dict.get("tradability_mode", "")),
                "execution_note": self._execution_note(action=action, intent=intent, tradability_mode=str(row_dict.get("tradability_mode", ""))),
                "is_new_position": bool(current_weight <= 0 and target_weight > 0),
                "hold_days": int(current.get("hold_days", 0) or 0),
                "is_held": bool(current_weight > 0),
                "latest_price": float(row_dict.get("close_price", 0.0)),
                "scores": {
                    "intra_score": float(row_dict.get("intra_score", 0.0)),
                    "category_score": float(row_dict.get("category_score", 0.0)),
                    "final_score": float(row_dict.get("final_score", 0.0)),
                },
                "score_breakdown": self._parse_json(row_dict.get("score_breakdown_json")),
            }
            if action != "no_trade":
                items.append(item)

        items.sort(key=lambda row: ({"buy": 0, "sell": 1, "hold": 2, "no_trade": 3}.get(row["action"], 9), row["rank"]))
        return items

    def _resolve_intent(
        self,
        *,
        current_weight: float,
        target_weight: float,
        delta_weight: float,
        delta_amount: float,
        tolerance: float,
        min_trade_amount: float,
        min_trade_weight_delta: float,
    ) -> tuple[str, str]:
        if current_weight <= 0 and target_weight <= 0:
            return "hold", "no_trade"
        if current_weight <= 0 and target_weight > 0:
            if delta_amount < min_trade_amount:
                return "hold", "no_trade"
            return "open", "buy"
        if current_weight > 0 and target_weight <= 0:
            return "exit", "sell"
        if abs(delta_weight) <= tolerance or abs(delta_weight) < min_trade_weight_delta or delta_amount < min_trade_amount:
            return "hold", "hold"
        if delta_weight > 0:
            return "add", "buy"
        return "reduce", "sell"

    def _replacement_context(
        self,
        *,
        symbol: str,
        category: str,
        current_holdings: list[dict[str, Any]],
        scored_df: pd.DataFrame,
    ) -> tuple[str, float]:
        incumbents = [row for row in current_holdings if row.get("category") == category and row["symbol"] != symbol]
        if not incumbents:
            return "", 0.0
        scored_lookup = {str(row["symbol"]): float(row["final_score"]) for _, row in scored_df.iterrows()}
        best = max(incumbents, key=lambda item: scored_lookup.get(str(item["symbol"]), 0.0))
        gap = scored_lookup.get(symbol, 0.0) - scored_lookup.get(str(best["symbol"]), 0.0)
        return str(best["symbol"]), float(gap)

    def _execution_note(self, *, action: str, intent: str, tradability_mode: str) -> str:
        if action == "no_trade":
            return "今天没有生成可执行交易。"
        if tradability_mode == "t1":
            return f"Intent={intent}. This ETF follows T+1 settlement, so intraday reversal is not part of the strategy layer."
        return f"Intent={intent}. This ETF is treated as T+0-capable, but timing remains an execution note, not a score driver."

    def _summary_action(self, items: list[dict[str, Any]]) -> str:
        actions = {item["action"] for item in items}
        if not actions:
            return "no_trade"
        if "buy" in actions:
            return "buy"
        if "sell" in actions:
            return "sell"
        if actions == {"hold"}:
            return "hold"
        return "no_trade"

    def _reason_code(self, items: list[dict[str, Any]]) -> str:
        if not items:
            return "no_target"
        actions = {item["action"] for item in items}
        if actions == {"hold"}:
            return "portfolio_hold"
        if "buy" in actions and "sell" in actions:
            return "rebalance"
        if "buy" in actions:
            return "new_entry_or_add"
        if "sell" in actions:
            return "reduce_or_exit"
        return "no_target"

    def _summary_text(
        self,
        action: str,
        items: list[dict[str, Any]],
        market_regime: dict[str, Any],
        allocation: dict[str, Any],
    ) -> str:
        if action == "no_trade":
            return "今天没有 ETF 同时通过分数和预算规则，因此系统保持暂不交易。"
        top_names = ", ".join(item["name"] for item in items[:3])
        market_regime_label = self._market_regime_label(market_regime["market_regime"])
        return (
            f"当前市场状态为 {market_regime_label}。"
            f"目标预算仓位为 {allocation['total_budget_pct'] * 100:.1f}%。"
            f"本次重点关注 {top_names}。"
        )

    def _reason_short(
        self,
        *,
        action: str,
        intent: str,
        row: dict[str, Any],
        current_weight: float,
        target_weight: float,
        replacement_symbol: str,
        score_gap_vs_holding: float,
    ) -> str:
        final_score = float(row.get("final_score", 0.0))
        global_rank = int(row.get("global_rank", 0) or 0)
        category_rank = int(row.get("category_rank", 0) or 0)
        if intent == "open":
            return (
                f"新开仓。最终分 {final_score:.1f}，全局排名 {global_rank}，类别排名 {category_rank}，"
                f"已进入目标组合。"
            )
        if intent == "add":
            return (
                f"对已有持仓继续加仓。目标权重 {target_weight * 100:.1f}% 高于当前权重 "
                f"{current_weight * 100:.1f}%，而且这只 ETF 的排名仍然靠前。"
            )
        if intent == "hold":
            return (
                f"继续持有。最终分 {final_score:.1f} 仍然具备竞争力，因此目标权重与当前权重接近。"
            )
        if intent == "reduce":
            return (
                f"降低仓位。目标权重 {target_weight * 100:.1f}% 低于当前权重 "
                f"{current_weight * 100:.1f}%，说明它相对其他候选已经转弱。"
            )
        if intent == "exit":
            replace_note = ""
            if replacement_symbol:
                replace_note = f" 它被 {replacement_symbol} 替代，两者分差为 {score_gap_vs_holding:.1f}。"
            return f"退出这只持仓，因为它已经掉出目标组合。{replace_note}"
        return f"今天不会触发 {action} 动作。"

    def _display_action_label(self, action: str) -> str:
        return ACTION_DISPLAY_LABELS.get(str(action), str(action))

    def _market_regime_label(self, market_regime: str) -> str:
        return MARKET_REGIME_LABELS.get(str(market_regime), str(market_regime))

    def _resolve_market_regime(self, scored_df: pd.DataFrame, raw_snapshot: dict[str, Any]) -> dict[str, Any]:
        if raw_snapshot:
            return {
                "market_regime": raw_snapshot.get("market_regime", "neutral"),
                "budget_total_pct": float(raw_snapshot.get("budget_total_pct", raw_snapshot.get("recommended_position_pct", 0.0)) or 0.0),
                "recommended_position_pct": float(raw_snapshot.get("recommended_position_pct", 0.0) or 0.0),
                "budget_by_category": raw_snapshot.get("budget_by_category", {}) or {},
                "broad_index_score": float(raw_snapshot.get("broad_index_score", 0.0) or 0.0),
                "risk_appetite_score": float(raw_snapshot.get("risk_appetite_score", 0.0) or 0.0),
                "trend_score": float(raw_snapshot.get("trend_score", 0.0) or 0.0),
            }
        return self.market_data_service.market_regime_service.evaluate(scored_df)

    def _persist_plan(self, session: Session, plan: dict[str, Any]) -> dict[str, Any]:
        record = AdviceRecord(
            advice_date=plan["advice_date"],
            created_at=plan["created_at"],
            session_mode=plan["session_mode"],
            action=plan["action"],
            display_action=plan["display_action"],
            market_regime=plan["market_regime"],
            action_code=plan["action_code"],
            reason_code=plan["reason_code"],
            strategy_version="score_v2",
            target_position_pct=float(plan["target_position_pct"]),
            current_position_pct=float(plan["current_position_pct"]),
            summary_text=plan["summary_text"],
            risk_text=plan["risk_text"],
            evidence_json=json.dumps(plan["evidence"], ensure_ascii=False),
            target_portfolio_json=json.dumps(plan["target_portfolio"], ensure_ascii=False),
            budget_context_json=json.dumps(plan["budget_context"], ensure_ascii=False),
            candidate_summary_json=json.dumps(plan["candidate_summary"], ensure_ascii=False),
        )
        session.add(record)
        session.flush()

        for item in plan["items"]:
            session.add(
                AdviceItem(
                    advice_id=record.id,
                    symbol=item["symbol"],
                    name=item["name"],
                    action_code=item["action_code"],
                    intent=item["intent"],
                    rank=item["rank"],
                    action=item["action"],
                    category=item["category"],
                    tradability_mode=item["tradability_mode"],
                    category_score=float(item["category_score"]),
                    suggested_amount=float(item["suggested_amount"]),
                    suggested_pct=float(item["suggested_pct"]),
                    trigger_price_low=None,
                    trigger_price_high=None,
                    stop_loss_pct=0.0,
                    take_profit_pct=0.0,
                    score=float(item["final_score"]),
                    score_gap=float(item["score_gap"]),
                    reason_short=str(item["reason_short"]),
                    risk_level=str(item["risk_level"]),
                    current_weight=float(item["current_weight"]),
                    target_weight=float(item["target_weight"]),
                    delta_weight=float(item["delta_weight"]),
                    is_new_position=bool(item["is_new_position"]),
                    replace_threshold_used=float(item["replace_threshold_used"]),
                    final_score=float(item["final_score"]),
                    intra_score=float(item["intra_score"]),
                    global_rank=int(item["global_rank"]),
                    category_rank=int(item["category_rank"]),
                    hold_days=int(item["hold_days"]),
                    score_gap_vs_holding=float(item["score_gap_vs_holding"]),
                    replacement_symbol=str(item["replacement_symbol"]),
                    current_amount=float(item["current_amount"]),
                    target_amount=float(item["target_amount"]),
                    rationale_json=json.dumps(
                        {"execution_note": item["execution_note"], "scores": item["scores"]},
                        ensure_ascii=False,
                    ),
                    score_breakdown_json=json.dumps(item["score_breakdown"], ensure_ascii=False),
                )
            )

        explanation = plan["explanation"]
        session.add(
            ExplanationRecord(
                advice_id=record.id,
                scope="overall",
                symbol=None,
                title="Overall",
                summary=plan["summary_text"],
                explanation_json=json.dumps(explanation["overall"], ensure_ascii=False),
            )
        )
        for item in explanation["items"]:
            session.add(
                ExplanationRecord(
                    advice_id=record.id,
                    scope="item",
                    symbol=item["symbol"],
                    title=item["title"],
                    summary=item["summary"],
                    explanation_json=json.dumps(item, ensure_ascii=False),
                )
            )

        session.commit()
        return {
            "id": record.id,
            "advice_date": record.advice_date.isoformat(),
            "created_at": record.created_at.isoformat(),
            "session_mode": record.session_mode,
            "action": record.action,
            "display_action": record.display_action,
            "action_code": record.action_code,
            "reason_code": record.reason_code,
            "market_regime": record.market_regime,
            "target_position_pct": record.target_position_pct,
            "current_position_pct": record.current_position_pct,
            "summary_text": record.summary_text,
            "risk_text": record.risk_text,
            "items": plan["items"],
            "evidence": plan["evidence"],
            "target_portfolio": plan["target_portfolio"],
            "budget_context": plan["budget_context"],
            "candidate_summary": plan["candidate_summary"],
        }

    def _features_to_frame(self, session: Session, rows: list[ETFFeature]) -> pd.DataFrame:
        symbols = [row.symbol for row in rows]
        universe_rows = list(session.scalars(select(ETFUniverse).where(ETFUniverse.symbol.in_(symbols))))
        universe_map = {row.symbol: row for row in universe_rows}
        payload = []
        for row in rows:
            universe = universe_map.get(row.symbol)
            payload.append(
                {
                    "trade_date": row.trade_date,
                    "symbol": row.symbol,
                    "name": getattr(universe, "name", row.symbol),
                    "category": getattr(universe, "category", ""),
                    "decision_category": row.decision_category,
                    "category_label": self.policy.get_category_label(row.decision_category),
                    "asset_class": getattr(universe, "asset_class", ""),
                    "market": getattr(universe, "market", ""),
                    "risk_level": getattr(universe, "risk_level", ""),
                    "trade_mode": getattr(universe, "trade_mode", row.tradability_mode),
                    "tradability_mode": row.tradability_mode,
                    "formal_eligible": row.formal_eligible,
                    "source_code": row.source_code,
                    "stale_data_flag": row.stale_data_flag,
                    "latest_row_date": row.latest_row_date,
                    "anomaly_flag": row.anomaly_flag,
                    "min_avg_amount": getattr(universe, "min_avg_amount", 0.0),
                    "close_price": row.close_price,
                    "latest_amount": row.latest_amount,
                    "avg_amount_20d": row.avg_amount_20d,
                    "momentum_5d": row.momentum_5d,
                    "momentum_10d": row.momentum_10d,
                    "momentum_20d": row.momentum_20d,
                    "trend_strength": row.trend_strength,
                    "volatility_20d": row.volatility_20d,
                    "drawdown_20d": row.drawdown_20d,
                    "liquidity_score": row.liquidity_score,
                    "above_ma20_flag": row.above_ma20_flag,
                    "score_breakdown_json": getattr(row, "score_breakdown_json", "{}"),
                }
            )
        return pd.DataFrame(payload)

    def _snapshot_payload(self, snapshot: MarketSnapshot | None) -> dict[str, Any]:
        if snapshot is None:
            return {}
        raw = self._parse_json(snapshot.raw_json)
        raw.update(
            {
                "market_regime": snapshot.market_regime,
                "broad_index_score": snapshot.broad_index_score,
                "risk_appetite_score": snapshot.risk_appetite_score,
                "trend_score": snapshot.trend_score,
                "recommended_position_pct": snapshot.recommended_position_pct,
                "budget_total_pct": snapshot.budget_total_pct,
                "budget_by_category": self._parse_json(snapshot.budget_by_category_json),
            }
        )
        return raw

    def _parse_json(self, value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str) and value.strip():
            try:
                loaded = json.loads(value)
            except json.JSONDecodeError:
                return {}
            return loaded if isinstance(loaded, dict) else {}
        return {}
