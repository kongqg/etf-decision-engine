from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.models import AdviceItem, AdviceRecord, ETFFeature, ETFUniverse, ExplanationRecord, MarketSnapshot, Position
from app.repositories.advice_repo import get_latest_advice
from app.repositories.market_repo import get_latest_market_snapshot, get_latest_trade_date
from app.repositories.portfolio_repo import list_trades
from app.repositories.user_repo import get_preferences, get_user
from app.services.decision_policy_service import get_decision_policy_service
from app.services.explanation_engine import ExplanationEngine
from app.services.market_data_service import MarketDataService
from app.services.performance_service import PerformanceService
from app.services.portfolio_service import PortfolioService
from app.services.position_action_service import BUY_ACTIONS, ORDER_ACTIONS, SELL_ACTIONS, PositionActionService
from app.services.risk_service import RiskService
from app.services.scoring_service import ScoringService
from app.services.universe_filter_service import UniverseFilterService
from app.utils.dates import detect_session_mode, get_now, latest_market_date
from app.utils.maths import round_money


class DecisionEngine:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.market_data_service = MarketDataService()
        self.portfolio_service = PortfolioService()
        self.performance_service = PerformanceService()
        self.filter_service = UniverseFilterService()
        self.scoring_service = ScoringService()
        self.position_action_service = PositionActionService()
        self.risk_service = RiskService()
        self.explanation_engine = ExplanationEngine()
        self.policy = get_decision_policy_service()
        self.thresholds = self.policy.action_thresholds

    def decide(self, session: Session, now=None) -> dict[str, Any]:
        user = get_user(session)
        preferences = get_preferences(session)
        if user is None or preferences is None:
            raise ValueError("请先初始化用户。")

        current_time = now or get_now()
        latest_trade = get_latest_trade_date(session)
        latest_snapshot = get_latest_market_snapshot(session)
        if (
            latest_trade is None
            or latest_trade < latest_market_date(current_time)
            or self._snapshot_requires_refresh(latest_snapshot, latest_trade)
        ):
            self.market_data_service.refresh_data(session, now=current_time)
            latest_trade = get_latest_trade_date(session)
            latest_snapshot = get_latest_market_snapshot(session)

        if latest_trade is None:
            raise ValueError("当前没有可用行情数据。")

        feature_rows = list(
            session.scalars(select(ETFFeature).where(ETFFeature.trade_date == latest_trade).order_by(ETFFeature.symbol))
        )
        feature_df = self._feature_frame(feature_rows)
        feature_df = self._enrich_with_universe(session, feature_df)
        filtered_df = self.filter_service.apply(feature_df, preferences)

        portfolio_summary = self.portfolio_service.get_portfolio_summary(session)
        positions_df = self.portfolio_service.positions_dataframe(session)
        holding_symbols = set(positions_df["symbol"].tolist()) if not positions_df.empty else set()
        market_snapshot = self._market_snapshot_dict(latest_snapshot)
        session_mode = detect_session_mode(current_time)

        candidates_df = filtered_df[filtered_df["filter_pass"]].copy()
        decision_universe_df = filtered_df[
            filtered_df["filter_pass"] | filtered_df["symbol"].isin(holding_symbols)
        ].copy()

        trade_context = self._trade_context(session, positions_df, current_time)
        previous_rank_map = self._previous_rank_map(session)
        evaluation = self.scoring_service.evaluate(
            candidates_df=candidates_df,
            positions_df=positions_df,
            target_holding_days=int(getattr(preferences, "target_holding_days", 5)),
            previous_rank_map=previous_rank_map,
            days_held_map=trade_context["days_held_map"],
        )
        if not decision_universe_df.empty:
            defensive_category = self.policy.defensive_category()
            decision_universe_df["is_current_holding"] = decision_universe_df["symbol"].isin(holding_symbols)
            decision_universe_df["entry_eligible"] = decision_universe_df["filter_pass"]
            decision_universe_df["profile_offensive_edge"] = decision_universe_df.apply(
                lambda row: bool(evaluation["offensive_edge"])
                or (
                    bool(row["symbol"] in holding_symbols)
                    and str(row["decision_category"]) != defensive_category
                ),
                axis=1,
            )
        scored_df = self.scoring_service.score_decision_universe(
            decision_df=decision_universe_df,
            category_scores_df=evaluation["category_scores_df"],
            target_holding_days=int(getattr(preferences, "target_holding_days", 5)),
            previous_rank_map=previous_rank_map,
            days_held_map=trade_context["days_held_map"],
            offensive_edge=bool(evaluation["offensive_edge"]),
        )
        self._sync_feature_scores(session, feature_rows, filtered_df, scored_df)

        decision_context = self._build_decision_context(
            scored_df=scored_df,
            evaluation=evaluation,
            portfolio_summary=portfolio_summary,
            preferences=preferences,
            session_mode=session_mode,
            current_time=current_time,
            trade_context=trade_context,
            market_snapshot=market_snapshot,
        )
        self._update_position_actions(
            session,
            decision_context["portfolio_review_items"],
            market_snapshot["market_regime"],
        )

        recommendation_groups = decision_context["recommendation_groups"]
        all_items = [
            *recommendation_groups["executable_recommendations"],
            *recommendation_groups["watchlist_recommendations"],
        ]
        selected_item = decision_context.get("primary_item")
        top_item = selected_item or (all_items[0] if all_items else None)

        advice_record = AdviceRecord(
            advice_date=latest_trade,
            created_at=current_time,
            session_mode=session_mode,
            action=self.policy.action_label(decision_context["action_code"]),
            action_code=decision_context["action_code"],
            market_regime=market_snapshot["market_regime"],
            winning_category=decision_context.get("winning_category", ""),
            target_holding_days=int(getattr(preferences, "target_holding_days", 5)),
            mapped_horizon_profile=str(top_item.get("mapped_horizon_profile", decision_context["mapped_horizon_profile"]))
            if top_item
            else decision_context["mapped_horizon_profile"],
            lifecycle_phase=str(top_item.get("lifecycle_phase", decision_context["lifecycle_phase"]))
            if top_item
            else decision_context["lifecycle_phase"],
            category_score=float(top_item.get("category_score", decision_context["selected_category_score"]))
            if top_item
            else float(decision_context["selected_category_score"]),
            executable_now=bool(decision_context["executable_now"]),
            blocked_reason=str(decision_context.get("blocked_reason", "")),
            planned_exit_days=decision_context.get("planned_exit_days"),
            planned_exit_rule_summary=str(decision_context.get("planned_exit_rule_summary", "")),
            target_position_pct=decision_context["target_position_pct"],
            current_position_pct=portfolio_summary["current_position_pct"],
            summary_text=decision_context["summary_text"],
            risk_text=self.risk_service.build_global_risk_note(session_mode, market_snapshot["market_regime"]),
            status="active",
            evidence_json=json.dumps(
                {
                    "market_snapshot": market_snapshot,
                    "category_scores": decision_context["category_scores"],
                    "winning_category": decision_context.get("winning_category", ""),
                    "winning_category_label": decision_context.get("winning_category_label", ""),
                    "fallback_triggered": bool(not evaluation["offensive_edge"]),
                    "fallback_action": evaluation["fallback_action"],
                    "reason_code": decision_context["reason_code"],
                    "candidate_count": int(filtered_df["filter_pass"].sum()) if not filtered_df.empty else 0,
                    "universe_count": int(len(filtered_df)),
                    "plan_facts": decision_context["facts"],
                    "recommendation_groups": recommendation_groups,
                    "target_portfolio": decision_context["target_portfolio"],
                    "transition_plan": decision_context["transition_plan"],
                    "daily_action_plan": decision_context["daily_action_plan"],
                    "portfolio_review_items": decision_context["portfolio_review_items"],
                    "action_counts": decision_context["action_counts"],
                },
                ensure_ascii=False,
            ),
        )
        session.add(advice_record)
        session.flush()

        persisted_items = decision_context["transition_plan"] or all_items
        session.add_all([self._build_advice_item_row(advice_record.id, payload) for payload in persisted_items])
        session.flush()

        advice_dict = {
            "id": advice_record.id,
            "advice_date": advice_record.advice_date,
            "created_at": advice_record.created_at,
            "session_mode": advice_record.session_mode,
            "action": advice_record.action,
            "action_code": advice_record.action_code,
            "market_regime": advice_record.market_regime,
            "target_position_pct": advice_record.target_position_pct,
            "current_position_pct": advice_record.current_position_pct,
            "summary_text": advice_record.summary_text,
            "risk_text": advice_record.risk_text,
            "items": recommendation_groups["executable_recommendations"],
            "executable_recommendations": recommendation_groups["executable_recommendations"],
            "best_unaffordable_recommendation": None,
            "affordable_but_weak_recommendations": recommendation_groups["affordable_but_weak_recommendations"],
            "watchlist_recommendations": recommendation_groups["watchlist_recommendations"],
            "cost_inefficient_recommendations": [],
            "show_watchlist_recommendations": True,
            "show_cost_inefficient_recommendations": False,
            "budget_filter_enabled": True,
            "fee_filter_enabled": False,
            "recommendation_counts": {
                "executable": len(recommendation_groups["executable_recommendations"]),
                "affordable_but_weak": len(recommendation_groups["affordable_but_weak_recommendations"]),
                "watchlist": len(recommendation_groups["watchlist_recommendations"]),
                "cost_inefficient": 0,
            },
            "reason_code": decision_context["reason_code"],
            "mapped_horizon_profile": advice_record.mapped_horizon_profile,
            "lifecycle_phase": advice_record.lifecycle_phase,
            "category_score": advice_record.category_score,
            "executable_now": advice_record.executable_now,
            "blocked_reason": advice_record.blocked_reason,
            "planned_exit_days": advice_record.planned_exit_days,
            "planned_exit_rule_summary": advice_record.planned_exit_rule_summary,
            "portfolio_review_items": decision_context["portfolio_review_items"],
            "transition_plan": decision_context["transition_plan"],
            "daily_action_plan": decision_context["daily_action_plan"],
            "target_portfolio": decision_context["target_portfolio"],
            "action_counts": decision_context["action_counts"],
        }

        explanation_payload = self.explanation_engine.build(
            advice=advice_dict,
            scored_df=scored_df,
            filtered_df=filtered_df,
            portfolio_summary=portfolio_summary,
            market_snapshot=market_snapshot,
            plan=decision_context,
        )

        explanation_rows = [
            ExplanationRecord(
                advice_id=advice_record.id,
                scope="overall",
                symbol=None,
                title="整体决策解释",
                summary=explanation_payload["overall"]["headline"],
                explanation_json=json.dumps(explanation_payload["overall"], ensure_ascii=False),
            )
        ]
        for item in explanation_payload["items"]:
            explanation_rows.append(
                ExplanationRecord(
                    advice_id=advice_record.id,
                    scope="item",
                    symbol=item["symbol"],
                    title=item["title"],
                    summary=item["summary"],
                    explanation_json=json.dumps(item, ensure_ascii=False),
                )
            )
        session.add_all(explanation_rows)
        session.commit()

        self.performance_service.capture_snapshot(session, snapshot_date=latest_trade)
        advice_dict["evidence"] = json.loads(advice_record.evidence_json or "{}")
        return advice_dict

    def _feature_frame(self, feature_rows: list[ETFFeature]) -> pd.DataFrame:
        if not feature_rows:
            return pd.DataFrame()
        return pd.DataFrame(
            [
                {
                    "symbol": row.symbol,
                    "close_price": row.close_price,
                    "pct_change": row.pct_change,
                    "ret_1d": row.ret_1d,
                    "latest_amount": row.latest_amount,
                    "avg_amount_20d": row.avg_amount_20d,
                    "avg_turnover_20d": row.avg_turnover_20d,
                    "momentum_3d": row.momentum_3d,
                    "momentum_5d": row.momentum_5d,
                    "momentum_10d": row.momentum_10d,
                    "momentum_20d": row.momentum_20d,
                    "ma5": row.ma5,
                    "ma10": row.ma10,
                    "ma20": row.ma20,
                    "ma_gap_5": row.ma_gap_5,
                    "ma_gap_10": row.ma_gap_10,
                    "trend_strength": row.trend_strength,
                    "volatility_5d": row.volatility_5d,
                    "volatility_10d": row.volatility_10d,
                    "volatility_20d": row.volatility_20d,
                    "rolling_max_20d": row.rolling_max_20d,
                    "drawdown_20d": row.drawdown_20d,
                    "liquidity_score": row.liquidity_score,
                    "category_return_10d": row.category_return_10d,
                    "relative_strength_10d": row.relative_strength_10d,
                    "above_ma20_flag": row.above_ma20_flag,
                    "decision_category": row.decision_category,
                    "tradability_mode": row.tradability_mode,
                    "anomaly_flag": row.anomaly_flag,
                }
                for row in feature_rows
            ]
        )

    def _enrich_with_universe(self, session: Session, feature_df: pd.DataFrame) -> pd.DataFrame:
        if feature_df.empty:
            return feature_df
        universe = {
            item.symbol: item for item in session.scalars(select(ETFUniverse).where(ETFUniverse.enabled.is_(True)))
        }
        enriched = feature_df.copy()
        enriched["name"] = enriched["symbol"].map(lambda symbol: universe[symbol].name)
        enriched["category"] = enriched["symbol"].map(lambda symbol: universe[symbol].category)
        enriched["asset_class"] = enriched["symbol"].map(lambda symbol: universe[symbol].asset_class)
        enriched["market"] = enriched["symbol"].map(lambda symbol: universe[symbol].market)
        enriched["benchmark"] = enriched["symbol"].map(lambda symbol: universe[symbol].benchmark)
        enriched["risk_level"] = enriched["symbol"].map(lambda symbol: universe[symbol].risk_level)
        enriched["min_avg_amount"] = enriched["symbol"].map(lambda symbol: universe[symbol].min_avg_amount)
        enriched["settlement_note"] = enriched["symbol"].map(lambda symbol: universe[symbol].settlement_note)
        enriched["trade_mode"] = enriched["symbol"].map(lambda symbol: universe[symbol].trade_mode)
        enriched["lot_size"] = enriched["symbol"].map(lambda symbol: universe[symbol].lot_size)
        enriched["fee_rate"] = enriched["symbol"].map(lambda symbol: universe[symbol].fee_rate)
        enriched["min_fee"] = enriched["symbol"].map(lambda symbol: universe[symbol].min_fee)
        metadata = enriched.apply(
            lambda row: self.policy.classify(
                symbol=str(row["symbol"]),
                universe_category=str(row["category"]),
                asset_class=str(row["asset_class"]),
                trade_mode=str(row["trade_mode"]),
            ),
            axis=1,
            result_type="expand",
        )
        enriched["decision_category"] = metadata["category"]
        enriched["category_label"] = metadata["category_label"]
        enriched["tradability_mode"] = metadata["tradability_mode"]
        enriched["trade_mode_display"] = enriched["tradability_mode"].map(self._trade_mode_label)
        return enriched

    def _sync_feature_scores(
        self,
        session: Session,
        feature_rows: list[ETFFeature],
        filtered_df: pd.DataFrame,
        scored_df: pd.DataFrame,
    ) -> None:
        score_map = scored_df.set_index("symbol").to_dict(orient="index") if not scored_df.empty else {}
        base_map = filtered_df.set_index("symbol").to_dict(orient="index") if not filtered_df.empty else {}
        for row in feature_rows:
            base_payload = base_map.get(row.symbol)
            score_payload = score_map.get(row.symbol)
            row.filter_pass = bool(base_payload is not None and bool(base_payload.get("filter_pass", False)))
            row.total_score = float(score_payload["decision_score"]) if score_payload is not None else 0.0
            row.rank_in_pool = int(score_payload["rank_in_pool"]) if score_payload is not None else None
            row.breakdown_json = score_payload["breakdown_json"] if score_payload is not None else json.dumps({}, ensure_ascii=False)
            if base_payload is not None:
                row.category_return_10d = float(base_payload.get("category_return_10d", 0.0))
                row.relative_strength_10d = float(base_payload.get("relative_strength_10d", 0.0))
                row.above_ma20_flag = bool(base_payload.get("above_ma20_flag", False))
                row.decision_category = str(base_payload.get("decision_category", ""))
                row.tradability_mode = str(base_payload.get("tradability_mode", ""))
        session.commit()

    def _attach_scores_to_positions(self, positions_df: pd.DataFrame, scored_df: pd.DataFrame) -> pd.DataFrame:
        if positions_df.empty:
            return positions_df
        if scored_df.empty:
            positions_df["score"] = 0.0
            positions_df["rank_in_pool"] = 999
            return positions_df
        lookup = scored_df[["symbol", "decision_score", "rank_in_pool"]].rename(columns={"decision_score": "score"})
        merged = positions_df.merge(lookup, on="symbol", how="left")
        merged["score"] = merged["score"].fillna(0.0)
        merged["rank_in_pool"] = merged["rank_in_pool"].fillna(999)
        return merged

    def _build_decision_context(
        self,
        *,
        scored_df: pd.DataFrame,
        evaluation: dict[str, Any],
        portfolio_summary: dict[str, Any],
        preferences,
        session_mode: str,
        current_time: datetime,
        trade_context: dict[str, Any],
        market_snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        target_position_pct = min(
            float(market_snapshot.get("recommended_position_pct", preferences.max_total_position_pct)),
            float(preferences.max_total_position_pct),
        )
        total_asset = float(portfolio_summary["total_asset"])
        available_cash = float(portfolio_summary["cash_balance"])
        current_position_pct = float(portfolio_summary["current_position_pct"])
        selected_category = str(evaluation["selected_category"])
        selected_category_label = self.policy.get_category_label(selected_category) if selected_category else ""
        target_portfolio = self._build_target_portfolio(
            scored_df=scored_df,
            portfolio_summary=portfolio_summary,
            preferences=preferences,
            target_position_pct=target_position_pct,
            selected_category=selected_category,
            offensive_edge=bool(evaluation["offensive_edge"]),
            fallback_action=str(evaluation["fallback_action"]),
        )
        primary_candidates = self._plan_rows(
            scored_df=scored_df,
            preferences=preferences,
            portfolio_summary=portfolio_summary,
            target_position_pct=target_position_pct,
            selected_category=selected_category,
            offensive_edge=bool(evaluation["offensive_edge"]),
            fallback_action=str(evaluation["fallback_action"]),
            session_mode=session_mode,
            current_time=current_time,
            trade_context=trade_context,
            target_portfolio=target_portfolio,
        )

        max_items = self.policy.max_selected_etfs()
        transition_plan = sorted(
            [
                item
                for item in primary_candidates
                if item["is_current_holding"] or item["target_weight"] > 0 or item["action_code"] in ORDER_ACTIONS
            ],
            key=lambda item: (
                self._transition_priority(item["action_code"]),
                abs(float(item["delta_weight"])),
                float(item["decision_score"]),
            ),
            reverse=True,
        )
        portfolio_review_items = [
            item for item in transition_plan if item["is_current_holding"]
        ]
        exit_candidates = [
            item for item in transition_plan if item["action_code"] in SELL_ACTIONS and item["recommendation_bucket"] != "ignored"
        ]
        buy_candidates = [
            item
            for item in transition_plan
            if item["category"] == selected_category
            and item["action_code"] in {"buy_open", "buy_add", "hold"}
            and item["recommendation_bucket"] != "ignored"
        ]
        money_candidates = [
            item
            for item in transition_plan
            if item["category"] == self.policy.defensive_category() and item["recommendation_bucket"] != "ignored"
        ]

        primary_items: list[dict[str, Any]]
        reason_code: str
        action_code: str

        if exit_candidates:
            exit_candidates = sorted(exit_candidates, key=lambda item: (item["exit_score"], item["decision_score"]), reverse=True)
            primary_items = exit_candidates[:max_items]
            action_code = primary_items[0]["action_code"]
            reason_code = "position_exit_signal"
            winning_category = primary_items[0]["category"]
            winning_category_label = primary_items[0]["asset_class"]
        elif evaluation["offensive_edge"] and buy_candidates:
            buy_candidates = sorted(buy_candidates, key=lambda item: item["decision_score"], reverse=True)
            primary_items = buy_candidates[:max_items]
            action_code = primary_items[0]["action_code"]
            reason_code = "category_first_selection"
            winning_category = selected_category
            winning_category_label = selected_category_label
        elif str(evaluation["fallback_action"]) == "park_in_money_etf" and money_candidates:
            money_candidates = sorted(money_candidates, key=lambda item: item["decision_score"], reverse=True)
            primary_items = money_candidates[:max_items]
            action_code = primary_items[0]["action_code"]
            reason_code = "defensive_money_fallback"
            winning_category = self.policy.defensive_category()
            winning_category_label = self.policy.get_category_label(winning_category)
        else:
            primary_items = []
            action_code = "no_trade"
            reason_code = "no_trade_offensive_weak" if not evaluation["offensive_edge"] else "no_trade_threshold_not_met"
            winning_category = selected_category
            winning_category_label = selected_category_label

        executable_items = [item for item in primary_items if item["recommendation_bucket"] == "executable_recommendations"]
        watchlist_items = [item for item in primary_items if item["recommendation_bucket"] == "watchlist_recommendations"]
        if not watchlist_items:
            watchlist_items = [
                item
                for item in transition_plan
                if item["recommendation_bucket"] == "watchlist_recommendations"
                and item["symbol"] not in {payload["symbol"] for payload in primary_items}
            ][: max(0, max_items)]
        affordable_but_weak_items = self._build_affordable_but_weak_recommendations(
            candidate_items=primary_candidates,
            excluded_symbols={payload["symbol"] for payload in primary_items},
            total_asset=total_asset,
            available_cash=available_cash,
            offensive_edge=bool(evaluation["offensive_edge"]),
            selected_category=selected_category,
            selected_category_label=selected_category_label,
        )

        primary_item = primary_items[0] if primary_items else None
        executable_now = bool(primary_item and primary_item.get("executable_now", False))
        blocked_reason = str(primary_item.get("blocked_reason", "")) if primary_item else ""
        action_counts = self._count_position_actions(transition_plan)
        summary_text = self._summary_text(
            action_code=action_code,
            winning_category_label=winning_category_label,
            selected_item=primary_item,
            offensive_edge=bool(evaluation["offensive_edge"]),
        )

        facts = {
            "available_cash": available_cash,
            "total_asset": total_asset,
            "current_position_pct": current_position_pct,
            "target_position_pct": target_position_pct,
            "selected_category": winning_category,
            "selected_category_label": winning_category_label,
            "selected_category_score": float(primary_item["category_score"]) if primary_item else float(evaluation["selected_category_score"]),
            "offensive_threshold": float(self.thresholds.get("fallback", {}).get("offensive_threshold", 55.0)),
            "open_threshold": float(self.thresholds.get("decision_thresholds", {}).get("open_threshold", 58.0)),
            "reduce_threshold": float(self.thresholds.get("decision_thresholds", {}).get("reduce_threshold", 58.0)),
            "full_exit_threshold": float(self.thresholds.get("decision_thresholds", {}).get("full_exit_threshold", 72.0)),
            "target_holding_days": int(getattr(preferences, "target_holding_days", 5)),
            "recommendation_count": len(primary_items),
            "transition_count": len(transition_plan),
            "holding_review_count": len(portfolio_review_items),
            "affordable_but_weak_count": len(affordable_but_weak_items),
            "target_portfolio_mode": target_portfolio["mode"],
            "action_counts": action_counts,
        }

        return {
            "action_code": action_code,
            "summary_text": summary_text,
            "reason_code": reason_code,
            "target_position_pct": target_position_pct,
            "recommendation_groups": {
                "executable_recommendations": executable_items,
                "best_unaffordable_recommendation": None,
                "affordable_but_weak_recommendations": affordable_but_weak_items,
                "watchlist_recommendations": watchlist_items,
                "cost_inefficient_recommendations": [],
                "show_watchlist_recommendations": True,
                "show_cost_inefficient_recommendations": False,
                "budget_filter_enabled": True,
                "fee_filter_enabled": False,
            },
            "primary_item": primary_item,
            "winning_category": winning_category,
            "winning_category_label": winning_category_label,
            "selected_category_score": float(primary_item["category_score"]) if primary_item else float(evaluation["selected_category_score"]),
            "mapped_horizon_profile": str(primary_item.get("mapped_horizon_profile", "")) if primary_item else "",
            "lifecycle_phase": str(primary_item.get("lifecycle_phase", "")) if primary_item else "",
            "executable_now": executable_now,
            "blocked_reason": blocked_reason,
            "planned_exit_days": primary_item.get("planned_exit_days") if primary_item else None,
            "planned_exit_rule_summary": str(primary_item.get("planned_exit_rule_summary", "")) if primary_item else "",
            "category_scores": self._category_score_cards(evaluation["category_scores_df"]),
            "target_portfolio": target_portfolio,
            "transition_plan": transition_plan,
            "daily_action_plan": transition_plan,
            "portfolio_review_items": portfolio_review_items,
            "action_counts": action_counts,
            "facts": facts,
        }

    def _plan_rows(
        self,
        *,
        scored_df: pd.DataFrame,
        preferences,
        portfolio_summary: dict[str, Any],
        target_position_pct: float,
        selected_category: str,
        offensive_edge: bool,
        fallback_action: str,
        session_mode: str,
        current_time: datetime,
        trade_context: dict[str, Any],
        target_portfolio: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if scored_df.empty:
            return []
        total_asset = float(portfolio_summary["total_asset"])
        available_cash = float(portfolio_summary["cash_balance"])
        current_position_pct = float(portfolio_summary["current_position_pct"])
        positions_lookup = {
            str(row["symbol"]): row
            for row in portfolio_summary["holdings"]
        }
        planned_rows = []
        for _, row in scored_df.iterrows():
            payload = row.to_dict()
            position = positions_lookup.get(str(row["symbol"]))
            target_weight = float(target_portfolio["target_weight_by_symbol"].get(str(row["symbol"]), 0.0))
            action_payload = self._decide_row_action(
                row=payload,
                position=position,
                preferences=preferences,
                total_asset=total_asset,
                available_cash=available_cash,
                current_position_pct=current_position_pct,
                target_position_pct=target_position_pct,
                target_weight=target_weight,
                selected_category=selected_category,
                offensive_edge=offensive_edge,
                fallback_action=fallback_action,
                trade_context=trade_context,
                current_time=current_time,
            )
            route_payload = self._route_action(
                symbol=str(row["symbol"]),
                action_code=action_payload["action_code"],
                tradability_mode=str(row["tradability_mode"]),
                session_mode=session_mode,
                current_time=current_time,
                trade_context=trade_context,
                decision_score=float(row["decision_score"]),
                entry_score=float(row["entry_score"]),
                exit_score=float(row["exit_score"]),
            )
            planned_rows.append(
                self._compose_item_payload(
                    row=payload,
                    position=position,
                    action_payload=action_payload,
                    route_payload=route_payload,
                    available_cash=available_cash,
                    min_trade_amount=float(preferences.min_trade_amount),
                )
            )
        return planned_rows

    def _decide_row_action(
        self,
        *,
        row: dict[str, Any],
        position: dict[str, Any] | None,
        preferences,
        total_asset: float,
        available_cash: float,
        current_position_pct: float,
        target_position_pct: float,
        target_weight: float,
        selected_category: str,
        offensive_edge: bool,
        fallback_action: str,
        trade_context: dict[str, Any],
        current_time: datetime,
    ) -> dict[str, Any]:
        return self.position_action_service.decide(
            row=row,
            position=position,
            preferences=preferences,
            total_asset=total_asset,
            available_cash=available_cash,
            current_position_pct=current_position_pct,
            target_position_pct=target_position_pct,
            target_weight=target_weight,
            selected_category=selected_category,
            offensive_edge=offensive_edge,
            fallback_action=fallback_action,
            trade_context=trade_context,
            current_time=current_time,
        )

        decision_thresholds = self.thresholds.get("decision_thresholds", {})
        reduce_fraction = float(self.thresholds.get("position_rules", {}).get("reduce_fraction", 0.5))
        full_exit_fraction = float(self.thresholds.get("position_rules", {}).get("full_exit_fraction", 1.0))
        rebalance_tolerance = float(self.thresholds.get("position_rules", {}).get("rebalance_tolerance_pct", 0.01))
        open_threshold = float(decision_thresholds.get("open_threshold", 58.0))
        add_threshold = float(decision_thresholds.get("add_threshold", 64.0))
        hold_threshold = float(decision_thresholds.get("hold_threshold", 48.0))
        reduce_threshold = float(decision_thresholds.get("reduce_threshold", 58.0))
        full_exit_threshold = float(decision_thresholds.get("full_exit_threshold", 72.0))
        strong_entry_threshold = float(decision_thresholds.get("strong_entry_threshold", 62.0))
        strong_hold_threshold = float(decision_thresholds.get("strong_hold_threshold", 55.0))

        decision_score = float(row["decision_score"])
        entry_score = float(row["entry_score"])
        hold_score = float(row["hold_score"])
        exit_score = float(row["exit_score"])
        category = str(row["decision_category"])
        min_order_amount = float(row["close_price"]) * float(row["lot_size"])

        action_code = "no_trade"
        reason = "当前没有足够强的开仓或加仓优势。"
        suggested_amount = 0.0
        suggested_pct = 0.0
        current_weight = float(position.get("weight_pct", 0.0)) if position is not None else 0.0
        market_value = float(position.get("market_value", 0.0)) if position is not None else 0.0
        current_amount = market_value if position is not None else 0.0
        target_weight = max(target_weight, 0.0)
        target_amount = max(target_weight, 0.0) * total_asset
        delta_weight = target_weight - current_weight
        delta_amount = max(delta_weight, 0.0) * total_asset
        trim_amount = max(current_weight - target_weight, 0.0) * total_asset
        defensive_category = self.policy.defensive_category()
        selected_for_target = target_weight > rebalance_tolerance

        if position is not None:
            if exit_score >= full_exit_threshold:
                action_code = "sell_exit"
                suggested_amount = market_value * full_exit_fraction
                suggested_pct = current_weight * full_exit_fraction
                reason = "退出分已经明显抬升，优先执行完整退出。"
            elif delta_weight < -rebalance_tolerance:
                if exit_score >= reduce_threshold:
                    action_code = "reduce"
                    suggested_amount = min(max(trim_amount, market_value * reduce_fraction), market_value)
                    suggested_pct = suggested_amount / total_asset if total_asset else 0.0
                    if target_weight <= rebalance_tolerance:
                        reason = "目标组合不再优先保留这只持仓，且退出分已经抬高，先减仓向目标靠拢。"
                    else:
                        reason = "当前持仓高于目标权重，退出分也抬高了，先减回目标附近。"
                elif hold_score >= hold_threshold:
                    action_code = "hold"
                    if target_weight <= rebalance_tolerance:
                        reason = "目标组合已降低这只持仓优先级，但退出证据还不够强，先继续持有观察。"
                    else:
                        reason = "当前仓位略高于目标，但退出证据不足，先继续持有。"
                else:
                    action_code = "hold"
                    reason = "目标组合希望降低仓位，但当前分数还没到明确退出，先持有等待更强信号。"
            elif delta_weight > rebalance_tolerance:
                addable_amount = min(delta_amount, available_cash)
                if (
                    category == defensive_category
                    and not offensive_edge
                    and fallback_action == "park_in_money_etf"
                ):
                    action_code = "park_in_money_etf"
                    suggested_amount = addable_amount
                    suggested_pct = suggested_amount / total_asset if total_asset else 0.0
                    reason = "防守状态下，目标组合需要把更多可用现金停泊到货币ETF。"
                elif (
                    offensive_edge
                    and category == selected_category
                    and decision_score >= add_threshold
                    and entry_score >= strong_entry_threshold
                    and hold_score >= strong_hold_threshold
                ):
                    action_code = "buy_add"
                    suggested_amount = addable_amount
                    suggested_pct = suggested_amount / total_asset if total_asset else 0.0
                    reason = "目标组合需要提高这只持仓权重，且入场分和持有分都支持加仓。"
                elif hold_score >= hold_threshold:
                    action_code = "hold"
                    reason = "目标组合偏向增配，但当前加仓证据还不够强，先继续持有。"
                else:
                    action_code = "hold"
                    reason = "这只持仓仍在目标组合里，但暂时没有更强的加仓信号。"
            elif exit_score >= reduce_threshold:
                action_code = "reduce"
                suggested_amount = min(max(market_value * reduce_fraction, trim_amount), market_value)
                suggested_pct = suggested_amount / total_asset if total_asset else 0.0
                reason = "虽然当前仓位接近目标，但退出分已经走高，先主动减仓控制风险。"
            elif hold_score >= hold_threshold and exit_score < reduce_threshold:
                action_code = "hold"
                reason = "当前仓位和目标组合大致一致，持有分稳定，继续持有。"
            else:
                action_code = "hold"
                reason = "当前没有更强的加仓理由，也还没到明确退出。"
        elif selected_for_target:
            open_amount = min(target_amount, available_cash)
            if category == defensive_category and not offensive_edge and fallback_action == "park_in_money_etf":
                action_code = "park_in_money_etf"
                suggested_amount = open_amount
                suggested_pct = suggested_amount / total_asset if total_asset else 0.0
                reason = "进攻类类别优势不够，目标组合把剩余可用资金停泊到货币ETF。"
            elif offensive_edge and category == selected_category and decision_score >= open_threshold:
                action_code = "buy_open"
                suggested_amount = open_amount
                suggested_pct = suggested_amount / total_asset if total_asset else 0.0
                reason = "这个类别先赢出，目标组合把新增仓位分配给这只ETF。"
            else:
                action_code = "no_trade"
                reason = "虽然它进入了目标组合候选，但当前分数还没达到开仓阈值。"

        if action_code in BUY_ACTIONS and (suggested_amount < float(preferences.min_trade_amount) or suggested_amount < min_order_amount):
            if action_code == "park_in_money_etf":
                reason = "防守切换成立，但当前剩余可停车资金还不到最小可执行门槛。"
            else:
                reason = "信号成立，但当前可执行金额还没覆盖最小建议金额或一手门槛。"

        return {
            "action_code": action_code,
            "reason": reason,
            "suggested_amount": round(float(suggested_amount), 2),
            "suggested_pct": float(suggested_pct),
            "min_order_amount": min_order_amount,
            "current_weight": current_weight,
            "target_weight": target_weight,
            "delta_weight": delta_weight,
            "current_amount": current_amount,
            "target_amount": target_amount,
        }

    def _route_action(
        self,
        *,
        symbol: str,
        action_code: str,
        tradability_mode: str,
        session_mode: str,
        current_time: datetime,
        trade_context: dict[str, Any],
        decision_score: float,
        entry_score: float,
        exit_score: float,
    ) -> dict[str, Any]:
        if action_code not in ORDER_ACTIONS:
            return {
                "requires_order": False,
                "executable_now": False,
                "blocked_reason": "",
                "planned_exit_days": None,
                "planned_exit_rule_summary": "",
                "edge_bps": 0.0,
            }

        if not self.policy.session_executable_now(session_mode):
            return {
                "requires_order": True,
                "executable_now": False,
                "blocked_reason": self.policy.session_blocked_reason(session_mode),
                "planned_exit_days": 1,
                "planned_exit_rule_summary": "当前时段只生成预案，下一交易时段再执行。",
                "edge_bps": 0.0,
            }

        today = current_time.date()
        same_day_buy = symbol in trade_context["same_day_buy_symbols"]
        if tradability_mode == "t1" and action_code in SELL_ACTIONS:
            t1_rules = self.thresholds.get("t1_rules", {})
            if bool(t1_rules.get("block_same_day_sell_after_buy", True)) and same_day_buy:
                return {
                    "requires_order": True,
                    "executable_now": False,
                    "blocked_reason": str(t1_rules.get("blocked_reason", "planned_exit_next_session_due_to_t1")),
                    "planned_exit_days": int(t1_rules.get("planned_exit_days", 1)),
                    "planned_exit_rule_summary": str(
                        t1_rules.get(
                            "planned_exit_rule_summary",
                            "T+1标的当天买入后不能卖出，退出计划顺延到下一交易时段。",
                        )
                    ),
                    "edge_bps": 0.0,
                }

        if tradability_mode == "t0":
            t0_rules = self.thresholds.get("t0_controls", {})
            symbol_trade_count = int(trade_context["trade_count_by_symbol_today"].get(symbol, 0))
            if symbol_trade_count >= int(t0_rules.get("max_decisions_per_symbol_per_day", 4)):
                return {
                    "requires_order": True,
                    "executable_now": False,
                    "blocked_reason": "t0_max_decisions_reached",
                    "planned_exit_days": None,
                    "planned_exit_rule_summary": "同一标的当日交易次数已到上限。",
                    "edge_bps": 0.0,
                }
            if int(trade_context["round_trips_by_symbol_today"].get(symbol, 0)) >= int(
                t0_rules.get("max_round_trips_per_day", 2)
            ):
                return {
                    "requires_order": True,
                    "executable_now": False,
                    "blocked_reason": "t0_max_round_trips_reached",
                    "planned_exit_days": None,
                    "planned_exit_rule_summary": "当日往返交易次数已到上限，避免过度交易。",
                    "edge_bps": 0.0,
                }
            last_trade = trade_context["last_trade_by_symbol"].get(symbol)
            if last_trade is not None:
                cooldown_minutes = int(t0_rules.get("cooldown_minutes_after_trade", 30))
                delta_minutes = (current_time - last_trade.executed_at).total_seconds() / 60.0
                if delta_minutes < cooldown_minutes:
                    return {
                        "requires_order": True,
                        "executable_now": False,
                        "blocked_reason": "t0_cooldown_blocked",
                        "planned_exit_days": None,
                        "planned_exit_rule_summary": f"距离上一笔成交不足 {cooldown_minutes} 分钟。",
                        "edge_bps": 0.0,
                    }
                last_trade_side = str(last_trade.side)
                if (last_trade_side == "buy" and action_code in SELL_ACTIONS) or (
                    last_trade_side == "sell" and action_code in BUY_ACTIONS
                ):
                    signal_delta = abs(entry_score - exit_score)
                    if signal_delta < float(t0_rules.get("minimum_score_delta_to_flip_signal", 10.0)):
                        return {
                            "requires_order": True,
                            "executable_now": False,
                            "blocked_reason": "t0_flip_signal_too_small",
                            "planned_exit_days": None,
                            "planned_exit_rule_summary": "同日反手信号变化太小，不足以支持翻多翻空。",
                            "edge_bps": 0.0,
                        }
            reference_threshold = (
                float(self.thresholds.get("decision_thresholds", {}).get("open_threshold", 58.0))
                if action_code in BUY_ACTIONS
                else float(self.thresholds.get("decision_thresholds", {}).get("reduce_threshold", 58.0))
            )
            edge_points = max(decision_score - reference_threshold, 0.0) if action_code in BUY_ACTIONS else max(exit_score - reference_threshold, 0.0)
            edge_bps = edge_points * float(t0_rules.get("score_to_edge_bps_multiplier", 2.0))
            if edge_bps < float(t0_rules.get("minimum_expected_edge_bps", 20.0)):
                return {
                    "requires_order": True,
                    "executable_now": False,
                    "blocked_reason": "t0_expected_edge_too_small",
                    "planned_exit_days": None,
                    "planned_exit_rule_summary": "预期优势不足以覆盖T+0反复交易的摩擦成本。",
                    "edge_bps": round(edge_bps, 2),
                }
            return {
                "requires_order": True,
                "executable_now": True,
                "blocked_reason": "",
                "planned_exit_days": None,
                "planned_exit_rule_summary": "",
                "edge_bps": round(edge_bps, 2),
            }

        return {
            "requires_order": True,
            "executable_now": True,
            "blocked_reason": "",
            "planned_exit_days": None,
            "planned_exit_rule_summary": "",
            "edge_bps": 0.0,
        }

    def _compose_item_payload(
        self,
        *,
        row: dict[str, Any],
        position: dict[str, Any] | None,
        action_payload: dict[str, Any],
        route_payload: dict[str, Any],
        available_cash: float,
        min_trade_amount: float,
    ) -> dict[str, Any]:
        action_code = action_payload["action_code"]
        requires_order = bool(route_payload["requires_order"])
        suggested_amount = float(action_payload["suggested_amount"])
        min_order_amount = float(action_payload["min_order_amount"])
        current_weight = float(action_payload["current_weight"])
        target_weight = float(action_payload["target_weight"])
        delta_weight = float(action_payload["delta_weight"])
        current_amount = float(action_payload["current_amount"])
        target_amount = float(action_payload["target_amount"])
        position_action = str(action_payload.get("position_action", "no_trade"))
        position_action_label = str(action_payload.get("position_action_label", self.policy.action_label(action_code)))
        action_reason = str(action_payload.get("action_reason", action_payload.get("reason", "")))
        rank_drop = int(action_payload.get("rank_drop", row.get("rank_drop", 0)) or 0)
        days_held = int(action_payload.get("days_held", row.get("days_held", 0)) or 0)
        filter_reasons = list(row.get("filter_reasons", [])) if isinstance(row.get("filter_reasons"), list) else []
        entry_eligible = bool(row.get("entry_eligible", row.get("filter_pass", False)))
        budget_blocked = action_code in BUY_ACTIONS and (suggested_amount < min_trade_amount or suggested_amount < min_order_amount)
        executable_now = bool(route_payload["executable_now"]) and not budget_blocked
        blocked_reason_code = "budget_below_min_trade_or_lot" if budget_blocked else str(route_payload["blocked_reason"])
        blocked_reason = self._blocked_reason_text(
            blocked_reason_code,
            suggested_amount=suggested_amount,
            min_required=max(min_trade_amount, min_order_amount),
            route_payload=route_payload,
        )
        recommendation_bucket = "ignored"
        if action_code in ORDER_ACTIONS or position is not None:
            recommendation_bucket = "executable_recommendations" if executable_now or action_code == "hold" else "watchlist_recommendations"

        execution_status = {
            "buy_open": "可执行开仓" if executable_now else "等待开仓",
            "buy_add": "可执行加仓" if executable_now else "等待加仓",
            "hold": "继续持有",
            "reduce": "可执行减仓" if executable_now else "计划减仓",
            "sell_exit": "可执行卖出" if executable_now else "计划卖出",
            "park_in_money_etf": "可执行转入货币ETF" if executable_now else "等待转入货币ETF",
            "no_trade": "暂不交易",
        }[action_code]
        execution_note = action_reason
        if blocked_reason:
            execution_note = f"{action_reason} {blocked_reason}"
        if position is not None and not entry_eligible:
            filter_summary = "、".join(filter_reasons) if filter_reasons else "未通过本轮新开仓筛选"
            execution_note = (
                f"{execution_note} 这只标的当前不适合继续新买，但因为你已经持有，"
                f"系统仍然会正式评估持有、减仓还是退出。筛选限制：{filter_summary}。"
            )

        current_return_pct = 0.0
        if position is not None:
            cost_basis = float(position.get("avg_cost", 0.0)) * float(position.get("quantity", 0.0))
            if cost_basis > 0:
                current_return_pct = float(position.get("unrealized_pnl", 0.0)) / cost_basis

        return {
            "symbol": row["symbol"],
            "name": row.get("name", row["symbol"]),
            "rank": int(row["rank_in_category"]),
            "action": position_action_label,
            "action_code": action_code,
            "position_action": position_action,
            "position_action_label": position_action_label,
            "action_reason": action_reason,
            "suggested_amount": suggested_amount,
            "suggested_pct": float(action_payload["suggested_pct"]),
            "trigger_price_low": round(float(row["close_price"]) * 0.995, 3),
            "trigger_price_high": round(float(row["close_price"]) * 1.01, 3),
            "stop_loss_pct": self.risk_service.get_stop_loss_pct(str(row["decision_category"])),
            "take_profit_pct": self.risk_service.get_take_profit_pct(str(row["decision_category"])),
            "score": float(row["decision_score"]),
            "decision_score": float(row["decision_score"]),
            "score_gap": round(float(row["score_gap"]), 2),
            "reason_short": self._reason_short(row, action_reason),
            "risk_level": str(row["risk_level"]),
            "category": str(row["decision_category"]),
            "asset_class": self.policy.get_category_label(str(row["decision_category"])),
            "trade_mode": self._trade_mode_label(str(row["tradability_mode"])),
            "tradability_mode": str(row["tradability_mode"]),
            "trade_mode_note": self._trade_mode_note(str(row["tradability_mode"])),
            "latest_price": float(row["close_price"]),
            "lot_size": float(row["lot_size"]),
            "fee_rate": float(row["fee_rate"]),
            "min_fee": float(row["min_fee"]),
            "estimated_fee": 0.0,
            "estimated_cost_rate": 0.0,
            "min_advice_amount": min_trade_amount,
            "min_order_amount": min_order_amount,
            "available_cash": available_cash,
            "is_executable": executable_now,
            "execution_status": execution_status,
            "recommendation_bucket": recommendation_bucket,
            "execution_note": execution_note,
            "transition_label": self._transition_label(
                action_code=action_code,
                is_current_holding=position is not None,
                current_weight=current_weight,
                target_weight=target_weight,
            ),
            "entry_score": float(row["entry_score"]),
            "hold_score": float(row["hold_score"]),
            "exit_score": float(row["exit_score"]),
            "category_score": float(row["category_score"]),
            "target_holding_days": int(row["target_holding_days"]),
            "mapped_horizon_profile": str(row["mapped_horizon_profile"]),
            "horizon_profile_label": self.policy.get_profile_label(str(row["mapped_horizon_profile"])),
            "lifecycle_phase": str(row["lifecycle_phase"]),
            "executable_now": executable_now,
            "blocked_reason": blocked_reason,
            "planned_exit_days": route_payload["planned_exit_days"],
            "planned_exit_rule_summary": route_payload["planned_exit_rule_summary"],
            "current_position": position or {},
            "is_current_holding": position is not None,
            "is_held": position is not None,
            "current_weight": current_weight,
            "target_weight": target_weight,
            "delta_weight": delta_weight,
            "current_amount": round(current_amount, 2),
            "target_amount": round(target_amount, 2),
            "current_return_pct": current_return_pct,
            "rank_in_category": int(row["rank_in_category"]),
            "previous_rank_in_category": int(row.get("previous_rank_in_category", row["rank_in_category"])),
            "rank_drop": rank_drop,
            "days_held": days_held,
            "entry_eligible": entry_eligible,
            "filter_pass": bool(row.get("filter_pass", False)),
            "filter_reasons": filter_reasons,
            "requires_order": requires_order,
            "scores": {
                "entry_score": float(row["entry_score"]),
                "hold_score": float(row["hold_score"]),
                "exit_score": float(row["exit_score"]),
                "decision_score": float(row["decision_score"]),
                "category_score": float(row["category_score"]),
            },
            "score_breakdown": self._parse_breakdown(row["breakdown_json"]),
        }

    def _summary_text(
        self,
        *,
        action_code: str,
        winning_category_label: str,
        selected_item: dict[str, Any] | None,
        offensive_edge: bool,
    ) -> str:
        if action_code == "no_trade":
            return "当前先给出目标组合和持仓复核结果，但没有形成需要立刻执行的新动作。"
        if selected_item is None:
            return f"{winning_category_label} 当前领先，但还没有形成可执行动作。"
        profile_label = self.policy.get_profile_label(str(selected_item.get("mapped_horizon_profile", "")))
        phase_label = self._phase_label(str(selected_item.get("lifecycle_phase", "")))
        if action_code == "park_in_money_etf":
            return f"进攻边不足，先切到 {selected_item['name']} 做防守停车。"
        if action_code in SELL_ACTIONS:
            return (
                f"{selected_item['name']} 当前权重高于目标组合要求，且退出分走高，"
                f"建议{self.policy.action_label(action_code)}。"
            )
        if action_code == "hold":
            return (
                f"{selected_item['name']} 当前仍在 {profile_label} 的{phase_label}，"
                f"组合目标和持有评分都支持继续持有。"
            )
        return (
            f"{winning_category_label} 先赢出，{selected_item['name']} 在 {profile_label} 的{phase_label}决策分最高，"
            f"并进入目标组合，建议{self.policy.action_label(action_code)}。"
        )

    def _reason_short(self, row: dict[str, Any], reason: str) -> str:
        return (
            f"{reason} 入场分 {float(row['entry_score']):.1f}，持有分 {float(row['hold_score']):.1f}，"
            f"退出分 {float(row['exit_score']):.1f}，决策分 {float(row['decision_score']):.1f}。"
        )

    def _build_target_portfolio(
        self,
        *,
        scored_df: pd.DataFrame,
        portfolio_summary: dict[str, Any],
        preferences,
        target_position_pct: float,
        selected_category: str,
        offensive_edge: bool,
        fallback_action: str,
    ) -> dict[str, Any]:
        current_weights = {
            str(row["symbol"]): float(row["weight_pct"])
            for row in portfolio_summary["holdings"]
        }
        total_asset = float(portfolio_summary["total_asset"])
        available_cash = float(portfolio_summary["cash_balance"])
        max_selected = max(1, self.policy.max_selected_etfs())
        max_single_weight = float(preferences.max_single_position_pct)
        target_weight_by_symbol: dict[str, float] = {}
        mode = "no_trade"
        notes: list[str] = []

        if offensive_edge and selected_category and not scored_df.empty:
            selected_rows = scored_df[
                scored_df["decision_category"] == selected_category
            ].sort_values(["decision_score", "entry_score"], ascending=[False, False])
            selected_rows = selected_rows.head(max_selected)
            target_total_weight = min(float(target_position_pct), float(max_selected) * max_single_weight)
            target_weight_by_symbol = self._allocate_target_weights(
                selected_rows,
                total_weight=target_total_weight,
                max_single_weight=max_single_weight,
            )
            mode = "offensive"
            if target_weight_by_symbol:
                notes.append(
                    f"先把目标仓位分配给当前胜出类别 {self.policy.get_category_label(selected_category)} 的头部ETF。"
                )
        elif fallback_action == "park_in_money_etf" and not scored_df.empty and total_asset > 0:
            defensive_category = self.policy.defensive_category()
            money_rows = scored_df[
                scored_df["decision_category"] == defensive_category
            ].sort_values(["decision_score", "entry_score"], ascending=[False, False])
            if not money_rows.empty:
                symbol = str(money_rows.iloc[0]["symbol"])
                current_weight = current_weights.get(symbol, 0.0)
                deployable_cash_weight = max(
                    available_cash - total_asset * float(preferences.cash_reserve_pct),
                    0.0,
                ) / total_asset
                target_weight_by_symbol[symbol] = min(current_weight + deployable_cash_weight, 1.0)
                mode = "defensive"
                notes.append("进攻边不足时，把新增可部署现金优先停泊到货币ETF。")

        rows = []
        for _, row in scored_df.iterrows():
            symbol = str(row["symbol"])
            current_weight = current_weights.get(symbol, 0.0)
            target_weight = float(target_weight_by_symbol.get(symbol, 0.0))
            if current_weight <= 0 and target_weight <= 0:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "name": row.get("name", symbol),
                    "category": str(row["decision_category"]),
                    "category_label": self.policy.get_category_label(str(row["decision_category"])),
                    "current_weight": round(current_weight, 4),
                    "target_weight": round(target_weight, 4),
                    "delta_weight": round(target_weight - current_weight, 4),
                    "current_amount": round(current_weight * total_asset, 2),
                    "target_amount": round(target_weight * total_asset, 2),
                    "decision_score": round(float(row["decision_score"]), 2),
                    "entry_eligible": bool(row.get("entry_eligible", row.get("filter_pass", False))),
                    "filter_reasons": list(row.get("filter_reasons", []))
                    if isinstance(row.get("filter_reasons"), list)
                    else [],
                    "is_current_holding": bool(current_weight > 0),
                }
            )
        rows.sort(
            key=lambda payload: (
                max(abs(float(payload["delta_weight"])), float(payload["target_weight"]), float(payload["current_weight"])),
                float(payload["decision_score"]),
            ),
            reverse=True,
        )
        if not rows:
            notes.append("当前没有需要纳入目标组合或复核的持仓。")

        return {
            "mode": mode,
            "selected_category": selected_category,
            "selected_category_label": self.policy.get_category_label(selected_category) if selected_category else "",
            "target_weight_by_symbol": target_weight_by_symbol,
            "rows": rows,
            "notes": notes,
        }

    def _allocate_target_weights(
        self,
        selected_rows: pd.DataFrame,
        *,
        total_weight: float,
        max_single_weight: float,
    ) -> dict[str, float]:
        if selected_rows.empty or total_weight <= 0:
            return {}

        weights = {str(row["symbol"]): 0.0 for _, row in selected_rows.iterrows()}
        scores = {
            str(row["symbol"]): max(float(row["decision_score"]), 1.0)
            for _, row in selected_rows.iterrows()
        }
        remaining_symbols = set(weights)
        remaining_weight = min(float(total_weight), float(len(remaining_symbols)) * max_single_weight)

        while remaining_symbols and remaining_weight > 1e-6:
            score_sum = sum(scores[symbol] for symbol in remaining_symbols)
            capped_this_round: list[str] = []
            for symbol in list(remaining_symbols):
                cap_room = max(max_single_weight - weights[symbol], 0.0)
                if cap_room <= 1e-6:
                    capped_this_round.append(symbol)
                    continue
                provisional = (
                    remaining_weight * scores[symbol] / score_sum
                    if score_sum > 0
                    else remaining_weight / len(remaining_symbols)
                )
                if provisional >= cap_room - 1e-6:
                    weights[symbol] += cap_room
                    remaining_weight -= cap_room
                    capped_this_round.append(symbol)
            if not capped_this_round:
                for symbol in remaining_symbols:
                    provisional = (
                        remaining_weight * scores[symbol] / score_sum
                        if score_sum > 0
                        else remaining_weight / len(remaining_symbols)
                    )
                    weights[symbol] += provisional
                remaining_weight = 0.0
            for symbol in capped_this_round:
                remaining_symbols.discard(symbol)
            remaining_weight = max(remaining_weight, 0.0)
        return {symbol: round(weight, 6) for symbol, weight in weights.items() if weight > 1e-6}

    def _transition_priority(self, action_code: str) -> int:
        return {
            "sell_exit": 6,
            "reduce": 5,
            "buy_open": 4,
            "buy_add": 3,
            "park_in_money_etf": 3,
            "hold": 2,
            "no_trade": 1,
        }.get(action_code, 0)

    def _transition_label(
        self,
        *,
        action_code: str,
        is_current_holding: bool,
        current_weight: float,
        target_weight: float,
    ) -> str:
        if action_code in {"sell_exit", "reduce"} and target_weight <= 0.0001:
            return "逐步退出当前持仓"
        if action_code in {"buy_open", "park_in_money_etf"} and not is_current_holding:
            return "纳入目标组合"
        if action_code == "buy_add":
            return "向目标权重增配"
        if action_code == "hold" and is_current_holding:
            if abs(target_weight - current_weight) <= 0.01:
                return "继续按目标附近持有"
            return "暂时维持，等待更强信号"
        if action_code == "reduce":
            return "向目标权重回落"
        return "保持观察"

    def _build_advice_item_row(self, advice_id: int, payload: dict[str, Any]) -> AdviceItem:
        return AdviceItem(
            advice_id=advice_id,
            symbol=str(payload["symbol"]),
            name=str(payload["name"]),
            action_code=str(payload["action_code"]),
            rank=int(payload["rank"]),
            action=str(payload["action"]),
            category=str(payload["category"]),
            tradability_mode=str(payload["tradability_mode"]),
            target_holding_days=int(payload["target_holding_days"]),
            mapped_horizon_profile=str(payload["mapped_horizon_profile"]),
            lifecycle_phase=str(payload["lifecycle_phase"]),
            entry_score=float(payload["entry_score"]),
            hold_score=float(payload["hold_score"]),
            exit_score=float(payload["exit_score"]),
            category_score=float(payload["category_score"]),
            decision_score=float(payload["decision_score"]),
            executable_now=bool(payload["executable_now"]),
            blocked_reason=str(payload["blocked_reason"]),
            planned_exit_days=payload.get("planned_exit_days"),
            planned_exit_rule_summary=str(payload.get("planned_exit_rule_summary", "")),
            suggested_amount=float(payload["suggested_amount"]),
            suggested_pct=float(payload["suggested_pct"]),
            trigger_price_low=payload.get("trigger_price_low"),
            trigger_price_high=payload.get("trigger_price_high"),
            stop_loss_pct=float(payload["stop_loss_pct"]),
            take_profit_pct=float(payload["take_profit_pct"]),
            score=float(payload["score"]),
            score_gap=float(payload["score_gap"]),
            reason_short=str(payload["reason_short"]),
            risk_level=str(payload["risk_level"]),
        )

    def _trade_context(self, session: Session, positions_df: pd.DataFrame, current_time: datetime) -> dict[str, Any]:
        trades = list_trades(session, limit=1000)
        today = current_time.date()
        current_symbols = set(positions_df["symbol"].tolist()) if not positions_df.empty else set()
        trade_count_by_symbol_today: dict[str, int] = {}
        same_day_buy_symbols: set[str] = set()
        round_trip_inputs: dict[str, dict[str, int]] = {}
        last_trade_by_symbol: dict[str, Any] = {}
        latest_buy_by_symbol: dict[str, datetime] = {}

        for trade in trades:
            symbol = str(trade.symbol)
            if symbol not in last_trade_by_symbol:
                last_trade_by_symbol[symbol] = trade
            if trade.executed_at.date() == today:
                trade_count_by_symbol_today[symbol] = trade_count_by_symbol_today.get(symbol, 0) + 1
                round_trip_inputs.setdefault(symbol, {"buy": 0, "sell": 0})[str(trade.side)] += 1
                if trade.side == "buy":
                    same_day_buy_symbols.add(symbol)
            if symbol in current_symbols and trade.side == "buy":
                latest_buy_by_symbol[symbol] = max(latest_buy_by_symbol.get(symbol, trade.executed_at), trade.executed_at)

        days_held_map = {
            symbol: max((today - buy_time.date()).days + 1, 0)
            for symbol, buy_time in latest_buy_by_symbol.items()
        }
        round_trips_by_symbol_today = {
            symbol: min(counts.get("buy", 0), counts.get("sell", 0)) for symbol, counts in round_trip_inputs.items()
        }
        return {
            "same_day_buy_symbols": same_day_buy_symbols,
            "trade_count_by_symbol_today": trade_count_by_symbol_today,
            "round_trips_by_symbol_today": round_trips_by_symbol_today,
            "last_trade_by_symbol": last_trade_by_symbol,
            "days_held_map": days_held_map,
        }

    def _previous_rank_map(self, session: Session) -> dict[str, int]:
        latest_advice = get_latest_advice(session)
        if latest_advice is None:
            return {}
        return {str(item.symbol): int(item.rank) for item in latest_advice.items}

    def _count_position_actions(self, items: list[dict[str, Any]]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for item in items:
            action = str(item.get("position_action", item.get("action_code", "no_trade")))
            counts[action] = counts.get(action, 0) + 1
        return counts

    def _build_affordable_but_weak_recommendations(
        self,
        *,
        candidate_items: list[dict[str, Any]],
        excluded_symbols: set[str],
        total_asset: float,
        available_cash: float,
        offensive_edge: bool,
        selected_category: str,
        selected_category_label: str,
    ) -> list[dict[str, Any]]:
        if available_cash <= 0:
            return []

        items: list[dict[str, Any]] = []
        for payload in candidate_items:
            symbol = str(payload.get("symbol", ""))
            min_order_amount = round_money(float(payload.get("min_order_amount", 0.0)))
            if (
                not symbol
                or symbol in excluded_symbols
                or bool(payload.get("is_current_holding"))
                or str(payload.get("action_code", "")) != "no_trade"
                or not bool(payload.get("entry_eligible", payload.get("filter_pass", False)))
                or min_order_amount <= 0
                or min_order_amount > float(available_cash)
            ):
                continue

            weak_reason = self._affordable_but_weak_reason(
                item=payload,
                offensive_edge=offensive_edge,
                selected_category=selected_category,
                selected_category_label=selected_category_label,
            )
            annotated = dict(payload)
            suggested_amount = min_order_amount
            suggested_pct = round(suggested_amount / total_asset, 4) if total_asset else 0.0
            asset_class = annotated.get("asset_class") or self.policy.get_category_label(str(annotated.get("category", "")))
            annotated.update(
                {
                    "suggested_amount": suggested_amount,
                    "suggested_pct": suggested_pct,
                    "is_executable": False,
                    "executable_now": False,
                    "is_affordable_but_weak": True,
                    "weak_signal_reason": weak_reason,
                    "not_executable_reason": weak_reason,
                    "recommendation_bucket": "affordable_but_weak_recommendations",
                    "execution_status": "买得起但当前不建议买",
                    "execution_note": (
                        f"这只 {asset_class} 当前买得起 1 手，但{weak_reason[:-1]}，"
                        "所以系统不建议现在马上下单。"
                    ),
                }
            )
            items.append(annotated)

        items.sort(
            key=lambda item: (
                float(item.get("category_score", 0.0)),
                float(item.get("decision_score", item.get("score", 0.0))),
                -float(item.get("min_order_amount", 0.0)),
            ),
            reverse=True,
        )
        return items[: int(self.settings.top_n_default)]

    def _affordable_but_weak_reason(
        self,
        *,
        item: dict[str, Any],
        offensive_edge: bool,
        selected_category: str,
        selected_category_label: str,
    ) -> str:
        reasons: list[str] = []
        category = str(item.get("category", ""))
        category_label = str(item.get("asset_class") or self.policy.get_category_label(category) or "这类ETF")
        category_score = float(item.get("category_score", 0.0))
        decision_score = float(item.get("decision_score", item.get("score", 0.0)))
        offensive_threshold = float(self.thresholds.get("fallback", {}).get("offensive_threshold", 55.0))
        open_threshold = float(self.thresholds.get("decision_thresholds", {}).get("open_threshold", 58.0))

        if not offensive_edge and category and category != self.policy.defensive_category():
            reasons.append(f"{category_label}当前类别分 {category_score:.1f} 还没达到出手阈值 {offensive_threshold:.1f}")
        elif offensive_edge and selected_category and category != selected_category:
            target_label = selected_category_label or self.policy.get_category_label(selected_category)
            reasons.append(f"当前主配置优先看 {target_label}")

        if decision_score < open_threshold:
            reasons.append(f"决策分 {decision_score:.1f} 还没达到开仓阈值 {open_threshold:.1f}")

        if not reasons:
            reasons.append("当前还不属于值得执行的主推荐")
        return "；".join(dict.fromkeys(reasons)) + "。"

    def _category_score_cards(self, category_scores_df: pd.DataFrame) -> list[dict[str, Any]]:
        if category_scores_df.empty:
            return []
        return [
            {
                "decision_category": str(row["decision_category"]),
                "category_label": str(row["category_label"]),
                "category_score": round(float(row["category_score"]), 2),
                "offensive_score": round(float(row["offensive_score"]), 2),
                "defensive_score": round(float(row["defensive_score"]), 2),
                "symbol_count": int(row["symbol_count"]),
                "raw_metrics": {
                    "momentum_10d": round(float(row["category_momentum"]), 3),
                    "trend_strength": round(float(row["category_trend"]), 3),
                    "breadth": round(float(row["category_breadth"]), 3),
                    "volatility_10d": round(float(row["category_volatility"]), 3),
                    "drawdown_20d": round(float(row["category_drawdown"]), 3),
                    "liquidity_score": round(float(row["defensive_liquidity"]), 3),
                },
                "breakdown": self._parse_breakdown(row["breakdown_json"]),
            }
            for _, row in category_scores_df.iterrows()
        ]

    def _trade_mode_label(self, value: str) -> str:
        return "T+0" if value == "t0" else "T+1"

    def _trade_mode_note(self, value: str) -> str:
        if value == "t0":
            return "T+0 标的允许同日买卖，但会额外检查冷却时间和翻转阈值。"
        return "T+1 标的若当天刚买入，则卖出信号会顺延到下一交易时段。"

    def _phase_label(self, value: str) -> str:
        return {
            "build_phase": "建仓阶段",
            "hold_phase": "持有阶段",
            "exit_phase": "退出阶段",
        }.get(value, value)

    def _blocked_reason_text(
        self,
        blocked_reason_code: str,
        *,
        suggested_amount: float,
        min_required: float,
        route_payload: dict[str, Any],
    ) -> str:
        if not blocked_reason_code:
            return ""
        messages = {
            "budget_below_min_trade_or_lot": (
                f"当前建议金额只有 {suggested_amount:.2f} 元，至少需要 {min_required:.2f} 元才能覆盖最小建议金额或一手门槛。"
            ),
            "market_not_open_planned_for_open": "当前还没进入连续交易时段，动作改成开盘后预案。",
            "market_closed_planned_for_next_session": "当前已收盘或休市，动作顺延到下一交易时段。",
            "planned_exit_next_session_due_to_t1": "这是 T+1 标的，当天刚买入后不能卖出，退出计划顺延到下一交易时段。",
            "t0_max_decisions_reached": "同一标的当日交易次数已到上限，为了防过度交易先阻塞。",
            "t0_max_round_trips_reached": "同一标的当日往返交易次数已到上限，为了防反复来回交易先阻塞。",
            "t0_cooldown_blocked": route_payload.get("planned_exit_rule_summary", "距离上一笔成交太近，仍在冷却时间内。"),
            "t0_flip_signal_too_small": "同日反手的分数差太小，不足以支撑翻向交易。",
            "t0_expected_edge_too_small": "当前信号优势太小，无法覆盖 T+0 反复交易的摩擦成本。",
        }
        return str(messages.get(blocked_reason_code, blocked_reason_code))

    def _parse_breakdown(self, value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                loaded = json.loads(value)
            except json.JSONDecodeError:
                return {}
            if isinstance(loaded, dict):
                return loaded
        return {}

    def _market_snapshot_dict(self, snapshot: MarketSnapshot | None) -> dict[str, Any]:
        if snapshot is None:
            return {
                "market_regime": "观望",
                "broad_index_score": 0.0,
                "risk_appetite_score": 0.0,
                "trend_score": 0.0,
                "recommended_position_pct": 0.05,
            }
        payload = json.loads(snapshot.raw_json or "{}")
        if "source" not in payload:
            legacy_source = payload.get("data_source", "unknown")
            payload["source"] = {
                "code": legacy_source,
                "label": "AKShare 公开行情" if legacy_source == "akshare" else "内置模拟数据",
                "api": "akshare.fund_etf_hist_em" if legacy_source == "akshare" else "local_fallback_generator",
                "note": (
                    "这是旧版本生成的快照，已按兼容逻辑补出数据来源说明。"
                    if legacy_source == "akshare"
                    else "这是旧版本生成的快照，且当时没有拿到真实行情。"
                ),
                "is_realtime": False,
                "trade_date": snapshot.trade_date.isoformat(),
                "captured_at": snapshot.captured_at.isoformat(),
            }
        if "formulas" not in payload:
            payload["formulas"] = {
                "broad_index_score": "min(max(50 + broad_momentum * 4 + broad_ma_gap * 3, 0), 100)",
                "risk_appetite_score": "min(max(50 + (offense_score - defense_score) * 5, 0), 100)",
                "trend_score": "min(max(trend_positive_ratio * 0.8 + trend_strength * 2.5, 0), 100)",
            }
        return {
            "market_regime": snapshot.market_regime,
            "broad_index_score": snapshot.broad_index_score,
            "risk_appetite_score": snapshot.risk_appetite_score,
            "trend_score": snapshot.trend_score,
            "recommended_position_pct": snapshot.recommended_position_pct,
            "raw": payload,
        }

    def _snapshot_requires_refresh(self, snapshot: MarketSnapshot | None, latest_trade) -> bool:
        if snapshot is None:
            return True
        if latest_trade is not None and snapshot.trade_date != latest_trade:
            return True
        raw = json.loads(snapshot.raw_json or "{}")
        required_keys = {"source", "formulas", "request_summary", "quality_summary", "quality_checks", "series_samples"}
        if not required_keys.issubset(raw.keys()):
            return True
        return False

    def _update_position_actions(self, session: Session, portfolio_review_items: list[dict[str, Any]], market_regime: str) -> None:
        lookup = {str(item["symbol"]): item for item in portfolio_review_items}
        positions = list(session.scalars(select(Position)))
        for position in positions:
            payload = lookup.get(position.symbol)
            if payload is None:
                position.last_action_suggestion = self.risk_service.position_action_hint({}, market_regime)
                continue
            position.last_action_suggestion = str(payload.get("execution_status") or payload.get("action") or "继续持有")
        session.commit()
