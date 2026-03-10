from __future__ import annotations

import json
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import AdviceItem, AdviceRecord, ETFFeature, ETFUniverse, ExplanationRecord, MarketSnapshot, Position
from app.repositories.market_repo import get_latest_market_snapshot, get_latest_trade_date
from app.repositories.user_repo import get_preferences, get_user
from app.services.allocation_service import AllocationService
from app.services.explanation_engine import ExplanationEngine
from app.services.market_data_service import MarketDataService
from app.services.performance_service import PerformanceService
from app.services.portfolio_service import PortfolioService
from app.services.risk_service import RiskService
from app.services.scoring_service import ScoringService
from app.services.universe_filter_service import UniverseFilterService
from app.utils.dates import detect_session_mode, get_now, latest_market_date


class DecisionEngine:
    def __init__(self) -> None:
        self.market_data_service = MarketDataService()
        self.portfolio_service = PortfolioService()
        self.performance_service = PerformanceService()
        self.filter_service = UniverseFilterService()
        self.scoring_service = ScoringService()
        self.allocation_service = AllocationService()
        self.risk_service = RiskService()
        self.explanation_engine = ExplanationEngine()

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

        features = list(
            session.scalars(select(ETFFeature).where(ETFFeature.trade_date == latest_trade).order_by(ETFFeature.symbol))
        )
        feature_df = pd.DataFrame(
            [
                {
                    "symbol": row.symbol,
                    "close_price": row.close_price,
                    "pct_change": row.pct_change,
                    "latest_amount": row.latest_amount,
                    "avg_amount_20d": row.avg_amount_20d,
                    "momentum_3d": row.momentum_3d,
                    "momentum_5d": row.momentum_5d,
                    "momentum_10d": row.momentum_10d,
                    "ma_gap_5": row.ma_gap_5,
                    "ma_gap_10": row.ma_gap_10,
                    "trend_strength": row.trend_strength,
                    "volatility_10d": row.volatility_10d,
                    "drawdown_20d": row.drawdown_20d,
                    "liquidity_score": row.liquidity_score,
                    "anomaly_flag": row.anomaly_flag,
                }
                for row in features
            ]
        )
        feature_df = self._enrich_with_universe(session, feature_df)

        filtered_df = self.filter_service.apply(feature_df, preferences)
        candidates_df = filtered_df[filtered_df["filter_pass"]].copy()
        scored_df = self.scoring_service.score(candidates_df)
        self._sync_feature_scores(session, features, scored_df)

        portfolio_summary = self.portfolio_service.get_portfolio_summary(session)
        positions_df = self.portfolio_service.positions_dataframe(session)
        positions_ranked = self._attach_scores_to_positions(positions_df, scored_df)
        market_snapshot = self._market_snapshot_dict(latest_snapshot)
        self._update_position_hints(session, positions_ranked, market_snapshot["market_regime"])
        session_mode = detect_session_mode(current_time)

        plan = self.allocation_service.plan(
            scored_df=scored_df,
            positions_df=positions_ranked,
            total_asset=portfolio_summary["total_asset"],
            available_cash=portfolio_summary["cash_balance"],
            current_position_pct=portfolio_summary["current_position_pct"],
            preferences=preferences,
            market_regime=market_snapshot["market_regime"],
        )

        executable_payloads = self._build_advice_items(
            plan_items=plan.get("items", []),
            scored_df=scored_df,
            full_df=feature_df,
            default_action=plan["action"],
        )
        watchlist_payloads = self._build_advice_items(
            plan_items=plan.get("watchlist_items", []),
            scored_df=scored_df,
            full_df=feature_df,
            default_action="关注",
        )
        recommendation_groups = {
            "executable_recommendations": executable_payloads,
            "watchlist_recommendations": watchlist_payloads,
            "show_watchlist_recommendations": self.allocation_service.settings.show_watchlist_recommendations,
            "budget_filter_enabled": self.allocation_service.settings.budget_filter_enabled,
        }

        advice_record = AdviceRecord(
            advice_date=latest_trade,
            created_at=current_time,
            session_mode=session_mode,
            action=plan["action"],
            market_regime=market_snapshot["market_regime"],
            target_position_pct=plan["target_position_pct"],
            current_position_pct=portfolio_summary["current_position_pct"],
            summary_text=plan["summary"],
            risk_text=self.risk_service.build_global_risk_note(session_mode, market_snapshot["market_regime"]),
            status="active",
            evidence_json=json.dumps(
                {
                    "market_snapshot": market_snapshot,
                    "plan_facts": plan.get("facts", {}),
                    "reason_code": plan.get("reason_code"),
                    "candidate_count": int(filtered_df["filter_pass"].sum()) if not filtered_df.empty else 0,
                    "universe_count": int(len(filtered_df)),
                    "recommendation_groups": recommendation_groups,
                },
                ensure_ascii=False,
            ),
        )
        session.add(advice_record)
        session.flush()

        db_item_keys = {
            "symbol",
            "name",
            "rank",
            "action",
            "suggested_amount",
            "suggested_pct",
            "trigger_price_low",
            "trigger_price_high",
            "stop_loss_pct",
            "take_profit_pct",
            "score",
            "score_gap",
            "reason_short",
            "risk_level",
        }
        session.add_all(
            [
                AdviceItem(
                    advice_id=advice_record.id,
                    **{key: value for key, value in payload.items() if key in db_item_keys},
                )
                for payload in executable_payloads
            ]
        )
        session.flush()

        advice_dict = {
            "id": advice_record.id,
            "advice_date": advice_record.advice_date,
            "created_at": advice_record.created_at,
            "session_mode": advice_record.session_mode,
            "action": advice_record.action,
            "market_regime": advice_record.market_regime,
            "target_position_pct": advice_record.target_position_pct,
            "current_position_pct": advice_record.current_position_pct,
            "summary_text": advice_record.summary_text,
            "risk_text": advice_record.risk_text,
            "items": executable_payloads,
            "executable_recommendations": executable_payloads,
            "watchlist_recommendations": watchlist_payloads,
            "show_watchlist_recommendations": recommendation_groups["show_watchlist_recommendations"],
            "budget_filter_enabled": recommendation_groups["budget_filter_enabled"],
            "recommendation_counts": {
                "executable": len(executable_payloads),
                "watchlist": len(watchlist_payloads),
            },
            "reason_code": plan.get("reason_code"),
        }
        explanation_payload = self.explanation_engine.build(
            advice=advice_dict,
            scored_df=scored_df,
            filtered_df=filtered_df,
            portfolio_summary=portfolio_summary,
            market_snapshot=market_snapshot,
            plan=plan,
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

    def _enrich_with_universe(self, session: Session, feature_df: pd.DataFrame) -> pd.DataFrame:
        if feature_df.empty:
            return feature_df
        universe = {
            item.symbol: item for item in session.scalars(select(ETFUniverse).where(ETFUniverse.enabled.is_(True)))
        }
        enriched = feature_df.copy()
        enriched["name"] = enriched["symbol"].map(lambda symbol: universe[symbol].name)
        enriched["category"] = enriched["symbol"].map(lambda symbol: universe[symbol].category)
        enriched["market"] = enriched["symbol"].map(lambda symbol: universe[symbol].market)
        enriched["benchmark"] = enriched["symbol"].map(lambda symbol: universe[symbol].benchmark)
        enriched["risk_level"] = enriched["symbol"].map(lambda symbol: universe[symbol].risk_level)
        enriched["min_avg_amount"] = enriched["symbol"].map(lambda symbol: universe[symbol].min_avg_amount)
        enriched["settlement_note"] = enriched["symbol"].map(lambda symbol: universe[symbol].settlement_note)
        return enriched

    def _sync_feature_scores(self, session: Session, feature_rows: list[ETFFeature], scored_df: pd.DataFrame) -> None:
        score_map = scored_df.set_index("symbol").to_dict(orient="index") if not scored_df.empty else {}
        for row in feature_rows:
            payload = score_map.get(row.symbol)
            row.filter_pass = bool(payload is not None)
            row.total_score = float(payload["total_score"]) if payload is not None else 0.0
            row.rank_in_pool = int(payload["rank_in_pool"]) if payload is not None else None
            row.breakdown_json = payload["breakdown_json"] if payload is not None else json.dumps({}, ensure_ascii=False)
        session.commit()

    def _attach_scores_to_positions(self, positions_df: pd.DataFrame, scored_df: pd.DataFrame) -> pd.DataFrame:
        if positions_df.empty:
            return positions_df
        if scored_df.empty:
            positions_df["score"] = 0.0
            positions_df["rank_in_pool"] = 999
            return positions_df
        lookup = scored_df[["symbol", "total_score", "rank_in_pool"]].rename(columns={"total_score": "score"})
        merged = positions_df.merge(lookup, on="symbol", how="left")
        merged["score"] = merged["score"].fillna(0.0)
        merged["rank_in_pool"] = merged["rank_in_pool"].fillna(999)
        return merged

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

    def _update_position_hints(self, session: Session, positions_ranked: pd.DataFrame, market_regime: str) -> None:
        if positions_ranked.empty:
            return
        lookup = positions_ranked.set_index("symbol").to_dict(orient="index")
        positions = list(session.scalars(select(Position)))
        for position in positions:
            row = lookup.get(position.symbol, {})
            position.last_action_suggestion = self.risk_service.position_action_hint(row, market_regime)
        session.commit()

    def _build_advice_items(
        self,
        plan_items: list[dict[str, Any]],
        scored_df: pd.DataFrame,
        full_df: pd.DataFrame,
        default_action: str,
    ) -> list[dict[str, Any]]:
        if not plan_items:
            return []
        top_score = float(scored_df.iloc[0]["total_score"]) if not scored_df.empty else 0.0
        items = []
        for item in plan_items:
            row = full_df[full_df["symbol"] == item["symbol"]]
            if row.empty:
                continue
            enriched = row.iloc[0].to_dict()
            stop_loss = self.risk_service.get_stop_loss_pct(enriched["category"])
            take_profit = self.risk_service.get_take_profit_pct(enriched["category"])
            current_price = float(enriched["close_price"])
            score_value = float(item.get("total_score", item.get("score", 0.0)))
            rank_value = int(item.get("rank_in_pool", item.get("rank", 1)))
            items.append(
                {
                    "symbol": item["symbol"],
                    "name": enriched["name"],
                    "rank": rank_value,
                    "action": item.get("action", default_action),
                    "suggested_amount": float(item["suggested_amount"]),
                    "suggested_pct": float(item["suggested_pct"]),
                    "trigger_price_low": round(current_price * 0.995, 3),
                    "trigger_price_high": round(current_price * 1.01, 3),
                    "stop_loss_pct": stop_loss,
                    "take_profit_pct": take_profit,
                    "score": score_value,
                    "score_gap": round(top_score - score_value, 2),
                    "reason_short": self._reason_short(enriched, rank_value, len(scored_df)),
                    "risk_level": enriched["risk_level"],
                    "latest_price": float(item.get("latest_price", current_price)),
                    "lot_size": float(item.get("lot_size", self.allocation_service.settings.default_lot_size)),
                    "min_advice_amount": float(item.get("min_advice_amount", 0.0)),
                    "min_order_amount": float(item.get("min_order_amount", 0.0)),
                    "available_cash": float(item.get("available_cash", 0.0)),
                    "budget_gap_to_min_order": float(item.get("budget_gap_to_min_order", 0.0)),
                    "is_executable": bool(item.get("is_executable", True)),
                    "execution_status": str(item.get("execution_status", "可执行买入")),
                    "recommendation_bucket": str(item.get("recommendation_bucket", "executable_recommendations")),
                    "not_executable_reason": str(item.get("not_executable_reason", "")),
                    "execution_note": str(item.get("execution_note", "")),
                    "small_account_override": bool(item.get("small_account_override", False)),
                }
            )
        return items

    def _reason_short(self, row: dict[str, Any], rank: int, total_count: int) -> str:
        return (
            f"近 5 日动量 {row['momentum_5d']:.2f}%，候选池排名 {rank}/{max(total_count, 1)}，"
            f"综合得分 {row.get('total_score', 0.0):.1f}。"
        )
