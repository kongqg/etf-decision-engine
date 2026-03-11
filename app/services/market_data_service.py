from __future__ import annotations

import json
from collections import Counter
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from app.core.config import get_settings, load_yaml_config
from app.db.models import ETFFeature, MarketSnapshot
from app.repositories.market_repo import add_market_snapshot, list_universe, replace_features_for_date
from app.services.decision_policy_service import get_decision_policy_service
from app.services.data_quality_service import DataQualityService
from app.services.feature_engine import FeatureEngine
from app.services.market_regime_service import MarketRegimeService
from app.utils.dates import detect_session_mode, get_now, latest_market_date


CATEGORY_DRIFT = {
    "宽基": 0.0007,
    "行业": 0.0009,
    "黄金": 0.0003,
    "债券": 0.0002,
    "跨境": 0.0008,
}

CATEGORY_VOL = {
    "宽基": 0.014,
    "行业": 0.020,
    "黄金": 0.010,
    "债券": 0.004,
    "跨境": 0.018,
}

SOURCE_META = {
    "akshare": {
        "label": "AKShare 公开行情",
        "api": "akshare.fund_etf_hist_em",
        "note": "使用公开 ETF 日线历史行情数据计算指标。",
        "data_type": "日线历史",
        "is_realtime": False,
    },
    "mixed": {
        "label": "AKShare + 本地补全",
        "api": "akshare.fund_etf_hist_em + local_fallback_generator",
        "note": "大部分标的来自 AKShare，个别抓取失败时会回退到本地模拟数据补全。",
        "data_type": "混合",
        "is_realtime": False,
    },
    "fallback": {
        "label": "内置模拟数据",
        "api": "local_fallback_generator",
        "note": "当前没有取到真实行情，系统改用本地模拟数据演示逻辑，不能视为最新市场数据。",
        "data_type": "模拟数据",
        "is_realtime": False,
    },
}


class MarketDataService:
    def __init__(self) -> None:
        settings = get_settings()
        self.settings = settings
        self.feature_engine = FeatureEngine()
        self.market_regime_service = MarketRegimeService()
        self.data_quality_service = DataQualityService()
        self.policy = get_decision_policy_service()
        self.risk_rules = load_yaml_config(settings.config_dir / "risk_rules.yaml")
        self.source_loader_map = {
            "akshare": self._load_history_from_akshare,
            "fallback": self._load_history_from_fallback,
        }

    def refresh_data(self, session: Session, now=None) -> dict[str, Any]:
        current_time = now or get_now()
        trade_date = latest_market_date(current_time)
        session_mode = detect_session_mode(current_time)
        universe = list_universe(session)

        rows = []
        source_counter: Counter[str] = Counter()
        quality_checks: list[dict[str, Any]] = []
        series_samples: dict[str, Any] = {}

        for etf in universe:
            bundle = self._load_history_bundle(
                symbol=etf.symbol,
                name=etf.name,
                category=etf.category,
                min_avg_amount=etf.min_avg_amount,
                trade_date=trade_date,
            )
            history = bundle["history"]
            row_source = bundle["source"]
            source_counter[row_source] += 1

            features = self.feature_engine.calculate(history)
            anomaly_flag = (
                abs(features["pct_change"]) >= float(self.risk_rules["anomaly_pct_change_threshold"])
                or features["volatility_10d"] >= float(self.risk_rules["anomaly_volatility_threshold"])
            )

            rows.append(
                {
                    "trade_date": trade_date,
                    "captured_at": current_time,
                    "symbol": etf.symbol,
                    "name": etf.name,
                    "category": etf.category,
                    "asset_class": etf.asset_class,
                    "market": etf.market,
                    "benchmark": etf.benchmark,
                    "risk_level": etf.risk_level,
                    "min_avg_amount": etf.min_avg_amount,
                    "settlement_note": etf.settlement_note,
                    "trade_mode": etf.trade_mode,
                    "latest_row_date": bundle["latest_row_date"],
                    "source_code": row_source,
                    "stale_data_flag": bool(bundle["quality_report"]["stale_data_flag"]),
                    "quality_status": str(bundle["quality_report"]["status"]),
                    "formal_eligible": bool(bundle["quality_report"]["formal_eligible"]),
                    "source_request_json": json.dumps(bundle["request_params"], ensure_ascii=False),
                    "anomaly_flag": anomaly_flag,
                    **features,
                }
            )

            quality_checks.append(bundle["quality_report"])
            series_samples[etf.symbol] = {
                "symbol": etf.symbol,
                "name": etf.name,
                "category": etf.category,
                "source": row_source,
                "latest_row_date": bundle["quality_report"]["latest_row_date"],
                "requested_trade_date": trade_date.isoformat(),
                "stale_data_flag": bool(bundle["quality_report"]["stale_data_flag"]),
                "formal_eligible": bool(bundle["quality_report"]["formal_eligible"]),
                "quality_status": str(bundle["quality_report"]["status"]),
                "request_params": bundle["request_params"],
                "rows": self._serialize_history(history.tail(11)),
            }

        features_df = pd.DataFrame(rows)
        features_df = self._apply_decision_metadata(features_df)
        formal_market_df = features_df[features_df["formal_eligible"]].copy()
        snapshot_payload = self.market_regime_service.evaluate(formal_market_df if not formal_market_df.empty else features_df)

        data_source = self._resolve_data_source(source_counter)
        source_meta = SOURCE_META[data_source]
        quality_summary = self.data_quality_service.build_summary(
            quality_reports=quality_checks,
            current_time=current_time,
            expected_trade_date=trade_date,
            session_mode=session_mode,
        )
        quality_summary["data_type"] = source_meta["data_type"]
        quality_summary["source_counts"] = dict(source_counter)
        quality_summary["regime_input_symbol_count"] = int(len(formal_market_df) if not formal_market_df.empty else len(features_df))
        quality_summary["regime_input_scope"] = (
            "fresh_real_only" if not formal_market_df.empty else "all_rows_demo_scope"
        )

        feature_rows = [
            ETFFeature(
                trade_date=trade_date,
                captured_at=current_time,
                symbol=str(row["symbol"]),
                latest_row_date=row["latest_row_date"],
                source_code=str(row["source_code"]),
                stale_data_flag=bool(row["stale_data_flag"]),
                quality_status=str(row["quality_status"]),
                formal_eligible=bool(row["formal_eligible"]),
                source_request_json=str(row["source_request_json"]),
                close_price=float(row["close_price"]),
                pct_change=float(row["pct_change"]),
                latest_amount=float(row["latest_amount"]),
                avg_amount_20d=float(row["avg_amount_20d"]),
                momentum_3d=float(row["momentum_3d"]),
                momentum_5d=float(row["momentum_5d"]),
                momentum_10d=float(row["momentum_10d"]),
                momentum_20d=float(row["momentum_20d"]),
                ma5=float(row["ma5"]),
                ma10=float(row["ma10"]),
                ma20=float(row["ma20"]),
                ma_gap_5=float(row["ma_gap_5"]),
                ma_gap_10=float(row["ma_gap_10"]),
                trend_strength=float(row["trend_strength"]),
                ret_1d=float(row["ret_1d"]),
                volatility_5d=float(row["volatility_5d"]),
                volatility_10d=float(row["volatility_10d"]),
                volatility_20d=float(row["volatility_20d"]),
                rolling_max_20d=float(row["rolling_max_20d"]),
                drawdown_20d=float(row["drawdown_20d"]),
                liquidity_score=float(row["liquidity_score"]),
                avg_turnover_20d=float(row["avg_turnover_20d"]),
                category_return_10d=float(row["category_return_10d"]),
                relative_strength_10d=float(row["relative_strength_10d"]),
                above_ma20_flag=bool(row["above_ma20_flag"]),
                decision_category=str(row["decision_category"]),
                tradability_mode=str(row["tradability_mode"]),
                anomaly_flag=bool(row["anomaly_flag"]),
                filter_pass=False,
                total_score=0.0,
                rank_in_pool=None,
                breakdown_json=json.dumps({}, ensure_ascii=False),
            )
            for _, row in features_df.iterrows()
        ]

        replace_features_for_date(session, trade_date, feature_rows)
        add_market_snapshot(
            session,
            MarketSnapshot(
                trade_date=trade_date,
                captured_at=current_time,
                data_source=data_source,
                quality_status=str(quality_summary["quality_status"]),
                formal_decision_ready=bool(quality_summary["formal_decision_ready"]),
                latest_available_date=(
                    date.fromisoformat(quality_summary["latest_available_date"])
                    if quality_summary.get("latest_available_date") not in {None, "", "-"}
                    else None
                ),
                session_mode=session_mode,
                market_regime=snapshot_payload["market_regime"],
                broad_index_score=float(snapshot_payload["broad_index_score"]),
                risk_appetite_score=float(snapshot_payload["risk_appetite_score"]),
                trend_score=float(snapshot_payload["trend_score"]),
                recommended_position_pct=float(snapshot_payload["recommended_position_pct"]),
                raw_json=json.dumps(
                    {
                        "evidence": snapshot_payload["evidence"],
                        "formulas": snapshot_payload["formulas"],
                        "source": {
                            "code": data_source,
                            "label": source_meta["label"],
                            "api": source_meta["api"],
                            "note": source_meta["note"],
                            "data_type": source_meta["data_type"],
                            "is_realtime": source_meta["is_realtime"],
                            "supports_live_execution": quality_summary["supports_live_execution"],
                            "trade_date": trade_date.isoformat(),
                            "captured_at": current_time.isoformat(),
                        },
                        "request_summary": {
                            "provider": source_meta["label"],
                            "api": source_meta["api"],
                            "requested_trade_date": trade_date.isoformat(),
                            "captured_at": current_time.isoformat(),
                            "symbols_count": len(universe),
                            "source_counts": dict(source_counter),
                            "default_params": {
                                "period": "daily",
                                "adjust": "qfq",
                                "start_date": (
                                    trade_date - timedelta(days=self.settings.min_refresh_history_days * 3)
                                ).strftime("%Y%m%d"),
                                "end_date": trade_date.strftime("%Y%m%d"),
                            },
                        },
                        "quality_summary": quality_summary,
                        "quality_checks": quality_checks,
                        "series_samples": series_samples,
                    },
                    ensure_ascii=False,
                ),
            ),
        )
        session.commit()

        return {
            "trade_date": trade_date.isoformat(),
            "captured_at": current_time.isoformat(),
            "session_mode": session_mode,
            "data_source": data_source,
            "count": len(feature_rows),
            "market_regime": snapshot_payload["market_regime"],
            "quality_status": quality_summary["verification_status"],
            "formal_decision_ready": bool(quality_summary["formal_decision_ready"]),
        }

    def _apply_decision_metadata(self, features_df: pd.DataFrame) -> pd.DataFrame:
        if features_df.empty:
            return features_df
        df = features_df.copy()
        decision_meta = df.apply(
            lambda row: self.policy.classify(
                symbol=str(row["symbol"]),
                universe_category=str(row["category"]),
                asset_class=str(row.get("asset_class", row["category"])),
                trade_mode=str(row.get("trade_mode", "")),
            ),
            axis=1,
            result_type="expand",
        )
        df["decision_category"] = decision_meta["category"]
        df["tradability_mode"] = decision_meta["tradability_mode"]
        category_return = df.groupby("decision_category")["momentum_10d"].transform("mean")
        df["category_return_10d"] = category_return.fillna(0.0)
        df["relative_strength_10d"] = df["momentum_10d"] - df["category_return_10d"]
        df["avg_turnover_20d"] = df["avg_turnover_20d"].fillna(df["avg_amount_20d"])
        df["above_ma20_flag"] = df["above_ma20_flag"].fillna(False).astype(bool)
        return df

    def _load_history_bundle(
        self,
        symbol: str,
        name: str,
        category: str,
        min_avg_amount: float,
        trade_date: date,
    ) -> dict[str, Any]:
        request_params = {
            "symbol": symbol,
            "period": "daily",
            "adjust": "qfq",
            "start_date": (trade_date - timedelta(days=self.settings.min_refresh_history_days * 3)).strftime("%Y%m%d"),
            "end_date": trade_date.strftime("%Y%m%d"),
        }
        akshare_history = self.source_loader_map["akshare"](symbol=symbol, trade_date=trade_date)
        if akshare_history is not None and not akshare_history.empty:
            quality_report = self.data_quality_service.assess_history(
                symbol=symbol,
                name=name,
                source="akshare",
                history=akshare_history,
                requested_trade_date=trade_date,
                min_avg_amount=min_avg_amount,
                anomaly_pct_change_threshold=float(self.risk_rules["anomaly_pct_change_threshold"]),
            )
            if len(quality_report.clean_history) >= 2:
                return {
                    "history": quality_report.clean_history,
                    "source": "akshare",
                    "latest_row_date": quality_report.latest_row_date,
                    "request_params": request_params,
                    "quality_report": quality_report.payload,
                }

        fallback_history = self.source_loader_map["fallback"](
            symbol=symbol,
            category=category,
            min_avg_amount=min_avg_amount,
            trade_date=trade_date,
        )
        fallback_report = self.data_quality_service.assess_history(
            symbol=symbol,
            name=name,
            source="fallback",
            history=fallback_history,
            requested_trade_date=trade_date,
            min_avg_amount=min_avg_amount,
            anomaly_pct_change_threshold=float(self.risk_rules["anomaly_pct_change_threshold"]),
        )
        return {
            "history": fallback_report.clean_history,
            "source": "fallback",
            "latest_row_date": fallback_report.latest_row_date,
            "request_params": {
                "symbol": symbol,
                "method": "local_fallback_generator",
                "periods": self.settings.min_refresh_history_days,
                "end_date": trade_date.isoformat(),
            },
            "quality_report": fallback_report.payload,
        }

    def _load_history_from_akshare(self, symbol: str, trade_date: date) -> pd.DataFrame | None:
        try:
            import akshare as ak
        except Exception:
            return None

        start_date = (trade_date - timedelta(days=self.settings.min_refresh_history_days * 3)).strftime("%Y%m%d")
        end_date = trade_date.strftime("%Y%m%d")
        try:
            raw = ak.fund_etf_hist_em(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            )
        except Exception:
            return None

        if raw is None or raw.empty:
            return None

        column_map = {
            "日期": "date",
            "收盘": "close",
            "成交额": "amount",
        }
        if any(key not in raw.columns for key in column_map):
            return None

        frame = raw.rename(columns=column_map)[["date", "close", "amount"]].copy()
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
        frame["amount"] = pd.to_numeric(frame["amount"], errors="coerce")
        frame = frame.sort_values("date").tail(self.settings.min_refresh_history_days)
        return frame if not frame.empty else None

    def _load_history_from_fallback(self, symbol: str, category: str, min_avg_amount: float, trade_date: date) -> pd.DataFrame:
        seed = int(symbol)
        rng = np.random.default_rng(seed)
        dates = pd.bdate_range(end=trade_date, periods=self.settings.min_refresh_history_days)
        base_price = 0.9 + (seed % 37) / 10
        phase = (trade_date.toordinal() + seed) / 13
        dynamic_drift = CATEGORY_DRIFT.get(category, 0.0005) + np.sin(phase) * 0.0015
        volatility = CATEGORY_VOL.get(category, 0.015)
        returns = rng.normal(loc=dynamic_drift, scale=volatility, size=len(dates))
        prices = base_price * np.cumprod(1 + returns)
        amount_floor = max(min_avg_amount * 0.85, 5_000_000)
        amounts = rng.uniform(amount_floor, amount_floor * 2.2, size=len(dates))
        return pd.DataFrame({"date": dates, "close": prices, "amount": amounts})

    def _resolve_data_source(self, source_counts: Counter[str]) -> str:
        if source_counts.get("akshare") and source_counts.get("fallback"):
            return "mixed"
        if source_counts.get("akshare"):
            return "akshare"
        return "fallback"

    def _serialize_history(self, history: pd.DataFrame) -> list[dict[str, Any]]:
        rows = []
        for _, row in history.iterrows():
            rows.append(
                {
                    "date": pd.Timestamp(row["date"]).date().isoformat(),
                    "close": round(float(row["close"]), 4),
                    "amount": round(float(row["amount"]), 2),
                }
            )
        return rows
