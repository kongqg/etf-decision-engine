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
        self.policy = get_decision_policy_service()
        self.risk_rules = load_yaml_config(settings.config_dir / "risk_rules.yaml")

    def refresh_data(self, session: Session, now=None) -> dict[str, Any]:
        current_time = now or get_now()
        trade_date = latest_market_date(current_time)
        session_mode = detect_session_mode(current_time)
        universe = list_universe(session)

        rows = []
        source_counter: Counter[str] = Counter()
        quality_checks = []
        series_samples: dict[str, Any] = {}
        latest_dates: list[date] = []

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
            latest_dates.append(bundle["latest_row_date"])

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
                "latest_row_date": bundle["latest_row_date"].isoformat(),
                "request_params": bundle["request_params"],
                "rows": self._serialize_history(history.tail(11)),
            }

        features_df = pd.DataFrame(rows)
        features_df = self._apply_decision_metadata(features_df)
        snapshot_payload = self.market_regime_service.evaluate(features_df)

        data_source = self._resolve_data_source(source_counter)
        source_meta = SOURCE_META[data_source]
        quality_summary = self._build_quality_summary(
            data_source=data_source,
            source_counts=source_counter,
            latest_dates=latest_dates,
            quality_checks=quality_checks,
            current_time=current_time,
            expected_trade_date=trade_date,
            session_mode=session_mode,
        )

        feature_rows = [
            ETFFeature(
                trade_date=trade_date,
                captured_at=current_time,
                symbol=str(row["symbol"]),
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
        history = self._load_history_from_akshare(symbol, trade_date)
        if history is not None and len(history) >= 15:
            request_params = {
                "symbol": symbol,
                "period": "daily",
                "adjust": "qfq",
                "start_date": (trade_date - timedelta(days=self.settings.min_refresh_history_days * 3)).strftime("%Y%m%d"),
                "end_date": trade_date.strftime("%Y%m%d"),
            }
            quality_report = self._build_quality_report(
                symbol=symbol,
                name=name,
                source="akshare",
                history=history,
                requested_trade_date=trade_date,
            )
            return {
                "history": history,
                "source": "akshare",
                "latest_row_date": quality_report["latest_row_date_obj"],
                "request_params": request_params,
                "quality_report": quality_report["payload"],
            }

        history = self._load_history_from_fallback(symbol, category, min_avg_amount, trade_date)
        quality_report = self._build_quality_report(
            symbol=symbol,
            name=name,
            source="fallback",
            history=history,
            requested_trade_date=trade_date,
        )
        return {
            "history": history,
            "source": "fallback",
            "latest_row_date": quality_report["latest_row_date_obj"],
            "request_params": {
                "symbol": symbol,
                "method": "local_fallback_generator",
                "periods": self.settings.min_refresh_history_days,
                "end_date": trade_date.isoformat(),
            },
            "quality_report": quality_report["payload"],
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
        frame["date"] = pd.to_datetime(frame["date"])
        frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
        frame["amount"] = pd.to_numeric(frame["amount"], errors="coerce")
        frame = frame.dropna().sort_values("date").tail(self.settings.min_refresh_history_days)
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

    def _build_quality_report(
        self,
        symbol: str,
        name: str,
        source: str,
        history: pd.DataFrame,
        requested_trade_date: date,
    ) -> dict[str, Any]:
        frame = history.copy().sort_values("date").reset_index(drop=True)
        latest_row_date = pd.Timestamp(frame["date"].iloc[-1]).date()
        abnormal_threshold = float(self.risk_rules["anomaly_pct_change_threshold"])
        daily_returns = frame["close"].pct_change().abs() * 100
        abnormal_days = int((daily_returns > abnormal_threshold).sum())

        checks = [
            {
                "label": "样本长度",
                "passed": len(frame) >= self.settings.min_refresh_history_days,
                "detail": f"共 {len(frame)} 行，目标至少 {self.settings.min_refresh_history_days} 行。",
            },
            {
                "label": "日期递增",
                "passed": bool(frame["date"].is_monotonic_increasing),
                "detail": "日期按时间顺序排列。",
            },
            {
                "label": "缺失值",
                "passed": bool(not frame[["date", "close", "amount"]].isna().any().any()),
                "detail": "价格和成交额没有空值。",
            },
            {
                "label": "成交额为正",
                "passed": bool((frame["amount"] > 0).all()),
                "detail": "所有样本点成交额都大于 0。",
            },
            {
                "label": "重复日期",
                "passed": int(frame["date"].duplicated().sum()) == 0,
                "detail": f"重复日期数量 {int(frame['date'].duplicated().sum())}。",
            },
            {
                "label": "异常涨跌幅",
                "passed": abnormal_days == 0,
                "detail": f"单日绝对涨跌幅超过 {abnormal_threshold:.1f}% 的天数为 {abnormal_days}。",
            },
            {
                "label": "最新日期有效",
                "passed": latest_row_date <= requested_trade_date,
                "detail": f"最新数据日期 {latest_row_date.isoformat()}，请求交易日 {requested_trade_date.isoformat()}。",
            },
        ]

        failed_count = sum(1 for item in checks if not item["passed"])
        if failed_count == 0:
            status = "pass"
        elif failed_count <= 2:
            status = "partial"
        else:
            status = "fail"

        return {
            "latest_row_date_obj": latest_row_date,
            "payload": {
                "symbol": symbol,
                "name": name,
                "source": source,
                "status": status,
                "latest_row_date": latest_row_date.isoformat(),
                "row_count": int(len(frame)),
                "failed_checks": failed_count,
                "checks": checks,
            },
        }

    def _build_quality_summary(
        self,
        data_source: str,
        source_counts: Counter[str],
        latest_dates: list[date],
        quality_checks: list[dict[str, Any]],
        current_time,
        expected_trade_date: date,
        session_mode: str,
    ) -> dict[str, Any]:
        latest_available_date = min(latest_dates).isoformat() if latest_dates else "-"
        failed_symbols = [item["symbol"] for item in quality_checks if item["status"] != "pass"]
        failed_checks = sum(int(item["failed_checks"]) for item in quality_checks)
        total_checks = sum(len(item["checks"]) for item in quality_checks)
        passed_checks = total_checks - failed_checks

        if data_source == "fallback":
            verification_status = "模拟数据"
            reliability_level = "低"
            freshness_label = "当前使用模拟数据，不代表真实最新行情。"
            supports_live_execution = False
        elif data_source == "mixed":
            verification_status = "部分真实、部分回退"
            reliability_level = "中"
            freshness_label = self._freshness_label(latest_available_date, expected_trade_date, session_mode)
            supports_live_execution = False
        elif failed_symbols:
            verification_status = "单源质检存在异常"
            reliability_level = "中"
            freshness_label = self._freshness_label(latest_available_date, expected_trade_date, session_mode)
            supports_live_execution = False
        else:
            verification_status = "单源质检通过"
            reliability_level = "中高"
            freshness_label = self._freshness_label(latest_available_date, expected_trade_date, session_mode)
            supports_live_execution = False

        return {
            "verification_status": verification_status,
            "reliability_level": reliability_level,
            "cross_source_status": "未启用第二数据源交叉验证",
            "supports_live_execution": supports_live_execution,
            "live_execution_note": (
                "当前只使用日线历史数据或模拟数据，适合做开盘计划、收盘后复盘和下一交易日预案，不适合伪装成盘中实时建议。"
            ),
            "data_type": SOURCE_META[data_source]["data_type"],
            "freshness_label": freshness_label,
            "latest_available_date": latest_available_date,
            "captured_at": current_time.isoformat(),
            "source_counts": dict(source_counts),
            "passed_checks": passed_checks,
            "failed_checks": failed_checks,
            "failed_symbols": failed_symbols,
        }

    def _freshness_label(self, latest_available_date: str, expected_trade_date: date, session_mode: str) -> str:
        if latest_available_date == "-":
            return "没有可用数据。"
        if latest_available_date < expected_trade_date.isoformat():
            return f"当前数据只更新到 {latest_available_date} 收盘。"
        if session_mode == "after_close":
            return f"当前数据可视为 {latest_available_date} 收盘后的最新日线结果。"
        return f"当前拿到的是截至 {latest_available_date} 的日线数据，不是盘中逐笔实时行情。"

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
