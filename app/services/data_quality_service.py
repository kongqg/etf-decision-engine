from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from app.core.config import get_settings, load_yaml_config


@dataclass
class SymbolQualityAssessment:
    clean_history: pd.DataFrame
    latest_row_date: date | None
    payload: dict[str, Any]


class DataQualityService:
    def __init__(self) -> None:
        settings = get_settings()
        self.settings = settings
        self.rules = load_yaml_config(settings.config_dir / "data_quality.yaml")
        self.minimum_history_rows = max(
            2,
            int(self.rules.get("minimum_history_rows_for_features", 21)),
        )
        self.minimum_formal_coverage_ratio = float(self.rules.get("minimum_formal_coverage_ratio", 0.85))
        self.maximum_fallback_ratio = float(self.rules.get("maximum_fallback_ratio_for_formal_decision", 0.15))
        self.maximum_stale_ratio = float(self.rules.get("maximum_stale_ratio_for_formal_decision", 0.20))
        self.latest_amount_ratio = float(self.rules.get("latest_amount_to_min_avg_amount_ratio", 0.05))
        self.minimum_latest_amount_floor = float(self.rules.get("minimum_latest_amount_floor", 1_000_000))
        self.critical_symbols = [str(symbol) for symbol in self.rules.get("critical_symbols", [])]

    def assess_history(
        self,
        *,
        symbol: str,
        name: str,
        source: str,
        history: pd.DataFrame,
        requested_trade_date: date,
        min_avg_amount: float,
        anomaly_pct_change_threshold: float,
    ) -> SymbolQualityAssessment:
        normalized = self._normalize_history(history)
        frame = normalized["clean_history"]
        latest_row_date = pd.Timestamp(frame["date"].iloc[-1]).date() if not frame.empty else None
        stale_days = (
            max((requested_trade_date - latest_row_date).days, 0)
            if latest_row_date is not None
            else None
        )
        stale_data_flag = latest_row_date is None or latest_row_date < requested_trade_date
        avg_amount_20d = float(frame["amount"].tail(20).mean()) if not frame.empty else 0.0
        latest_amount = float(frame["amount"].iloc[-1]) if not frame.empty else 0.0
        daily_returns = frame["close"].pct_change().abs() * 100 if not frame.empty else pd.Series(dtype=float)
        abnormal_jump_days = int((daily_returns > anomaly_pct_change_threshold).sum())
        low_liquidity_flag = bool(min_avg_amount > 0 and avg_amount_20d < float(min_avg_amount))
        suspicious_latest_amount_flag = bool(
            min_avg_amount > 0
            and latest_amount < max(float(min_avg_amount) * self.latest_amount_ratio, self.minimum_latest_amount_floor)
        )
        history_ready = len(frame) >= self.minimum_history_rows
        source_is_fallback = source in {"fallback", "mock", "simulated"}

        blocking_issues: list[str] = []
        warning_issues: list[str] = []

        if not history_ready:
            blocking_issues.append("history_too_short")
        if normalized["missing_core_field_rows"] > 0:
            blocking_issues.append("missing_core_fields")
        if normalized["non_positive_value_rows"] > 0:
            blocking_issues.append("invalid_non_positive_values")
        if stale_data_flag:
            warning_issues.append("stale_data")
        if normalized["duplicate_date_count"] > 0:
            warning_issues.append("duplicate_dates")
        if abnormal_jump_days > 0:
            warning_issues.append("abnormal_jumps")
        if low_liquidity_flag:
            warning_issues.append("low_liquidity")
        if suspicious_latest_amount_flag:
            warning_issues.append("suspicious_latest_amount")
        if source_is_fallback:
            warning_issues.append("fallback_source")

        checks = [
            {
                "code": "history_rows",
                "label": "样本长度",
                "passed": history_ready,
                "detail": f"有效样本 {len(frame)} 行，正式特征至少需要 {self.minimum_history_rows} 行。",
            },
            {
                "code": "core_fields",
                "label": "核心字段完整",
                "passed": normalized["missing_core_field_rows"] == 0,
                "detail": (
                    f"缺失 date/close/amount 的原始行数 {normalized['missing_core_field_rows']}。"
                ),
            },
            {
                "code": "positive_values",
                "label": "价格与成交额有效",
                "passed": normalized["non_positive_value_rows"] == 0,
                "detail": f"close<=0 或 amount<=0 的原始行数 {normalized['non_positive_value_rows']}。",
            },
            {
                "code": "duplicate_dates",
                "label": "无重复日期",
                "passed": normalized["duplicate_date_count"] == 0,
                "detail": f"重复日期数量 {normalized['duplicate_date_count']}。",
            },
            {
                "code": "latest_trade_date",
                "label": "真实最新日期",
                "passed": not stale_data_flag,
                "detail": (
                    "没有拿到有效行情日期。"
                    if latest_row_date is None
                    else f"真实最新日期 {latest_row_date.isoformat()}，请求交易日 {requested_trade_date.isoformat()}。"
                ),
            },
            {
                "code": "abnormal_jumps",
                "label": "异常跳变",
                "passed": abnormal_jump_days == 0,
                "detail": (
                    f"单日绝对涨跌幅超过 {anomaly_pct_change_threshold:.1f}% 的天数为 {abnormal_jump_days}。"
                ),
            },
            {
                "code": "low_liquidity",
                "label": "成交额有效性",
                "passed": not low_liquidity_flag and not suspicious_latest_amount_flag,
                "detail": (
                    f"20日均成交额 {avg_amount_20d:.0f}，最新成交额 {latest_amount:.0f}，"
                    f"标的最低成交额门槛 {float(min_avg_amount):.0f}。"
                ),
            },
            {
                "code": "consecutive_missing",
                "label": "连续缺失",
                "passed": normalized["max_consecutive_invalid_rows"] == 0,
                "detail": f"原始样本里最长连续无效行数 {normalized['max_consecutive_invalid_rows']}。",
            },
        ]

        if blocking_issues:
            status = "fail"
        elif warning_issues:
            status = "partial"
        else:
            status = "pass"

        formal_eligible = bool(
            not source_is_fallback
            and not stale_data_flag
            and history_ready
            and normalized["missing_core_field_rows"] == 0
            and normalized["non_positive_value_rows"] == 0
        )

        payload = {
            "symbol": symbol,
            "name": name,
            "source": source,
            "requested_trade_date": requested_trade_date.isoformat(),
            "latest_row_date": latest_row_date.isoformat() if latest_row_date is not None else "",
            "stale_data_flag": stale_data_flag,
            "stale_days": stale_days,
            "row_count": int(len(frame)),
            "history_ready": history_ready,
            "formal_eligible": formal_eligible,
            "status": status,
            "missing_date_count": normalized["missing_date_count"],
            "missing_close_count": normalized["missing_close_count"],
            "missing_amount_count": normalized["missing_amount_count"],
            "missing_core_field_rows": normalized["missing_core_field_rows"],
            "duplicate_date_count": normalized["duplicate_date_count"],
            "non_positive_value_rows": normalized["non_positive_value_rows"],
            "max_consecutive_invalid_rows": normalized["max_consecutive_invalid_rows"],
            "abnormal_jump_days": abnormal_jump_days,
            "avg_amount_20d": round(avg_amount_20d, 2),
            "latest_amount": round(latest_amount, 2),
            "low_liquidity_flag": low_liquidity_flag,
            "suspicious_latest_amount_flag": suspicious_latest_amount_flag,
            "blocking_issues": blocking_issues,
            "warning_issues": warning_issues,
            "checks": checks,
            "failed_checks": sum(1 for item in checks if not item["passed"]),
        }
        return SymbolQualityAssessment(clean_history=frame, latest_row_date=latest_row_date, payload=payload)

    def build_summary(
        self,
        *,
        quality_reports: list[dict[str, Any]],
        expected_trade_date: date,
        current_time,
        session_mode: str,
    ) -> dict[str, Any]:
        total_symbols = len(quality_reports)
        if total_symbols == 0:
            return {
                "quality_status": "blocked",
                "verification_status": "数据质量不足",
                "reliability_level": "低",
                "formal_decision_ready": False,
                "supports_formal_decision": False,
                "supports_live_execution": False,
                "live_execution_note": "当前没有可用数据，无法生成正式建议。",
                "cross_source_status": "未启用第二数据源交叉验证",
                "latest_available_date": "-",
                "latest_real_available_date": "-",
                "captured_at": current_time.isoformat(),
                "requested_trade_date": expected_trade_date.isoformat(),
                "coverage_ratio": 0.0,
                "fresh_coverage_ratio": 0.0,
                "fallback_ratio": 0.0,
                "stale_ratio": 0.0,
                "source_distribution": {"real": 0, "fallback": 0, "stale": 0},
                "core_symbols": self.critical_symbols,
                "missing_core_symbols": self.critical_symbols,
                "stale_core_symbols": [],
                "fallback_symbols": [],
                "stale_symbols": [],
                "warning_symbols": [],
                "failed_symbols": [],
                "blocking_reasons": ["没有任何 ETF 数据写入。"],
                "warning_reasons": [],
                "passed_checks": 0,
                "failed_checks": 0,
                "freshness_label": "没有可用数据。",
            }

        latest_dates = [
            date.fromisoformat(item["latest_row_date"])
            for item in quality_reports
            if item.get("latest_row_date")
        ]
        latest_real_dates = [
            date.fromisoformat(item["latest_row_date"])
            for item in quality_reports
            if item.get("latest_row_date") and item.get("source") not in {"fallback", "mock", "simulated"}
        ]

        fresh_real_symbols = [
            item["symbol"] for item in quality_reports if bool(item.get("formal_eligible"))
        ]
        fallback_symbols = [
            item["symbol"] for item in quality_reports if str(item.get("source")) in {"fallback", "mock", "simulated"}
        ]
        stale_symbols = [
            item["symbol"] for item in quality_reports if bool(item.get("stale_data_flag"))
        ]
        warning_symbols = [
            item["symbol"] for item in quality_reports if str(item.get("status")) == "partial"
        ]
        failed_symbols = [
            item["symbol"] for item in quality_reports if str(item.get("status")) == "fail"
        ]
        missing_core_symbols = [
            symbol for symbol in self.critical_symbols if symbol not in set(fresh_real_symbols)
        ]
        stale_core_symbols = [
            item["symbol"]
            for item in quality_reports
            if item["symbol"] in self.critical_symbols and bool(item.get("stale_data_flag"))
        ]

        coverage_ratio = round(len(fresh_real_symbols) / total_symbols, 4) if total_symbols else 0.0
        fallback_ratio = round(len(fallback_symbols) / total_symbols, 4) if total_symbols else 0.0
        stale_ratio = round(len(stale_symbols) / total_symbols, 4) if total_symbols else 0.0

        blocking_reasons: list[str] = []
        warning_reasons: list[str] = []

        if missing_core_symbols:
            blocking_reasons.append(f"核心 ETF 缺少新鲜真实数据：{', '.join(missing_core_symbols)}。")
        if coverage_ratio < self.minimum_formal_coverage_ratio:
            blocking_reasons.append(
                f"当前新鲜真实数据覆盖率只有 {coverage_ratio * 100:.1f}%，低于正式决策门槛 {self.minimum_formal_coverage_ratio * 100:.1f}%。"
            )
        if fallback_ratio > self.maximum_fallback_ratio:
            blocking_reasons.append(
                f"当前回退/模拟数据占比 {fallback_ratio * 100:.1f}%，高于允许上限 {self.maximum_fallback_ratio * 100:.1f}%。"
            )
        if stale_ratio > self.maximum_stale_ratio:
            blocking_reasons.append(
                f"当前滞后数据占比 {stale_ratio * 100:.1f}%，高于允许上限 {self.maximum_stale_ratio * 100:.1f}%。"
            )
        if failed_symbols and not blocking_reasons:
            warning_reasons.append(f"存在未通过完整质检的标的：{', '.join(failed_symbols)}。")
        if stale_symbols and stale_ratio <= self.maximum_stale_ratio:
            warning_reasons.append(f"存在滞后数据标的：{', '.join(stale_symbols)}。")
        if fallback_symbols and fallback_ratio <= self.maximum_fallback_ratio:
            warning_reasons.append(f"存在回退/模拟数据标的：{', '.join(fallback_symbols)}。")
        if warning_symbols:
            warning_reasons.append(f"存在需要谨慎解释的数据标的：{', '.join(warning_symbols)}。")

        if blocking_reasons:
            quality_status = "blocked"
            verification_status = "数据质量不足"
            reliability_level = "低"
            freshness_label = "当前数据质量不足，不生成正式建议。"
        elif warning_reasons:
            quality_status = "weak"
            verification_status = "部分滞后/异常"
            reliability_level = "中"
            freshness_label = (
                f"当前数据能覆盖大部分标的，但仍有滞后或弱质量情况；请求交易日 {expected_trade_date.isoformat()}。"
            )
        else:
            quality_status = "ok"
            verification_status = "数据质检通过"
            reliability_level = "中高"
            freshness_label = (
                f"当前数据覆盖完整，请求交易日 {expected_trade_date.isoformat()}，可进入正式决策。"
            )

        total_checks = sum(len(item.get("checks", [])) for item in quality_reports)
        failed_checks = sum(int(item.get("failed_checks", 0)) for item in quality_reports)
        passed_checks = total_checks - failed_checks

        return {
            "quality_status": quality_status,
            "verification_status": verification_status,
            "reliability_level": reliability_level,
            "formal_decision_ready": quality_status != "blocked",
            "supports_formal_decision": quality_status != "blocked",
            "supports_live_execution": False,
            "live_execution_note": (
                "当前仍以日线数据为主，即使通过正式决策门槛，也更适合做收盘后复盘和下一交易日预案，不是盘中实时信号。"
            ),
            "cross_source_status": "当前只启用单一主源，结构已预留第二数据源交叉验证接口。",
            "latest_available_date": min(latest_dates).isoformat() if latest_dates else "-",
            "latest_real_available_date": min(latest_real_dates).isoformat() if latest_real_dates else "-",
            "captured_at": current_time.isoformat(),
            "requested_trade_date": expected_trade_date.isoformat(),
            "session_mode": session_mode,
            "total_symbols": total_symbols,
            "fresh_symbol_count": len(fresh_real_symbols),
            "coverage_ratio": coverage_ratio,
            "fresh_coverage_ratio": coverage_ratio,
            "fallback_ratio": fallback_ratio,
            "stale_ratio": stale_ratio,
            "source_distribution": {
                "real": len(fresh_real_symbols),
                "fallback": len(fallback_symbols),
                "stale": len(stale_symbols),
            },
            "core_symbols": self.critical_symbols,
            "missing_core_symbols": missing_core_symbols,
            "stale_core_symbols": stale_core_symbols,
            "fallback_symbols": fallback_symbols,
            "stale_symbols": stale_symbols,
            "warning_symbols": warning_symbols,
            "failed_symbols": failed_symbols,
            "blocking_reasons": blocking_reasons,
            "warning_reasons": list(dict.fromkeys(warning_reasons)),
            "passed_checks": passed_checks,
            "failed_checks": failed_checks,
            "freshness_label": freshness_label,
        }

    def _normalize_history(self, history: pd.DataFrame) -> dict[str, Any]:
        frame = history.copy() if history is not None else pd.DataFrame(columns=["date", "close", "amount"])
        for column in ["date", "close", "amount"]:
            if column not in frame.columns:
                frame[column] = pd.NA
        frame = frame[["date", "close", "amount"]].copy()
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
        frame["amount"] = pd.to_numeric(frame["amount"], errors="coerce")
        frame = frame.sort_values("date", na_position="last").reset_index(drop=True)

        missing_date_count = int(frame["date"].isna().sum())
        missing_close_count = int(frame["close"].isna().sum())
        missing_amount_count = int(frame["amount"].isna().sum())
        missing_core_field_rows = int((frame[["date", "close", "amount"]].isna().any(axis=1)).sum())
        duplicate_date_count = int(frame["date"].duplicated().sum())

        invalid_value_mask = (frame["close"] <= 0) | (frame["amount"] <= 0)
        invalid_value_mask = invalid_value_mask.fillna(False)
        non_positive_value_rows = int(invalid_value_mask.sum())
        invalid_row_mask = frame[["date", "close", "amount"]].isna().any(axis=1) | invalid_value_mask

        clean_history = (
            frame.loc[~invalid_row_mask]
            .drop_duplicates(subset=["date"], keep="last")
            .sort_values("date")
            .tail(self.settings.min_refresh_history_days)
            .reset_index(drop=True)
        )

        return {
            "clean_history": clean_history,
            "missing_date_count": missing_date_count,
            "missing_close_count": missing_close_count,
            "missing_amount_count": missing_amount_count,
            "missing_core_field_rows": missing_core_field_rows,
            "duplicate_date_count": duplicate_date_count,
            "non_positive_value_rows": non_positive_value_rows,
            "max_consecutive_invalid_rows": self._max_consecutive_true(invalid_row_mask),
        }

    def _max_consecutive_true(self, mask: pd.Series) -> int:
        longest = 0
        current = 0
        for value in mask.tolist():
            if bool(value):
                current += 1
                longest = max(longest, current)
            else:
                current = 0
        return longest
