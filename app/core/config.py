from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import tomli
import yaml


BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_DIR = BASE_DIR / "config"


@dataclass
class Settings:
    app_name: str
    timezone: str
    database_url: str
    default_user_id: int
    data_cache_days: int
    min_refresh_history_days: int
    data_stale_minutes: int
    top_n_default: int
    min_score_to_buy: float
    min_score_gap_for_single: float
    initial_build_ratio: float
    default_min_advice_amount: float
    budget_filter_enabled: bool
    default_lot_size: float
    show_watchlist_recommendations: bool
    fee_filter_enabled: bool
    default_fee_rate: float
    default_min_fee: float
    max_fee_rate_for_execution: float
    show_cost_inefficient_recommendations: bool
    budget_substitute_enabled: bool
    budget_substitute_top_n: int
    timing_optimization_enabled: bool
    show_timing_suggestions: bool
    currency_symbol: str
    show_debug_badges: bool
    base_dir: Path = BASE_DIR
    config_dir: Path = CONFIG_DIR


def _load_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as file_obj:
        return tomli.load(file_obj)


def load_yaml_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file_obj:
        return yaml.safe_load(file_obj) or {}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    raw = _load_toml(CONFIG_DIR / "settings.toml")
    decision = raw["decision"]
    execution = raw.get("execution", {})

    database_url = os.getenv("ETF_ASSISTANT_DATABASE_URL", raw["app"]["database_url"])
    return Settings(
        app_name=raw["app"]["name"],
        timezone=raw["app"]["timezone"],
        database_url=database_url,
        default_user_id=int(raw["app"]["default_user_id"]),
        data_cache_days=int(raw["app"]["data_cache_days"]),
        min_refresh_history_days=int(raw["app"]["min_refresh_history_days"]),
        data_stale_minutes=int(raw["app"]["data_stale_minutes"]),
        top_n_default=int(decision["top_n_default"]),
        min_score_to_buy=float(decision["min_score_to_buy"]),
        min_score_gap_for_single=float(decision["min_score_gap_for_single"]),
        initial_build_ratio=float(decision["initial_build_ratio"]),
        default_min_advice_amount=float(decision.get("default_min_advice_amount", decision.get("default_min_trade_amount", 1000.0))),
        budget_filter_enabled=bool(decision.get("budget_filter_enabled", True)),
        default_lot_size=float(decision.get("default_lot_size", 100.0)),
        show_watchlist_recommendations=bool(decision.get("show_watchlist_recommendations", True)),
        fee_filter_enabled=bool(decision.get("fee_filter_enabled", True)),
        default_fee_rate=float(decision.get("default_fee_rate", 0.0003)),
        default_min_fee=float(decision.get("default_min_fee", 1.0)),
        max_fee_rate_for_execution=float(decision.get("max_fee_rate_for_execution", 0.015)),
        show_cost_inefficient_recommendations=bool(decision.get("show_cost_inefficient_recommendations", True)),
        budget_substitute_enabled=bool(decision.get("budget_substitute_enabled", True)),
        budget_substitute_top_n=max(1, int(decision.get("budget_substitute_top_n", 1))),
        timing_optimization_enabled=bool(execution.get("timing_optimization_enabled", True)),
        show_timing_suggestions=bool(execution.get("show_timing_suggestions", True)),
        currency_symbol=raw["ui"]["currency_symbol"],
        show_debug_badges=bool(raw["ui"]["show_debug_badges"]),
    )
