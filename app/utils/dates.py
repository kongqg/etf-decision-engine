from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.core.config import get_settings, load_yaml_config


def get_now() -> datetime:
    settings = get_settings()
    try:
        tzinfo = ZoneInfo(settings.timezone)
    except ZoneInfoNotFoundError:
        return datetime.now()
    return datetime.now(tzinfo)


def get_market_holidays() -> set[date]:
    settings = get_settings()
    config = load_yaml_config(settings.config_dir / "market_calendar.yaml")
    return {date.fromisoformat(item) for item in config.get("holidays", [])}


def is_trading_day(target_date: date) -> bool:
    if target_date.weekday() >= 5:
        return False
    return target_date not in get_market_holidays()


def previous_trading_day(target_date: date) -> date:
    current = target_date
    while not is_trading_day(current):
        current -= timedelta(days=1)
    return current


def next_trading_day(target_date: date) -> date:
    current = target_date + timedelta(days=1)
    while not is_trading_day(current):
        current += timedelta(days=1)
    return current


def latest_market_date(now: datetime | None = None) -> date:
    current = now or get_now()
    if is_trading_day(current.date()):
        return current.date()
    return previous_trading_day(current.date())


def detect_session_mode(now: datetime | None = None) -> str:
    current = now or get_now()
    current_time = current.time()

    if not is_trading_day(current.date()):
        return "closed"
    if current_time < time(9, 15):
        return "preopen"
    if time(9, 15) <= current_time <= time(9, 25):
        return "preopen"
    if time(9, 30) <= current_time <= time(11, 30):
        return "intraday"
    if time(13, 0) <= current_time <= time(15, 0):
        return "intraday"
    if time(11, 30) < current_time < time(13, 0):
        return "preopen"
    return "after_close"
