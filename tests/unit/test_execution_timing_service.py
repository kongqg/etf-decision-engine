from datetime import datetime

from app.services.execution_timing_service import ExecutionTimingService


def build_item(asset_class: str, trade_mode: str) -> dict:
    return {
        "symbol": "TEST",
        "name": "测试ETF",
        "asset_class": asset_class,
        "trade_mode": trade_mode,
    }


def test_stock_execution_timing_has_windows_and_avoids_open_chasing():
    item = ExecutionTimingService().annotate_item(
        build_item(asset_class="股票", trade_mode="T+1"),
        session_mode="preopen",
        now=datetime(2026, 3, 10, 9, 20, 0),
    )

    assert item is not None
    assert item["execution_timing_mode"] == "stock_windowed"
    assert item["timing_rule_applied"] is True
    assert item["recommended_execution_windows"] == ["09:35-10:30", "13:30-14:30"]
    assert item["avoid_execution_windows"] == ["09:15-09:30"]
    assert "机械追价" in item["timing_note"]
    assert "09:35-10:30" in item["timing_note"]


def test_stock_execution_timing_is_more_specific_during_intraday_window():
    item = ExecutionTimingService().annotate_item(
        build_item(asset_class="股票", trade_mode="T+1"),
        session_mode="intraday",
        now=datetime(2026, 3, 10, 10, 0, 0),
    )

    assert item is not None
    assert item["timing_rule_applied"] is True
    assert "上午窗口" in item["timing_note"]
    assert "09:35-10:30" in item["timing_note"]


def test_bond_execution_timing_does_not_force_stock_windows():
    item = ExecutionTimingService().annotate_item(
        build_item(asset_class="债券", trade_mode="T+0"),
        session_mode="intraday",
        now=datetime(2026, 3, 10, 10, 0, 0),
    )

    assert item is not None
    assert item["execution_timing_mode"] == "allocation_first"
    assert item["timing_rule_applied"] is False
    assert item["recommended_execution_windows"] == []
    assert item["avoid_execution_windows"] == []
    assert "不需要像股票那样" in item["timing_note"]
    assert "1 手" in item["timing_note"]


def test_money_execution_timing_focuses_on_cash_management():
    item = ExecutionTimingService().annotate_item(
        build_item(asset_class="货币", trade_mode="T+0"),
        session_mode="intraday",
        now=datetime(2026, 3, 10, 10, 0, 0),
    )

    assert item is not None
    assert item["execution_timing_mode"] == "cash_management"
    assert item["timing_rule_applied"] is False
    assert item["recommended_execution_windows"] == []
    assert "现金管理" in item["timing_note"]
