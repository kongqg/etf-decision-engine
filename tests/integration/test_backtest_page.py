from __future__ import annotations

from datetime import date

from starlette.requests import Request

from app.services.backtest_service import BacktestRequest, BacktestService
from tests.unit.backtest_helpers import build_dataset, seed_user, setup_test_db


def test_backtest_page_renders_result(monkeypatch):
    session_local = setup_test_db(monkeypatch)

    from app.main import create_app
    from app.web import pages as pages_module
    from app.web.pages import backtest_page

    with session_local()() as session:
        seed_user(session)
        dataset = build_dataset(session, start_date=date(2026, 2, 3), end_date=date(2026, 3, 10))
        result = BacktestService().run(
            session,
            BacktestRequest(
                start_date=date(2026, 2, 3),
                end_date=date(2026, 3, 10),
                initial_capital=100000,
                threshold_overrides={
                    "fallback": {"offensive_threshold": 0.0},
                    "decision_thresholds": {
                        "open_threshold": 30.0,
                        "strong_entry_threshold": 30.0,
                        "strong_hold_threshold": 30.0,
                    },
                    "t0_controls": {"minimum_expected_edge_bps": 0.0},
                },
            ),
            dataset=dataset,
            persist_output=False,
        )

        monkeypatch.setattr(pages_module.backtest_service, "load_saved_run", lambda run_id: result)
        monkeypatch.setattr(
            pages_module.backtest_service,
            "list_saved_runs",
            lambda limit=12: [
                {
                    "run_id": "demo_run",
                    "run_type": "backtest",
                    "created_at": "2026-03-11T10:00:00",
                    "start_date": "2026-02-03",
                    "end_date": "2026-03-10",
                    "overview": result["overview"],
                }
            ],
        )

        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/backtest",
                "headers": [],
                "query_string": b"",
                "app": create_app(),
            }
        )
        response = backtest_page(request=request, run_id="demo_run", db=session)
        body = response.body.decode("utf-8")
        assert "历史回测" in body
        assert "给小白看的结果" in body
        assert "专业指标" in body
        assert result["overview"]["one_line_conclusion"] in body
