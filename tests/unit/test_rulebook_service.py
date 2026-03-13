from __future__ import annotations

from types import SimpleNamespace

from app.services.rulebook_service import RulebookService


def test_rulebook_service_reads_current_config_and_active_bucket():
    service = RulebookService()

    payload = service.build(SimpleNamespace(target_holding_days=30))

    assert payload["current_profile"]["active_bucket_name"] == "medium"
    assert payload["score_rules"]["final_score"]["min_final_score_for_target"] == 55.0
    assert payload["execution_rules"]["channel_b"]["params"]["breakout_entry_threshold"] == 75.0
    assert payload["execution_cost_rules"]["execution_cost_bps"] == 5.0


def test_rulebook_service_builds_decision_score_breakdown():
    service = RulebookService()

    breakdown = service.build_decision_score_breakdown(
        scores={"entry_score": 80.0, "hold_score": 70.0, "exit_score": 30.0},
        is_held=False,
        preferences=SimpleNamespace(target_holding_days=30),
    )

    assert breakdown["bucket_name"] == "medium"
    assert breakdown["weights"]["entry"] == 0.70
    assert breakdown["stay_score"] == 70.0
    assert breakdown["decision_score"] == 77.0


def test_rulebook_service_exposes_money_etf_exit_as_opportunity_and_risk_switch():
    service = RulebookService()

    payload = service.build(SimpleNamespace(target_holding_days=30))
    money_exit = payload["category_heads"]["money_etf"]["heads"]["exit"]["components"]
    component_keys = [component["key"] for component in money_exit]

    assert "opportunity_cost" in component_keys
    assert "risk_switch" in component_keys
    assert "time_decay" not in component_keys
    assert "不代表持满天数就自动卖出" in payload["score_rules"]["decision_score"]["meaning"]
