from app.services.decision_policy_service import get_decision_policy_service


def test_target_holding_days_map_to_stable_profiles():
    policy = get_decision_policy_service()

    assert policy.map_horizon_profile(0, tradability_mode="t0", offensive_edge=True) == "intraday_t0"
    assert policy.map_horizon_profile(1, tradability_mode="t1", offensive_edge=True) == "swing"
    assert policy.map_horizon_profile(5, tradability_mode="t1", offensive_edge=True) == "swing"
    assert policy.map_horizon_profile(15, tradability_mode="t1", offensive_edge=True) == "swing"
    assert policy.map_horizon_profile(25, tradability_mode="t1", offensive_edge=True) == "rotation"
    assert policy.map_horizon_profile(5, tradability_mode="t1", offensive_edge=False) == "defensive_cash"


def test_lifecycle_phase_changes_with_remaining_days_ratio():
    policy = get_decision_policy_service()

    assert policy.resolve_lifecycle_phase(9, 8) == "build_phase"
    assert policy.resolve_lifecycle_phase(9, 4) == "hold_phase"
    assert policy.resolve_lifecycle_phase(9, 2) == "exit_phase"
