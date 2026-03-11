from types import SimpleNamespace

from app.services.decision_policy_service import get_decision_policy_service
from app.services.risk_mode_service import RiskModeService


def _preferences(
    *,
    risk_mode: str,
    max_total_position_pct: float = 0.7,
    target_holding_days: int = 5,
    cash_reserve_pct: float = 0.2,
):
    return SimpleNamespace(
        risk_level="中性",
        risk_mode=risk_mode,
        allow_gold=True,
        allow_bond=True,
        allow_overseas=True,
        min_trade_amount=100.0,
        target_holding_days=target_holding_days,
        max_total_position_pct=max_total_position_pct,
        max_single_position_pct=0.35,
        cash_reserve_pct=cash_reserve_pct,
    )


def test_balanced_mode_preserves_baseline_preferences_and_thresholds():
    service = RiskModeService()
    base_thresholds = get_decision_policy_service().action_thresholds

    resolved = service.resolve(_preferences(risk_mode="balanced"), base_thresholds)

    assert resolved.risk_mode == "balanced"
    assert resolved.risk_mode_label == "正常"
    assert resolved.preferences.max_total_position_pct == 0.7
    assert resolved.preferences.target_holding_days == 5
    assert resolved.action_thresholds["fallback"]["offensive_threshold"] == base_thresholds["fallback"]["offensive_threshold"]
    assert resolved.action_thresholds["decision_thresholds"]["open_threshold"] == base_thresholds["decision_thresholds"]["open_threshold"]
    assert resolved.category_score_adjustments == {}


def test_conservative_mode_tightens_thresholds_and_caps_position():
    service = RiskModeService()
    base_thresholds = get_decision_policy_service().action_thresholds

    resolved = service.resolve(_preferences(risk_mode="conservative", max_total_position_pct=0.8), base_thresholds)

    assert resolved.risk_mode == "conservative"
    assert resolved.preferences.max_total_position_pct == 0.55
    assert resolved.preferences.target_holding_days == 4
    assert resolved.action_thresholds["fallback"]["offensive_threshold"] > base_thresholds["fallback"]["offensive_threshold"]
    assert resolved.action_thresholds["decision_thresholds"]["open_threshold"] > base_thresholds["decision_thresholds"]["open_threshold"]
    assert resolved.action_thresholds["decision_thresholds"]["reduce_threshold"] < base_thresholds["decision_thresholds"]["reduce_threshold"]
    assert resolved.action_thresholds["decision_thresholds"]["full_exit_threshold"] < base_thresholds["decision_thresholds"]["full_exit_threshold"]
    assert resolved.category_score_adjustments["stock_etf"] < 0


def test_aggressive_mode_loosens_entry_and_uses_higher_cap():
    service = RiskModeService()
    base_thresholds = get_decision_policy_service().action_thresholds

    resolved = service.resolve(_preferences(risk_mode="aggressive", max_total_position_pct=0.4, target_holding_days=2), base_thresholds)

    assert resolved.risk_mode == "aggressive"
    assert resolved.preferences.max_total_position_pct == 0.8
    assert resolved.preferences.target_holding_days == 8
    assert resolved.action_thresholds["fallback"]["offensive_threshold"] < base_thresholds["fallback"]["offensive_threshold"]
    assert resolved.action_thresholds["decision_thresholds"]["open_threshold"] < base_thresholds["decision_thresholds"]["open_threshold"]
    assert resolved.action_thresholds["decision_thresholds"]["reduce_threshold"] > base_thresholds["decision_thresholds"]["reduce_threshold"]
    assert resolved.action_thresholds["decision_thresholds"]["full_exit_threshold"] > base_thresholds["decision_thresholds"]["full_exit_threshold"]
    assert resolved.category_score_adjustments["cross_border_etf"] > 0
