from app.services.execution_cost_service import ExecutionCostService


def test_execution_cost_config_loads_default_values():
    service = ExecutionCostService()

    assert service.execution_cost_bps() == 5.0
    assert service.min_trade_amount() == 100.0
    assert service.estimate_execution_cost(2000.0) == 1.0
