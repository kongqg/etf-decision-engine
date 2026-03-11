from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.core.database import get_session_local, init_db
from app.services.threshold_calibration_service import CalibrationRequest, ThresholdCalibrationService
from app.services.validation_service import RollingValidationRequest, ValidationService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="运行 ETF 阈值校准 / 滚动验证")
    parser.add_argument("--start-date", required=True, help="开始日期，例如 2025-01-01")
    parser.add_argument("--end-date", required=True, help="结束日期，例如 2025-03-31")
    parser.add_argument("--initial-capital", type=float, required=True, help="初始资金")
    parser.add_argument("--risk-mode", default=None, choices=["conservative", "balanced", "aggressive"], help="风险模式")
    parser.add_argument("--use-live-trades", action="store_true", help="把开始日前的真实历史成交作为初始仓位背景")
    parser.add_argument("--slippage-bps", type=float, default=None, help="滑点，单位 bps")
    parser.add_argument("--execution-cost-bps", type=float, default=None, help="统一交易成本，单位 bps")
    parser.add_argument("--fee-rate-override", type=float, default=None, help="统一覆盖手续费率")
    parser.add_argument("--min-fee-override", type=float, default=None, help="统一覆盖最小手续费")
    parser.add_argument("--allow-weak-data", action="store_true", help="弱质量数据也继续回放")
    parser.add_argument("--rolling-validation", action="store_true", help="启用滚动验证而不是单区间校准")
    parser.add_argument("--train-days", type=int, default=None, help="训练窗口长度")
    parser.add_argument("--validation-days", type=int, default=None, help="验证窗口长度")
    parser.add_argument("--step-days", type=int, default=None, help="滚动窗口步长")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    init_db()
    session_local = get_session_local()
    with session_local() as session:
        if args.rolling_validation:
            result = ValidationService().run(
                session,
                RollingValidationRequest(
                    start_date=date.fromisoformat(args.start_date),
                    end_date=date.fromisoformat(args.end_date),
                    initial_capital=float(args.initial_capital),
                    train_days=args.train_days,
                    validation_days=args.validation_days,
                    step_days=args.step_days,
                    use_live_trades=bool(args.use_live_trades),
                    risk_mode=args.risk_mode,
                    slippage_bps=args.slippage_bps,
                    execution_cost_bps_override=args.execution_cost_bps,
                    fee_rate_override=args.fee_rate_override,
                    min_fee_override=args.min_fee_override,
                    strict_data_quality=not args.allow_weak_data,
                ),
            )
        else:
            result = ThresholdCalibrationService().run(
                session,
                CalibrationRequest(
                    start_date=date.fromisoformat(args.start_date),
                    end_date=date.fromisoformat(args.end_date),
                    initial_capital=float(args.initial_capital),
                    use_live_trades=bool(args.use_live_trades),
                    risk_mode=args.risk_mode,
                    slippage_bps=args.slippage_bps,
                    execution_cost_bps_override=args.execution_cost_bps,
                    fee_rate_override=args.fee_rate_override,
                    min_fee_override=args.min_fee_override,
                    strict_data_quality=not args.allow_weak_data,
                ),
            )
        print(json.dumps(
            {
                "run_id": result["run_id"],
                "run_type": result["run_type"],
                "recommended_candidate": result.get("recommended_candidate"),
                "output_files": result["output_files"],
            },
            ensure_ascii=False,
            indent=2,
        ))


if __name__ == "__main__":
    main()
