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
from app.services.backtest_service import BacktestRequest, BacktestService
from app.services.validation_service import RollingValidationRequest, ValidationService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="运行 ETF 历史回测")
    parser.add_argument("--start-date", required=True, help="开始日期，例如 2025-01-01")
    parser.add_argument("--end-date", required=True, help="结束日期，例如 2025-03-31")
    parser.add_argument("--initial-capital", type=float, required=True, help="初始资金")
    parser.add_argument("--risk-mode", default=None, choices=["conservative", "balanced", "aggressive"], help="风险模式")
    parser.add_argument("--target-holding-days", type=int, default=None, help="覆盖持有周期")
    parser.add_argument("--use-live-trades", action="store_true", help="把开始日前的真实历史成交作为初始仓位背景")
    parser.add_argument("--slippage-bps", type=float, default=None, help="滑点，单位 bps")
    parser.add_argument("--execution-cost-bps", type=float, default=None, help="统一交易成本，单位 bps")
    parser.add_argument("--fee-rate-override", type=float, default=None, help="统一覆盖手续费率")
    parser.add_argument("--min-fee-override", type=float, default=None, help="统一覆盖最小手续费")
    parser.add_argument("--allow-weak-data", action="store_true", help="弱质量数据也继续回放，不额外报错")
    parser.add_argument("--auto-calibrate", action="store_true", help="先做滚动验证和阈值校准，再输出推荐阈值回测")
    parser.add_argument("--train-days", type=int, default=None, help="滚动验证训练窗口长度")
    parser.add_argument("--validation-days", type=int, default=None, help="滚动验证验证窗口长度")
    parser.add_argument("--step-days", type=int, default=None, help="滚动验证步长")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    init_db()
    session_local = get_session_local()
    with session_local() as session:
        if args.auto_calibrate:
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
            print(json.dumps(
                {
                    "run_id": result["run_id"],
                    "recommended_candidate": result["recommended_candidate"],
                    "aggregate_validation": result["aggregate_validation"],
                    "output_files": result["output_files"],
                },
                ensure_ascii=False,
                indent=2,
            ))
            return

        result = BacktestService().run(
            session,
            BacktestRequest(
                start_date=date.fromisoformat(args.start_date),
                end_date=date.fromisoformat(args.end_date),
                initial_capital=float(args.initial_capital),
                use_live_trades=bool(args.use_live_trades),
                target_holding_days=args.target_holding_days,
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
                "overview": result["overview"],
                "metrics": result["metrics"],
                "quality_overview": result["quality_overview"],
                "output_files": result["output_files"],
            },
            ensure_ascii=False,
            indent=2,
        ))


if __name__ == "__main__":
    main()
