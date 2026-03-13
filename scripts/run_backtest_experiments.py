from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.core.database import get_session_local, init_db
from app.services.backtest_service import BacktestRequest, BacktestService


@dataclass
class ExperimentSpec:
    group: str
    variant_id: str
    title: str
    hypothesis: str
    config_overrides: dict[str, Any] = field(default_factory=dict)
    request_overrides: dict[str, Any] = field(default_factory=dict)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="运行回测参数实验，并生成中文实验报告。")
    parser.add_argument("--start-date", default="2025-01-01", help="开始日期")
    parser.add_argument("--end-date", default="2026-03-11", help="结束日期")
    parser.add_argument("--initial-capital", type=float, default=4000.0, help="初始资金")
    parser.add_argument("--risk-mode", default="balanced", choices=["conservative", "balanced", "aggressive"], help="风险模式")
    parser.add_argument("--target-holding-days", type=int, default=30, help="执行周期偏好")
    parser.add_argument("--strict-data-quality", action="store_true", help="严格数据质量门禁")
    parser.add_argument("--output-dir", default=None, help="输出目录，默认写到 data/backtest_experiments 下")
    return parser.parse_args()


def build_experiments() -> list[ExperimentSpec]:
    return [
        ExperimentSpec(
            group="动量权重组",
            variant_id="G1_A",
            title="短动量加强",
            hypothesis="提高 5 日和 10 日动量权重，观察 4000 小资金是否更容易出现可执行信号。",
            config_overrides={
                "intra_score_weights.momentum_20d_rank": 0.20,
                "intra_score_weights.momentum_10d_rank": 0.25,
                "intra_score_weights.momentum_5d_rank": 0.20,
                "intra_score_weights.trend_rank": 0.15,
                "intra_score_weights.liquidity_rank": 0.10,
                "intra_score_weights.volatility_rank": 0.05,
                "intra_score_weights.drawdown_rank": 0.05,
            },
        ),
        ExperimentSpec(
            group="动量权重组",
            variant_id="G1_B",
            title="主趋势加强",
            hypothesis="提高 20 日动量权重，观察更稳的主趋势锚是否减少无效切换。",
            config_overrides={
                "intra_score_weights.momentum_20d_rank": 0.40,
                "intra_score_weights.momentum_10d_rank": 0.20,
                "intra_score_weights.momentum_5d_rank": 0.05,
                "intra_score_weights.trend_rank": 0.15,
                "intra_score_weights.liquidity_rank": 0.10,
                "intra_score_weights.volatility_rank": 0.05,
                "intra_score_weights.drawdown_rank": 0.05,
            },
        ),
        ExperimentSpec(
            group="趋势过滤组",
            variant_id="G2_A",
            title="降低最低候选阈值",
            hypothesis="用更低的正式候选阈值测试当前系统是不是太难形成目标组合。",
            config_overrides={
                "selection.min_final_score_for_target": 50.0,
            },
        ),
        ExperimentSpec(
            group="趋势过滤组",
            variant_id="G2_B",
            title="提高最低候选阈值",
            hypothesis="用更高的正式候选阈值验证当前无交易是否来自严格的候选层过滤。",
            config_overrides={
                "selection.min_final_score_for_target": 60.0,
            },
        ),
        ExperimentSpec(
            group="通道A/B组",
            variant_id="G3_A",
            title="回撤反弹优先",
            hypothesis="扩大回撤区间、抬高突破门槛，测试回调买入是否更适合当前区间。",
            config_overrides={
                "execution_overlay.pullback_low_pct": -8.0,
                "execution_overlay.pullback_high_pct": -3.0,
                "execution_overlay.breakout_entry_threshold": 80.0,
            },
        ),
        ExperimentSpec(
            group="通道A/B组",
            variant_id="G3_B",
            title="强趋势突破优先",
            hypothesis="缩窄回撤区间、降低突破门槛，测试强势突破是否能更快形成可执行仓位。",
            config_overrides={
                "execution_overlay.pullback_low_pct": -5.0,
                "execution_overlay.pullback_high_pct": -1.5,
                "execution_overlay.breakout_entry_threshold": 70.0,
            },
        ),
        ExperimentSpec(
            group="相对强弱组",
            variant_id="G4_A",
            title="强化相对强弱",
            hypothesis="更强调 relative_strength_10d，观察系统是否更容易抓到同类龙头。",
            config_overrides={
                "category_heads.stock_etf.entry.relative_strength_10d": 0.24,
                "category_heads.stock_etf.entry.momentum_10d": 0.16,
                "category_heads.stock_etf.hold.relative_strength_10d": 0.30,
                "category_heads.stock_etf.hold.momentum_20d": 0.18,
            },
        ),
        ExperimentSpec(
            group="相对强弱组",
            variant_id="G4_B",
            title="弱化相对强弱",
            hypothesis="降低 relative_strength_10d 的影响，观察系统是否更稳但更慢。",
            config_overrides={
                "category_heads.stock_etf.entry.relative_strength_10d": 0.10,
                "category_heads.stock_etf.entry.momentum_10d": 0.30,
                "category_heads.stock_etf.hold.relative_strength_10d": 0.16,
                "category_heads.stock_etf.hold.momentum_20d": 0.32,
            },
        ),
        ExperimentSpec(
            group="风险控制组",
            variant_id="G5_A",
            title="强化低波低回撤过滤",
            hypothesis="更强调低波和低回撤，验证是否能减少短周期噪音开仓。",
            config_overrides={
                "category_heads.stock_etf.entry.volatility_10d": -0.08,
                "category_heads.stock_etf.entry.abs_drawdown_20d": -0.08,
                "category_heads.stock_etf.hold.volatility_10d": -0.14,
                "category_heads.stock_etf.hold.abs_drawdown_20d": -0.18,
            },
        ),
        ExperimentSpec(
            group="风险控制组",
            variant_id="G5_B",
            title="放松低波低回撤过滤",
            hypothesis="减弱低波和低回撤过滤，观察当前系统是否因为风控过强而长期空仓。",
            config_overrides={
                "category_heads.stock_etf.entry.volatility_10d": -0.02,
                "category_heads.stock_etf.entry.abs_drawdown_20d": -0.02,
                "category_heads.stock_etf.hold.volatility_10d": -0.06,
                "category_heads.stock_etf.hold.abs_drawdown_20d": -0.10,
            },
        ),
        ExperimentSpec(
            group="REDUCE组",
            variant_id="G6_A",
            title="轻减仓",
            hypothesis="趋势转弱时只减到 70%，测试是否能保留更多趋势延续收益。",
            config_overrides={
                "execution_overlay.internals.reduced_target_multiplier": 0.7,
            },
        ),
        ExperimentSpec(
            group="REDUCE组",
            variant_id="G6_B",
            title="重减仓",
            hypothesis="趋势转弱时只保留 30%，测试是否能更快控制回撤。",
            config_overrides={
                "execution_overlay.internals.reduced_target_multiplier": 0.3,
            },
        ),
        ExperimentSpec(
            group="交易摩擦组",
            variant_id="G7_A",
            title="低交易摩擦",
            hypothesis="降低滑点和执行成本，判断策略是否主要被摩擦成本吞掉。",
            request_overrides={
                "slippage_bps": 1.0,
                "execution_cost_bps_override": 2.0,
            },
        ),
        ExperimentSpec(
            group="交易摩擦组",
            variant_id="G7_B",
            title="高交易摩擦",
            hypothesis="抬高滑点和执行成本，验证收益是否对交易摩擦高度敏感。",
            request_overrides={
                "slippage_bps": 5.0,
                "execution_cost_bps_override": 10.0,
            },
        ),
        ExperimentSpec(
            group="替换与保护组",
            variant_id="G8_A",
            title="加快换仓",
            hypothesis="降低替换阈值和最短持有期，观察是否能更快切到新龙头。",
            config_overrides={
                "selection.replace_threshold": 6.0,
                "selection.min_hold_days_before_replace": 2,
                "selection.hold_guard_global_rank": 4,
            },
        ),
        ExperimentSpec(
            group="替换与保护组",
            variant_id="G8_B",
            title="增强保护",
            hypothesis="提高替换阈值和最短持有期，观察是否能减少频繁切换。",
            config_overrides={
                "selection.replace_threshold": 12.0,
                "selection.min_hold_days_before_replace": 8,
                "selection.hold_guard_global_rank": 8,
            },
        ),
        ExperimentSpec(
            group="仓位组",
            variant_id="G9_A",
            title="更集中",
            hypothesis="提高单票上限并降低最小正式持仓权重，测试 4000 小资金是否更容易形成有效仓位。",
            config_overrides={
                "budget.max_single_weight": 0.40,
                "budget.min_position_weight": 0.05,
                "category_caps.stock_etf": 0.80,
            },
        ),
        ExperimentSpec(
            group="仓位组",
            variant_id="G9_B",
            title="更分散",
            hypothesis="降低单票上限并提高最小正式持仓权重，观察是否进一步压缩可执行信号。",
            config_overrides={
                "budget.max_single_weight": 0.20,
                "budget.min_position_weight": 0.10,
                "category_caps.stock_etf": 0.60,
            },
        ),
        ExperimentSpec(
            group="货币ETF防守切换组",
            variant_id="G10_A",
            title="机会成本优先",
            hypothesis="更强调外部机会成本，测试货币 ETF 是否会更早让位给进攻仓。",
            config_overrides={
                "category_heads.money_etf.exit.opportunity_cost": 0.75,
                "category_heads.money_etf.exit.risk_switch": 0.25,
            },
        ),
        ExperimentSpec(
            group="货币ETF防守切换组",
            variant_id="G10_B",
            title="市场切换优先",
            hypothesis="更强调整体市场切换，测试货币 ETF 是否更愿意等待环境确认。",
            config_overrides={
                "category_heads.money_etf.exit.opportunity_cost": 0.40,
                "category_heads.money_etf.exit.risk_switch": 0.60,
            },
        ),
        ExperimentSpec(
            group="time_decay对照组",
            variant_id="G11_A",
            title="去掉 time_decay",
            hypothesis="验证 time_decay 对当前短周期滚动复利框架是否有必要。",
            config_overrides={
                "category_heads.stock_etf.exit.time_decay": 0.0,
                "category_heads.bond_etf.exit.time_decay": 0.0,
                "category_heads.gold_etf.exit.time_decay": 0.0,
                "category_heads.cross_border_etf.exit.time_decay": 0.0,
            },
        ),
        ExperimentSpec(
            group="time_decay对照组",
            variant_id="G11_B",
            title="强化 time_decay",
            hypothesis="提高各进攻类别的 time_decay，测试持有时长是否会显著推高退出频率。",
            config_overrides={
                "category_heads.stock_etf.exit.time_decay": 0.27,
                "category_heads.bond_etf.exit.time_decay": 0.42,
                "category_heads.gold_etf.exit.time_decay": 0.33,
                "category_heads.cross_border_etf.exit.time_decay": 0.30,
            },
        ),
    ]


def run_single(
    *,
    service: BacktestService,
    session: Any,
    dataset: dict[str, Any],
    start_date: date,
    end_date: date,
    initial_capital: float,
    risk_mode: str,
    target_holding_days: int,
    strict_data_quality: bool,
    config_overrides: dict[str, Any] | None = None,
    request_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    request_kwargs = dict(request_overrides or {})
    result = service.run(
        session,
        BacktestRequest(
            start_date=start_date,
            end_date=end_date,
            initial_capital=float(initial_capital),
            risk_mode=risk_mode,
            strict_data_quality=strict_data_quality,
            config_overrides={
                "target_holding_days": int(target_holding_days),
                **dict(config_overrides or {}),
            },
            profile=True,
            **request_kwargs,
        ),
        dataset=dataset,
        persist_output=False,
    )
    return result


def ranking_tuple(row: dict[str, Any]) -> tuple[Any, ...]:
    metrics = row["metrics"]
    drawdown_abs = abs(float(metrics["max_drawdown_pct"]))
    return (
        float(metrics["total_return_pct"]),
        -drawdown_abs,
        -float(metrics["trade_count"]),
    )


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "type",
        "group",
        "variant_id",
        "title",
        "hypothesis",
        "total_return_pct",
        "annualized_return_pct",
        "max_drawdown_pct",
        "trade_count",
        "open_count",
        "add_count",
        "reduce_count",
        "exit_count",
        "replacement_frequency",
        "final_asset",
        "total_execution_cost",
        "daily_loop_sec",
        "scoring_sec",
        "allocation_execution_sec",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            metrics = row["metrics"]
            profiling = row.get("profiling", {})
            writer.writerow(
                {
                    "type": row["type"],
                    "group": row.get("group", ""),
                    "variant_id": row["variant_id"],
                    "title": row["title"],
                    "hypothesis": row["hypothesis"],
                    "total_return_pct": metrics["total_return_pct"],
                    "annualized_return_pct": metrics["annualized_return_pct"],
                    "max_drawdown_pct": metrics["max_drawdown_pct"],
                    "trade_count": metrics["trade_count"],
                    "open_count": metrics["open_count"],
                    "add_count": metrics["add_count"],
                    "reduce_count": metrics["reduce_count"],
                    "exit_count": metrics["exit_count"],
                    "replacement_frequency": metrics["replacement_frequency"],
                    "final_asset": metrics["final_asset"],
                    "total_execution_cost": metrics["total_execution_cost"],
                    "daily_loop_sec": profiling.get("daily_loop_sec", 0.0),
                    "scoring_sec": profiling.get("scoring_sec", 0.0),
                    "allocation_execution_sec": profiling.get("allocation_execution_sec", 0.0),
                }
            )


def build_report(
    *,
    baseline: dict[str, Any],
    rows: list[dict[str, Any]],
    combos: list[dict[str, Any]],
    metadata: dict[str, Any],
) -> str:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(row["group"], []).append(row)
    for variants in grouped.values():
        variants.sort(key=ranking_tuple, reverse=True)
    ranked_rows = sorted(rows, key=ranking_tuple, reverse=True)
    executable_rows = [row for row in ranked_rows if row["metrics"]["trade_count"] > 0]
    best_executable = executable_rows[0] if executable_rows else None
    best_overall = ranked_rows[0] if ranked_rows else None

    lines = [
        "# 回测实验记录",
        "",
        "## 1. 实验设定",
        f"- 回测区间：`{metadata['start_date']} ~ {metadata['end_date']}`",
        f"- 初始资金：`{metadata['initial_capital']:.2f}` 元",
        f"- 风格模式：`{metadata['risk_mode']}`",
        f"- 执行周期偏好：`{metadata['target_holding_days']}` 天",
        f"- 数据质量口径：`{'严格门禁' if metadata['strict_data_quality'] else '允许弱质量数据继续回放'}`",
        f"- 预计算数据集：`{metadata['dataset_trading_days']}` 个交易日，`{metadata['dataset_symbol_count']}` 个标的",
        "- 实验方法：每组只改一组核心参数，其余保持当前规则不变。",
        "",
        "## 2. 基线结果",
        f"- 基线累计收益：`{baseline['metrics']['total_return_pct']:.2f}%`",
        f"- 基线最大回撤：`{baseline['metrics']['max_drawdown_pct']:.2f}%`",
        f"- 基线成交笔数：`{baseline['metrics']['trade_count']}`",
        f"- 基线最终资产：`{baseline['metrics']['final_asset']:.2f}` 元",
        f"- 基线结论：{baseline['overview']['one_line_conclusion']}",
        "",
    ]

    if baseline["metrics"]["trade_count"] == 0:
        lines.extend(
            [
                "### 基线观察",
                "- 在 `4000` 元资金约束下，当前规则基线没有形成任何成交，说明这次实验除了比较收益，更重要的是比较“哪些参数组能够先把可执行交易打开”。",
                "- 这种结果通常意味着：候选阈值、替换保护、执行门槛和一手门槛叠加后，把小资金样本压成了长期观望。",
                "",
            ]
        )

    lines.extend(
        [
            "## 3. 每组实验结果",
            "",
        ]
    )
    for group, variants in grouped.items():
        lines.append(f"### {group}")
        for row in variants:
            metrics = row["metrics"]
            delta_return = float(metrics["total_return_pct"]) - float(baseline["metrics"]["total_return_pct"])
            lines.extend(
                [
                    f"- `{row['variant_id']} {row['title']}`：收益 `{metrics['total_return_pct']:.2f}%`，回撤 `{metrics['max_drawdown_pct']:.2f}%`，成交 `{metrics['trade_count']}` 次，较基线收益变化 `{delta_return:+.2f}%`。",
                    f"  假设：{row['hypothesis']}",
                ]
            )
        lines.append("")

    lines.extend(
        [
            "## 4. 综合排名",
            "",
        ]
    )
    if best_overall:
        lines.append(
            f"- 数值最优（不区分是否有成交）：`{best_overall['variant_id']} {best_overall['title']}`，收益 `{best_overall['metrics']['total_return_pct']:.2f}%`，成交 `{best_overall['metrics']['trade_count']}` 次。"
        )
    if best_executable:
        lines.append(
            f"- 可执行最优（至少有 1 次成交）：`{best_executable['variant_id']} {best_executable['title']}`，收益 `{best_executable['metrics']['total_return_pct']:.2f}%`，回撤 `{best_executable['metrics']['max_drawdown_pct']:.2f}%`，成交 `{best_executable['metrics']['trade_count']}` 次。"
        )
    else:
        lines.append("- 本轮单因素实验里，没有任何参数组在 `4000` 元资金口径下形成实际成交。")
    lines.append("")

    if combos:
        lines.extend(["## 5. 组合复核", ""])
        for row in sorted(combos, key=ranking_tuple, reverse=True):
            metrics = row["metrics"]
            lines.append(
                f"- `{row['variant_id']} {row['title']}`：收益 `{metrics['total_return_pct']:.2f}%`，回撤 `{metrics['max_drawdown_pct']:.2f}%`，成交 `{metrics['trade_count']}` 次。组合来源：{row['hypothesis']}"
            )
        lines.append("")

    lines.extend(
        [
            "## 6. 结论与建议",
        ]
    )
    if best_executable:
        lines.extend(
            [
                f"- 如果目标是先让 `4000` 元的小资金样本形成可执行交易，优先关注 `{best_executable['group']}` 这组参数；当前最优可执行方案是 `{best_executable['variant_id']} {best_executable['title']}`。",
                "- 如果某些组收益最好但仍然 0 成交，那么它们更像“保持观望”的保守配置，而不是适合拿来判断短周期滚动复利效率的配置。",
            ]
        )
    else:
        lines.extend(
            [
                "- 当前这套规则在 `4000` 元资金口径下仍然过于克制，优先要解决的是“能不能形成可执行交易”，而不是“收益率多高”。",
                "- 从实验结构上看，最值得继续细化的是：候选阈值、通道 A/B、替换保护和仓位/一手门槛相关参数。",
            ]
        )
    lines.extend(
        [
            "- 这份报告是单因素对照，适合定位“哪组参数在起作用”；如果后续要做正式调参，再把单因素里表现最好的 2~3 组组合起来复核。",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    strict_data_quality = bool(args.strict_data_quality)

    run_id = f"exp_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    output_dir = Path(args.output_dir) if args.output_dir else ROOT_DIR / "data" / "backtest_experiments" / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    init_db()
    session_local = get_session_local()
    service = BacktestService()

    with session_local() as session:
        dataset = service.prepare_dataset(session, start_date=start_date, end_date=end_date)
        baseline = run_single(
            service=service,
            session=session,
            dataset=dataset,
            start_date=start_date,
            end_date=end_date,
            initial_capital=args.initial_capital,
            risk_mode=args.risk_mode,
            target_holding_days=args.target_holding_days,
            strict_data_quality=strict_data_quality,
        )

        rows: list[dict[str, Any]] = []
        for spec in build_experiments():
            result = run_single(
                service=service,
                session=session,
                dataset=dataset,
                start_date=start_date,
                end_date=end_date,
                initial_capital=args.initial_capital,
                risk_mode=args.risk_mode,
                target_holding_days=args.target_holding_days,
                strict_data_quality=strict_data_quality,
                config_overrides=spec.config_overrides,
                request_overrides=spec.request_overrides,
            )
            rows.append(
                {
                    "type": "single_factor",
                    "group": spec.group,
                    "variant_id": spec.variant_id,
                    "title": spec.title,
                    "hypothesis": spec.hypothesis,
                    "config_overrides": spec.config_overrides,
                    "request_overrides": spec.request_overrides,
                    "metrics": result["metrics"],
                    "overview": result["overview"],
                    "profiling": result.get("profiling", {}),
                }
            )

        executable_rows = [row for row in sorted(rows, key=ranking_tuple, reverse=True) if row["metrics"]["trade_count"] > 0]
        combos: list[dict[str, Any]] = []
        if len(executable_rows) >= 2:
            distinct_rows: list[dict[str, Any]] = []
            seen_groups: set[str] = set()
            for row in executable_rows:
                if row["group"] in seen_groups:
                    continue
                distinct_rows.append(row)
                seen_groups.add(row["group"])
                if len(distinct_rows) >= 3:
                    break
            if len(distinct_rows) >= 2:
                combo_overrides: dict[str, Any] = {}
                combo_request_overrides: dict[str, Any] = {}
                combo_parts = []
                for row in distinct_rows[:3]:
                    combo_overrides.update(row["config_overrides"])
                    combo_request_overrides.update(row["request_overrides"])
                    combo_parts.append(row["variant_id"])
                combo_result = run_single(
                    service=service,
                    session=session,
                    dataset=dataset,
                    start_date=start_date,
                    end_date=end_date,
                    initial_capital=args.initial_capital,
                    risk_mode=args.risk_mode,
                    target_holding_days=args.target_holding_days,
                    strict_data_quality=strict_data_quality,
                    config_overrides=combo_overrides,
                    request_overrides=combo_request_overrides,
                )
                combos.append(
                    {
                        "type": "combo",
                        "group": "组合复核",
                        "variant_id": "COMBO_TOP",
                        "title": "单因素最优组合",
                        "hypothesis": f"组合来源：{' + '.join(combo_parts)}",
                        "config_overrides": combo_overrides,
                        "request_overrides": combo_request_overrides,
                        "metrics": combo_result["metrics"],
                        "overview": combo_result["overview"],
                        "profiling": combo_result.get("profiling", {}),
                    }
                )

        metadata = {
            "run_id": run_id,
            "created_at": datetime.now().isoformat(),
            "start_date": args.start_date,
            "end_date": args.end_date,
            "initial_capital": float(args.initial_capital),
            "risk_mode": args.risk_mode,
            "target_holding_days": int(args.target_holding_days),
            "strict_data_quality": strict_data_quality,
            "dataset_trading_days": len(dataset.get("trading_dates", [])),
            "dataset_symbol_count": len(dataset.get("history_by_symbol", {})),
        }

    payload = {
        "metadata": metadata,
        "baseline": {
            "type": "baseline",
            "variant_id": "BASELINE",
            "title": "当前规则基线",
            "hypothesis": "不改任何参数，只观察当前规则在 4000 元资金口径下的表现。",
            "metrics": baseline["metrics"],
            "overview": baseline["overview"],
            "profiling": baseline.get("profiling", {}),
        },
        "single_factor_results": rows,
        "combo_results": combos,
    }
    (output_dir / "results.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    write_csv(output_dir / "results.csv", rows + combos)
    report = build_report(
        baseline=payload["baseline"],
        rows=rows,
        combos=combos,
        metadata=metadata,
    )
    (output_dir / "report.md").write_text(report, encoding="utf-8")

    print(json.dumps(
        {
            "run_id": run_id,
            "output_dir": str(output_dir),
            "baseline": payload["baseline"]["metrics"],
            "best_single": max(rows, key=ranking_tuple) if rows else None,
            "best_combo": max(combos, key=ranking_tuple) if combos else None,
        },
        ensure_ascii=False,
        indent=2,
    ))


if __name__ == "__main__":
    main()
