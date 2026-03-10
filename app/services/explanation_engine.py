from __future__ import annotations

import json
from collections import Counter
from typing import Any

import pandas as pd

from app.core.session_mode import SESSION_MODE_HINTS


ETF_SCORE_FORMULA = (
    "综合分 = 0.20×3日动量分位 + 0.25×5日动量分位 + 0.25×10日动量分位 + "
    "0.15×趋势强度分位 + 0.10×均线位置分位 - 0.03×波动率惩罚 - "
    "0.02×回撤惩罚 + 0.10×流动性分位"
)

ETF_INPUT_FORMULAS = [
    {"label": "3日动量", "formula": "(最新收盘价 / 3个交易日前收盘价 - 1) × 100"},
    {"label": "5日动量", "formula": "(最新收盘价 / 5个交易日前收盘价 - 1) × 100"},
    {"label": "10日动量", "formula": "(最新收盘价 / 10个交易日前收盘价 - 1) × 100"},
    {"label": "均线位置", "formula": "用最新价相对 5 日均线和 10 日均线的偏离度做分位排名"},
    {"label": "趋势强度", "formula": "0.4 × 5日均线偏离 + 0.6 × 10日均线偏离"},
    {"label": "10日波动率", "formula": "最近 10 个交易日收益波动的年化结果，越高扣分越多"},
    {"label": "20日回撤", "formula": "(最新收盘价 / 最近 20 日最高价 - 1) × 100，离高点越远扣分越多"},
    {"label": "流动性", "formula": "按近 20 日平均成交额做分位排名，成交越活跃越加分"},
]


class ExplanationEngine:
    def build(
        self,
        advice: dict[str, Any],
        scored_df: pd.DataFrame,
        filtered_df: pd.DataFrame,
        portfolio_summary: dict[str, Any],
        market_snapshot: dict[str, Any],
        plan: dict[str, Any],
    ) -> dict[str, Any]:
        candidate_count = int(filtered_df["filter_pass"].sum()) if not filtered_df.empty else 0
        total_count = int(len(filtered_df))
        top_score = float(scored_df.iloc[0]["total_score"]) if not scored_df.empty else 0.0
        facts = plan.get("facts", {})
        executable_recommendations = advice.get("executable_recommendations", advice["items"])
        best_unaffordable_recommendation = advice.get("best_unaffordable_recommendation")
        affordable_but_weak_recommendations = advice.get("affordable_but_weak_recommendations", [])
        watchlist_recommendations = advice.get("watchlist_recommendations", [])
        cost_inefficient_recommendations = advice.get("cost_inefficient_recommendations", [])
        all_recommendations = [
            *executable_recommendations,
            *([best_unaffordable_recommendation] if best_unaffordable_recommendation else []),
            *affordable_but_weak_recommendations,
            *watchlist_recommendations,
            *cost_inefficient_recommendations,
        ]
        raw = market_snapshot.get("raw", {})
        source = raw.get("source", {})
        quality_summary = raw.get("quality_summary", {})

        overall = {
            "mode": advice["session_mode"],
            "headline": advice["summary_text"],
            "market_regime": advice["market_regime"],
            "decision_code": plan.get("reason_code"),
            "target_position_pct": round(advice["target_position_pct"] * 100, 2),
            "reasons": self._overall_reasons(
                advice=advice,
                plan=plan,
                portfolio_summary=portfolio_summary,
                market_snapshot=market_snapshot,
                candidate_count=candidate_count,
                total_count=total_count,
                top_score=top_score,
                recommendations=all_recommendations,
            ),
            "risks": [
                advice["risk_text"],
                "系统只做辅助决策，不会替你自动下单，最终成交价格和执行时点仍会影响结果。",
            ],
            "evidence": [
                {"label": "市场状态", "value": advice["market_regime"]},
                {"label": "宽基强弱分", "value": round(market_snapshot["broad_index_score"], 1)},
                {"label": "风险偏好分", "value": round(market_snapshot["risk_appetite_score"], 1)},
                {"label": "趋势分", "value": round(market_snapshot["trend_score"], 1)},
                {"label": "当前仓位", "value": f"{portfolio_summary['current_position_pct'] * 100:.1f}%"},
                {"label": "目标仓位", "value": f"{advice['target_position_pct'] * 100:.1f}%"},
                {"label": "可用现金", "value": f"{facts.get('available_cash', 0):.0f} 元"},
                {"label": "通过筛选数量", "value": f"{candidate_count}/{total_count}"},
                {"label": "候选池最高分", "value": f"{top_score:.1f}" if candidate_count else "-"},
                {"label": "出手阈值", "value": f"{facts.get('buy_score_threshold', 0):.1f}"},
                {"label": "最小建议金额", "value": f"{facts.get('min_advice_amount', 0):.0f} 元"},
                {"label": "默认一手份额", "value": f"{facts.get('lot_size', 0):.0f} 份"},
                {"label": "当前安全单笔上限", "value": f"{facts.get('practical_buy_cap_amount', 0):.0f} 元"},
                {"label": "手续费上限", "value": f"{facts.get('max_fee_rate_for_execution', 0) * 100:.2f}%"},
                {"label": "活跃资产类别", "value": str(facts.get("active_asset_class_count", 0))},
                {"label": "可执行标的", "value": str(len(executable_recommendations))},
                {"label": "买得起但不建议买", "value": str(len(affordable_but_weak_recommendations))},
                {"label": "关注标的", "value": str(len(watchlist_recommendations))},
                {"label": "手续费不划算", "value": str(len(cost_inefficient_recommendations))},
            ],
            "execution_rule": self._execution_rule(
                advice,
                facts,
                top_score,
                executable_recommendations,
                affordable_but_weak_recommendations,
                watchlist_recommendations,
                cost_inefficient_recommendations,
            ),
            "source_info": self._source_info(source, quality_summary),
            "source_note": source.get("note", ""),
            "market_score_details": self._market_score_details(market_snapshot),
            "etf_score_formula": ETF_SCORE_FORMULA,
            "etf_input_formulas": ETF_INPUT_FORMULAS,
            "executable_recommendations": executable_recommendations,
            "best_unaffordable_recommendation": best_unaffordable_recommendation,
            "affordable_but_weak_recommendations": affordable_but_weak_recommendations,
            "watchlist_recommendations": watchlist_recommendations,
            "cost_inefficient_recommendations": cost_inefficient_recommendations,
            "asset_class_plan": facts.get("asset_class_plan", []),
            "watchlist": self._watchlist(scored_df, plan, watchlist_recommendations),
            "rejected_summary": self._rejected_summary(filtered_df),
            "session_hint": SESSION_MODE_HINTS[advice["session_mode"]],
        }

        items = []
        for item in [
            *executable_recommendations,
            *affordable_but_weak_recommendations,
            *watchlist_recommendations,
            *cost_inefficient_recommendations,
        ]:
            row = scored_df[scored_df["symbol"] == item["symbol"]]
            if row.empty:
                continue
            data = row.iloc[0].to_dict()
            breakdown = self._parse_breakdown(data.get("breakdown_json"))
            is_executable = bool(item.get("is_executable", True))
            asset_class = str(item.get("asset_class") or data.get("asset_class") or data.get("category") or "股票")
            peer_df = scored_df[scored_df.get("asset_class", scored_df["category"]) == asset_class].copy()
            if peer_df.empty:
                peer_df = scored_df.copy()
            peer_df = peer_df.sort_values(["total_score", "momentum_5d"], ascending=[False, False]).head(3)
            comparison_rows = []
            for _, peer in peer_df.iterrows():
                comparison_rows.append(
                    {
                        "symbol": str(peer["symbol"]),
                        "name": str(peer["name"]),
                        "rank": int(peer["rank_in_pool"]),
                        "score": round(float(peer["total_score"]), 1),
                        "momentum_5d": round(float(peer["momentum_5d"]), 2),
                        "volatility_10d": round(float(peer["volatility_10d"]), 2),
                        "selected": str(peer["symbol"]) == item["symbol"],
                    }
                )

            second = peer_df.iloc[1] if len(peer_df) > 1 else None
            rank_in_asset_class = int(data.get("rank_in_asset_class", data.get("rank_in_pool", 1)))
            if second is not None and rank_in_asset_class == 1:
                comparison_text = f"它在 {asset_class} 这一类里比第 2 名高 {data['total_score'] - float(second['total_score']):.1f} 分。"
            else:
                comparison_text = f"它当前在 {asset_class} 这一类里排第 {rank_in_asset_class}。"

            if item.get("recommendation_bucket") == "cost_inefficient_recommendations":
                item_outcome = (
                    f"这次没有进入主执行建议，不是因为它差，而是按当前建议金额测算，"
                    f"手续费占比约 {item.get('estimated_cost_rate', 0) * 100:.2f}%，不划算。"
                )
            elif item.get("recommendation_bucket") == "affordable_but_weak_recommendations":
                item_outcome = (
                    "这只 ETF 在当前仓位内买得起 1 手，但系统仍没把它列入主执行建议。"
                    f"原因不是钱不够，而是 {item.get('weak_signal_reason', '当前信号还不够强')}。"
                )
            elif item.get("is_best_unaffordable"):
                item_outcome = (
                    "这次没有进入主执行建议，不是因为它不够好，而是它仍然是当前更优先的一只，"
                    "只是你这次还买不起 1 手。"
                )
            elif item.get("is_budget_substitute"):
                item_outcome = (
                    f"按当前主配置，系统优先看 {item.get('primary_asset_class', '主配置资产')}，"
                    f"但这类 ETF 现在买不起 1 手，所以补充给出它作为预算内替代执行。"
                )
            elif is_executable:
                item_outcome = (
                    f"这次纳入主执行建议，因为系统给它分配的建议金额 {item['suggested_amount']:.2f} 元，"
                    f"已经覆盖 1 手所需的 {item.get('min_order_amount', 0):.2f} 元，且手续费占比可接受。"
                )
            else:
                item_outcome = (
                    f"这次没有进入主执行建议，不是因为它差，而是系统给它分配的建议金额只有 {item['suggested_amount']:.2f} 元，"
                    f"低于 1 手所需的 {item.get('min_order_amount', 0):.2f} 元。"
                )

            items.append(
                {
                    "symbol": item["symbol"],
                    "title": (
                        f"{item['name']} 为什么可执行"
                        if item.get("recommendation_bucket") == "executable_recommendations"
                        else f"{item['name']} 为什么买得起但现在不建议买"
                        if item.get("recommendation_bucket") == "affordable_but_weak_recommendations"
                        else f"{item['name']} 为什么先关注"
                        if item.get("recommendation_bucket") == "watchlist_recommendations"
                        else f"{item['name']} 为什么暂不执行"
                    ),
                    "summary": item.get("execution_note") or item.get("cost_reason") or item["reason_short"],
                    "execution_status": item.get("execution_status", "可执行买入"),
                    "execution_note": item.get("execution_note", ""),
                    "recommendation_bucket": item.get("recommendation_bucket", "executable_recommendations"),
                    "asset_class": asset_class,
                    "trade_mode": item.get("trade_mode", ""),
                    "trade_mode_note": item.get("trade_mode_note", ""),
                    "execution_timing_mode": item.get("execution_timing_mode", ""),
                    "execution_timing_label": item.get("execution_timing_label", ""),
                    "recommended_execution_windows": item.get("recommended_execution_windows", []),
                    "avoid_execution_windows": item.get("avoid_execution_windows", []),
                    "timing_note": item.get("timing_note", ""),
                    "timing_rule_applied": bool(item.get("timing_rule_applied", False)),
                    "is_budget_substitute": bool(item.get("is_budget_substitute", False)),
                    "primary_asset_class": item.get("primary_asset_class", ""),
                    "is_best_unaffordable": bool(item.get("is_best_unaffordable", False)),
                    "best_unaffordable_reason": item.get("best_unaffordable_reason", ""),
                    "is_affordable_but_weak": bool(item.get("is_affordable_but_weak", False)),
                    "weak_signal_reason": item.get("weak_signal_reason", ""),
                    "why_selected": [
                        f"它属于 {asset_class}，当前按 {item.get('trade_mode', '-') } 的交易节奏来理解执行窗口。",
                        f"近 5 日动量 {data['momentum_5d']:.2f}%，说明它最近一周比多数候选 ETF 更强。",
                        f"综合得分 {data['total_score']:.1f}，候选池排名 {int(data['rank_in_pool'])}/{len(scored_df)}。",
                        comparison_text,
                        item.get("timing_note", ""),
                        item_outcome,
                    ],
                    "score_formula": ETF_SCORE_FORMULA,
                    "score_breakdown": breakdown,
                    "score_calculation": self._etf_score_calculation(data, breakdown),
                    "score_substitution": self._etf_score_substitution(breakdown),
                    "proofs": [
                        {"label": "3日动量", "value": f"{data['momentum_3d']:.2f}%"},
                        {"label": "5日动量", "value": f"{data['momentum_5d']:.2f}%"},
                        {"label": "10日动量", "value": f"{data['momentum_10d']:.2f}%"},
                        {"label": "5日均线偏离", "value": f"{data['ma_gap_5']:.2f}%"},
                        {"label": "10日均线偏离", "value": f"{data['ma_gap_10']:.2f}%"},
                        {"label": "趋势强度", "value": f"{data['trend_strength']:.2f}"},
                        {"label": "10日波动率", "value": f"{data['volatility_10d']:.2f}%"},
                        {"label": "20日回撤", "value": f"{data['drawdown_20d']:.2f}%"},
                        {"label": "近20日平均成交额", "value": f"{data['avg_amount_20d'] / 100000000:.2f} 亿元"},
                        {"label": "综合得分", "value": f"{data['total_score']:.1f}"},
                        {"label": "资产类别", "value": asset_class},
                        {"label": "交易模式", "value": item.get("trade_mode", "-")},
                        {"label": "最新价格", "value": f"{item.get('latest_price', 0):.3f} 元"},
                        {"label": "一手份额", "value": f"{item.get('lot_size', 0):.0f} 份"},
                        {"label": "最小可买金额", "value": f"{item.get('min_order_amount', 0):.2f} 元"},
                        {"label": "本次建议金额", "value": f"{item['suggested_amount']:.2f} 元"},
                        {"label": "预计手续费", "value": f"{item.get('estimated_fee', 0):.2f} 元"},
                        {"label": "手续费占比", "value": f"{item.get('estimated_cost_rate', 0) * 100:.2f}%"},
                        {"label": "是否替代执行", "value": "是" if item.get("is_budget_substitute") else "否"},
                        {"label": "是否当前最优但买不起", "value": "是" if item.get("is_best_unaffordable") else "否"},
                        {"label": "是否买得起但不建议买", "value": "是" if item.get("is_affordable_but_weak") else "否"},
                        {"label": "执行层模式", "value": item.get("execution_timing_label") or item.get("execution_timing_mode", "-")},
                        {"label": "是否启用时间优化", "value": "是" if item.get("timing_rule_applied") else "否"},
                        {
                            "label": "建议执行时段",
                            "value": " / ".join(item.get("recommended_execution_windows", [])) or "-",
                        },
                        {
                            "label": "尽量避开时段",
                            "value": " / ".join(item.get("avoid_execution_windows", [])) or "-",
                        },
                    ],
                    "comparison": {
                        "rank_in_pool": int(data["rank_in_pool"]),
                        "rank_in_asset_class": rank_in_asset_class,
                        "score_gap": round(item["score_gap"], 2),
                        "rows": comparison_rows,
                    },
                    "allocation_reason": (
                        item.get("budget_substitute_reason")
                        if item.get("is_budget_substitute")
                        else item.get("best_unaffordable_reason")
                        if item.get("is_best_unaffordable")
                        else item.get("weak_signal_reason")
                        if item.get("recommendation_bucket") == "affordable_but_weak_recommendations"
                        else
                        f"系统先给 {asset_class} 这一类分配仓位，再在类内选出得分更高的 ETF。"
                        f"这只 ETF 当前拿到的建议金额是 {item['suggested_amount']:.2f} 元。"
                        if item.get("recommendation_bucket") == "executable_recommendations"
                        else f"系统先给 {asset_class} 这一类分配仓位，再在类内挑出更强的 ETF。"
                        f"这只 ETF 当前拿到的建议金额是 {item['suggested_amount']:.2f} 元，"
                        f"{item.get('not_executable_reason') or item.get('cost_reason')}"
                    ),
                    "risks": [
                        f"风险等级：{item['risk_level']}。",
                        f"默认止损 {item['stop_loss_pct'] * 100:.1f}%，默认止盈 {item['take_profit_pct'] * 100:.1f}%。",
                        item.get("trade_mode_note", ""),
                        item.get("timing_note", ""),
                        (
                            "这是预算内替代执行，不是当前主配置首选，所以仓位仍应更轻。"
                            if item.get("is_budget_substitute")
                            else
                            "这只 ETF 虽然买得起，但当前信号还不够强，不能为了凑一次交易就硬上。"
                            if item.get("recommendation_bucket") == "affordable_but_weak_recommendations"
                            else
                            "这不是因为它质量差，而是当前预算下还不够形成一笔可执行的场内交易。"
                            if item.get("recommendation_bucket") == "watchlist_recommendations"
                            else "不是它不好，而是当前金额下手续费占比偏高。"
                            if item.get("recommendation_bucket") == "cost_inefficient_recommendations"
                            else "即使可执行，也仍要注意分批建仓，避免一次性重仓。"
                        ),
                    ],
                }
            )

        return {"overall": overall, "items": items}

    def _overall_reasons(
        self,
        advice: dict[str, Any],
        plan: dict[str, Any],
        portfolio_summary: dict[str, Any],
        market_snapshot: dict[str, Any],
        candidate_count: int,
        total_count: int,
        top_score: float,
        recommendations: list[dict[str, Any]],
    ) -> list[str]:
        facts = plan.get("facts", {})
        current_pct = portfolio_summary["current_position_pct"] * 100
        target_pct = advice["target_position_pct"] * 100
        code = plan.get("reason_code")

        reasons = [
            f"当前市场状态是 {advice['market_regime']}。",
            f"当前仓位 {current_pct:.1f}%，系统目标仓位 {target_pct:.1f}%。",
        ]
        if any(item.get("timing_rule_applied") for item in recommendations):
            reasons.append("股票类 ETF 会额外给出执行时间段建议；债券、黄金、货币和跨境 ETF 不会被强行套用同一套开盘节奏。")

        if code == "no_candidates":
            reasons.append(f"今天通过筛选的 ETF 数量是 0/{total_count}，所以系统不建议强行出手。")
        elif code == "weak_market":
            reasons.append(
                f"当前市场偏弱，宽基强弱分 {market_snapshot['broad_index_score']:.1f}，趋势分 {market_snapshot['trend_score']:.1f}，所以先观望。"
            )
        elif code == "weak_score":
            reasons.append(
                f"候选池最高分只有 {top_score:.1f}，还没超过出手阈值 {facts.get('buy_score_threshold', 0):.1f}。"
            )
            reasons.append(f"虽然已有 {candidate_count}/{total_count} 只 ETF 通过筛选，但信号强度还不够。")
        elif code == "near_target_position":
            reasons.append("虽然有可关注的 ETF，但你当前仓位已经接近目标仓位，没有必要今天硬做一笔。")
            reasons.append(f"候选池最高分是 {top_score:.1f}，但仓位差距不大，所以系统优先选择不折腾。")
        elif code == "amount_below_min_advice":
            reasons.append(
                f"本次建议实际可投入金额约 {facts.get('deploy_amount', 0):.0f} 元，但按资产类别拆分后，单笔仍低于最小建议金额 {facts.get('min_advice_amount', 0):.0f} 元。"
            )
        elif code == "no_active_asset_classes":
            reasons.append("系统先看大类资产，再看类内 ETF。今天还没有资产类别同时满足配置权重和绝对趋势过滤。")
            reasons.append("这意味着不是单只 ETF 排名不够，而是当前整类资产都还没到适合出手的状态。")
        elif code == "watchlist_only_budget_limited":
            reasons.append(
                f"系统已经筛出适合参与的资产类别，但按当前可用现金 {facts.get('available_cash', 0):.0f} 元和本次分配金额，暂时没有标的能覆盖 1 手最小可买金额。"
            )
            reasons.append(
                f"因此系统把 {facts.get('watchlist_count', 0)} 只 ETF 先列入关注标的，而不是误导你去执行一笔实际上买不了的交易。"
            )
            if facts.get("best_unaffordable_symbol"):
                reasons.append(f"其中 {facts.get('best_unaffordable_symbol')} 仍是当前更优先的一只，只是这次预算还买不起。")
            if facts.get("affordable_but_weak_count", 0):
                reasons.append(
                    f"另外还有 {facts.get('affordable_but_weak_count', 0)} 只 ETF 虽然仓位内买得起 1 手，但分数或趋势还不够强，所以系统没有为了凑交易去推荐它们。"
                )
        elif code == "watchlist_only_budget_and_position_limited":
            reasons.append(
                f"系统已经筛出适合参与的资产类别，但主配置标的按当前可用现金 {facts.get('available_cash', 0):.0f} 元仍买不起 1 手。"
            )
            reasons.append(
                f"另外虽然有更便宜的 ETF 能买 1 手，但当前安全单笔上限只有 {facts.get('practical_buy_cap_amount', 0):.0f} 元，"
                "直接买入会明显超过当前目标仓位或单笔仓位上限。"
            )
            if facts.get("best_unaffordable_symbol"):
                reasons.append(f"当前最优但暂时买不起的是 {facts.get('best_unaffordable_symbol')}。")
            if facts.get("affordable_but_weak_count", 0):
                reasons.append(
                    f"同时还有 {facts.get('affordable_but_weak_count', 0)} 只 ETF 在当前仓位内买得起 1 手，但分数或趋势还不够强，系统会单独告诉你“买得起，但现在不建议买”。"
                )
        elif code == "affordable_but_weak_only":
            reasons.append(
                f"当前确实有 {facts.get('affordable_but_weak_count', 0)} 只 ETF 在本轮仓位内买得起 1 手。"
            )
            reasons.append("但它们的分数、趋势或资产类别优先级还不够强，所以系统不建议为了有交易而交易。")
        elif code == "cost_inefficient_only":
            reasons.append("系统已经筛出趋势不错的 ETF，但按这次建议金额估算，手续费占比偏高。")
            reasons.append(
                f"因此这次不急着执行，先等更大的可分配金额，或者等下一次更明确的配置机会。"
            )
        elif code == "asset_class_constraints_only":
            reasons.append("系统已经先完成了资产类别配置，再在类内挑了更强的 ETF。")
            reasons.append(
                f"但这批候选要么还买不起 1 手，要么手续费占比偏高，所以本次先不执行。"
            )
        elif code == "buy_candidates_one_lot_override":
            reasons.append(
                f"按常规分批节奏，这次计划投入约 {facts.get('deploy_amount', 0):.0f} 元，按资产类别拆开后很难形成一笔可执行交易。"
            )
            reasons.append(
                f"但你当前现金 {facts.get('available_cash', 0):.0f} 元足够买入 1 手 {facts.get('fallback_symbol', '')}，"
                f"所以系统切换成小资金可执行方案，优先给出这只当前买得起的高分 ETF。"
            )
        elif code == "budget_substitute_buy_candidates":
            reasons.append(
                f"按当前主配置，系统优先看 {facts.get('budget_substitute_primary_asset_class', '主配置资产')}，"
                "但对应 ETF 现在都买不起 1 手。"
            )
            substitute_count = int(facts.get("budget_substitute_count", 0))
            substitute_names = facts.get("budget_substitute_names", [])
            substitute_label = "、".join(substitute_names) if substitute_names else "预算内替代标的"
            reasons.append(
                f"为了让小资金用户也能看到真正买得起的标的，系统补充给出 {substitute_count or 1} 只预算内替代执行：{substitute_label}。"
            )
        elif code == "asset_class_buy_candidates":
            reasons.append(f"候选池最高分 {top_score:.1f}，高于出手阈值 {facts.get('buy_score_threshold', 0):.1f}。")
            reasons.append(
                f"系统先挑出 {facts.get('active_asset_class_count', 0)} 个当前趋势成立的资产类别，再在每一类里挑出更强的 ETF。"
            )
            reasons.append(f"当前仓位低于目标仓位，系统计划先分批投入约 {facts.get('deploy_amount', 0):.0f} 元，而不是一次满仓。")
            if facts.get("watchlist_count", 0):
                reasons.append(
                    f"其中有 {facts.get('watchlist_count', 0)} 只高分 ETF 因当前建议金额还不够买 1 手，被单独放进了关注标的。"
                )
            if facts.get("affordable_but_weak_count", 0):
                reasons.append(
                    f"另有 {facts.get('affordable_but_weak_count', 0)} 只 ETF 虽然在当前仓位内买得起，但信号还不够强，被单独放进了“买得起但现在不建议买”。"
                )
            if facts.get("cost_inefficient_count", 0):
                reasons.append(
                    f"另有 {facts.get('cost_inefficient_count', 0)} 只 ETF 因手续费占比偏高，被单独放进了暂不执行区。"
                )
        elif code == "trim_positions":
            reasons.append(f"当前仓位高于目标仓位，系统建议先回收约 {facts.get('reduction_amount', 0):.0f} 元，把风险降下来。")
        elif code == "no_positions_to_reduce":
            reasons.append("系统判断仓位应更低，但你当前没有持仓可减，所以最终给出不操作。")
        elif code == "trim_amount_below_min_advice":
            reasons.append(f"理论上应该减仓，但这次可减金额低于最小建议金额 {facts.get('min_advice_amount', 0):.0f} 元。")

        return reasons

    def _execution_rule(
        self,
        advice: dict[str, Any],
        facts: dict[str, Any],
        top_score: float,
        executable_recommendations: list[dict[str, Any]],
        affordable_but_weak_recommendations: list[dict[str, Any]],
        watchlist_recommendations: list[dict[str, Any]],
        cost_inefficient_recommendations: list[dict[str, Any]],
    ) -> dict[str, Any]:
        threshold = float(facts.get("buy_score_threshold", 0))
        market_passed = advice["market_regime"] != "观望"
        score_passed = top_score >= threshold
        budget_passed = bool(executable_recommendations) or not watchlist_recommendations
        fee_passed = bool(executable_recommendations) or not cost_inefficient_recommendations
        return {
            "rule": "系统先判断市场和分数是否达标，再按大类资产分配仓位，在类内做绝对趋势过滤和排序，最后检查单笔建议金额、1 手门槛、手续费占比，并按 ETF 类型补充执行时间建议。",
            "threshold": round(threshold, 2),
            "top_score": round(top_score, 2),
            "min_advice_amount": round(float(facts.get("min_advice_amount", 0)), 2),
            "lot_size": round(float(facts.get("lot_size", 0)), 2),
            "available_cash": round(float(facts.get("available_cash", 0)), 2),
            "practical_buy_cap_amount": round(float(facts.get("practical_buy_cap_amount", 0)), 2),
            "max_fee_rate_for_execution": round(float(facts.get("max_fee_rate_for_execution", 0)) * 100, 2),
            "active_asset_class_count": int(facts.get("active_asset_class_count", 0)),
            "market_passed": market_passed,
            "score_passed": score_passed,
            "budget_passed": budget_passed,
            "fee_passed": fee_passed,
            "executable_count": len(executable_recommendations),
            "affordable_but_weak_count": len(affordable_but_weak_recommendations),
            "watchlist_count": len(watchlist_recommendations),
            "cost_inefficient_count": len(cost_inefficient_recommendations),
            "result": (
                "满足出手条件，且存在可执行标的"
                if market_passed and score_passed and executable_recommendations
                else "仓位内买得起的标的存在，但当前分数或趋势还不够强"
                if market_passed and score_passed and affordable_but_weak_recommendations and not watchlist_recommendations
                else "分数和趋势都通过了，但当前手续费占比偏高"
                if market_passed and score_passed and cost_inefficient_recommendations and not watchlist_recommendations
                else "高分标的存在，但当前预算下暂时不可执行"
                if market_passed and score_passed and watchlist_recommendations
                else "暂不满足出手条件"
            ),
        }

    def _source_info(self, source: dict[str, Any], quality_summary: dict[str, Any]) -> list[dict[str, Any]]:
        captured_at = source.get("captured_at", "-")
        if captured_at != "-":
            captured_at = captured_at.replace("T", " ")[:16]
        cards = [
            {"label": "数据来源", "value": source.get("label", "-")},
            {"label": "接口/方法", "value": source.get("api", "-")},
            {"label": "数据类型", "value": source.get("data_type", "-")},
            {"label": "使用交易日", "value": source.get("trade_date", "-")},
            {"label": "抓取时间", "value": captured_at},
        ]
        if quality_summary:
            cards.extend(
                [
                    {"label": "验证状态", "value": quality_summary.get("verification_status", "-")},
                    {"label": "最新可用日期", "value": quality_summary.get("latest_available_date", "-")},
                    {
                        "label": "是否支持实时建议",
                        "value": "是" if quality_summary.get("supports_live_execution") else "否",
                    },
                ]
            )
        return cards

    def _market_score_details(self, market_snapshot: dict[str, Any]) -> list[dict[str, Any]]:
        raw = market_snapshot.get("raw", {})
        evidence = raw.get("evidence", {})
        formulas = raw.get("formulas", {})

        broad_momentum = float(evidence.get("broad_momentum", 0))
        broad_ma_gap = float(evidence.get("broad_ma_gap", 0))
        offense_score = float(evidence.get("offense_score", 0))
        defense_score = float(evidence.get("defense_score", 0))
        trend_positive_ratio = float(evidence.get("trend_positive_ratio", 0))
        trend_strength = float(evidence.get("trend_strength", 0))

        return [
            {
                "title": "宽基强弱分",
                "score": round(market_snapshot["broad_index_score"], 1),
                "formula": formulas.get("broad_index_score", "-"),
                "formula_with_values": (
                    f"50 + {broad_momentum:.2f} × 4 + {broad_ma_gap:.2f} × 3 = "
                    f"{market_snapshot['broad_index_score']:.1f}"
                ),
                "inputs": [
                    {"label": "宽基 5/10 日动量均值", "value": f"{broad_momentum:.2f}%"},
                    {"label": "宽基 10 日均线偏离", "value": f"{broad_ma_gap:.2f}%"},
                ],
                "meaning": "这个分数越高，说明主流宽基 ETF 最近越强。",
            },
            {
                "title": "风险偏好分",
                "score": round(market_snapshot["risk_appetite_score"], 1),
                "formula": formulas.get("risk_appetite_score", "-"),
                "formula_with_values": (
                    f"50 + ({offense_score:.2f} - {defense_score:.2f}) × 5 = "
                    f"{market_snapshot['risk_appetite_score']:.1f}"
                ),
                "inputs": [
                    {"label": "进攻池 5 日动量均值", "value": f"{offense_score:.2f}%"},
                    {"label": "防守池 5 日动量均值", "value": f"{defense_score:.2f}%"},
                ],
                "meaning": "这个分数越高，说明市场更偏向进攻，而不是躲到黄金和债券。",
            },
            {
                "title": "趋势分",
                "score": round(market_snapshot["trend_score"], 1),
                "formula": formulas.get("trend_score", "-"),
                "formula_with_values": (
                    f"{trend_positive_ratio:.2f} × 0.8 + {trend_strength:.2f} × 2.5 = "
                    f"{market_snapshot['trend_score']:.1f}"
                ),
                "inputs": [
                    {"label": "宽基站上 10 日均线比例", "value": f"{trend_positive_ratio:.2f}%"},
                    {"label": "宽基趋势强度均值", "value": f"{trend_strength:.2f}"},
                ],
                "meaning": "这个分数越高，说明不只是个别 ETF 强，而是整体趋势更整齐。",
            },
        ]

    def _watchlist(
        self,
        scored_df: pd.DataFrame,
        plan: dict[str, Any],
        watchlist_recommendations: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if watchlist_recommendations:
            watchlist = []
            for item in watchlist_recommendations:
                row = scored_df[scored_df["symbol"] == item["symbol"]]
                if row.empty:
                    continue
                data = row.iloc[0].to_dict()
                breakdown = self._parse_breakdown(data.get("breakdown_json"))
                watchlist.append(
                    {
                        "symbol": item["symbol"],
                        "name": item["name"],
                        "asset_class": item.get("asset_class", data.get("asset_class", data.get("category", "-"))),
                        "trade_mode": item.get("trade_mode", "-"),
                        "rank": item["rank"],
                        "score": round(float(item["score"]), 1),
                        "momentum_5d": round(float(data["momentum_5d"]), 2),
                        "volatility_10d": round(float(data["volatility_10d"]), 2),
                        "drawdown_20d": round(float(data["drawdown_20d"]), 2),
                        "note": item.get("execution_note", ""),
                        "min_order_amount": round(float(item.get("min_order_amount", 0)), 2),
                        "suggested_amount": round(float(item.get("suggested_amount", 0)), 2),
                        "budget_gap_to_min_order": round(float(item.get("budget_gap_to_min_order", 0)), 2),
                        "estimated_fee": round(float(item.get("estimated_fee", 0)), 2),
                        "estimated_cost_rate": round(float(item.get("estimated_cost_rate", 0)), 4),
                        "score_substitution": self._etf_score_substitution(breakdown),
                        "score_calculation": self._etf_score_calculation(data, breakdown),
                    }
                )
            return watchlist
        watchlist = []
        for _, row in scored_df.head(3).iterrows():
            data = row.to_dict()
            breakdown = self._parse_breakdown(data.get("breakdown_json"))
            watchlist.append(
                {
                    "symbol": str(row["symbol"]),
                    "name": str(row["name"]),
                    "rank": int(row["rank_in_pool"]),
                    "score": round(float(row["total_score"]), 1),
                    "momentum_5d": round(float(row["momentum_5d"]), 2),
                    "volatility_10d": round(float(row["volatility_10d"]), 2),
                    "drawdown_20d": round(float(row["drawdown_20d"]), 2),
                    "note": (
                        "今天暂不执行，但它仍是当前候选池里相对更强的 ETF。"
                        if plan.get("action") == "不操作"
                        else "它是当前建议重点关注的 ETF。"
                    ),
                    "score_substitution": self._etf_score_substitution(breakdown),
                    "score_calculation": self._etf_score_calculation(data, breakdown),
                }
            )
        return watchlist

    def _rejected_summary(self, filtered_df: pd.DataFrame) -> list[dict[str, Any]]:
        if filtered_df.empty or "filter_reasons" not in filtered_df.columns:
            return []
        counter: Counter[str] = Counter()
        rejected = filtered_df[~filtered_df["filter_pass"]]
        for reasons in rejected["filter_reasons"]:
            for reason in reasons:
                counter[str(reason)] += 1
        return [{"reason": reason, "count": count} for reason, count in counter.most_common(5)]

    def _parse_breakdown(self, value: Any) -> dict[str, float]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                loaded = json.loads(value)
            except json.JSONDecodeError:
                return {}
            if isinstance(loaded, dict):
                return loaded
        return {}

    def _etf_score_calculation(self, data: dict[str, Any], breakdown: dict[str, Any]) -> list[dict[str, Any]]:
        ma_total = float(data["ma_gap_5"]) + float(data["ma_gap_10"])
        rows = [
            {
                "label": "3日动量分位",
                "raw_value": f"{float(data['momentum_3d']):.2f}%",
                "score_value": float(breakdown.get("momentum_3d_score", 0)),
                "formula": self._term_formula(0.20, float(breakdown.get('momentum_3d_score', 0))),
            },
            {
                "label": "5日动量分位",
                "raw_value": f"{float(data['momentum_5d']):.2f}%",
                "score_value": float(breakdown.get("momentum_5d_score", 0)),
                "formula": self._term_formula(0.25, float(breakdown.get('momentum_5d_score', 0))),
            },
            {
                "label": "10日动量分位",
                "raw_value": f"{float(data['momentum_10d']):.2f}%",
                "score_value": float(breakdown.get("momentum_10d_score", 0)),
                "formula": self._term_formula(0.25, float(breakdown.get('momentum_10d_score', 0))),
            },
            {
                "label": "趋势强度分位",
                "raw_value": f"{float(data['trend_strength']):.2f}",
                "score_value": float(breakdown.get("trend_score", 0)),
                "formula": self._term_formula(0.15, float(breakdown.get('trend_score', 0))),
            },
            {
                "label": "均线位置分位",
                "raw_value": f"{ma_total:.2f}%",
                "score_value": float(breakdown.get("ma_score", 0)),
                "formula": self._term_formula(0.10, float(breakdown.get('ma_score', 0))),
            },
            {
                "label": "波动率惩罚",
                "raw_value": f"{float(data['volatility_10d']):.2f}%",
                "score_value": float(breakdown.get("volatility_penalty", 0)),
                "formula": self._term_formula(-0.03, float(breakdown.get('volatility_penalty', 0))),
            },
            {
                "label": "回撤惩罚",
                "raw_value": f"{float(data['drawdown_20d']):.2f}%",
                "score_value": float(breakdown.get("drawdown_penalty", 0)),
                "formula": self._term_formula(-0.02, float(breakdown.get('drawdown_penalty', 0))),
            },
            {
                "label": "流动性分位",
                "raw_value": f"{float(data['avg_amount_20d']) / 100000000:.2f} 亿元",
                "score_value": float(breakdown.get("liquidity_score", 0)),
                "formula": self._term_formula(0.10, float(breakdown.get('liquidity_score', 0))),
            },
        ]
        for row in rows:
            row["contribution"] = round(self._contribution_from_formula(row["formula"]), 2)
        return rows

    def _etf_score_substitution(self, breakdown: dict[str, Any]) -> str:
        return " ".join(
            [
                self._term_formula(0.20, float(breakdown.get("momentum_3d_score", 0))),
                self._term_formula(0.25, float(breakdown.get("momentum_5d_score", 0))),
                self._term_formula(0.25, float(breakdown.get("momentum_10d_score", 0))),
                self._term_formula(0.15, float(breakdown.get("trend_score", 0))),
                self._term_formula(0.10, float(breakdown.get("ma_score", 0))),
                self._term_formula(-0.03, float(breakdown.get("volatility_penalty", 0))),
                self._term_formula(-0.02, float(breakdown.get("drawdown_penalty", 0))),
                self._term_formula(0.10, float(breakdown.get("liquidity_score", 0))),
            ]
        ) + f" = {float(breakdown.get('formula_score', 0)):.2f}"

    def _term_formula(self, weight: float, score_value: float) -> str:
        sign = "+" if weight >= 0 else "-"
        factor = abs(weight)
        contribution = factor * score_value
        return f"{sign}{factor:.2f} × {score_value:.2f} = {contribution:.2f}"

    def _contribution_from_formula(self, formula: str) -> float:
        sign = -1 if formula.startswith("-") else 1
        value = formula.split("=")[-1].strip()
        return sign * float(value)
