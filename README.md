# ETF 实时决策助手

一个本地运行的 ETF 决策辅助系统。它不自动下单，核心职责是：

- 基于公开行情和本地配置生成 ETF 建议
- 把建议拆成可解释的类别分、入场分、持有分、退出分
- 记录建议、解释、成交和后续结果，方便复盘

## 当前架构

当前版本已经从旧的单一统一打分，重构成了：

1. 先做共享特征
2. 再做类别优先筛选
3. 再做类别内 ETF 评分
4. 最后按 T+0 / T+1 规则决定是否能立刻执行

核心类别固定为：

- `stock_etf`
- `bond_etf`
- `gold_etf`
- `cross_border_etf`
- `money_etf`

默认交易属性：

- `stock_etf -> t1`
- `bond_etf -> t0`
- `gold_etf -> t0`
- `cross_border_etf -> t0`
- `money_etf -> t0`

同时支持在 [config/tradability_map.yaml](config/tradability_map.yaml) 里做按代码覆盖。

## 决策流程

`DecisionEngine` 的主链路现在是：

1. 刷新或读取市场数据
2. 计算共享特征
3. 标准化 ETF 类别和交易属性
4. 先做类别分 `category_score`
5. 若进攻类边际不够，输出 `no_trade` 或 `park_in_money_etf`
6. 若类别通过，再做类别内 ETF 评分
7. 把 `target_holding_days` 映射到稳定周期档
8. 按生命周期阶段混合 `entry_score / hold_score / exit_score`
9. 生成最终动作
10. 按 T+0 / T+1 路由是否可立刻执行
11. 保存建议、解释和分项拆解

## 共享特征层

当前所有 ETF 都会先算一套共享基础特征，包括：

- 动量：`momentum_3d / 5d / 10d / 20d`
- 均线：`ma5 / ma10 / ma20`
- 趋势：`trend_strength`
- 均线偏离：`ma_gap_5 / ma_gap_10`
- 收益：`ret_1d`
- 波动：`volatility_5d / 10d / 20d`
- 回撤：`drawdown_20d`
- 流动性：`avg_turnover_20d / liquidity_score`
- 类别相对强弱：`category_return_10d / relative_strength_10d`
- 广度：`above_ma20_flag`

特征本身保存在 `etf_features` 表里，便于后续回测或复盘。

## 类别优先公式

进攻类类别先按下面的结构打分，权重来自 [config/category_profiles.yaml](config/category_profiles.yaml)：

```text
category_score =
    w_cat_mom   * category_momentum
  + w_cat_trend * category_trend
  + w_cat_br    * category_breadth
  - w_cat_vol   * category_volatility
  - w_cat_dd    * category_drawdown
```

其中：

- `category_momentum = mean(momentum_10d)`
- `category_trend = mean(trend_strength)`
- `category_breadth = mean(above_ma20_flag)`
- `category_volatility = mean(volatility_10d)`
- `category_drawdown = mean(abs(drawdown_20d))`

`money_etf` 不和进攻类走同一套抢 alpha 逻辑，而是用防守分：

```text
defensive_score =
    w_def_liq * liquidity_score
  - w_def_vol * volatility_10d
  - w_def_dd * abs(drawdown_20d)
```

## 类别内评分头

类别赢出后，再进入类别内评分头。每个头都会输出：

- `entry_score`
- `hold_score`
- `exit_score`

配置文件：

- [config/category_profiles.yaml](config/category_profiles.yaml)

当前头部逻辑：

- `stock_etf`
  - 更强调中短动量、趋势、相对强弱
- `bond_etf`
  - 更强调稳定性，弱化短期追涨
- `gold_etf`
  - 可以跟趋势，但更重视波动约束
- `cross_border_etf`
  - 看趋势，同时对波动和执行摩擦更严格
- `money_etf`
  - 只做防守停车，不参与激进 winner-take-all

最终决策分：

```text
decision_score =
    phase_entry * entry_score
  + phase_hold  * hold_score
  - phase_exit  * exit_score
```

## 持有周期与生命周期

系统不为每个精确天数单独建公式，而是映射到稳定档位：

- `0~1 天 -> intraday_t0`
- `2~10 天 -> swing`
- `11~19 天 -> 默认仍走 swing`
- `20~40 天 -> rotation`
- 进攻边不足时转 `defensive_cash`

配置文件：

- [config/horizon_profiles.yaml](config/horizon_profiles.yaml)
- [config/phase_blending.yaml](config/phase_blending.yaml)

生命周期按剩余天数比例切：

- `build_phase`
- `hold_phase`
- `exit_phase`

阶段不会改写公式，只会改变三段分的混合权重。

## 动作与执行路由

当前支持的动作码：

- `buy_open`
- `buy_add`
- `hold`
- `reduce`
- `sell_exit`
- `no_trade`
- `park_in_money_etf`

阈值和路由规则都在配置里：

- [config/action_thresholds.yaml](config/action_thresholds.yaml)

T+0：

- 允许同日买卖
- 但会检查：
  - 当日单标的最大交易次数
  - 当日最大 round trip 次数
  - 上笔成交后的冷却时间
  - 反手信号分差
  - 最小预期优势

T+1：

- 当天刚买入的标的，不允许同日卖出
- 系统仍会保留退出信号
- 会把动作标记成下一交易时段预案

## 解释与持久化

每条建议现在会额外保存：

- `category`
- `tradability_mode`
- `target_holding_days`
- `mapped_horizon_profile`
- `lifecycle_phase`
- `entry_score / hold_score / exit_score`
- `category_score`
- `executable_now`
- `blocked_reason`
- `planned_exit_days`
- `planned_exit_rule_summary`

解释页会说明：

- 为什么是这个类别先赢
- 类别分拆解
- 为什么是这个 ETF 胜出
- 三段分拆解
- 映射到哪个持有周期
- 当前处于哪个阶段
- 为什么能立刻执行，或者为什么不能
- 为什么是 `no_trade / park_in_money_etf`

## 配置文件

新引擎的核心配置已经拆到：

- [config/tradability_map.yaml](config/tradability_map.yaml)
- [config/category_profiles.yaml](config/category_profiles.yaml)
- [config/horizon_profiles.yaml](config/horizon_profiles.yaml)
- [config/phase_blending.yaml](config/phase_blending.yaml)
- [config/action_thresholds.yaml](config/action_thresholds.yaml)

权重不再深埋在业务逻辑里。

## 技术栈

- Python
- FastAPI
- Jinja2
- SQLite
- SQLAlchemy
- Pandas / Numpy
- AKShare

## 运行

建议 Python 3.10+。

```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
```

启动：

```bash
python scripts/run_local.py
```

或：

```bash
uvicorn app.main:app --reload
```

## 测试

```bash
pytest
```

当前已补的测试重点：

- 共享特征计算
- 类别优先评分
- 持有天数映射
- 生命周期切换
- T+0 / T+1 路由
- 解释里是否带类别分和三段分拆解

## 当前限制

- 还没有 broker 集成，也不会自动下单
- 不是高频系统，持有周期是稳定档位，不支持逐日重写公式
- T+0 的 expected edge 仍是分数代理，不是完整成交成本模型
- 类别分目前仍偏启发式，后续应继续结合回测校准
- 历史回放和建议复盘链路已经有基础存储，但还可以继续增强到更细粒度
- 若环境无法联网或没装依赖，仍可能退回模拟数据或无法完整跑测试
