# 回测性能优化说明

## 原来的时间主要花在哪里

回测最重的热点在 `BacktestRunner.run()` 的逐日循环里：

- 每个交易日
- 每只 ETF
- 都要把历史截到 `trade_date`
- 再跑一次 `DataQualityService.assess_history(...)`
- 再跑一次 `FeatureEngine.calculate(...)`

这样做虽然结果正确，但在同一时间区间反复调参、反复回测时，会重复做大量完全一样的原始数据准备工作。

## 这次优化了什么

### 1. 把数据准备拆成两层

- `load_raw_dataset(...)`
  - 只负责加载原始历史数据
- `prepare_precomputed_dataset(...)`
  - 负责把同一时间区间里可复用的日级特征和质量摘要预先算好

### 2. 预计算并缓存可复用结果

准备好的 dataset 现在会额外缓存：

- 每只 ETF、每个交易日的原始特征行
- 每只 ETF、每个交易日的数据质量 payload
- 每个交易日的截面特征表 `daily_feature_frames`
- 每个交易日的质量摘要 `daily_quality_summaries`

这样同一份 dataset 可以被多次回测直接复用，不必在每次 `run(...)` 里重新做日级特征准备。

### 3. `BacktestRunner` 优先走缓存快路径

`_build_daily_features(...)` 现在会：

- 如果 dataset 里已经有 `daily_feature_frames / daily_quality_summaries`
  - 直接取缓存
- 如果没有
  - 继续走原来的逐日现算路径

这保证了：

- 优化后可以提速
- 同时保留旧路径作为结果一致性的回归对照

### 4. 顺手减少了日循环里的 pandas 开销

这次还做了几处不改逻辑的小优化：

- fallback 路径里不再每次用布尔筛选整段历史，而是用预排序后的日期索引做前缀切片
- 不再在交易执行里反复 `to_dict(orient="records")`
- 简单价格映射不再用 `iterrows()`

## 为什么行为不变

这次没有改：

- 打分公式
- 分仓公式
- 执行规则
- 动作语义
- 阈值配置

预计算路径用的仍然是同一套：

- `DataQualityService.assess_history(...)`
- `FeatureEngine.calculate(...)`

只是把这些“原本在每次回测里每天都要重算”的步骤，提前到 dataset 准备阶段一次性完成，再在多次回测里复用。

## 如何验证没有逻辑漂移

测试里保留了两条路径做对照：

1. 原始 dataset 直接跑回测
2. 预计算后的 dataset 再跑回测

要求两边结果完全一致，至少覆盖：

- `metrics`
- `trades`
- `daily_decisions`
- `daily_curve`
- `effective_preferences`
- `overview`

另外还验证了：

- 同一份预计算 dataset 被重复 `run(...)` 时结果完全一致
