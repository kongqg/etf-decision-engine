# Advice View Model

`advice` 页面需要这些字段：

- `action`
- `market_regime`
- `summary_text`
- `risk_text`
- `target_position_pct`
- `current_position_pct`
- `items[]`

`items[]` 每项需要：

- `symbol`
- `name`
- `suggested_amount`
- `score`
- `reason_short`
- `risk_level`
- `trigger_price_low`
- `trigger_price_high`
- `stop_loss_pct`
- `take_profit_pct`
