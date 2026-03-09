SESSION_MODE_LABELS = {
    "intraday": "立即生成今天建议",
    "preopen": "生成开盘计划",
    "after_close": "生成明日预案",
    "closed": "生成下一交易日预案",
}

SESSION_MODE_HINTS = {
    "intraday": "当前处于交易时段，可以给出今天可执行的场内 ETF 建议。",
    "preopen": "当前处于开盘前阶段，更适合输出开盘计划而不是即时成交建议。",
    "after_close": "当前已经收盘，系统输出的是明日预案，不是假装现在能成交。",
    "closed": "当前为周末或休市，系统输出下一交易日预案。",
}
