# ETF 实时决策助手

一个面向投资小白的、本地运行的 ETF 决策辅助系统。它不做自动下单，只做三件事：

- 基于公开数据筛选 ETF
- 给出当天可执行建议或下一交易日预案
- 解释为什么这样建议，并记录你的实际交易与绩效

## V1 能做什么

- 初始化用户资金与风险偏好
- 刷新 ETF 数据
- 生成一次今日建议 / 开盘计划 / 明日预案 / 下一交易日预案
- 展示推荐 ETF、建议金额、仓位、风险和解释
- 记录一笔实际成交
- 查看当前持仓与简单绩效

## 技术栈

- Python
- FastAPI
- Jinja2
- SQLite
- SQLAlchemy
- Pandas / Numpy
- AKShare

## 项目结构

```text
app/                 后端应用
templates/           页面模板
static/              CSS/JS
config/              ETF 白名单与规则配置
data/                SQLite 数据库与缓存目录
tests/               基础测试
scripts/             辅助脚本
frontend/            前端文案与 UI 契约补充文件
```

## 安装

建议使用 Python 3.10+。

```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
```

## 启动

方式一：

```bash
python scripts/run_local.py
```

方式二：

```bash
uvicorn app.main:app --reload
```

启动后访问：

- `http://127.0.0.1:8000/`

## 页面说明

- 首页 / 仪表盘
  - 显示总资金、现金、持仓市值、累计收益、主操作按钮
- 今日建议页
  - 显示建议动作、推荐 ETF、每只建议金额、综合得分
- 解释详情页
  - 显示整体市场判断、ETF 分项得分、数据证明、风险说明
- 持仓页
  - 显示当前持仓和成交录入表单
- 绩效页
  - 显示累计收益曲线、资产分布、交易记录

## 核心 API

- `POST /api/init-user`
- `POST /api/refresh-data`
- `POST /api/decide-now`
- `POST /api/record-trade`
- `GET /api/portfolio`
- `GET /api/performance`
- `GET /api/last-advice`
- `GET /api/advice/{id}`
- `GET /api/explanation/{id}`

## 交易时段逻辑

主按钮全天可点，但建议语义会自动切换：

- 盘中：`立即生成今天建议`
- 开盘前：`生成开盘计划`
- 收盘后：`生成明日预案`
- 周末 / 休市：`生成下一交易日预案`

系统不会在夜间伪装成“实时可成交建议”。

## 数据源说明

- 优先尝试使用 AKShare 拉取 ETF 历史数据
- 如果本地未安装 AKShare 或当前环境无法联网，系统会自动退回到内置模拟数据

这保证了项目可以本地跑通页面、数据库、策略和解释链路，但模拟数据不代表真实市场判断。

## 测试

```bash
pytest
```

## 当前限制

- 中国市场休市日默认未内置完整年度日历，`config/market_calendar.yaml` 可扩展
- 建议命中率和胜率统计目前是 MVP 版本
- 没有对接券商，不支持自动下单
- 若没有真实行情数据，系统会回退到模拟数据
