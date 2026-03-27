# Strategy Integration Guide

## 目标

本仓不提供生产策略，只提供一个最小样例，说明私有策略如何接入执行框架。

## 接入边界

策略侧需要负责两件事：

1. 生成统一的 `MarketSnapshot`
2. 把策略意图转换成 `FinalActionPlan`

执行框架侧负责：

- 读取与保存状态
- reconcile
- submit guardrail
- post-trade confirm
- operator-facing status / notify

## `FinalActionPlan` 的最小理解

策略最终并不是直接“发单”，而是给执行器一个结构化动作意图。

典型字段包括：

- `action_type`：例如 `hold` / `open` / `close` / `trim` / `add`
- `target_strategy`：仓位归属策略标签
- `target_side`：`long` / `short`
- `reason`：动作原因，给日志和 operator 看
- `qty_mode` / `qty`：数量模式与数量
- `price_hint` / `stop_price` / `risk_fraction`
- `requires_execution`：是否真的进入执行链路

## 占位样例说明

见 `examples/sample_strategy_adapter.py`。

该样例只做三件事：

- 当状态不一致时返回 `hold`
- 当已经有仓位时返回 `hold`
- 当状态允许且市场状态正常时，给出一个最小 `open` 示例

它只是说明格式，不代表生产策略逻辑。

## 推荐的 private 仓编排方式

private 仓建议自行保留：

- 市场数据提供层
- 特征构建层
- 真实策略模块
- runtime worker

接线顺序建议：

1. market provider 生成 `MarketSnapshot`
2. state store 读出 `LiveStateSnapshot`
3. reconcile 得到统一状态快照
4. strategy adapter 输出 `FinalActionPlan`
5. executor 根据 guardrail 与交易所适配层决定是否执行
6. state store 落盘 `ExecutionResult`

## 重要提醒

`FinalActionPlan` 只是策略意图，不是成交事实。

真实成交、均价、剩余仓位、费用、是否仍持仓，必须以后续交易所确认与 reconcile 结果为准，不能因为策略已经产出 `FinalActionPlan` 就直接把状态视作已确认。
