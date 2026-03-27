# Public / Private Boundary

## 结论

推荐采用双仓结构：

- public 仓提供 execution framework
- private 仓保留生产策略、市场特征、运行编排与真实部署资产

## public 仓应包含

- `exec_framework/` 包本身
- 通用数据模型、engine、state store、runtime guard、status CLI
- Binance futures readonly / submit / reconcile / post-trade 相关实现
- Discord sender bridge / notify / publisher
- 通用测试
- 部署模板与接入文档
- 最小策略接入样例

## private 仓应包含

- 生产策略逻辑与 alpha
- 私有 strategy adapter
- 市场数据拼装与特征构建
- runtime worker / orchestration entrypoint
- rollout、handoff、runbook、operator 内部文档
- runtime 产物、脱敏前样本、临时文件
- 任何账户、权限、频道、主机路径与环境约束

## 当前仓为什么不直接附带完整策略

因为执行框架与策略语义不是同一层。

本仓保留的是：

- `MarketSnapshot`
- `LiveStateSnapshot`
- `FinalActionPlan`
- `ExecutionResult`
- `LiveEngine`

这些定义的是执行层与策略层之间的接缝，而不是生产策略本身。

## 推荐的 private 接入结构

例如 private 仓可保留：

- `private_runtime/runtime_worker.py`
- `private_runtime/strategy_adapter.py`
- `private_runtime/market_provider.py`

职责建议：

- `runtime_worker.py`：组装运行链路
- `strategy_adapter.py`：把私有策略输出转成 `FinalActionPlan`
- `market_provider.py`：把私有市场/特征数据转成 `MarketSnapshot`

## 明确不要放进 public 仓的内容

- 当前生产策略源码
- 真实调度入口
- 实盘运行快照
- 真实账户样本
- 值班痕迹、交接文档、事故记录
- 任何会让外部误解为“克隆下来即可完整实盘”的私有接线代码
