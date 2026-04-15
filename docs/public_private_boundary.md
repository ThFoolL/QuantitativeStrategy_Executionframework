# Public / Private Boundary

## 结论

后续推荐采用“public 提供 execution framework，private 负责 strategy 与运行编排”的双仓结构。

## Public 仓应包含

补充边界说明：

- `models.py` 中允许存在少量“caller-owned extension state”字段，用于承接 private 仓已有状态结构；这不等于 public 框架对这些字段背后的策略语义做了承诺
- `mock_modules.py` 只作为本地 smoke/replay 兼容桩，允许保留少量 legacy 状态推进逻辑，但不应被视为真实执行器或公开策略参考实现

- `exec_framework/` 包本身
- 通用 exchange adapter、state store、runtime guard、status CLI
- 与策略无关的 post-trade / notify / sender bridge 逻辑
- 通用测试与最小脱敏样例骨架
- 与具体部署环境无关的模板文档

## Private 仓应包含

- 策略逻辑与特定 adapter
- runtime worker、调度入口、市场数据拼装
- 真实 rollout、演练记录、handoff、operator 文档
- runtime 输出、脱敏前样本、tmp/out 等工作区产物
- 任何账户、频道、路径、服务器约束

## 推荐依赖方式

短期不必立刻做成 pip 发布，private 仓可以先用下面任一种方式依赖 public 仓：

- 方式 A：把 public 仓放到 `external/QuantitativeStrategy_Executionframework/`
- 方式 B：用 git submodule 挂到 private 仓
- 方式 C：后续补 `pip install -e ../QuantitativeStrategy_Executionframework`

## Private 仓接入建议

建议 private 仓新增一层自己的 runtime entrypoint，例如：

- `private_runtime/runtime_worker.py`
- `private_runtime/strategy_adapter.py`
- `private_runtime/market_provider.py`

其中：

- `private_runtime/runtime_worker.py` 负责把策略模块、市场数据模块和 `exec_framework.LiveEngine` 组装起来
- `private_runtime/strategy_adapter.py` 负责把私有策略输出转成 `FinalActionPlan`
- `private_runtime/market_provider.py` 负责把私有市场/特征数据转成 `MarketSnapshot`

## 不建议的做法

- 把 `models.py` 中 legacy 字段默认解释为 public 策略模板
- 把 `mock_modules.py` 中的本地仓位推进逻辑当成真实执行语义或策略示例

- 继续把 private `live/` 整体复制到 public 仓
- 在 public README 中保留私有策略代号、值班说明或 rollout 语义
- 把 handoff、runtime、out、tmp 样本作为 public 测试数据或样例发布
- 让 public 仓声明自己可直接运行完整实盘 worker，但实际上缺少 strategy wiring
