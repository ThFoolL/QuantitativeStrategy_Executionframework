# Public Repository Transition Plan

## 当前状态

当前 public 仓不是一个已经完全收口的纯 execution framework 仓。
它仍然保留了历史上直接在 public 仓开发 runtime 时留下的一部分内容，包括：

- runtime worker
- strategy-coupled adapter
- 市场数据与特征装配
- runtime-facing replay / compare / manual probe 工具
- 与当前真实运行版本高度耦合的测试

因此，当前仓库应该被视为：

> 处于过渡期的 public/export 仓，而不是已经彻底净化完成的最终发布仓。

## 新口径

后续治理口径固定为：

1. private 策略仓中的 `strategy_runtime/` 是 runtime 真相源
2. public 仓不再作为日常开发主现场
3. public 仓的更新方式改成“从 private runtime 中挑选、清洗、整理后发布”

## 当前 public 仓内容分层

### A. 后续大概率继续保留为 public 的内容
- 执行引擎核心模型与协议
- Binance readonly / submit / reconcile / post-trade 通用能力
- state store / runtime guard / status CLI
- sender bridge / publisher / 通用通知能力
- 与这些模块直接对应的通用测试

### B. 当前保留但后续应继续评估/拆分的内容
- `runtime_worker.py`
- `market_data.py`
- `feature_builder.py`
- `strategy_adapter_selector.py`
- `v6c_adapter_baseline.py`
- `v6c_adapter_wide_range_guard_w1.py`
- runtime-facing replay / compare / probe 工具

这些内容目前仍在仓内，属于过渡期遗留；后续要么迁回 private 真相源主开发、要么抽出为更中性的 public 版本。

### C. 不应再作为 public 主开发依据的内容
- 根目录 `runtime/` 下的现场产物
- 任何运行过程中的 state / receipt / dispatch preview
- 临时诊断和一次性排障输出

## 后续整理原则

1. 不再直接把 public 仓当成“当前真实运行版本”
2. private 先改，再决定 public 是否吸收
3. public 每次更新都按“发布整理”对待，而不是直接把所有私有改动平移进来
4. 优先让文档、目录语义和实际内容一致，避免再次出现 README/边界文档与仓内现实冲突
