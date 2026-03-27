# QuantitativeStrategy Execution Framework

这是一个面向实盘执行层的 Python 框架草案，核心围绕 Binance futures API 的只读探测、下单守卫、成交确认、对账、状态落盘和通知桥接构造。

它不是完整交易系统，也不是生产策略仓的公开镜像。

## 当前定位

本仓只保留适合提交到 Git 的执行框架内容：

- 运行时核心数据模型、engine、state store
- Binance futures readonly / submit / post-trade / reconcile 相关代码
- Discord 通知桥接、状态查看 CLI、部署模板
- 通用单元测试
- 策略接入格式示例与部署文档

本仓**不包含生产策略本身**，只提供一个最小占位样例，用于说明策略如何产出 `FinalActionPlan` 并接入 `exec_framework.LiveEngine`。

## 重要边界

### 1. 当前仓库是 Binance-specific execution framework

虽然仓库在结构上保留了 `StrategyModule`、`ExecutorModule`、`PreRunReconcileModule` 等抽象边界，但当前真正完成实现并经过当前项目语义约束整理的，是围绕 **Binance futures API / 规则** 的执行链路。

不要把它理解成已经完全交易所无关。

当前 Binance-specific 主要体现在：

- 只读账户 / 持仓 / open orders / exchange rules 的读取方式
- 下单请求结构与 guardrail
- post-trade confirmation 语义
- reconcile 过程中使用的账户与持仓事实源
- symbol rules / qty step / min notional 等规则映射
- 部分通知与 operator 文案默认围绕 Binance 执行过程组织

### 2. 如果接入其他交易所或其他交易场景，需要额外适配

至少需要重新适配或重写这些层：

- readonly adapter
- submit adapter
- post-trade confirm / execution receipt parser
- reconcile logic
- exchange rules mapping
- notify / operator message mapping
- env 配置项与 deployment wiring

详见 `docs/binance_specific_boundary.md` 与 `docs/strategy_integration.md`。

## 目录结构

- `exec_framework/`：执行框架核心包
- `tests/`：通用测试
- `examples/`：策略占位样例与环境变量样例
- `deploy/systemd/`：systemd 部署模板
- `docs/`：仓库边界、接入说明、Binance-specific 说明

## 最小接入方式

私有策略仓通常需要自己保留一层 orchestration：

- 读取市场与特征数据
- 生成统一 `MarketSnapshot`
- 读取 / 对账 `LiveStateSnapshot`
- 调用私有策略适配器产出 `FinalActionPlan`
- 再由 `exec_framework` 的 executor / reconcile / state store 执行闭环

可参考：

- `examples/sample_strategy_adapter.py`
- `docs/strategy_integration.md`
- `deploy/systemd/runtime-worker.service.example`

## 安装

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .
python -m unittest discover -s tests
```

## 提交边界

建议提交本目录中的源码、测试、文档和模板；不要提交：

- 真实策略代码
- 真实 API key / secret / channel id
- runtime 状态文件与回执
- rollout/handoff 产物
- 私有 incident 样本

仓库边界说明见 `docs/public_private_boundary.md`。
