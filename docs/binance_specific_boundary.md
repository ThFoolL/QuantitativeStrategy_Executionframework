# Binance-Specific Boundary

## 结论

当前执行框架是以 **Binance futures API / 规则** 为核心构造的，不应对外表述成“已经完全交易所无关”。

## Binance-specific 具体落点

### readonly

当前只读链路默认围绕 Binance futures 的账户、持仓、挂单、规则与相关返回结构组织。

涉及模块：

- `exec_framework/binance_readonly.py`
- `exec_framework/binance_readonly_probe.py`
- `exec_framework/binance_readonly_pack.py`
- `exec_framework/binance_readonly_sample_capture.py`

### submit

下单侧当前默认使用 Binance 风格的下单请求、参数与 guardrail。

涉及模块：

- `exec_framework/binance_submit.py`
- `exec_framework/executor_real.py`

### reconcile / confirm

执行确认、持仓事实回读、open orders 分类、post-trade 状态确认，当前都按 Binance futures 的执行事实源设计。

涉及模块：

- `exec_framework/binance_posttrade.py`
- `exec_framework/binance_reconcile.py`
- `exec_framework/binance_exception_policy.py`
- `exec_framework/binance_exception_helpers.py`

### rules mapping

交易规则映射目前默认依赖 Binance 的 symbol rules 语义，例如：

- qty step
- min qty
- min notional
- symbol allowlist

### notify

虽然通知桥接本身相对通用，但很多 operator message 的上下文仍默认来自 Binance 执行链路，例如 submit gate、post-trade confirm、readonly recheck 等阶段。

## 如果接入其他交易所，需要改哪些层

至少要评估并适配：

1. `readonly`：账户 / 持仓 / open orders / fills 的读取器
2. `submit`：订单请求结构、签名、路由与错误处理
3. `reconcile`：执行前后状态回读逻辑
4. `post-trade confirm`：成交确认与未决状态分类
5. `rules mapping`：交易所规则到本框架守卫字段的映射
6. `notify`：告警、确认、operator 文案的阶段与字段
7. `runtime env`：配置项、secret、部署模板

## 什么不需要强行改成交易所无关

不建议为了“看起来抽象”而提前过度泛化。

在当前阶段，更合理的做法是：

- 明确说明当前是 Binance-first
- 把抽象边界保留在 `engine/models` 层
- 把交易所适配工作集中在 adapter / reconcile / submit / rules mapping 层

这样边界更诚实，也更利于后续逐层迁移。
