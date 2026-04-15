# Public Extraction Notes

## 定位

这个 public 仓承载的是可复用的 execution framework 层，不是策略仓的公开镜像，也不是可直接实盘启动的完整 trading system。

## 当前保留为 public 的内容

- execution data models、engine、state store
- Binance readonly / submit / post-trade / reconcile helpers
- runtime env、runtime guard、runtime status CLI
- Discord payload builder 与 sender bridge
- 通用最小单测骨架、通用 systemd 模板

## 明确保持 private 的内容

- 策略信号、alpha、仓位规则与任何策略特定 adapter
- 私有 runtime worker / orchestration wiring
- rollout、handoff、out、tmp、runtime 样本与环境产物
- 私有 incident 文档、值班样本、频道配置、真实部署路径

## 去策略化处理原则

public 仓内允许保留“strategy module interface”这类抽象边界，因为 execution engine 需要上层决策模块接入；
但不应继续暴露：

- `v6c`、私有策略代号
- 私有部署批注
- 默认指向单一策略场景的文案
- 会让外部误解为仓库自带完整策略的 README/模板

## 当前整理方向

1. 让 README 直接表述“这是 execution framework 包”
2. 保留抽象协议与数据模型，但不补入任何私有策略实现
3. 默认示例改用更中性的样本值与占位符
4. private 仓通过文档声明依赖 public 仓，而不是把 private 资产继续复制进 public 仓
