# public 仓提交前清理清单（2026-03-26）

## 结论

public 仓当前主体边界基本正确，但在提交前还需要做一次**产物清理 + 提交分组**，避免把缓存文件、边界不清内容或未来易漂移的说明一起打包。

## 已确认适合保留的内容

### package / code

- `pyproject.toml`
- `exec_framework/` 下通用执行框架代码
- `tests/` 下公共单测与最小样例骨架
- `deploy/systemd/runtime-worker.service.example`

### docs

- `README.md`
- `docs/public_private_boundary.md`
- `docs/public_extraction_notes.md`

## 本次发现的提交前清理点

### 必清理

- `exec_framework/__pycache__/`
- `tests/__pycache__/`

这些都属于本地产物，不应进入 commit。

### 建议复查

- `README.md`：确认不再出现任何 private 策略代号、私有频道、私有路径
- `docs/`：保持边界文档，不混入 rollout / handoff / oncall / 值班语义
- `tests/test_posttrade_confirmation_scenarios.py`：已改为文件内最小样例骨架；提交前确认不再依赖外部测试数据目录
- 默认值：`exec_framework/runtime_env.py` 里目前默认 symbol 为 `BTCUSDT`、Discord channel 为 placeholder，属于可公开的 closed-by-default 口径

## 建议 commit 分组

### commit 1：framework skeleton

- `pyproject.toml`
- `exec_framework/*.py`
- `deploy/systemd/runtime-worker.service.example`

说明：建立 public execution framework 包与最小部署模板。

### commit 2：tests and minimal scenarios

- `tests/test_*.py`
- `tests/test_posttrade_confirmation_scenarios.py`

说明：补公共行为测试与最小脱敏样例骨架，证明 execution framework 边界完整。

### commit 3：boundary docs

- `README.md`
- `docs/public_private_boundary.md`
- `docs/public_extraction_notes.md`
- `docs/precommit_cleanup_checklist_2026-03-26.md`

说明：明确 public/private 边界、提取背景与提交前清理结论。

## 不应提交到 public 的内容

- private strategy adapter / alpha logic
- private rollout / handoff / operator runbook
- runtime/、tmp/、out/ 等运行产物
- 任何真实 env、channel id、server path、订单样本、账户痕迹

## 推荐提交命令前自检

可在 public 仓执行：

```bash
git status --short
find exec_framework tests -type d -name '__pycache__'
pytest -q
```

若 `git status` 只剩 package / tests / docs / deploy 模板，则可以进入 commit 阶段。
