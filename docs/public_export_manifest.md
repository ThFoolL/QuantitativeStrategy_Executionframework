# Public Export Manifest

This manifest records the intended public surface for the current clean export refresh.

## Package modules

- `exec_framework/__init__.py`
- `exec_framework/async_operation.py`
- `exec_framework/binance_exception_helpers.py`
- `exec_framework/binance_exception_policy.py`
- `exec_framework/binance_posttrade.py`
- `exec_framework/binance_readonly.py`
- `exec_framework/binance_readonly_pack.py`
- `exec_framework/binance_reconcile.py`
- `exec_framework/binance_submit.py`
- `exec_framework/discord_notify.py`
- `exec_framework/discord_publisher.py`
- `exec_framework/discord_sender_bridge.py`
- `exec_framework/engine.py`
- `exec_framework/executor_real.py`
- `exec_framework/mock_modules.py`
- `exec_framework/models.py`
- `exec_framework/position_fact_reconciler.py`
- `exec_framework/protective_orders.py`
- `exec_framework/runtime_env.py`
- `exec_framework/runtime_guard.py`
- `exec_framework/runtime_prepare_only_gate_check.py`
- `exec_framework/runtime_status_cli.py`
- `exec_framework/state_store.py`
- `exec_framework/strategy_protection_intent.py`
- `exec_framework/unified_risk_action.py`

## Docs and packaging

- `README.md`
- `pyproject.toml`
- `.gitignore`
- `deploy/systemd/runtime-worker.service.example`
- `docs/public_private_boundary.md`
- `docs/public_extraction_notes.md`
- `docs/public_export_manifest.md`

## Excluded from public export

- runtime worker / orchestration wiring
- strategy adapters and strategy-specific market/feature builders
- manual probes and real-trade sampling scripts
- rollout, incident, handoff, and operator-history documents
- runtime state, receipts, dispatch previews, account snapshots, and local output artifacts
