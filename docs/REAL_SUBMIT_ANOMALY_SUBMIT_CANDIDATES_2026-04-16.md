# Real Submit Anomaly Submit Candidates - 2026-04-16

> Purpose: keep the anomaly-matrix work separable from older dirty worktree changes.

## Core Code

- `live/binance_submit.py`
- `live/protective_orders.py`
- `live/runtime_status_cli.py`

## New Real-Money Probe Entrypoints

- `live/manual_filled_with_residual_open_order_probe.py`
- `live/manual_partial_protective_missing_probe.py`
- `live/manual_protection_semantic_mismatch_probe.py`
- `live/manual_protective_missing_emergency_close_probe.py`
- `live/manual_query_mismatch_probe.py`
- `live/manual_reduce_only_not_flat_probe.py`

## Tests / Fixtures

- `tests/test_binance_time_sync.py`
- `tests/test_posttrade_confirm_classification.py`
- `tests/test_readonly_pack_bridge.py`
- `tests/test_reconcile_protective_orders.py`
- `tests/test_runtime_status_runtime_like_views.py`
- `tests/test_manual_filled_with_residual_open_order_probe.py`
- `tests/test_manual_partial_protective_missing_probe.py`
- `tests/test_manual_protection_semantic_mismatch_probe.py`
- `tests/test_manual_protective_missing_emergency_close_probe.py`
- `tests/test_manual_query_mismatch_probe.py`
- `tests/test_manual_reduce_only_not_flat_probe.py`
- `tests/fixtures/readonly_pack/real_split_fill_close_pack.json`

## Docs

- `docs/deploy_v6c/TODO.md`
- `docs/deploy_v6c/REAL_SUBMIT_ANOMALY_MATRIX_2026-04-16.md`
- `docs/deploy_v6c/REAL_SUBMIT_ANOMALY_SUBMIT_CANDIDATES_2026-04-16.md`

## Key Passing Samples To Keep

- `docs/deploy_v6c/samples/manual_full_chain_acceptance/20260415T132514612652Z/20260415T132514612652Z_ETHUSDT_full_chain_acceptance_summary.json`
- `docs/deploy_v6c/samples/manual_protection_semantic_mismatch/20260415T143902667562Z/20260415T143902667562Z_ETHUSDT_protection_semantic_mismatch_summary.json`
- `docs/deploy_v6c/samples/manual_reduce_only_not_flat/20260415T145833903255Z/20260415T145833903255Z_ETHUSDT_reduce_only_not_flat_summary.json`
- `docs/deploy_v6c/samples/manual_partial_protective_missing/20260415T170335975128Z/20260415T170335975128Z_ETHUSDT_partial_protective_missing_summary.json`
- `docs/deploy_v6c/samples/manual_filled_with_residual_open_order/20260415T181900446574Z/20260415T181900446574Z_ETHUSDT_filled_with_residual_open_order_summary.json`
- `docs/deploy_v6c/samples/readonly_capture/manual_partial_fill_like_20260415T_order_8389766156216231335.json`

## Optional Failure/Debug Samples

Failure/debug samples under these directories are useful for audit, but can be excluded from a minimal commit if repository size/noise matters:

- `docs/deploy_v6c/samples/manual_filled_with_residual_open_order/`
- `docs/deploy_v6c/samples/manual_partial_protective_missing/`
- `docs/deploy_v6c/samples/manual_protective_missing_emergency_close/`
- `docs/deploy_v6c/samples/manual_query_mismatch/`

## Verified Regression Slices

Large relevant slice:

```bash
python3 -m unittest \
  tests.test_binance_time_sync \
  tests.test_reconcile_protective_orders \
  tests.test_runtime_status_runtime_like_views \
  tests.test_posttrade_confirm_classification \
  tests.test_position_fact_reconciler \
  tests.test_runtime_worker_autonomous \
  tests.test_runtime_worker_readonly_recheck \
  tests.test_async_operation_protection_followup \
  tests.test_async_position_confirmed_writeback \
  tests.test_runtime_guard_readonly_recheck \
  tests.test_runtime_status_cli_readonly_recheck \
  tests.test_manual_partial_protective_missing_probe \
  tests.test_manual_protective_missing_emergency_close_probe \
  tests.test_manual_protection_semantic_mismatch_probe \
  tests.test_manual_query_mismatch_probe \
  tests.test_manual_reduce_only_not_flat_probe \
  tests.test_manual_filled_with_residual_open_order_probe
```

Result: `Ran 179 tests ... OK`.

Focused final slice:

```bash
python3 -m unittest \
  tests.test_binance_time_sync \
  tests.test_manual_filled_with_residual_open_order_probe \
  tests.test_posttrade_confirm_classification \
  tests.test_runtime_status_runtime_like_views \
  tests.test_reconcile_protective_orders
```

Result: `Ran 30 tests ... OK`.

## Latest Exchange Safety Check

The latest readonly probe after the final residual-open-order run showed:

- `position=0`
- `open_orders=0`
- `nonzero_position_count=0`
