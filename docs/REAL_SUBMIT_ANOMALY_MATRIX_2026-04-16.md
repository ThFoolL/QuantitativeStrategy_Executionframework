# Real Submit Anomaly Matrix - 2026-04-16

> Scope: small-capital ETHUSDT live probes for v6c runtime/live execution hardening.
> Env path: `/root/.openclaw/workspace-mike/secrets/binance_api.env`.
> Safety rule: every real probe starts from exchange flat/open_orders=0 and must cleanup or stop before moving on.
> Submit/review boundary: see `docs/deploy_v6c/REAL_SUBMIT_ANOMALY_SUBMIT_CANDIDATES_2026-04-16.md`.

## Fully Passed / Closed

### Minimal real open/close sampling

- Sample: `docs/deploy_v6c/samples/real_trade_sampling/manual_runs/20260415T124844500183Z/`
- Result: passed.
- Covered:
  - real market entry/close around minimum notional
  - short confirmation window
  - final exchange flat/open_orders=0

### Full-chain closePosition protection

- Sample: `docs/deploy_v6c/samples/manual_full_chain_acceptance/20260415T132514612652Z/20260415T132514612652Z_ETHUSDT_full_chain_acceptance_summary.json`
- Result: passed.
- Covered:
  - real entry
  - closePosition protective submission
  - management stop update -> protective rebuild
  - final strict-flat-ready cleanup

### Protection semantic mismatch

- Sample: `docs/deploy_v6c/samples/manual_protection_semantic_mismatch/20260415T143902667562Z/20260415T143902667562Z_ETHUSDT_protection_semantic_mismatch_summary.json`
- Result: passed.
- Covered:
  - Binance-accepted protection-like order that is not runtime-accepted closePosition semantics
  - semantic mismatch classification path

### Partial protective missing / TP missing

- Sample: `docs/deploy_v6c/samples/manual_partial_protective_missing/20260415T170335975128Z/20260415T170335975128Z_ETHUSDT_partial_protective_missing_summary.json`
- Result: passed.
- Fix:
  - `live/protective_orders.py` now promotes one-leg missing cases into explicit `protection_tp_missing` / `protection_stop_missing` plus `partial_protective_missing`, instead of only generic `protective_orders_invalid` / `protective_order_missing`.
- Regression:
  - `tests.test_reconcile_protective_orders`
  - `tests.test_manual_partial_protective_missing_probe`

### Protective missing -> emergency reduce-only close

- Result: live chain passed; summary/operator projection tail fixed.
- Observed chain:
  - `protective_order_missing`
  - `risk_action=FORCE_CLOSE`
  - `stop_condition=position_open_without_protection`
  - reduce-only cleanup closes the real position
- Fix:
  - `live/runtime_status_cli.py` now selects effective recover facts from `recover_timeline` so later terminal confirmation does not hide the prior emergency-close recover fact.
- Regression:
  - `tests.test_runtime_status_runtime_like_views`
  - `tests.test_manual_protective_missing_emergency_close_probe`

### Lingering protective cleanup after flat

- Sample: `docs/deploy_v6c/samples/manual_lingering_protective_cleanup/20260415T210722861796Z/20260415T210722861796Z_ETHUSDT_lingering_protective_cleanup_summary.json`
- Result: passed.
- Covered:
  - real entry
  - closePosition protective order left behind after manual reduce-only close to flat
  - runtime cleanup path cancels lingering protective order
  - final exchange state flat/open_orders=0
- Regression:
  - `tests.test_manual_lingering_protective_cleanup_probe`
  - `tests.test_runtime_worker_autonomous`

### Submit auto-repair (-1021 / -2022)

- Checklist item: #8.
- Result: controlled non-live validation passed.
- Reason for non-live validation:
  - `-1021` requires deliberately invalid signed timestamps / recvWindow behavior.
  - `-2022` requires deliberately creating reduce-only conflict conditions.
  - Both are unsafe and noisy to force repeatedly in live small-capital probes.
- Covered by focused regression:
  - `tests.test_binance_exception_policy`
  - `tests.test_binance_exception_helpers`
  - `tests.test_async_operation_protection_followup`
  - `tests.test_executor_real_submit_gate`
  - `tests.test_runtime_guard_readonly_recheck`
- Verified command:

```bash
python3 -m unittest \
  tests.test_binance_exception_policy \
  tests.test_binance_exception_helpers \
  tests.test_async_operation_protection_followup \
  tests.test_executor_real_submit_gate \
  tests.test_runtime_guard_readonly_recheck
```

Result: `Ran 119 tests ... OK`.

### Multi-active async arbitration and history lifecycle

- Checklist items: #9 and #10.
- Result: controlled state-machine validation passed.
- Reason for non-live validation:
  - Multi-active arbitration and history migration are runtime state-machine invariants, not exchange behaviors.
  - Forcing overlapping `execution_confirm` / `protection_followup` / `submit_auto_repair` families live would create unnecessary order risk and does not add exchange-side evidence.
- Covered:
  - active operation priority: `submit_auto_repair` > `protection_followup` > `execution_confirm`
  - stale operation superseding
  - terminal migration into `async_operations.history`
  - lifecycle statuses: `succeeded`, `failed`, `exhausted`, `cancelled`, `superseded`
- Verified command:

```bash
python3 -m unittest \
  tests.test_async_operation_protection_followup \
  tests.test_runtime_guard_readonly_recheck \
  tests.test_runtime_worker_readonly_recheck
```

Result: `Ran 76 tests ... OK`.

### Freeze / recover operator consistency

- Checklist item: #11.
- Result: controlled runtime/status validation passed, backed by live samples from #5/#6/#7 and partial-missing probes.
- Covered:
  - emergency close facts are preserved in operator summary via effective `recover_timeline` selection
  - `protective_order_missing` + `FORCE_CLOSE` is not hidden by later terminal confirmation
  - `partial_protective_missing` maps to explicit `protection_tp_missing` / `protection_stop_missing`
  - runtime guard and status CLI report aligned stop reasons / stop conditions
- Verified command:

```bash
python3 -m unittest \
  tests.test_runtime_status_runtime_like_views \
  tests.test_runtime_status_cli_readonly_recheck \
  tests.test_runtime_guard_readonly_recheck \
  tests.test_runtime_worker_autonomous \
  tests.test_manual_protective_missing_emergency_close_probe \
  tests.test_manual_partial_protective_missing_probe
```

Result: `Ran 75 tests ... OK`.

## Real Samples Captured / Fixed In Tests

### Split-fill trade rows

- Fixture: `tests/fixtures/readonly_pack/real_split_fill_close_pack.json`
- Real behavior:
  - one close order was split into two user trade rows (`0.009 + 0.001`)
- Regression:
  - `tests.test_posttrade_confirm_classification`
  - `tests.test_readonly_pack_bridge`
- Expected behavior:
  - total executed qty is summed correctly
  - `fill_count=2`
  - confirmation remains stable

### reduceOnly filled but position not flat

- Sample: `docs/deploy_v6c/samples/manual_reduce_only_not_flat/20260415T145833903255Z/20260415T145833903255Z_ETHUSDT_reduce_only_not_flat_summary.json`
- Result: target sample captured.
- Real behavior:
  - reduce-only close filled part of the position
  - post position remained non-flat
  - note `reduce_only_filled_but_position_not_flat` appeared
- Probe:
  - `live/manual_reduce_only_not_flat_probe.py`

### Query mismatch short-window probe

- Probe: `live/manual_query_mismatch_probe.py`
- Current result:
  - no stable `order visible / userTrades missing` mismatch captured at `capture-delay-ms=50`
  - Binance returned order/trades/position facts quickly enough in the observed run
- Safety fix:
  - probe now uses `try/finally` cleanup so capture exceptions do not leave a position behind

## Remaining / Not Yet Closed

### filled_but_open_orders_still_live

- Probe: `live/manual_filled_with_residual_open_order_probe.py`
- Sample: `docs/deploy_v6c/samples/manual_filled_with_residual_open_order/20260415T181900446574Z/20260415T181900446574Z_ETHUSDT_filled_with_residual_open_order_summary.json`
- Result: target sample captured.
- Final working shape:
  - ordinary `LIMIT` GTC order without `reduceOnly`
  - short-position probe used a buy limit below market so it remained visible in `/fapi/v1/openOrders`
- Notes observed:
  - `filled_but_open_orders_still_live`
  - `residual_open_orders_after_fill`
  - `filled_but_executed_qty_less_than_requested`
  - `filled_but_position_changed_late`
  - `filled_without_user_trades`
- Cleanup:
  - residual order canceled
  - remaining position closed reduce-only
  - final exchange state flat/open_orders=0
- Historical failed attempts:
  - `LIMIT + reduceOnly`: rejected by Binance HTTP 400
  - `STOP_MARKET closePosition`: rejected by Binance HTTP 400 in the attempted payload shape
  - `TAKE_PROFIT_MARKET closePosition`: accepted by Binance, but did not remain visible in the ordinary `openOrders` view for the posttrade confirmer

## Current Relevant Regression Slice

Last verified command:

```bash
python3 -m unittest \
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

Result: `Ran 171 tests ... OK`.

## Latest Exchange Safety Check

Latest readonly probe during this work showed:

- `position=0`
- `open_orders=0`
- `nonzero_position_count=0`

No live position was left open after the probes.

## Final Checklist Closure - 2026-04-16

The remaining original checklist items have now been closed as follows:

- #7 `cleanup lingering protective`: real probe passed with `manual_lingering_protective_cleanup_probe`.
- #8 `submit auto-repair`: controlled non-live validation passed for timestamp/reduce-only exception handling.
- #9 `multi-active async arbitration`: controlled state-machine regression passed.
- #10 `async history lifecycle`: controlled state-machine regression passed.
- #11 `freeze/recover operator consistency`: controlled runtime/status validation passed, backed by live samples.
- #12 final exchange safety closure: readonly probe confirmed flat/open_orders=0.

Final checklist closure command:

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
  tests.test_manual_lingering_protective_cleanup_probe \
  tests.test_manual_partial_protective_missing_probe \
  tests.test_manual_protective_missing_emergency_close_probe \
  tests.test_manual_protection_semantic_mismatch_probe \
  tests.test_manual_query_mismatch_probe \
  tests.test_manual_reduce_only_not_flat_probe \
  tests.test_manual_filled_with_residual_open_order_probe
```

Result: `Ran 183 tests ... OK`.

Final exchange safety probe:

- `position=0`
- `open_orders=0`
- `nonzero_position_count=0`
