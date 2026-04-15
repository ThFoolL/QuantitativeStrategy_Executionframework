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
