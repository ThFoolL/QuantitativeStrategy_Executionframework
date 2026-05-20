# QuantitativeStrategy Execution Framework

A public, strategy-agnostic execution framework extracted from a private trading runtime.

This repository is intentionally **not** a complete trading system. It provides reusable execution-layer components that a caller-owned strategy/runtime can import and wire together.

## Scope

This public repository contains:

- execution data models and engine contracts
- local state persistence helpers
- runtime environment and guard helpers
- Binance readonly / submit / reconcile / post-trade helpers
- protective-order and position-fact reconciliation helpers
- Discord notification payload/sender bridge utilities
- systemd service template examples
- public, strategy-neutral tests for the reusable execution layer

## Out of scope

The following belong in the caller/private runtime and are deliberately not included here:

- strategy signal generation, alpha logic, and strategy-specific adapters
- runtime worker / orchestration wiring
- market-data and feature-building pipelines
- rollout notes, incident samples, handoff records, and operator runbooks
- runtime output directories, dispatch previews, receipts, account snapshots, and local state
- real deployment paths, channel IDs, account-specific configuration, and secrets

## Repository layout

- `exec_framework/` — reusable execution-layer package
- `tests/` — public tests for the reusable execution-layer surface
- `deploy/systemd/` — generic service template examples
- `docs/` — public/private boundary and extraction guidance
- `runtime/` — ignored local runtime output directory, not part of the public export

## Integration model

A private or downstream runtime should provide its own orchestration layer, for example:

- strategy adapter: converts caller-owned strategy output into `FinalActionPlan`
- market provider: converts caller-owned market/feature data into `MarketSnapshot`
- runtime worker: wires strategy, market provider, `LiveEngine`, persistence, and notification hooks

The public framework should be treated as an importable execution package, not as the runtime truth source.

## Boundary reference

See:

- `docs/public_private_boundary.md`
- `docs/public_extraction_notes.md`
