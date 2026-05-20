# Public / Private Boundary

## Conclusion

This repository is the public execution-framework surface. Caller-owned private repositories remain the source of truth for strategy logic, runtime orchestration, deployment operations, and account-specific state.

## Public repository includes

- `exec_framework/` package modules that are reusable across strategies
- execution data models, engine contracts, state store, runtime env, runtime guard, and runtime status helpers
- exchange readonly / submit / reconcile / post-trade helpers
- protective-order and position-fact reconciliation helpers
- notification payload and sender bridge utilities
- generic tests and deployment templates that do not depend on a private strategy or rollout history

Some model fields are intentionally extension-friendly so downstream runtimes can carry caller-owned state through the execution layer. Those fields do not make this repository a strategy implementation.

## Private / downstream repository includes

- strategy signal generation and strategy-specific adapters
- runtime worker / scheduler / orchestration wiring
- market-data and feature-building pipelines
- real rollout notes, incident samples, handoff records, operator runbooks, and account-specific procedures
- runtime outputs, receipts, dispatch previews, snapshots, local state, tmp/out artifacts, and secrets
- real deployment paths, channel IDs, owner IDs, credentials, and environment files

## Recommended integration

A downstream runtime should depend on this repository as a package or submodule, then provide its own orchestration layer, for example:

- `private_runtime/runtime_worker.py` wires the strategy, market provider, `exec_framework.LiveEngine`, persistence, and notification hooks
- `private_runtime/strategy_adapter.py` converts caller-owned strategy output into `FinalActionPlan`
- `private_runtime/market_provider.py` converts caller-owned market data into `MarketSnapshot`

## Do not do this

- Do not copy a full private runtime into this public repository.
- Do not publish rollout, incident, probe, handoff, or runtime-state artifacts as public examples.
- Do not make the public README imply this repository can directly run a complete live trading worker.
- Do not treat compatibility helpers or test fixtures as public strategy reference implementations.
