# Public Extraction Notes

## Purpose

This repository is a curated export of reusable execution-framework code. It is not a mirror of a private strategy repository and it is not a complete live trading system.

## Keep in public

- execution data models and engine contracts
- state persistence helpers
- exchange readonly / submit / reconcile / post-trade helpers
- runtime env, guard, status, and recovery helpers
- protective-order and position-fact reconciliation helpers
- notification payload and sender bridge utilities
- generic systemd templates
- tests that validate public execution-layer behavior without depending on private strategy semantics

## Keep private

- strategy signals, alpha logic, position rules, and strategy-specific adapters
- runtime worker / orchestration wiring
- market-data and feature-building pipelines
- real deployment notes, rollout records, incident matrices, handoff documents, and operator runbooks
- runtime output, receipts, dispatch previews, account snapshots, tmp/out artifacts, and environment files
- real account, channel, path, owner, or server-specific details

## Export rules

1. Prefer an allowlist export: copy only files that are intended to be public.
2. Treat runtime state and probe output as private, even if sanitized.
3. Keep examples generic and caller-owned.
4. Use neutral terminology for strategy hooks and caller-owned state.
5. Run a tracked-file check and keyword scan before committing each public export refresh.
