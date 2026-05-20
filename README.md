# QuantitativeStrategy Execution Framework

Public-facing execution-layer framework extracted from a private trading runtime.

## Current positioning

This repository is the **public/export surface** for the execution framework layer.
It is not the runtime truth-source, and it is not the primary day-to-day development surface.

The private strategy repository keeps:

- strategy logic and alpha semantics
- runtime worker and runtime wiring
- strategy-coupled adapters
- market/feature assembly that depends on private runtime semantics
- real runtime state, rollout artifacts, and operational outputs

This public repository keeps only the parts intended to be externalizable as an execution-layer framework.

## What this repository contains

- execution/runtime core models and engine
- Binance readonly / submit / reconcile / post-trade helpers
- runtime guard, runtime env, state store, runtime status utilities
- sender bridge / publisher / operator-facing execution support
- public tests for those reusable execution-layer capabilities
- deployment templates and boundary documentation

## What this repository does not contain

- strategy algorithms or alpha logic
- strategy-specific adapters
- runtime worker / orchestration wiring
- private market/feature assembly logic
- private rollout, handoff, tmp/out/runtime artifacts
- local runtime state/output files

## Repository layout

- `exec_framework/`: public execution framework package surface
- `tests/`: public framework tests
- `deploy/systemd/`: service templates/examples
- `docs/`: boundary notes and export/release guidance
- `runtime/`: local runtime state/output directory, ignored from git

## Packaging

This repository ships as a Python package via `pyproject.toml`.

## Boundary reference

See:

- `docs/public_private_boundary.md`
- `docs/public_extraction_notes.md`
- `docs/public_repo_transition_plan.md`
