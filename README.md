# QuantitativeStrategy Execution Framework

Reusable execution-layer framework extracted from a private trading system.

## What this repository is

This repository is meant to look and behave like a standalone execution framework, not a strategy repository copy.
It keeps only the generic runtime pieces needed to:

- load environment and runtime state
- read exchange state through readonly adapters
- submit orders behind explicit guardrails
- classify post-trade confirmation outcomes
- persist execution state and operator-facing status
- prepare Discord notifications through a guarded sender bridge

## What this repository does not include

The following stay outside this public repository:

- strategy algorithms and alpha logic
- strategy-specific adapters and orchestration entrypoints
- private rollout notes and deployment handoff bundles
- runtime outputs, temporary files, and local secrets
- private incident samples or environment-specific channel IDs

## Repository layout

- `exec_framework/`: reusable Python package for execution/runtime primitives
- `tests/`: public unit tests and minimal in-file sample scenarios
- `deploy/systemd/`: generic service template example
- `docs/`: repository boundary and extraction notes

## Packaging

This repository ships as a small Python package via `pyproject.toml`.
The package is intentionally lightweight and does not bundle a strategy worker entrypoint.

## Sample defaults

- `BTCUSDT` is used as a neutral sample symbol in tests and CLI examples; override it through env in real deployments.
- Discord targets, env paths, and transport settings use placeholders or closed-by-default defaults.

## Private repository relationship

The intended follow-up model is:

1. private repository keeps strategy logic and runtime worker wiring
2. public repository provides reusable execution framework modules
3. private repository imports from this package by path dependency, git submodule/subtree, or later pip packaging

See `docs/public_private_boundary.md` for the concrete boundary.
