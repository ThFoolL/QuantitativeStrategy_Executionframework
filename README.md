# QuantitativeStrategy Execution Framework

Public-facing execution/runtime repository derived from a private trading runtime.

## Current repository status

This repository is **currently a transition-stage public/export repository**.

It currently contains a transition-stage public/export surface focused on reusable execution-layer modules, while strategy wiring and runtime truth-source development are being moved and kept in the private strategy repository.

So at this moment, this repository should **not** be interpreted as a perfectly cleaned standalone framework package.
It is better understood as:

> a public/export snapshot that will be further cleaned as the private strategy repository becomes the primary runtime development source.

## Target direction

The intended steady-state model is:

1. the **private strategy repository** keeps the runtime truth-source and day-to-day runtime development
2. this **public repository** receives only the parts that are suitable for external release
3. public updates are prepared as curated release-style exports, not as the primary development surface

## What this repository currently contains

At the moment, the repository includes:

- execution/runtime core modules
- Binance readonly / submit / reconcile / post-trade helpers
- runtime guard, runtime env, runtime status, sender bridge
- tests and examples for those reusable execution-layer capabilities

## What should not be inferred from the current layout

The repository should not be interpreted as the long-term runtime truth-source.
The private strategy repository keeps the real runtime code and strategy wiring; this public repository keeps only the externalizable execution-layer framework surface.

## Repository layout

- `exec_framework/`: current public/export code surface
- `tests/`: current public/export test surface
- `deploy/systemd/`: service templates/examples
- `docs/`: boundary notes and extraction guidance
- `runtime/`: local runtime state/output directory, ignored from git

## Packaging

This repository ships as a Python package via `pyproject.toml`, but the current repository contents should still be treated as a transition-stage export surface rather than a fully finalized standalone framework product.

## Boundary reference

See `docs/public_private_boundary.md` and `docs/public_extraction_notes.md` for the intended long-term split between private runtime truth-source and public release/export content.
