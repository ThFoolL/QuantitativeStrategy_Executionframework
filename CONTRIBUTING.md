# Contributing

## Scope

This repository is intended to stay focused on the execution framework layer.
Please keep contributions inside these boundaries:

- runtime engine, state, reconcile, confirm, and submit flow
- Binance futures adapters and rule handling
- notification bridge and operator-facing status helpers
- deployment templates and framework-level docs
- strategy integration examples that demonstrate interfaces only

Please do not add:

- private production strategy logic
- secrets, runtime outputs, audit artifacts, or handoff bundles
- exchange-agnostic claims unless the code has really been generalized

## Development

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .
python -m unittest discover -s tests
```

## Style

- Prefer small, reviewable changes.
- Preserve current runtime semantics unless the change explicitly fixes a bug.
- When changing operator/status wording, keep execution facts and preview facts clearly separated.
- When changing exchange behavior, update the relevant docs under `docs/`.

## Strategy examples

Examples may show how to produce `FinalActionPlan`, but they should remain placeholders.
Do not upstream proprietary alpha logic into this repository.
