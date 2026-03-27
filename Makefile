PYTHON ?= python3

.PHONY: test lint smoke

test:
	$(PYTHON) -m unittest discover -s tests

smoke:
	$(PYTHON) -m unittest tests.test_sample_strategy_adapter

lint:
	@echo "No dedicated linter configured yet; run tests for validation."
