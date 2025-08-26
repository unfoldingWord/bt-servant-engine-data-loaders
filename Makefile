PYFILES := $(shell git ls-files "*.py")
# Preferred interpreter. Override with `make PY=.venv/bin/python check`.
PY ?= python
RUFF_CMD ?= $(PY) -m ruff
PYLINT ?= .venv/bin/pylint
PYTEST ?= .venv/bin/pytest

.PHONY: fmt lint fix ruff-format ruff-check pylint typecheck pyright test check

fmt: ruff-format

ruff-format:
	$(RUFF_CMD) format .

ruff-check:
	$(RUFF_CMD) check .

pylint:
	$(PYLINT) $(PYFILES)

lint: ruff-check pylint

fix:
	$(RUFF_CMD) check --fix .
	$(RUFF_CMD) format .

typecheck:
	.venv/bin/mypy .

pyright:
	@if command -v pyright >/dev/null 2>&1; then \
	  pyright ; \
	elif command -v npx >/dev/null 2>&1; then \
	  npx --yes pyright ; \
	else \
	  echo "Pyright not installed (requires Node). Skipping." ; \
	fi

test:
	$(PY) -m pytest -q

check: fix lint typecheck test
