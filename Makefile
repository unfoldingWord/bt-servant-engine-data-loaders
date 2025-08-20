PYFILES := $(shell git ls-files "*.py")
RUFF ?= .venv/bin/ruff
PYLINT ?= .venv/bin/pylint

.PHONY: fmt lint fix ruff-format ruff-check pylint typecheck pyright

fmt: ruff-format

ruff-format:
	$(RUFF) format .

ruff-check:
	$(RUFF) check .

pylint:
	$(PYLINT) $(PYFILES)

lint: ruff-check pylint

fix:
	$(RUFF) check --fix .
	$(RUFF) format .

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
