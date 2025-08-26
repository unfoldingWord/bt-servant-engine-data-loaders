# Repository Guidelines

## Project Structure & Module Organization
- `config.py`: Central settings via `pydantic-settings` (reads `.env`). Keys: `OPENAI_API_KEY`, `DATA_LOADERS_LOG_LEVEL`.
- `logger.py`: Shared logger factory. Writes to `logs/bt_servant_dataloaders.log` and console.
- `old_load_bsb.py`: Loader prototype that fetches BSB text and groups semantic chunks using OpenAI.
- `.venv/`: Local virtual environment (ignored). `logs/` is created at runtime.

## Build, Test, and Development Commands
- Create env: `python -m venv .venv && source .venv/bin/activate`
- Install deps: `pip install -r requirements.txt`
- Configure env: `export OPENAI_API_KEY=...` or add to `.env`; optional: `export DATA_LOADERS_LOG_LEVEL=debug`.
- Run loader locally: `python load_bsb.py` (no LLM calls) or `python old_load_bsb.py` (prototype).
- Lint (ruff + pylint): `make lint` (auto-fix with `make fix`).
- Type check (mypy): `make typecheck`.
- Tests (pytest): `make test` (or `python -m pytest -q`).
- All checks (format → lint → typecheck → tests): `make check`.
  - Tip for agents/CI: you can force the interpreter used by Make by running `make PY=.venv/bin/python check`.
- Example one-shot: `OPENAI_API_KEY=... python old_load_bsb.py`

## Coding Style & Naming Conventions
- Follow PEP 8. Indentation: 4 spaces. Use type hints.
- Tools: Ruff (formatter + lint) and Pylint. Keep both clean before committing.
- Modules/files: `snake_case.py`; functions/variables: `snake_case`; constants: `UPPER_CASE`; classes: `CapWords`.
- Logging: use `get_logger(__name__)`; avoid `print` in modules.
- Logger placement: initialize once at module scope near the top (after imports), e.g. `logger = get_logger(__name__)`. Do not create loggers inside functions or mid-file unless necessary to avoid a circular import.
- Side effects: avoid heavy work at import time; logger creation at import is fine.
- Configuration: read from `config` (do not `os.getenv` directly in modules).

## Testing Guidelines
- Framework: pytest. Place tests in `tests/` mirroring modules, e.g., `tests/test_logger.py`.
- Run tests: `make test` or `pytest -q`.
- Isolate network/API: mock `requests.get` and OpenAI client; do not hit real endpoints in unit tests.
- Aim for fast, deterministic tests; add docstring examples where appropriate.

## Commit & Pull Request Guidelines
- History is minimal; adopt Conventional Commits for clarity, e.g., `feat: add BSB loader`, `fix: handle empty reference lines`.
- Before pushing: `make fix && make lint` and loop until both are clean (no warnings/errors). CI may enforce this.
- Pull Requests: include purpose, linked issues, how to run/reproduce, and any configuration notes (`.env` keys). Attach sample output when relevant.
- Keep PRs focused and small; add tests for new behavior where feasible.

## Security & Configuration Tips
- Never commit secrets. Prefer `.env` locally; CI should use secure secrets storage.
- Network calls incur cost/latency; always set reasonable timeouts (e.g., `requests.get(..., timeout=30)`).
- Log level defaults to `info`; use `debug` only when needed to avoid noisy logs.

## Linting Workflow (Always Run)
- Format: `ruff format .`
- Static checks: `ruff check .` (fixable issues via `ruff check --fix .`).
- Deeper analysis: `pylint $(git ls-files "*.py")`.
- Type checks: `mypy .` (uses Pydantic plugin).
- Tests: `pytest -q` (or `make test`).
- Preferred: run `make check` to do all the above in order.
- Repeat fix → check until zero issues and all tests pass. Commit only when clean.

## Dependencies Hygiene
- When adding new imports, update `requirements.txt` accordingly (runtime vs dev).
- Install deps consistently via `pip install -r requirements.txt` to keep local dev aligned.

## Session Bootstrap (New Shells/CI Agents)
- Activate env: `source .venv/bin/activate` (create if missing, then install).
- Install/refresh deps: `pip install -r requirements.txt` (always after pulling changes).
- Verify toolchain: run `make check` before starting work; fix issues locally.
- Prefer `python -m pytest` and `python -m ruff` to avoid PATH/version confusion.
  - If `python` is not resolvable in subshells (e.g., Make), run with `PY=.venv/bin/python` (see Makefile).

## Agent Runbook and Non-Negotiables
- Test suite MUST run: Never skip `pytest`. If tests cannot run, fix the environment first.
- When Make cannot find `python` but `python3` or venv python works:
  - Create/refresh venv: `python3 -m venv .venv && .venv/bin/python -m pip install -r requirements.txt`.
  - Run checks using venv interpreter explicitly:
    - `.venv/bin/python -m ruff check --fix . && .venv/bin/python -m ruff format .`
    - `.venv/bin/pylint $(git ls-files "*.py")`
    - `.venv/bin/mypy .`
    - `.venv/bin/python -m pytest -q`
  - Or use Make with override: `make PY=.venv/bin/python check`.
- Recording environment issues: If an agent encounters interpreter path issues, add a brief note in the PR/commit body about the workaround used.

## Commit Titles from Agents
- Prefix all agent-authored commits with `(CODEX) ` to make authorship clear.
  - Example: `(CODEX) refactor: extract shared Aquifer loader`
  - Commit bodies should include concise details of changes and any environment notes.

## Data Prerequisites
- OpenBible boundaries: ensure `datasets/bible-section-counts.txt` exists.
  - Download: `curl -fsSL https://a.openbible.info/data/bible-section-counts.txt -o datasets/bible-section-counts.txt`
- Network note: running `python load_bsb.py` fetches BSB plaintext (`requests.get`). Tests mock network calls.

## Common Pitfalls & Tips
- Tool version drift: always reinstall from `requirements.txt` after pulling to align Ruff/Pylint/Pytest versions.
- Line length: Ruff enforces 100 chars; split long f-strings or assign to variables to satisfy E501.
- Tests not found: use `make test` or `python -m pytest -q` from repo root so pytest discovers `tests/`.
- Unnecessary parentheses: avoid extra tuple parentheses and similar; pyupgrade (UP) flags these.
- Broad except: avoid `except Exception`; Ruff BLE rules flag overly broad exception handlers.

## Editor Type Checking (Pyright/Pylance)
- IDEs like VS Code run Pylance/Pyright in the editor. These can show hints not surfaced by CLI tools.
- Optional CLI: `make pyright` (requires Node/npm). Uses `pyrightconfig.json` and the `.venv` for types.
- If IDE warnings differ from CLI, prefer fixing them; otherwise, align settings or silence noisy hints.
