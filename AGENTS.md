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
- Run loader locally: `python old_load_bsb.py`
- Lint (ruff + pylint): `make lint` (auto-fix with `make fix`).
- Type check (mypy): `make typecheck`.
- Example one-shot: `OPENAI_API_KEY=... python old_load_bsb.py`

## Coding Style & Naming Conventions
- Follow PEP 8. Indentation: 4 spaces. Use type hints.
- Tools: Ruff (formatter + lint) and Pylint. Keep both clean before committing.
- Modules/files: `snake_case.py`; functions/variables: `snake_case`; constants: `UPPER_CASE`; classes: `CapWords`.
- Logging: use `get_logger(__name__)`; avoid `print` in modules.
- Configuration: read from `config` (do not `os.getenv` directly in modules).

## Testing Guidelines
- Framework: pytest (not yet added). Place tests in `tests/` mirroring modules, e.g., `tests/test_logger.py`.
- Run tests (after adding pytest): `pytest -q`.
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
- Type checks: `mypy .` (uses Pydantic plugin). Resolve or ignore judiciously.
- Repeat fix → check until zero issues (style, lint, type). Commit only when clean.

## Editor Type Checking (Pyright/Pylance)
- IDEs like VS Code run Pylance/Pyright in the editor. These can show hints not surfaced by CLI tools.
- Optional CLI: `make pyright` (requires Node/npm). Uses `pyrightconfig.json` and the `.venv` for types.
- If IDE warnings differ from CLI, prefer fixing them; otherwise, align settings or silence noisy hints.
