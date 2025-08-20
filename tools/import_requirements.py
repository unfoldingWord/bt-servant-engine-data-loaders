"""Sanity import of all runtime requirements.

Run: python tools/import_requirements.py
This helps surface encoding/pinning issues early.
"""

from __future__ import annotations

from collections.abc import Iterable
import importlib
import sys

RUNTIME_PACKAGES: list[str] = [
    # Names correspond to importable modules
    "annotated_types",
    "pydantic",
    "pydantic_settings",
    "pydantic_core",
    "dotenv",  # python-dotenv
    "typing_inspection",
    "typing_extensions",
    "requests",
    "openai",
]


def try_import(names: Iterable[str]) -> int:
    failures = 0
    for name in names:
        try:
            mod = importlib.import_module(name)
            version = getattr(mod, "__version__", "unknown")
            print(f"OK   {name} {version}")
        except Exception as exc:  # noqa: BLE001 - report any failure
            failures += 1
            print(f"FAIL {name}: {exc}", file=sys.stderr)
    return failures


def main() -> None:
    failed = try_import(RUNTIME_PACKAGES)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
