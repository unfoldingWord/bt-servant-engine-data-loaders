"""Verifier for UW Translation Notes insertions.

Builds the expected set of document IDs from TSV files under
datasets/uw_translation_notes (using the same qualification scheme as the
loader) and compares them with the IDs reported by the servant engine for
the collection. Requires the servant to expose:

  GET /chroma/collection/{collection}/ids -> { "collection": str, "count": int, "ids": [str, ...] }

Run: `python verify_uw_translation_notes.py`
Options:
  --collection: collection name (default: uw_translation_notes)
  --dataset-root: override dataset folder
  --output: path to write missing IDs (default: logs/uw_tn_missing_ids.txt)
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable
import csv
from pathlib import Path

import requests

from config import config
from logger import get_logger

logger = get_logger(__name__)


DEFAULT_COLLECTION = "uw_translation_notes"
DEFAULT_DATASET_ROOT = Path(__file__).resolve().parent / "datasets" / "uw_translation_notes"


def _gather_expected_ids(dataset_root: Path) -> set[str]:
    expected: set[str] = set()
    if not dataset_root.exists():  # pragma: no cover - filesystem path dependency
        logger.error("Dataset root not found: %s", dataset_root)
        return expected

    tsv_files = sorted(dataset_root.glob("*.tsv"))
    if not tsv_files:
        logger.warning("No TSV files found under %s", dataset_root)
        return expected

    for path in tsv_files:
        stem = path.stem  # e.g., tn_ACT
        try:
            with path.open("r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh, delimiter="\t")
                if not reader.fieldnames or "ID" not in reader.fieldnames:
                    logger.warning("Skipping %s due to missing ID column", path)
                    continue
                for row in reader:
                    raw_id = (row.get("ID") or "").strip()
                    if not raw_id:
                        continue
                    qualified = f"{stem}_{raw_id}"
                    expected.add(qualified)
        except OSError as exc:  # pragma: no cover - IO path
            logger.error("Failed reading %s: %s", path, exc)
            continue
    return expected


def _fetch_collection_ids(
    *, base_url: str, token: str, collection: str, timeout: int = 30
) -> set[str]:
    if not base_url or not token:
        raise RuntimeError("SERVANT_API_BASE_URL and SERVANT_API_TOKEN must be configured")
    url = base_url.rstrip("/") + f"/chroma/collection/{collection}/ids"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as exc:  # pragma: no cover - network error path
        logger.error("GET %s failed: %s", url, exc)
        raise

    try:
        data = resp.json()
        ids = data.get("ids", [])
        if not isinstance(ids, list):
            logger.error("Unexpected response format from %s: %r", url, data)
            return set()
        return {str(x) for x in ids}
    except ValueError as exc:
        logger.error("Failed to parse JSON from %s: %s", url, exc)
        return set()


def _write_lines(lines: Iterable[str], path: Path) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            for line in lines:
                fh.write(f"{line}\n")
        logger.info("Wrote %s", path)
    except OSError as exc:  # pragma: no cover - IO path
        logger.error("Failed writing %s: %s", path, exc)


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify tN documents loaded in servant engine")
    parser.add_argument("--collection", default=DEFAULT_COLLECTION)
    parser.add_argument("--dataset-root", default=str(DEFAULT_DATASET_ROOT))
    parser.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parent / "logs" / "uw_tn_missing_ids.txt"),
    )
    args = parser.parse_args()

    dataset_root = Path(args.dataset_root)

    # Build expected from dataset
    expected = _gather_expected_ids(dataset_root)
    logger.info("Expected %d document IDs from dataset", len(expected))

    # Fetch actual from servant
    try:
        actual = _fetch_collection_ids(
            base_url=config.servant_api_base_url,
            token=config.servant_api_token,
            collection=args.collection,
        )
    except RuntimeError:
        return

    logger.info("Servant reported %d document IDs in collection '%s'", len(actual), args.collection)

    missing = sorted(expected - actual)
    extras = sorted(actual - expected)

    logger.info(
        "Verification summary: missing=%d, unexpected=%d, intersection=%d",
        len(missing),
        len(extras),
        len(expected & actual),
    )

    if missing:
        _write_lines(missing, Path(args.output))
    if extras:
        # Also write extras for inspection next to missing file
        _write_lines(extras, Path(str(args.output).replace("missing", "unexpected")))


if __name__ == "__main__":
    main()
