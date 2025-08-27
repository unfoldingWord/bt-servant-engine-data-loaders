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
from servant_client import post_documents_to_servant

logger = get_logger(__name__)


DEFAULT_COLLECTION = "uw_translation_notes"
DEFAULT_DATASET_ROOT = Path(__file__).resolve().parent / "datasets" / "uw_translation_notes"


def _docs_from_tsv(path: Path, collection: str) -> dict[str, dict]:
    docs: dict[str, dict] = {}
    stem = path.stem  # e.g., tn_ACT
    try:
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh, delimiter="\t")
            if not reader.fieldnames or not {"ID", "Reference", "Note"}.issubset(
                set(reader.fieldnames)
            ):
                logger.warning("Skipping %s due to missing required columns", path)
                return docs
            for row in reader:
                ref = (row.get("Reference") or "").strip()
                raw_id = (row.get("ID") or "").strip()
                note = row.get("Note") or ""
                if not raw_id or not ref:
                    continue
                qid = f"{stem}_{raw_id}"
                qref = f"{stem}_{ref}"
                docs[qid] = {
                    "document_id": qid,
                    "collection": collection,
                    "name": qref,
                    "text": f"{qref}\n\n{note}",
                    "metadata": {"source": qid},
                }
    except OSError as exc:  # pragma: no cover - IO path
        logger.error("Failed reading %s: %s", path, exc)
    return docs


def _gather_documents_map(dataset_root: Path, collection: str) -> dict[str, dict]:
    """Build qualified_id -> document payload from TSV dataset."""
    docs: dict[str, dict] = {}
    if not dataset_root.exists():  # pragma: no cover - filesystem path dependency
        logger.error("Dataset root not found: %s", dataset_root)
        return docs

    tsv_files = sorted(dataset_root.glob("*.tsv"))
    if not tsv_files:
        logger.warning("No TSV files found under %s", dataset_root)
        return docs

    for path in tsv_files:
        docs.update(_docs_from_tsv(path, collection))
    return docs


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


def _reinsert_missing_documents(missing_ids: list[str], docs_map: dict[str, dict]) -> None:
    missing_docs = [docs_map[mid] for mid in missing_ids if mid in docs_map]
    if not missing_docs:
        logger.warning("No matching documents found in dataset for missing IDs")
        return
    delay_sec = max(0.0, float(getattr(config, "uw_tn_post_delay_ms", 200.0)) / 1000.0)
    logger.info(
        "Reinserting %d missing documents with delay %.3fs between requests",
        len(missing_docs),
        delay_sec,
    )
    ok, fail = post_documents_to_servant(
        missing_docs,
        base_url=config.servant_api_base_url,
        token=config.servant_api_token,
        delay_between_requests=delay_sec,
    )
    logger.info("Reinsert complete: %d success, %d failed", ok, fail)


def run_verification(
    collection: str, dataset_root_str: str, output_str: str, reinsert: bool
) -> None:
    dataset_root = Path(dataset_root_str)
    docs_map = _gather_documents_map(dataset_root, collection=collection)
    expected = set(docs_map.keys())
    logger.info("Expected %d document IDs from dataset", len(expected))

    try:
        actual = _fetch_collection_ids(
            base_url=config.servant_api_base_url,
            token=config.servant_api_token,
            collection=collection,
        )
    except RuntimeError:
        return

    logger.info("Servant reported %d document IDs in collection '%s'", len(actual), collection)

    missing = sorted(expected - actual)
    extras = sorted(actual - expected)

    logger.info(
        "Verification summary: missing=%d, unexpected=%d, intersection=%d",
        len(missing),
        len(extras),
        len(expected & actual),
    )

    output_path = Path(output_str)
    if missing:
        _write_lines(missing, output_path)
    if extras:
        _write_lines(extras, Path(str(output_path).replace("missing", "unexpected")))

    if reinsert and missing:
        _reinsert_missing_documents(missing, docs_map)


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify tN documents loaded in servant engine")
    parser.add_argument("--collection", default=DEFAULT_COLLECTION)
    parser.add_argument("--dataset-root", default=str(DEFAULT_DATASET_ROOT))
    parser.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parent / "logs" / "uw_tn_missing_ids.txt"),
    )
    parser.add_argument(
        "--reinsert-missing",
        action="store_true",
        help="If set, attempt to reinsert missing documents using the dataset",
    )
    args = parser.parse_args()

    run_verification(
        collection=args.collection,
        dataset_root_str=args.dataset_root,
        output_str=args.output,
        reinsert=bool(args.reinsert_missing),
    )


if __name__ == "__main__":
    main()
