"""Loader for UnfoldingWord Translation Notes (tN).

Parses TSV files under datasets/uw_translation_notes and posts each row
as a document to the servant engine via the shared client.

TSV header columns: Reference, ID, Tags, SupportReference, Quote, Occurrence, Note

Document mapping per row:
- document_id: value from ID column
- collection: "uw_translation_notes"
- name: value from Reference column
- text: f"{Reference}\n\n{Note}"
- metadata.source: same as document_id

Run directly with: `python load_uw_translation_notes.py`.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from config import config
from logger import get_logger
from servant_client import post_documents_to_servant

logger = get_logger(__name__)


DATASET_ROOT = Path(__file__).resolve().parent / "datasets" / "uw_translation_notes"
COLLECTION = "uw_translation_notes"


def _iter_tsv_rows(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        # Ensure required fields exist in header
        required = {"Reference", "ID", "Note"}
        if reader.fieldnames is None or not required.issubset(set(reader.fieldnames)):
            logger.warning("Skipping %s due to missing required columns", path)
            return rows
        for row in reader:
            # Normalize to str keys/values and keep only relevant fields
            ref = (row.get("Reference") or "").strip()
            row_id = (row.get("ID") or "").strip()
            note = row.get("Note") or ""
            if not row_id or not ref:
                # Without ID or Reference we cannot construct a valid document
                continue
            rows.append({"Reference": ref, "ID": row_id, "Note": note})
    return rows


def _gather_tsv_files() -> list[Path]:
    if not DATASET_ROOT.exists():  # pragma: no cover - filesystem presence
        logger.warning("Dataset folder missing: %s", DATASET_ROOT)
        return []
    return sorted(DATASET_ROOT.glob("*.tsv"))


def _build_document(row: dict[str, str]) -> dict[str, Any]:
    ref = row["Reference"]
    row_id = row["ID"]
    note = row["Note"]
    text = f"{ref}\n\n{note}"
    return {
        "document_id": row_id,
        "collection": COLLECTION,
        "name": ref,
        "text": text,
        "metadata": {"source": row_id},
    }


def add_uw_translation_notes_documents() -> None:
    """Load tN TSV rows and insert as documents."""
    if not config.servant_api_base_url or not config.servant_api_token:
        logger.error("Missing SERVANT_API_BASE_URL or SERVANT_API_TOKEN. Skipping insertion.")
        return None

    tsv_files = _gather_tsv_files()
    if not tsv_files:
        logger.warning("No translation notes TSV files found under %s", DATASET_ROOT)
        return None

    all_rows: list[dict[str, str]] = []
    for path in tsv_files:
        rows = _iter_tsv_rows(path)
        if not rows:
            logger.debug("No valid rows in %s", path)
        all_rows.extend(rows)

    if not all_rows:
        logger.warning("No rows found across %d TSV files", len(tsv_files))
        return None

    documents = [_build_document(r) for r in all_rows]
    logger.info(
        "Prepared %d tN documents from %d TSV files in %s",
        len(documents),
        len(tsv_files),
        DATASET_ROOT,
    )

    ok, fail = post_documents_to_servant(
        documents,
        base_url=config.servant_api_base_url,
        token=config.servant_api_token,
    )
    logger.info(
        "Posted %d tN documents to collection '%s': %d success, %d failed",
        len(documents),
        COLLECTION,
        ok,
        fail,
    )
    return None


def main() -> None:
    add_uw_translation_notes_documents()


if __name__ == "__main__":
    main()
