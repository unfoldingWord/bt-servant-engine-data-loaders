"""Shared utilities to load Translation Questions (tQ) datasets into the servant engine.

This module provides a generic runner that parses TSV files from a given
dataset subdirectory, builds documents, and optionally posts them to the
servant engine.

TSV header columns: Reference, ID, Tags, Quote, Occurrence, Question, Response
Required columns: Reference, ID, Question, Response
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any

from config import config
from logger import get_logger
from servant_client import post_documents_to_servant

logger = get_logger(__name__)

ROOT_DIR = Path(__file__).resolve().parent


def gather_tsv_files(dataset_root: Path) -> list[Path]:
    """Find all TSV files in the dataset directory."""
    if not dataset_root.exists():
        logger.warning("Dataset folder missing: %s", dataset_root)
        return []
    return sorted(dataset_root.glob("*.tsv"))


def iter_tsv_rows(path: Path) -> list[dict[str, str]]:
    """Parse a TSV file and return validated rows with file stem attached."""
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        required = {"Reference", "ID", "Question", "Response"}
        if reader.fieldnames is None or not required.issubset(set(reader.fieldnames)):
            logger.warning("Skipping %s due to missing required columns", path)
            return rows
        file_stem = path.stem  # e.g., tq_1CO
        for row in reader:
            ref = (row.get("Reference") or "").strip()
            row_id = (row.get("ID") or "").strip()
            question = row.get("Question") or ""
            response = row.get("Response") or ""
            if not row_id or not ref:
                continue
            rows.append({
                "Reference": ref,
                "ID": row_id,
                "Question": question,
                "Response": response,
                "_file_stem": file_stem,
            })
    return rows


def build_tq_document(
    row: dict[str, str],
    collection: str,
    doc_prefix: str = "tq_",
    source_name: str = "",
) -> dict[str, Any]:
    """Transform a TSV row into a servant document payload."""
    ref = row["Reference"]
    row_id = row["ID"]
    question = row["Question"]
    response = row["Response"]
    file_stem = row.get("_file_stem", "").strip()

    # Qualify reference and ID with file stem for uniqueness across books
    qualified_ref = f"{file_stem}_{ref}" if file_stem else ref
    qualified_id = f"{doc_prefix}{file_stem}_{row_id}" if file_stem else f"{doc_prefix}{row_id}"

    # Format text as question and answer
    text = f"{qualified_ref}\n\nQuestion: {question}\n\nResponse: {response}"
    return {
        "document_id": qualified_id,
        "collection": collection,
        "name": qualified_ref,
        "text": text,
        "metadata": {"source": source_name or qualified_id},
    }


def run_tq_loader(
    dataset_subdir: str,
    collection: str,
    *,
    print_only: bool = False,
    doc_prefix: str = "tq_",
    source_name: str = "",
    log_only: bool = False,
) -> tuple[int, int] | None:
    """End-to-end runner for a Translation Questions dataset.

    Args:
        dataset_subdir: directory under ./datasets/ containing .tsv files
        collection: servant collection to target
        print_only: when True, prints documents JSON instead of posting
        doc_prefix: prefix for document IDs (e.g., "tq_" or "ne_tq_")
        source_name: value for metadata.source (defaults to doc_id if empty)
        log_only: when True, logs what would be posted but doesn't post

    Returns:
        Tuple of (successes, failures) or None if print_only/log_only
    """
    dataset_dir = ROOT_DIR / "datasets" / dataset_subdir
    tsv_files = gather_tsv_files(dataset_dir)

    if not tsv_files:
        logger.warning("No translation questions TSV files found under %s", dataset_dir)
        return None

    all_rows: list[dict[str, str]] = []
    for path in tsv_files:
        rows = iter_tsv_rows(path)
        if not rows:
            logger.debug("No valid rows in %s", path)
        all_rows.extend(rows)

    if not all_rows:
        logger.warning("No rows found across %d TSV files", len(tsv_files))
        return None

    documents = [build_tq_document(r, collection, doc_prefix, source_name) for r in all_rows]
    logger.info(
        "Prepared %d tQ documents from %d TSV files in %s",
        len(documents),
        len(tsv_files),
        dataset_dir,
    )

    if print_only:
        output = json.dumps(documents, ensure_ascii=False, indent=2)
        sys.stdout.buffer.write(output.encode("utf-8"))
        sys.stdout.buffer.write(b"\n")
        return None

    if log_only:
        logger.info("Log-only mode: would insert %d documents to '%s'", len(documents), collection)
        return None

    if not config.servant_api_base_url or not config.servant_api_token:
        logger.error("Missing SERVANT_API_BASE_URL or SERVANT_API_TOKEN. Skipping insertion.")
        return None

    ok, fail = post_documents_to_servant(
        documents,
        base_url=config.servant_api_base_url,
        token=config.servant_api_token,
    )
    logger.info(
        "Posted %d tQ documents to collection '%s': %d success, %d failed",
        len(documents),
        collection,
        ok,
        fail,
    )
    return ok, fail
