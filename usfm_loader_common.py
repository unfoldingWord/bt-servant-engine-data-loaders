"""Shared utilities to load USFM datasets into the servant engine.

This module provides a generic runner that parses USFM files from a given
dataset subdirectory, groups verses into semantic chunks, formats documents,
and optionally posts them to the servant engine.
"""

from __future__ import annotations

from glob import glob
import json
from pathlib import Path
from typing import Any

from bible_chunking import group_semantic_chunks
from config import config as settings
from logger import get_logger
from servant_client import post_documents_to_servant
from usfm_common import USFM_CODE_TO_BOOK, parse_usfm_verses

logger = get_logger(__name__)


ROOT_DIR = Path(__file__).resolve().parent


def _format_id(prefix: str, book_code: str, s_ch: str, s_vs: str, e_ch: str, e_vs: str) -> str:
    book = book_code.lower()
    return f"{prefix}_{book}_{s_ch}_{s_vs}-{e_ch}_{e_vs}"


def _format_range_header(book_code: str, s_ch: str, s_vs: str, e_ch: str, e_vs: str) -> str:
    if s_ch == e_ch and s_vs == e_vs:
        return f"{book_code} {s_ch}:{s_vs}"
    return f"{book_code} {s_ch}:{s_vs}-{e_ch}:{e_vs}"


def build_documents(
    chunks: list[dict[str, Any]],
    collection: str,
    doc_id_prefix: str | None = None,
) -> list[dict[str, Any]]:
    """Convert chunk metadata into servant document payloads.

    - collection: target collection name to attach to documents
    - doc_id_prefix: prefix for document IDs (defaults to collection name)
    """
    prefix = doc_id_prefix or collection
    docs: list[dict[str, Any]] = []
    for ch in chunks:
        start = ch.get("start", {})
        end = ch.get("end", {})
        s_ch, s_vs = str(start.get("chapter")), str(start.get("verse"))
        e_ch, e_vs = str(end.get("chapter")), str(end.get("verse"))
        code = str(ch.get("start_code") or ch.get("end_code") or "").upper()
        if not code:
            # derive from canonical book name
            book_name = str(start.get("book"))
            rev = {v: k for k, v in USFM_CODE_TO_BOOK.items()}
            code = rev.get(book_name, book_name[:3].upper())
        doc_id = _format_id(prefix, code, s_ch, s_vs, e_ch, e_vs)
        header = _format_range_header(code, s_ch, s_vs, e_ch, e_vs)
        text = ch.get("text", "").strip()
        doc = {
            "document_id": doc_id,
            "collection": collection,
            "name": doc_id,
            "text": f"{header}\n\n{text}",
            "metadata": {"source": doc_id},
        }
        logger.debug(json.dumps(doc, indent=2))
        docs.append(doc)
    return docs


def run_usfm_loader(
    dataset_subdir: str,
    collection: str,
    *,
    print_only: bool,
    doc_id_prefix: str | None = None,
) -> None:
    """End-to-end runner for a USFM dataset.

    - dataset_subdir: directory under ./datasets/ containing .usfm files
    - collection: servant collection to target
    - print_only: when True, prints documents JSON instead of posting
    - doc_id_prefix: prefix for document IDs (defaults to collection name)
    """
    dataset_dir = ROOT_DIR / "datasets" / dataset_subdir
    files = sorted(glob(str(dataset_dir / "*.usfm")))
    if not files:
        logger.error("No USFM files found under %s", dataset_dir)
        return

    verses: list[dict[str, Any]] = []
    for fp in files:
        logger.info("Parsing %s...", fp)
        verses.extend(parse_usfm_verses(fp))
    logger.info(
        "Parsed %d verses from %d %s books",
        len(verses),
        len(files),
        dataset_subdir.upper(),
    )

    chunks = group_semantic_chunks(verses, include_text=True)
    logger.info("Prepared %d %s chunks", len(chunks), collection.upper())

    documents = build_documents(chunks, collection=collection, doc_id_prefix=doc_id_prefix)
    if print_only:
        print(json.dumps(documents, ensure_ascii=False, indent=3))
        return

    if not settings.servant_api_base_url or not settings.servant_api_token:
        logger.error("Missing SERVANT_API_BASE_URL or SERVANT_API_TOKEN. Skipping insertion.")
        return

    ok, fail = post_documents_to_servant(
        documents,
        base_url=settings.servant_api_base_url,
        token=settings.servant_api_token,
    )
    logger.info("%s insertion complete: %d success, %d failed", collection.upper(), ok, fail)
