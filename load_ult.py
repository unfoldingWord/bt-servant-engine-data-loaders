"""Loader for ULT USFM files with section-based chunking.

Parses USFM from datasets/ult, chunks verses using the same logic as
the BSB loader (OpenBible section counts), and posts documents to the
servant engine collection "ult".
"""

from __future__ import annotations

from glob import glob
import argparse
import json
from pathlib import Path
from typing import Any

from bible_chunking import group_semantic_chunks
from config import config as settings
from logger import get_logger
from servant_client import post_documents_to_servant
from usfm_common import USFM_CODE_TO_BOOK, parse_usfm_verses

logger = get_logger(__name__)


DATASET_DIR = Path(__file__).resolve().parent / "datasets" / "ult"


def _format_id(collection: str, book_code: str, s_ch: str, s_vs: str, e_ch: str, e_vs: str) -> str:
    book = book_code.lower()
    return f"{collection}_{book}_{s_ch}_{s_vs}-{e_ch}_{e_vs}"


def _format_range_header(book_code: str, s_ch: str, s_vs: str, e_ch: str, e_vs: str) -> str:
    if s_ch == e_ch and s_vs == e_vs:
        return f"{book_code} {s_ch}:{s_vs}"
    return f"{book_code} {s_ch}:{s_vs}-{e_ch}:{e_vs}"


def _build_documents(chunks: list[dict[str, Any]], collection: str) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    for ch in chunks:
        start = ch.get("start", {})
        end = ch.get("end", {})
        s_ch, s_vs = str(start.get("chapter")), str(start.get("verse"))
        e_ch, e_vs = str(end.get("chapter")), str(end.get("verse"))
        code = str(ch.get("start_code") or ch.get("end_code") or "").upper()
        if not code:
            book_name = str(start.get("book"))
            rev = {v: k for k, v in USFM_CODE_TO_BOOK.items()}
            code = rev.get(book_name, book_name[:3].upper())
        doc_id = _format_id(collection, code, s_ch, s_vs, e_ch, e_vs)
        header = _format_range_header(code, s_ch, s_vs, e_ch, e_vs)
        text = ch.get("text", "").strip()
        docs.append(
            {
                "document_id": doc_id,
                "collection": collection,
                "name": doc_id,
                "text": f"{header}\n\n{text}",
                "metadata": {"source": doc_id},
            }
        )
    return docs


def main() -> None:
    parser = argparse.ArgumentParser(description="Load ULT USFM into servant, or print chunks")
    parser.add_argument(
        "--print-chunks-only",
        action="store_true",
        dest="print_only",
        help="Print the documents that would be posted, as JSON, and do not post",
    )
    args = parser.parse_args()

    files = sorted(glob(str(DATASET_DIR / "*.usfm")))
    if not files:
        logger.error("No USFM files found under %s", DATASET_DIR)
        return
    verses: list[dict[str, Any]] = []
    for fp in files:
        verses.extend(parse_usfm_verses(fp))
    logger.info("Parsed %d verses from %d ULT books", len(verses), len(files))

    chunks = group_semantic_chunks(verses, include_text=True)
    logger.info("Prepared %d ULT chunks", len(chunks))

    documents = _build_documents(chunks, collection="ult")
    if args.print_only:
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
    logger.info("ULT insertion complete: %d success, %d failed", ok, fail)


if __name__ == "__main__":
    main()
