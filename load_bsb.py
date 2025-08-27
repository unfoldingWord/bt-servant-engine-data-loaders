"""Simple loader for the Berean Standard Bible (BSB) with section-based chunking.

Fetches the public BSB text file and parses each line into a structured
verse record. Groups verses into higher-level chunks using cross-translation
section boundaries from ``datasets/bible-section-counts.txt`` – no LLM calls.

Run directly with: `python load_bsb.py`.
"""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any
import uuid

import requests

from bible_chunking import (
    group_semantic_chunks,
)
from config import config as settings
from logger import get_logger
from servant_client import post_documents_to_servant

logger = get_logger(__name__)

# Public source of BSB plaintext
BSB_URL = "https://bereanbible.com/bsb.txt"

# Example line: "Genesis 1:1\tIn the beginning God created the heavens and the earth."
# The BSB plaintext uses a TAB between the reference and the verse text.
VERSE_RE = re.compile(r"^(?P<book>.+?) (?P<chapter>\d+):(?P<verse>\d+)\t(?P<text>.+)$")


def fetch_verses(url: str = BSB_URL) -> list[dict[str, str]]:
    """Fetch and parse BSB verses into structured records.

    Args:
        url: Source URL for the BSB plaintext file.

    Returns:
        A list of verse records with keys: id, book, chapter, verse, ref, text.
    """
    logger.info("Fetching BSB text from %s", url)
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:  # pragma: no cover - network error path
        logger.error("Failed to fetch BSB: %s", exc)
        raise

    verses: list[dict[str, str]] = []
    for line in resp.text.splitlines():
        match = VERSE_RE.match(line)
        if not match:
            continue
        book = match.group("book")
        chapter = match.group("chapter")
        verse = match.group("verse")
        text = match.group("text").strip()
        ref = f"{book} {chapter}:{verse}"
        verses.append(
            {
                "id": str(uuid.uuid4()),
                "book": book,
                "chapter": chapter,
                "verse": verse,
                "ref": ref,
                "text": text,
            }
        )

    logger.info("Parsed %d verses", len(verses))
    logger.debug("First verse: %s", verses[0]["ref"] if verses else "<none>")
    return verses


SECTION_COUNTS_PATH = Path(__file__).resolve().parent / "datasets" / "bible-section-counts.txt"


def _end_of_chapter(verses: list[dict[str, str]], idx: int) -> bool:  # legacy helper (unused)
    if idx >= len(verses) - 1:
        return True
    return verses[idx]["chapter"] != verses[idx + 1]["chapter"]


def main() -> None:
    """Entrypoint to fetch, chunk, and insert into servant engine."""
    verses = fetch_verses()
    logger.info("Fetched %d BSB verses", len(verses))

    # Build chunks including the text for insertion
    chunks = group_semantic_chunks(verses, include_text=True)
    logger.info("Prepared %d chunks for insertion", len(chunks))

    # Insert into servant engine

    if not settings.servant_api_base_url or not settings.servant_api_token:
        logger.error("Missing SERVANT_API_BASE_URL or SERVANT_API_TOKEN. Skipping insertion.")
        return
    post_chunks_to_servant(
        chunks,
        base_url=settings.servant_api_base_url,
        token=settings.servant_api_token,
        collection="bsb",
    )


def post_chunks_to_servant(
    chunks: list[dict[str, Any]],
    *,
    base_url: str,
    token: str,
    collection: str = "bsb",
    timeout: int = 30,
) -> tuple[int, int]:
    """Convert BSB chunks into documents and post via shared client."""
    documents: list[dict[str, Any]] = []
    for ch in chunks:
        ref = ch.get("ref") or ch.get("id") or "<unknown>"
        text = ch.get("text", "")
        included = ch.get("included_verses", "")
        header = (
            f"Reference: {ref}\nIncluded Verses: {included}" if included else f"Reference: {ref}"
        )
        document_id = str(ref)
        documents.append(
            {
                "document_id": document_id,
                "collection": collection,
                "name": ref,
                "text": f"{header}\n\n{text}",
                "metadata": {"name": ref, "ref": ref, "source": "bsb"},
            }
        )

    return post_documents_to_servant(documents, base_url=base_url, token=token, timeout=timeout)


if __name__ == "__main__":
    main()
