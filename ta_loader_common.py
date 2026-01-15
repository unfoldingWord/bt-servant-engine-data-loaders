"""Shared utilities to load Translation Academy (tA) datasets into the servant engine.

This module provides a generic runner that parses markdown article folders,
builds documents, and optionally posts them to the servant engine.

Each article folder contains:
- title.md - Short title
- sub-title.md - Longer description
- 01.md - Main content

Large articles are chunked by markdown headers to stay under token limits.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

from config import config
from logger import get_logger
from servant_client import post_documents_to_servant

logger = get_logger(__name__)

ROOT_DIR = Path(__file__).resolve().parent

# Max chars per document (leaving room for embedding overhead)
MAX_CHARS = 7500


def gather_article_folders(dataset_root: Path) -> list[Path]:
    """Find all article folders (those containing 01.md)."""
    if not dataset_root.exists():
        logger.warning("Dataset folder missing: %s", dataset_root)
        return []
    return sorted([p.parent for p in dataset_root.rglob("01.md")])


def read_file_text(path: Path) -> str:
    """Read file content, return empty string if missing."""
    if path.exists():
        return path.read_text(encoding="utf-8").strip()
    return ""


def chunk_by_headers(content: str, max_chars: int) -> list[str]:
    """Split content by markdown headers (## or ###) into chunks under max_chars.

    Each chunk tries to include complete sections. If a single section exceeds
    max_chars, it gets split by paragraphs.
    """
    # Split on ## or ### headers, keeping the header with its content
    sections = re.split(r'(?=^#{2,3}\s)', content, flags=re.MULTILINE)
    sections = [s.strip() for s in sections if s.strip()]

    if not sections:
        # No headers found, treat whole content as one section
        sections = [content]

    chunks: list[str] = []
    current_chunk = ""

    for section in sections:
        # If adding this section would exceed limit
        if len(current_chunk) + len(section) + 2 > max_chars:
            # Save current chunk if non-empty
            if current_chunk.strip():
                chunks.append(current_chunk.strip())

            # If section itself is too large, split by paragraphs
            if len(section) > max_chars:
                para_chunks = chunk_by_paragraphs(section, max_chars)
                chunks.extend(para_chunks)
                current_chunk = ""
            else:
                current_chunk = section
        else:
            if current_chunk:
                current_chunk += "\n\n" + section
            else:
                current_chunk = section

    # Don't forget the last chunk
    if current_chunk.strip():
        chunks.append(current_chunk.strip())

    return chunks if chunks else [content[:max_chars]]


def chunk_by_paragraphs(content: str, max_chars: int) -> list[str]:
    """Split content by paragraphs (double newlines) into chunks under max_chars."""
    paragraphs = re.split(r'\n\n+', content)
    paragraphs = [p.strip() for p in paragraphs if p.strip()]

    chunks: list[str] = []
    current_chunk = ""

    for para in paragraphs:
        if len(current_chunk) + len(para) + 2 > max_chars:
            if current_chunk.strip():
                chunks.append(current_chunk.strip())
            # If single paragraph is too large, hard truncate
            if len(para) > max_chars:
                chunks.append(para[:max_chars])
                current_chunk = ""
            else:
                current_chunk = para
        else:
            if current_chunk:
                current_chunk += "\n\n" + para
            else:
                current_chunk = para

    if current_chunk.strip():
        chunks.append(current_chunk.strip())

    return chunks if chunks else [content[:max_chars]]


def build_ta_documents(
    folder: Path,
    dataset_root: Path,
    collection: str,
    doc_prefix: str = "ta_",
    source_name: str = "",
) -> list[dict[str, Any]]:
    """Build one or more documents from an article folder.

    Returns multiple documents if the article needs to be chunked.
    """
    title = read_file_text(folder / "title.md")
    subtitle = read_file_text(folder / "sub-title.md")
    content = read_file_text(folder / "01.md")

    if not content:
        return []

    # Build document ID from path: category/article -> category_article
    rel_path = folder.relative_to(dataset_root)
    parts = rel_path.parts  # e.g., ('checking', 'acceptable')
    article_id = "_".join(parts)
    base_doc_id = f"{doc_prefix}{article_id}"

    # Combine title, subtitle, content
    header = title
    if subtitle:
        header = f"{title}\n\n{subtitle}"

    full_text = f"{header}\n\n{content}" if header else content

    # Check if chunking needed
    if len(full_text) <= MAX_CHARS:
        return [{
            "document_id": base_doc_id,
            "collection": collection,
            "name": title or article_id,
            "text": full_text,
            "metadata": {"source": source_name or base_doc_id},
        }]

    # Need to chunk - split content, prepend header to each chunk
    content_chunks = chunk_by_headers(content, MAX_CHARS - len(header) - 50)

    documents = []
    for i, chunk in enumerate(content_chunks, 1):
        chunk_id = f"{base_doc_id}_chunk{i}"
        chunk_text = f"{header}\n\n{chunk}" if header else chunk

        # Safety truncate if still too long
        if len(chunk_text) > MAX_CHARS:
            chunk_text = chunk_text[:MAX_CHARS]

        documents.append({
            "document_id": chunk_id,
            "collection": collection,
            "name": f"{title or article_id} (part {i})",
            "text": chunk_text,
            "metadata": {"source": source_name or base_doc_id},
        })

    return documents


def run_ta_loader(
    dataset_subdir: str,
    collection: str,
    *,
    print_only: bool = False,
    doc_prefix: str = "ta_",
    source_name: str = "",
    log_only: bool = False,
) -> tuple[int, int] | None:
    """End-to-end runner for a Translation Academy dataset.

    Args:
        dataset_subdir: directory under ./datasets/ containing article folders
        collection: servant collection to target
        print_only: when True, prints documents JSON instead of posting
        doc_prefix: prefix for document IDs (e.g., "ta_" or "ne_ta_")
        source_name: value for metadata.source (defaults to doc_id if empty)
        log_only: when True, logs what would be posted but doesn't post

    Returns:
        Tuple of (successes, failures) or None if print_only/log_only
    """
    dataset_dir = ROOT_DIR / "datasets" / dataset_subdir
    article_folders = gather_article_folders(dataset_dir)

    if not article_folders:
        logger.warning("No article folders found under %s", dataset_dir)
        return None

    documents: list[dict[str, Any]] = []
    chunked_count = 0

    for folder in article_folders:
        docs = build_ta_documents(folder, dataset_dir, collection, doc_prefix, source_name)
        if len(docs) > 1:
            chunked_count += 1
        documents.extend(docs)

    logger.info(
        "Prepared %d tA documents from %d articles (%d chunked) in %s",
        len(documents),
        len(article_folders),
        chunked_count,
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
        "Posted %d tA documents to collection '%s': %d success, %d failed",
        len(documents),
        collection,
        ok,
        fail,
    )
    return ok, fail
