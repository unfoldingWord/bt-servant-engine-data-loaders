"""Shared helpers for translation word dataset loaders."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

from config import config
from logger import get_logger
from servant_client import post_documents_to_servant

logger = get_logger(__name__)

DEFAULT_SUBFOLDERS: tuple[str, ...] = ("kt", "names", "other")
DOCUMENT_ID_PREFIX = "tw_"


def gather_markdown_files(
    dataset_root: Path,
    subfolders: Sequence[str] = DEFAULT_SUBFOLDERS,
) -> list[Path]:
    files: list[Path] = []
    for sub in subfolders:
        folder = dataset_root / sub
        if not folder.exists():  # pragma: no cover - filesystem dependency
            logger.warning("Missing subfolder: %s", folder)
            continue
        files.extend(sorted(folder.glob("*.md")))
    return files


def build_document_from_file(
    path: Path,
    collection: str,
    document_prefix: str = DOCUMENT_ID_PREFIX,
    source_name: str = "",
) -> dict[str, Any]:
    name = path.stem
    document_id = f"{document_prefix}{name}"
    text = path.read_text(encoding="utf-8")
    return {
        "document_id": document_id,
        "collection": collection,
        "name": name,
        "text": text,
        "metadata": {"source": source_name or document_id},
    }


def load_translation_words_documents(
    dataset_root: Path,
    collection: str,
    *,
    subfolders: Sequence[str] = DEFAULT_SUBFOLDERS,
    document_prefix: str = DOCUMENT_ID_PREFIX,
    source_name: str = "",
) -> None:
    """Load markdown files and post them to the servant engine."""
    if not config.servant_api_base_url or not config.servant_api_token:
        logger.error("Missing SERVANT_API_BASE_URL or SERVANT_API_TOKEN. Skipping insertion.")
        return None

    md_files = gather_markdown_files(dataset_root, subfolders=subfolders)
    if not md_files:
        logger.warning("No translation word files found under %s", dataset_root)
        return None

    documents = [
        build_document_from_file(path, collection, document_prefix=document_prefix, source_name=source_name)
        for path in md_files
    ]
    logger.info("Prepared %d tW documents from %s", len(documents), dataset_root)

    ok, fail = post_documents_to_servant(
        documents,
        base_url=config.servant_api_base_url,
        token=config.servant_api_token,
    )
    logger.info(
        "Posted %d tW documents: %d success, %d failed",
        len(documents),
        ok,
        fail,
    )
    return None
