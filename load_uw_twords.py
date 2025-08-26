"""Loader for UnfoldingWord Translation Words (tW).

Scans markdown files under datasets/uw_translation_words/{kt,names,other}
and inserts each as a document into the servant engine using the shared
client `post_documents_to_servant`.

Run directly with: `python load_uw_twords.py`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from config import config
from logger import get_logger
from servant_client import post_documents_to_servant

logger = get_logger(__name__)


DATASET_ROOT = Path(__file__).resolve().parent / "datasets" / "uw_translation_words"
SUBFOLDERS = ("kt", "names", "other")
COLLECTION = "uw_translation_words"


def _gather_markdown_files() -> list[Path]:
    files: list[Path] = []
    for sub in SUBFOLDERS:
        folder = DATASET_ROOT / sub
        if not folder.exists():  # pragma: no cover - filesystem path dependency
            logger.warning("Missing subfolder: %s", folder)
            continue
        files.extend(sorted(folder.glob("*.md")))
    return files


def _build_document_from_file(path: Path) -> dict[str, Any]:
    name = path.stem
    document_id = f"tw_{name}"
    text = path.read_text(encoding="utf-8")
    return {
        "document_id": document_id,
        "collection": COLLECTION,
        "name": name,
        "text": text,
        "metadata": {"source": document_id},
    }


def add_uw_translation_words_documents() -> None:
    """Load tW markdown files and insert as documents."""
    if not config.servant_api_base_url or not config.servant_api_token:
        logger.error("Missing SERVANT_API_BASE_URL or SERVANT_API_TOKEN. Skipping insertion.")
        return None

    md_files = _gather_markdown_files()
    if not md_files:
        logger.warning("No translation word files found under %s", DATASET_ROOT)
        return None

    documents = [_build_document_from_file(p) for p in md_files]
    logger.info("Prepared %d tW documents from %s", len(documents), DATASET_ROOT)

    ok, fail = post_documents_to_servant(
        documents,
        base_url=config.servant_api_base_url,
        token=config.servant_api_token,
    )
    logger.info("Posted %d tW documents: %d success, %d failed", len(documents), ok, fail)
    return None


def main() -> None:
    add_uw_translation_words_documents()


if __name__ == "__main__":
    main()
