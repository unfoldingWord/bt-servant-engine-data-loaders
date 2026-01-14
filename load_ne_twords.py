"""Loader for Nepali Translation Words (tW).

Scans markdown files under datasets/ne_tw/{kt,names,other}
and inserts each as a document into the servant engine using the shared
client `post_documents_to_servant`.

Run directly with: `python load_ne_twords.py`.
"""

from __future__ import annotations

from pathlib import Path

from translation_words_loader_common import load_translation_words_documents

DATASET_ROOT = Path(__file__).resolve().parent / "datasets" / "ne_tw"
COLLECTION = "ne_resources"


def add_ne_translation_words_documents() -> None:
    """Load tW markdown files and insert as documents."""
    load_translation_words_documents(
        DATASET_ROOT,
        COLLECTION,
        document_prefix="ne_tw_",
        source_name="nepali_translation_words",
    )


def main() -> None:
    add_ne_translation_words_documents()


if __name__ == "__main__":
    main()
