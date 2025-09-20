"""Loader for Bahtraku Translation Words (tW).

Scans markdown files under datasets/bahtraku_translation_words/{kt,names,other}
and inserts each as a document into the servant engine using the shared
client `post_documents_to_servant`.

Run directly with: `python load_bahtraku_twords.py`.
"""

from __future__ import annotations

from pathlib import Path

from translation_words_loader_common import load_translation_words_documents

DATASET_ROOT = Path(__file__).resolve().parent / "datasets" / "bahtraku_translation_words"
COLLECTION = "id_resources"


def add_bahtraku_translation_words_documents() -> None:
    """Load tW markdown files and insert as documents."""
    load_translation_words_documents(DATASET_ROOT, COLLECTION)


def main() -> None:
    add_bahtraku_translation_words_documents()


if __name__ == "__main__":
    main()
