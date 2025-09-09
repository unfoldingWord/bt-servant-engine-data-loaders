"""Loader for TBI USFM files with section-based chunking.

Parses USFM from datasets/tbi, chunks verses using the same logic as the BSB
loader (OpenBible section counts), and posts documents to the servant engine
collection "id_resources".
"""

from __future__ import annotations

import argparse

from logger import get_logger
from usfm_loader_common import run_usfm_loader

logger = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load TBI USFM into servant, or print chunks")
    parser.add_argument(
        "--print-chunks-only",
        action="store_true",
        dest="print_only",
        help=("Print the documents that would be posted, as JSON, and do not post"),
    )
    args = parser.parse_args()

    run_usfm_loader("tbi", collection="id_resources", print_only=args.print_only)


if __name__ == "__main__":
    main()
