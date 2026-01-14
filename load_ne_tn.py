"""Loader for Nepali Translation Notes (tN).

Parses TSV files from datasets/ne_tn and posts documents to the servant
engine collection "ne_resources".
"""

from __future__ import annotations

import argparse

from logger import get_logger
from tn_loader_common import run_tn_loader

logger = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load Nepali Translation Notes into servant, or print documents"
    )
    parser.add_argument(
        "--print-chunks-only",
        action="store_true",
        dest="print_only",
        help="Print the documents that would be posted, as JSON, and do not post",
    )
    parser.add_argument(
        "--log-only",
        action="store_true",
        dest="log_only",
        help="Log what would be posted without actually posting",
    )
    args = parser.parse_args()

    run_tn_loader(
        "ne_tn",
        collection="ne_resources",
        print_only=args.print_only,
        log_only=args.log_only,
        doc_prefix="ne_tn_",
        source_name="nepali_translation_notes",
    )


if __name__ == "__main__":
    main()
