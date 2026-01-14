"""Loader for Nepali Translation Questions (tQ).

Usage:
    python load_ne_tq.py                   # Post to servant engine
    python load_ne_tq.py --print-only      # Print documents as JSON
    python load_ne_tq.py --log-only        # Log what would be posted
"""

from __future__ import annotations

import argparse

from tq_loader_common import run_tq_loader


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Nepali Translation Questions")
    parser.add_argument(
        "--print-only",
        action="store_true",
        help="Print documents as JSON instead of posting to servant",
    )
    parser.add_argument(
        "--log-only",
        action="store_true",
        help="Log what would be posted but don't actually post",
    )
    args = parser.parse_args()

    run_tq_loader(
        dataset_subdir="ne_tq",
        collection="ne_resources",
        print_only=args.print_only,
        log_only=args.log_only,
        doc_prefix="ne_tq_",
        source_name="nepali_translation_questions",
    )


if __name__ == "__main__":
    main()
