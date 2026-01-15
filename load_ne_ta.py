"""Loader for Nepali Translation Academy (tA).

Usage:
    python load_ne_ta.py                   # Post to servant engine
    python load_ne_ta.py --print-only      # Print documents as JSON
    python load_ne_ta.py --log-only        # Log what would be posted
"""

from __future__ import annotations

import argparse

from ta_loader_common import run_ta_loader


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Nepali Translation Academy")
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

    run_ta_loader(
        dataset_subdir="ne_ta",
        collection="ne_resources",
        print_only=args.print_only,
        log_only=args.log_only,
        doc_prefix="ne_ta_",
        source_name="nepali_translation_academy",
    )


if __name__ == "__main__":
    main()
