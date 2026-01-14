"""Loader for UnfoldingWord Translation Notes (tN).

Parses TSV files under datasets/uw_translation_notes and posts each row
as a document to the servant engine via the shared client.

TSV header columns: Reference, ID, Tags, SupportReference, Quote, Occurrence, Note

Document mapping per row:
- document_id: {file_stem}_{ID} (e.g., tn_ACT_abc123)
- collection: "uw_translation_notes"
- name: {file_stem}_{Reference}
- text: f"{name}\n\n{Note}"
- metadata.source: same as document_id

Run directly with: `python load_uw_translation_notes.py`.

Resume capability: Set UW_TN_RESUME_AFTER_DOCUMENT_ID env var to skip to a specific ID.
Throttling: Set UW_TN_POST_DELAY_MS env var (default 200ms).
"""

from __future__ import annotations

import argparse

from config import config
from logger import get_logger
from tn_loader_common import run_tn_loader

logger = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load UnfoldingWord Translation Notes into servant, or print documents"
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

    # Read resume and throttle settings from config (env vars)
    resume_after = (config.uw_tn_resume_after_document_id or "").strip()
    delay_sec = max(0.0, float(config.uw_tn_post_delay_ms) / 1000.0)

    run_tn_loader(
        "uw_translation_notes",
        collection="uw_translation_notes",
        print_only=args.print_only,
        log_only=args.log_only,
        doc_prefix="",  # file stems already include "tn_" (e.g., tn_ACT)
        resume_after_id=resume_after,
        delay_between_requests=delay_sec,
    )


if __name__ == "__main__":
    main()
