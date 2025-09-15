from __future__ import annotations

import argparse

from aquifer_common import add_aquifer_documents


def main() -> None:
    parser = argparse.ArgumentParser(description="Load FIA documents from Aquifer into Servant")
    parser.add_argument(
        "--log-only",
        action="store_true",
        help=("Log transformed documents without posting to Servant. " "Useful for dry runs."),
    )
    args = parser.parse_args()

    add_aquifer_documents(
        collection_code="CBBTER",
        collection="en_fia_resources",
        language_code="eng",
        limit=100,
        log_only=args.log_only,
    )


if __name__ == "__main__":
    main()
