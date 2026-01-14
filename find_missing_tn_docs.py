"""Find missing Nepali TN documents by comparing expected vs actual."""

from __future__ import annotations

import argparse
import requests
from pathlib import Path

from config import config
from tn_loader_common import gather_tsv_files, iter_tsv_rows

ROOT_DIR = Path(__file__).resolve().parent


def get_expected_doc_ids(dataset_subdir: str, doc_prefix: str) -> set[str]:
    """Generate all expected document IDs from TSV files."""
    dataset_dir = ROOT_DIR / "datasets" / dataset_subdir
    tsv_files = gather_tsv_files(dataset_dir)

    doc_ids = set()
    for path in tsv_files:
        rows = iter_tsv_rows(path)
        for row in rows:
            row_id = row["ID"]
            file_stem = row.get("_file_stem", "").strip()
            qualified_id = f"{doc_prefix}{file_stem}_{row_id}" if file_stem else f"{doc_prefix}{row_id}"
            doc_ids.add(qualified_id)

    return doc_ids


def get_actual_doc_ids(collection: str, base_url: str, token: str) -> set[str]:
    """Fetch all document IDs from the collection via API."""
    url = f"{base_url.rstrip('/')}/admin/chroma/collection/{collection}/ids"
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return set(data.get("ids", []))


def main() -> None:
    parser = argparse.ArgumentParser(description="Find missing TN documents")
    parser.add_argument("--base-url", default=config.servant_api_base_url, help="Servant API base URL")
    args = parser.parse_args()

    token = config.servant_api_token
    base_url = args.base_url

    print(f"Target: {base_url}")

    # Get expected doc IDs
    expected = get_expected_doc_ids("ne_tn", "ne_tn_")
    print(f"Expected TN docs: {len(expected)}")

    # Get actual doc IDs from API
    print("Fetching actual doc IDs from collection...")
    actual = get_actual_doc_ids("ne_resources", base_url, token)
    actual_tn = {doc_id for doc_id in actual if doc_id.startswith("ne_tn_")}
    print(f"Actual TN docs: {len(actual_tn)}")

    # Find missing
    missing = expected - actual_tn
    print(f"Missing: {len(missing)}")

    if missing:
        print("\nMissing document IDs:")
        for doc_id in sorted(missing):
            print(f"  {doc_id}")


if __name__ == "__main__":
    main()
