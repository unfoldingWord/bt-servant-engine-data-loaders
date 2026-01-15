"""Verify Nepali resources are loaded in QA and prod collections."""

from __future__ import annotations

import argparse
from collections import Counter

import requests

from config import config


def get_document_ids(base_url: str, collection: str, token: str) -> list[str]:
    """Fetch all document IDs from a collection."""
    url = f"{base_url.rstrip('/')}/admin/chroma/collection/{collection}/ids"
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    return data.get("ids", [])


def count_by_prefix(ids: list[str], prefixes: list[str]) -> dict[str, int]:
    """Count documents matching each prefix."""
    counts = {p: 0 for p in prefixes}
    for doc_id in ids:
        for prefix in prefixes:
            if doc_id.startswith(prefix):
                counts[prefix] += 1
                break
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify Nepali resources in collections")
    parser.add_argument("--collection", default="ne_resources", help="Collection name")
    args = parser.parse_args()

    token = config.servant_api_token
    if not token:
        print("ERROR: No SERVANT_API_TOKEN found")
        return

    environments = {
        "QA": "https://qa.servant.bible",
        "Prod": "https://app.servant.bible",
    }

    prefixes = ["ne_glt_", "ne_gst_", "ne_tn_", "ne_tw_", "ne_tq_", "ne_ta_"]

    expected = {
        "ne_tn_": 22370,
        "ne_tw_": 1056,
        "ne_tq_": 1063,
        "ne_ta_": 244,
    }

    for env_name, base_url in environments.items():
        print(f"\n{'='*50}")
        print(f"{env_name}: {base_url}")
        print("="*50)

        try:
            ids = get_document_ids(base_url, args.collection, token)
            print(f"Total documents in {args.collection}: {len(ids)}")

            counts = count_by_prefix(ids, prefixes)

            print(f"\n{'Prefix':<12} {'Count':>8} {'Expected':>10} {'Status':<10}")
            print("-" * 45)

            for prefix in prefixes:
                count = counts[prefix]
                exp = expected.get(prefix, "?")
                if exp == "?":
                    status = ""
                elif count == exp:
                    status = "OK"
                elif count == 0:
                    status = "MISSING"
                else:
                    status = f"DIFF ({exp - count:+d})"

                print(f"{prefix:<12} {count:>8} {str(exp):>10} {status:<10}")

            # Count any other docs
            ne_total = sum(counts.values())
            other = len(ids) - ne_total
            if other > 0:
                print(f"{'(other)':<12} {other:>8}")

        except Exception as e:
            print(f"ERROR: {e}")


if __name__ == "__main__":
    main()
