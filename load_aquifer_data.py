import json
from typing import Any

import requests

from config import config
from logger import get_logger

# Module-level logger should live near the top of the module
logger = get_logger(__name__)


def _transform_detail(detail: Any) -> dict[str, Any]:
    """Transform a raw resource detail into the standardized object.

    Extracts name, concatenates content with double newlines, adds collection,
    document_id, and metadata fields.
    """
    name_val = ""
    text_val = ""
    document_id_val = ""
    if isinstance(detail, dict):
        name_val = str(detail.get("name") or "")
        content = detail.get("content")
        if isinstance(content, list):
            text_val = "\n\n".join(str(x) for x in content)
        document_id_val = str(detail.get("id") or "")

    return {
        "name": name_val,
        "text": text_val,
        "collection": "tyndale_dictionary",
        "document_id": document_id_val,
        "metadata": {"source": name_val},
    }


def fetch_aquifer_api_data(endpoint: str, params: dict[str, Any] | None = None) -> Any:
    """Fetch JSON data from Aquifer API with a safe timeout.

    Note: Aquifer endpoints may return a dict or a list depending on route.
    """
    url = f"{config.aquifer_base_url}/{endpoint}"
    response = requests.get(
        url,
        headers={"api-key": config.aquifer_api_key},
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def fetch_aquifer_resource_types() -> Any:
    return fetch_aquifer_api_data("resources/types")


def search_aquifer_resources(
    collection_code: str = "TyndaleBibleDictionary",
    language_code: str = "eng",
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Return all Aquifer resources for a collection/language.

    - Paginates with ``limit`` and accumulates ``items`` until no more pages
      or the reported ``totalItemCount`` is reached.
    - Logs compact progress at debug level after each page.
    """
    detailed_items: list[dict[str, Any]] = []
    offset = 0
    total_count: int | None = None

    while True:
        params = {
            "ResourceCollectionCode": collection_code,
            "LanguageCode": language_code,
            "Limit": limit,
            "Offset": offset,
        }

        page = fetch_aquifer_api_data(endpoint="resources/search", params=params)

        if isinstance(page, dict):
            if total_count is None:
                total_count = page.get("totalItemCount")
            items = page.get("items", []) or []
        elif isinstance(page, list):
            items = page
        else:
            items = []
        if not items:
            break

        # For each search result, fetch the detailed resource by id.
        for item in items:
            resource_id = item.get("id") if isinstance(item, dict) else None
            if not resource_id:
                logger.debug("Skipping item without 'id': %s", item)
                continue

            detail = fetch_aquifer_api_data(
                endpoint=f"resources/{resource_id}",
                params={"ContentTextType": "Markdown"},
            )
            # Transform detail into standardized object
            transformed = _transform_detail(detail)

            logger.info("Transformed resource:\n%s", json.dumps(transformed, indent=3))
            detailed_items.append(transformed)
        offset += len(items)

        if total_count is not None:
            logger.debug("processed %s/%s", offset, total_count)
            print(f"processed {offset}/{total_count}")
        else:
            logger.debug("processed %s", offset)
            print(f"processed {offset}")

        # Stop if we've reached the reported total or the server returned a short page.
        if (total_count is not None and offset >= int(total_count)) or len(items) < limit:
            break

    logger.info("Fetched %d Aquifer resources (transformed)", len(detailed_items))
    return detailed_items


def main():
    data = fetch_aquifer_resource_types()
    logger.info("Aquifer resource types:\n%s", json.dumps(data, indent=3))
    logger.info("%s", "-" * 44)

    data = search_aquifer_resources()
    logger.info("Aquifer resources:\n%s", json.dumps(data, indent=3))


if __name__ == "__main__":
    main()
