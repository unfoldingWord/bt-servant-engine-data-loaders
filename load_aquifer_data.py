import requests
import json
from config import config


def fetch_aquifer_api_data(endpoint, params=None):
    """Fetch JSON data from Aquifer API"""
    url = f"{config.aquifer_base_url}/{endpoint}"
    response = requests.get(url, headers={"api-key": config.aquifer_api_key}, params=params)
    if response.status_code != 200:
        raise Exception(f"API request to {endpoint} failed with status {response.status_code}")
    return response.json()


def fetch_aquifer_resource_types():
    return fetch_aquifer_api_data("resources/types")

def search_aquifer_resources():
    """Search Aquifer resources and return all items across pages.

    Uses a page size of 100 and prints progress as
    "processed <count>/<TotalItemCount>" after each page. Avoids
    issuing an extra request once all items have been processed.
    """
    all_items = []
    offset = 0
    limit = 100
    total_count = None

    while True:
        search_params = {
            "ResourceCollectionCode": "TyndaleBibleDictionary",
            "LanguageCode": "eng",
            "Limit": limit,
            "Offset": offset,
        }
        page = fetch_aquifer_api_data(endpoint="resources/search", params=search_params)

        if total_count is None and isinstance(page, dict):
            total_count = page.get("totalItemCount")

        items = page.get("items", []) if isinstance(page, dict) else []
        if not items:
            break

        all_items.extend(items)
        offset += len(items)

        processed = offset
        if total_count is not None:
            print(f"processed {processed}/{total_count}")
        else:
            print(f"processed {processed}")

        # Do not issue another request if we've reached the total.
        if total_count is not None and offset >= total_count:
            break
        # Also stop if the page was short (no more results server-side).
        if len(items) < limit:
            break

    return all_items


def main():
    data = fetch_aquifer_resource_types()
    print(json.dumps(data, indent=3))
    print('--------------------------------------------')

    data = search_aquifer_resources()
    print(json.dumps(data, indent=3))


if __name__ == "__main__":
    main()
