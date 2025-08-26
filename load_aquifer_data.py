from typing import Any

from aquifer_common import add_aquifer_documents, fetch_aquifer_api_data
from logger import get_logger

logger = get_logger(__name__)


def fetch_aquifer_resource_types() -> Any:
    return fetch_aquifer_api_data("resources/types")


def add_tyndale_dictionary_documents(
    collection_code: str = "TyndaleBibleDictionary",
    language_code: str = "eng",
    limit: int = 100,
) -> None:
    """Backwards-compatible entry to load Tyndale Bible Dictionary data."""
    return add_aquifer_documents(
        collection_code=collection_code,
        collection="tyndale_dictionary",
        language_code=language_code,
        limit=limit,
    )


def main() -> None:
    add_tyndale_dictionary_documents()


if __name__ == "__main__":
    main()
