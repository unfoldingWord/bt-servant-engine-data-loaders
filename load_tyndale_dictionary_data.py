from __future__ import annotations

from aquifer_common import add_aquifer_documents


def main() -> None:
    add_aquifer_documents(
        collection_code="TyndaleBibleDictionary",
        collection="tyndale_dictionary",
        language_code="eng",
        limit=100,
    )


if __name__ == "__main__":
    main()

