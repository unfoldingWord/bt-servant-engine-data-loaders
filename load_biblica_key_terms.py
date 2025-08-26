from __future__ import annotations

from aquifer_common import add_aquifer_documents


def main() -> None:
    add_aquifer_documents(
        collection_code="BiblicaStudyNotesKeyTerms",
        collection="biblical_study_notes_key_terms",
        language_code="eng",
        limit=100,
    )


if __name__ == "__main__":
    main()

