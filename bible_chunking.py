"""Shared Bible chunking utilities used across loaders.

Extracted from load_bsb.py to keep logic DRY for section-aware
chunking based on OpenBible section boundary support counts.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Dataset of section boundaries (OpenBible), 4-column TSV with header.
SECTION_COUNTS_PATH = Path(__file__).resolve().parent / "datasets" / "bible-section-counts.txt"


# Map OpenBible abbreviations to canonical book names used by chunking
OPENBIBLE_TO_BOOK: dict[str, str] = {
    # OT
    "Gen": "Genesis",
    "Exod": "Exodus",
    "Lev": "Leviticus",
    "Num": "Numbers",
    "Deut": "Deuteronomy",
    "Josh": "Joshua",
    "Judg": "Judges",
    "Ruth": "Ruth",
    "1Sam": "1 Samuel",
    "2Sam": "2 Samuel",
    "1Kgs": "1 Kings",
    "2Kgs": "2 Kings",
    "1Chr": "1 Chronicles",
    "2Chr": "2 Chronicles",
    "Ezra": "Ezra",
    "Neh": "Nehemiah",
    "Esth": "Esther",
    "Job": "Job",
    "Ps": "Psalm",
    "Prov": "Proverbs",
    "Eccl": "Ecclesiastes",
    "Song": "Song of Solomon",
    "Isa": "Isaiah",
    "Jer": "Jeremiah",
    "Lam": "Lamentations",
    "Ezek": "Ezekiel",
    "Dan": "Daniel",
    "Hos": "Hosea",
    "Joel": "Joel",
    "Amos": "Amos",
    "Obad": "Obadiah",
    "Jonah": "Jonah",
    "Mic": "Micah",
    "Nah": "Nahum",
    "Hab": "Habakkuk",
    "Zeph": "Zephaniah",
    "Hag": "Haggai",
    "Zech": "Zechariah",
    "Mal": "Malachi",
    # NT
    "Matt": "Matthew",
    "Mark": "Mark",
    "Luke": "Luke",
    "John": "John",
    "Acts": "Acts",
    "Rom": "Romans",
    "1Cor": "1 Corinthians",
    "2Cor": "2 Corinthians",
    "Gal": "Galatians",
    "Eph": "Ephesians",
    "Phil": "Philippians",
    "Col": "Colossians",
    "1Thess": "1 Thessalonians",
    "2Thess": "2 Thessalonians",
    "1Tim": "1 Timothy",
    "2Tim": "2 Timothy",
    "Titus": "Titus",
    "Phlm": "Philemon",
    "Heb": "Hebrews",
    "Jas": "James",
    "1Pet": "1 Peter",
    "2Pet": "2 Peter",
    "1John": "1 John",
    "2John": "2 John",
    "3John": "3 John",
    "Jude": "Jude",
    "Rev": "Revelation",
}


@dataclass(frozen=True)
class VerseRef:
    book: str
    chapter: int
    verse: int

    @property
    def key(self) -> tuple[str, int, int]:
        return self.book, self.chapter, self.verse

    @property
    def human(self) -> str:
        return f"{self.book} {self.chapter}:{self.verse}"


def _parse_openbible_ref(ref: str) -> VerseRef | None:
    """Parse a reference like "Gen.1.31" into a VerseRef with canonical book name.

    Returns None if the book is not recognized.
    """
    try:
        book_abbr, chap_s, verse_s = ref.split(".")
        book = OPENBIBLE_TO_BOOK.get(book_abbr)
        if not book:
            return None
        return VerseRef(book=book, chapter=int(chap_s), verse=int(verse_s))
    except (ValueError, TypeError):
        return None


def load_section_boundary_scores(
    path: Path = SECTION_COUNTS_PATH,
) -> dict[str, dict[tuple[int, int], int]]:
    """Load boundary scores per book and end-verse from the OpenBible dataset.

    Returns a mapping: { book: { (chapter, verse): support_count } } where
    support_count sums the number of translations that end a section at that verse.
    """
    if not path.exists():  # pragma: no cover - filesystem path dependency
        raise FileNotFoundError(f"Section counts dataset not found: {path}")

    scores: dict[str, dict[tuple[int, int], int]] = {}
    with path.open("r", encoding="utf-8") as fh:
        header_seen = False
        for line in fh:
            line = line.rstrip("\n")
            if not line:
                continue
            if line.startswith("#") and not header_seen:
                header_seen = True
                continue
            parts = line.split("\t")
            if len(parts) != 4:
                continue
            _start, end, _next, count_s = parts
            end_ref = _parse_openbible_ref(end)
            if not end_ref:
                continue
            try:
                count = int(count_s)
            except ValueError:
                continue

            book_scores = scores.setdefault(end_ref.book, {})
            key = (end_ref.chapter, end_ref.verse)
            book_scores[key] = book_scores.get(key, 0) + count
    return scores


def _to_int(s: str | int) -> int:
    try:
        return int(s)  # type: ignore[arg-type]
    except (ValueError, TypeError):
        return 0


# pylint: disable=too-many-arguments,too-many-locals,too-many-branches,too-many-statements,too-many-nested-blocks
def group_semantic_chunks(
    verses: Iterable[dict[str, Any]],
    *,
    section_scores: dict[str, dict[tuple[int, int], int]] | None = None,
    support_threshold: int = 1,
    min_chunk_verses: int = 1,
    target_chunk_verses: int = 8,
    include_text: bool = False,
) -> list[dict[str, Any]]:
    """Group contiguous verses into section-aware chunks without any LLM.

    Constraints:
    - Never cross chapter boundaries (max chunk = one chapter).
    - Allow minimum chunk size down to a single verse.
    - Prefer boundaries with highest cross-translation support; tie-break by closeness
      to the target length.

    If verse items contain optional keys like 'book_code', these will be attached
    to chunk boundaries as 'start_code'/'end_code'.
    """
    verses_list = list(verses)
    if not verses_list:
        return []

    scores = section_scores or load_section_boundary_scores()

    # Group verses by book to avoid cross-book chunks
    by_book: dict[str, list[dict[str, Any]]] = {}
    for v in verses_list:
        by_book.setdefault(str(v["book"]), []).append(v)

    chunks: list[dict[str, Any]] = []
    for book, vlist in by_book.items():
        book_scores = scores.get(book, {})
        n = len(vlist)
        idx = 0
        while idx < n:
            chap = vlist[idx]["chapter"]
            chap_end = idx
            while chap_end + 1 < n and vlist[chap_end + 1]["chapter"] == chap:
                chap_end += 1

            i = idx
            while i <= chap_end:
                j_min = min(i + max(1, min_chunk_verses) - 1, chap_end)
                j_max = chap_end

                best_j: int | None = None
                best_score = -1
                best_delta = 10**9
                limit_end = min(j_max, i + max(1, int(target_chunk_verses)) - 1)
                for j in range(j_min, limit_end + 1):
                    ch_i = _to_int(vlist[j]["chapter"])  # constant within chapter
                    vs_i = _to_int(vlist[j]["verse"])
                    score = book_scores.get((ch_i, vs_i), 0)
                    if score >= support_threshold:
                        seg_len = (j - i) + 1
                        delta = abs(seg_len - max(1, target_chunk_verses))
                        if score > best_score or (score == best_score and delta < best_delta):
                            best_score = score
                            best_delta = delta
                            best_j = j

                if best_j is None:
                    chosen: int | None = None
                    for j in range(limit_end + 1, j_max + 1):
                        ch_i = _to_int(vlist[j]["chapter"])  # constant within chapter
                        vs_i = _to_int(vlist[j]["verse"])
                        if book_scores.get((ch_i, vs_i), 0) >= support_threshold:
                            chosen = j
                            break
                    if chosen is None:
                        chosen = j_max
                else:
                    chosen = best_j

                start_v = vlist[i]
                end_v = vlist[chosen]
                start_ref = f"{start_v['book']} {start_v['chapter']}:{start_v['verse']}"
                end_ref = f"{end_v['book']} {end_v['chapter']}:{end_v['verse']}"
                ref = start_ref if i == chosen else f"{start_ref}–{end_ref}"

                included_verses = ", ".join(
                    f"{v['book']} {v['chapter']}:{v['verse']}" for v in vlist[i : chosen + 1]
                )

                chunk: dict[str, Any] = {
                    "ref": ref,
                    "included_verses": included_verses,
                    "start": {
                        "book": start_v["book"],
                        "chapter": start_v["chapter"],
                        "verse": start_v["verse"],
                    },
                    "end": {
                        "book": end_v["book"],
                        "chapter": end_v["chapter"],
                        "verse": end_v["verse"],
                    },
                }
                # Optional book codes if present on verse items
                if "book_code" in start_v:
                    chunk["start_code"] = start_v["book_code"]
                if "book_code" in end_v:
                    chunk["end_code"] = end_v["book_code"]
                if include_text:
                    text = " ".join(
                        str(v.get("text", "")).strip() for v in vlist[i : chosen + 1]
                    ).strip()
                    chunk["text"] = text
                chunks.append(chunk)

                i = chosen + 1

            idx = chap_end + 1
    return chunks
