"""USFM parsing helpers shared by UST/ULT loaders.

Parses .usfm files using usfm-grammar and returns a flat list of
verse records compatible with bible_chunking.group_semantic_chunks.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from usfm_grammar import USFMParser

# Map USFM 3-letter book codes to canonical English names used by chunking
USFM_CODE_TO_BOOK: dict[str, str] = {
    # OT
    "GEN": "Genesis",
    "EXO": "Exodus",
    "LEV": "Leviticus",
    "NUM": "Numbers",
    "DEU": "Deuteronomy",
    "JOS": "Joshua",
    "JDG": "Judges",
    "RUT": "Ruth",
    "1SA": "1 Samuel",
    "2SA": "2 Samuel",
    "1KI": "1 Kings",
    "2KI": "2 Kings",
    "1CH": "1 Chronicles",
    "2CH": "2 Chronicles",
    "EZR": "Ezra",
    "NEH": "Nehemiah",
    "EST": "Esther",
    "JOB": "Job",
    "PSA": "Psalm",
    "PRO": "Proverbs",
    "ECC": "Ecclesiastes",
    "SNG": "Song of Solomon",
    "ISA": "Isaiah",
    "JER": "Jeremiah",
    "LAM": "Lamentations",
    "EZK": "Ezekiel",
    "DAN": "Daniel",
    "HOS": "Hosea",
    "JOL": "Joel",
    "AMO": "Amos",
    "OBA": "Obadiah",
    "JON": "Jonah",
    "MIC": "Micah",
    "NAM": "Nahum",
    "HAB": "Habakkuk",
    "ZEP": "Zephaniah",
    "HAG": "Haggai",
    "ZEC": "Zechariah",
    "MAL": "Malachi",
    # NT
    "MAT": "Matthew",
    "MRK": "Mark",
    "LUK": "Luke",
    "JHN": "John",
    "ACT": "Acts",
    "ROM": "Romans",
    "1CO": "1 Corinthians",
    "2CO": "2 Corinthians",
    "GAL": "Galatians",
    "EPH": "Ephesians",
    "PHP": "Philippians",
    "COL": "Colossians",
    "1TH": "1 Thessalonians",
    "2TH": "2 Thessalonians",
    "1TI": "1 Timothy",
    "2TI": "2 Timothy",
    "TIT": "Titus",
    "PHM": "Philemon",
    "HEB": "Hebrews",
    "JAS": "James",
    "1PE": "1 Peter",
    "2PE": "2 Peter",
    "1JN": "1 John",
    "2JN": "2 John",
    "3JN": "3 John",
    "JUD": "Jude",
    "REV": "Revelation",
}


def parse_usfm_verses(path: Path | str) -> list[dict[str, Any]]:
    """Parse a USFM file into a list of verse dicts.

    Each returned dict contains: book (canonical name), book_code (USFM code),
    chapter (str), verse (str), text (str).
    """
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    rows = USFMParser(text).to_list()
    verses: list[dict[str, Any]] = []
    cur: dict[str, Any] | None = None
    book_code: str | None = None

    for row in rows[1:]:
        # Row format: [Book, Chapter, Verse, Text, Type, Marker]
        b, ch, vs, txt, typ, marker = row
        if typ == "verse" and marker == "v":
            # New verse starts; close previous
            if cur:
                verses.append(cur)
            # Capture book code from the row's book column (preferred)
            bc = (b or "").strip().upper() or (book_code or "")
            book_name = USFM_CODE_TO_BOOK.get(bc, bc)
            cur = {
                "book": book_name,
                "book_code": bc,
                "chapter": str(ch),
                "verse": str(vs),
                "text": "",
            }
        elif cur:
            if txt:
                cur["text"] += txt
    if cur:
        verses.append(cur)

    # Clean up whitespace
    for v in verses:
        v["text"] = " ".join(v["text"].split())
    return verses
