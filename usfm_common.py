"""USFM parsing helpers shared by UST/ULT loaders.

Parses .usfm files using usfm-grammar and returns a flat list of
verse records compatible with bible_chunking.group_semantic_chunks.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from usfm_grammar import USFMParser

from logger import get_logger

logger = get_logger(__name__)

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
    parser = USFMParser(text)
    # Some USFM sources may include non-fatal parse errors (e.g., diacritics in content
    # confused as markers). Be lenient and continue to generate rows while logging.
    try:
        # Exclude word-level inline markers (e.g., \w ...\w*) which often carry
        # complex attributes and can trip the converter for some sources, while they
        # aren't needed for our verse-level chunking.
        rows = parser.to_list(ignore_errors=True, exclude_markers=["w"])
        errs = getattr(parser, "errors", None)
        if errs:
            try:
                err_count = len(errs)  # type: ignore[arg-type]
            except Exception:  # noqa: BLE001 - defensive; errs could be any sequence-like
                err_count = 0
            logger.warning(
                "USFM parse reported %s issue(s) in %s; proceeding with ignore_errors",
                err_count,
                p,
            )
    except Exception as exc:  # noqa: BLE001 - fallback for sources usfm_grammar can't handle
        logger.warning("USFM grammar failed for %s (%s); using naive fallback parser", p, exc)
        return _fallback_parse_usfm_verses(text, source_path=p)
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

    # Normalize text consistently across datasets: remove any residual word-level
    # attributes (e.g., |strong=..., x-morph=...), drop stray USFM markers, and
    # collapse whitespace so embeddings stay clean and comparable.
    for v in verses:
        v["text"] = _normalize_usfm_text(v["text"])
    return verses


def _fallback_parse_usfm_verses(text: str, *, source_path: Path | str) -> list[dict[str, Any]]:
    """Naive USFM verse parser that handles basic \\c and \v markers.

    Intended only as a safety net when usfm_grammar cannot parse. Strips common
    inline markers like \\w ...\\w* and ignores non-verse content.
    """
    import re

    # Derive book code from filename like "01-GEN.usfm" -> "GEN"
    p = Path(source_path)
    stem = p.stem
    code = stem.split("-", 1)[-1].upper() if "-" in stem else stem.upper()
    code = (code or "").strip()
    book_name = USFM_CODE_TO_BOOK.get(code, code)

    verses: list[dict[str, Any]] = []
    cur_ch: str | None = None

    # Pre-compiled regexes
    re_ch = re.compile(r"^\\c\s+(\d+)")
    re_vs = re.compile(r"^\\v\s+(\d+)\s+(.*)$")
    # Match a \\w ... \\w* segment; capture inner content to strip attributes like |strong=, |x-morph=, etc.
    re_w_segment = re.compile(r"\\w\s+(.*?)\\w\*")
    # After \\w processing, drop any remaining bare markers like \\s5, \\p, etc.
    re_any_marker = re.compile(r"\\(?!v\b)[a-zA-Z0-9]+\*?")

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        m = re_ch.match(line)
        if m:
            cur_ch = m.group(1)
            continue
        m = re_vs.match(line)
        if m and cur_ch is not None:
            vs = m.group(1)
            body = m.group(2)

            # Replace each \\w segment with just the surface text before the first '|'
            def _strip_w(mw: re.Match[str]) -> str:
                inner = mw.group(1)
                head = inner.split("|", 1)[0]
                return head

            body = re_w_segment.sub(_strip_w, body)
            # Drop any remaining bare markers like \\s5, \\p, etc. (but keep the \\v we already parsed)
            body = re_any_marker.sub("", body)
            body = " ".join(body.split())
            verses.append(
                {
                    "book": book_name,
                    "book_code": code,
                    "chapter": str(cur_ch),
                    "verse": str(vs),
                    "text": body,
                }
            )
    return verses


def _normalize_usfm_text(text: str) -> str:
    """Normalize verse text to raw human-readable content.

    - Removes inline attribute tails like "|strong=H430" and "|x-morph=..."
    - Removes stray USFM markers like "\\s5", "\\p", etc.
    - Collapses internal whitespace.
    """
    import re

    if not text:
        return ""

    # Remove any pipe-based attribute shards that may survive parsing
    text = re.sub(r"\|[^\s]+", "", text)
    # Remove any lingering USFM markers (defensive)
    text = re.sub(r"\\[A-Za-z0-9]+\*?", "", text)
    # Collapse whitespace
    text = " ".join(text.split())
    return text
