"""Simple loader for the Berean Standard Bible (BSB) with section-based chunking.

Fetches the public BSB text file and parses each line into a structured
verse record. Groups verses into higher-level chunks using cross-translation
section boundaries from ``datasets/bible-section-counts.txt`` – no LLM calls.

Run directly with: `python load_bsb.py`.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
import re
import uuid

import requests

from config import config as settings
from logger import get_logger
from servant_client import post_documents_to_servant

logger = get_logger(__name__)

# Public source of BSB plaintext
BSB_URL = "https://bereanbible.com/bsb.txt"

# Example line: "Genesis 1:1\tIn the beginning God created the heavens and the earth."
# The BSB plaintext uses a TAB between the reference and the verse text.
VERSE_RE = re.compile(r"^(?P<book>.+?) (?P<chapter>\d+):(?P<verse>\d+)\t(?P<text>.+)$")

# Dataset of section boundaries (OpenBible), 4-column TSV with header.
SECTION_COUNTS_PATH = Path(__file__).resolve().parent / "datasets" / "bible-section-counts.txt"

# Map OpenBible abbreviations to BSB book names
OPENBIBLE_TO_BSB: dict[str, str] = {
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
    "Ps": "Psalm",  # BSB lines use "Psalm X:Y"
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


def fetch_verses(url: str = BSB_URL) -> list[dict[str, str]]:
    """Fetch and parse BSB verses into structured records.

    Args:
        url: Source URL for the BSB plaintext file.

    Returns:
        A list of verse records with keys: id, book, chapter, verse, ref, text.
    """
    logger.info("Fetching BSB text from %s", url)
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:  # pragma: no cover - network error path
        logger.error("Failed to fetch BSB: %s", exc)
        raise

    verses: list[dict[str, str]] = []
    for line in resp.text.splitlines():
        match = VERSE_RE.match(line)
        if not match:
            continue
        book = match.group("book")
        chapter = match.group("chapter")
        verse = match.group("verse")
        text = match.group("text").strip()
        ref = f"{book} {chapter}:{verse}"
        verses.append(
            {
                "id": str(uuid.uuid4()),
                "book": book,
                "chapter": chapter,
                "verse": verse,
                "ref": ref,
                "text": text,
            }
        )

    logger.info("Parsed %d verses", len(verses))
    logger.debug("First verse: %s", verses[0]["ref"] if verses else "<none>")
    return verses


def _parse_openbible_ref(ref: str) -> VerseRef | None:
    """Parse a reference like "Gen.1.31" into a VerseRef with BSB book name.

    Returns None if the book is not recognized.
    """
    try:
        book_abbr, chap_s, verse_s = ref.split(".")
        book = OPENBIBLE_TO_BSB.get(book_abbr)
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


def _end_of_chapter(verses: list[dict[str, str]], idx: int) -> bool:
    if idx >= len(verses) - 1:
        return True
    return verses[idx]["chapter"] != verses[idx + 1]["chapter"]


def _to_int(s: str) -> int:
    try:
        return int(s)
    except (ValueError, TypeError):
        return 0


# pylint: disable=too-many-arguments,too-many-locals,too-many-branches,too-many-statements,too-many-nested-blocks
def group_semantic_chunks(
    verses: Iterable[dict[str, str]],
    *,
    section_scores: dict[str, dict[tuple[int, int], int]] | None = None,
    support_threshold: int = 1,
    min_chunk_verses: int = 1,
    target_chunk_verses: int = 8,
    include_text: bool = False,
) -> list[dict[str, str]]:
    """Group contiguous verses into section-aware chunks without any LLM.

    Constraints:
    - Never cross chapter boundaries (max chunk = one chapter).
    - Allow minimum chunk size down to a single verse.
    - Prefer boundaries with highest cross-translation support; tie-break by closeness
      to the target length.
    """
    verses_list = list(verses)
    if not verses_list:
        return []

    # Load scores if not supplied (allows injecting precomputed scores in callers/tests)
    scores = section_scores or load_section_boundary_scores()

    # Group verses by book to avoid cross-book chunks
    by_book: dict[str, list[dict[str, str]]] = {}
    for v in verses_list:
        by_book.setdefault(v["book"], []).append(v)

    chunks: list[dict[str, str]] = []
    for book, vlist in by_book.items():
        book_scores = scores.get(book, {})
        n = len(vlist)
        idx = 0
        while idx < n:
            # Identify the next chapter slice [idx .. chap_end]
            chap = vlist[idx]["chapter"]
            chap_end = idx
            while chap_end + 1 < n and vlist[chap_end + 1]["chapter"] == chap:
                chap_end += 1

            # Greedy within chapter
            i = idx
            while i <= chap_end:
                # window bounds within the chapter
                j_min = min(i + max(1, min_chunk_verses) - 1, chap_end)
                j_max = chap_end  # hard cap: do not cross chapter

                # Select best supported boundary in a local window near target length
                best_j = None
                best_score = -1
                best_delta = 10**9
                # Keep search tightly local: only up to the target length
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
                    # No supported boundary in the local window: advance to the next
                    # supported boundary in the remainder of the chapter, else end of chapter.
                    chosen = None
                    for j in range(limit_end + 1, j_max + 1):
                        ch_i = _to_int(vlist[j]["chapter"])  # constant within chapter
                        vs_i = _to_int(vlist[j]["verse"])
                        if book_scores.get((ch_i, vs_i), 0) >= support_threshold:
                            chosen = j
                            break
                    if chosen is None:
                        chosen = j_max  # chapter end
                else:
                    chosen = best_j

                # Emit chunk [i..chosen]
                start_v = vlist[i]
                end_v = vlist[chosen]
                start_ref = f"{start_v['book']} {start_v['chapter']}:{start_v['verse']}"
                end_ref = f"{end_v['book']} {end_v['chapter']}:{end_v['verse']}"
                ref = start_ref if i == chosen else f"{start_ref}–{end_ref}"
                # Compose chunk with reference and included verses list
                included_verses = ", ".join(
                    f"{v['book']} {v['chapter']}:{v['verse']}" for v in vlist[i : chosen + 1]
                )
                chunk: dict[str, str] = {
                    "id": str(uuid.uuid4()),
                    "ref": ref,
                    "included_verses": included_verses,
                }
                if include_text:
                    text = " ".join(v["text"] for v in vlist[i : chosen + 1])
                    chunk["text"] = text
                chunks.append(chunk)

                i = chosen + 1

            idx = chap_end + 1
    logger.info("Generated %d section-based chunks", len(chunks))
    return chunks


def main() -> None:
    """Entrypoint to fetch, chunk, and insert into servant engine."""
    verses = fetch_verses()
    logger.info("Fetched %d BSB verses", len(verses))

    # Build chunks including the text for insertion
    chunks = group_semantic_chunks(verses, include_text=True)
    logger.info("Prepared %d chunks for insertion", len(chunks))

    # Insert into servant engine

    if not settings.servant_api_base_url or not settings.servant_api_token:
        logger.error("Missing SERVANT_API_BASE_URL or SERVANT_API_TOKEN. Skipping insertion.")
        return
    post_chunks_to_servant(
        chunks,
        base_url=settings.servant_api_base_url,
        token=settings.servant_api_token,
        collection="bsb",
    )


def post_chunks_to_servant(
    chunks: list[dict[str, str]],
    *,
    base_url: str,
    token: str,
    collection: str = "bsb",
    timeout: int = 30,
) -> tuple[int, int]:
    """Convert BSB chunks into documents and post via shared client."""
    documents: list[dict[str, str]] = []
    chunk_id = 1
    for ch in chunks:
        ref = ch.get("ref", ch["id"])
        text = ch.get("text", "")
        included = ch.get("included_verses", "")
        header = (
            f"Reference: {ref}\nIncluded Verses: {included}" if included else f"Reference: {ref}"
        )
        document_id = str(chunk_id)
        documents.append(
            {
                "document_id": document_id,
                "collection": collection,
                "name": ref,
                "text": f"{header}\n\n{text}",
                "metadata": {"name": ref, "ref": ref, "source": "bsb"},
            }
        )
        chunk_id += 1

    return post_documents_to_servant(documents, base_url=base_url, token=token, timeout=timeout)


if __name__ == "__main__":
    main()
