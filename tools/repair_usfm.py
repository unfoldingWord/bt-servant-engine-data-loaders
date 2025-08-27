from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable

import usfm_grammar as ug

from logger import get_logger

logger = get_logger(__name__)


ROOT = Path(__file__).resolve().parents[1]
ULT_DIR = ROOT / "datasets" / "ult"
UST_DIR = ROOT / "datasets" / "ust"


PARA_RE = re.compile(r"^\\(p|m|q\d*|q)\b")
CH_RE = re.compile(r"^\\c\s+\d+\b")
VERSE_RE = re.compile(r"^\\v\s+\d+\b")


def _needs_paragraph_after(lines: list[str], idx: int) -> bool:
    """Return True if a chapter line at idx is followed by a verse without a paragraph marker.

    Skips over blank lines and title markers (s1/s2/sr). If the next significant
    content is a verse (\\v) and we haven't seen a paragraph-like marker (p/m/q*),
    we insert a \\p.
    """
    i = idx + 1
    while i < len(lines):
        line = lines[i].rstrip("\n")
        if not line.strip():
            i += 1
            continue
        if line.startswith("\\ts") or line.startswith("\\ts*"):
            i += 1
            continue
        if line.startswith("\\s1") or line.startswith("\\s2") or line.startswith("\\sr"):
            i += 1
            continue
        # If we encounter a paragraph-like marker first, no need to insert
        if PARA_RE.match(line):
            return False
        # If we encounter a verse first, we need a paragraph
        if VERSE_RE.match(line):
            return True
        # For any other marker (e.g., \p already handled; other para-like?), treat as safe
        return False
    return False


def fix_file(path: Path) -> int:
    lines = path.read_text(encoding="utf-8").splitlines(True)
    added = 0

    # First pass: ensure a paragraph after chapter markers
    insert_positions: list[int] = []
    for i, line in enumerate(lines):
        if CH_RE.match(line):
            if _needs_paragraph_after(lines, i):
                insert_positions.append(i + 1)

    for offset, pos in enumerate(insert_positions):
        lines.insert(pos + offset, "\\p\n")
        added += 1

    # Second pass: split combined markers like "\\p\\v" into separate lines
    def split_para_and_verse(s: str) -> str:
        replacements = ["\\p\\v", "\\pm\\v", "\\m\\v", "\\q\\v", "\\q1\\v", "\\q2\\v", "\\q3\\v"]
        for pat in replacements:
            if pat in s:
                s = s.replace(pat, pat[:-2] + "\n\\v")
        return s

    # Third pass: ensure space after \qs when followed by text (not a closing * )
    qs_re = re.compile(r"\\qs(?!\s|\*)")

    changed = False
    for i, line in enumerate(lines):
        orig = line
        line = split_para_and_verse(line)
        line = qs_re.sub(r"\\qs ", line)
        if line != orig:
            lines[i] = line
            changed = True

    if added or changed:
        path.write_text("".join(lines), encoding="utf-8")

    return added


def try_parse(path: Path) -> list[str]:
    """Return parser errors (strings) for the given USFM file, if any."""
    text = path.read_text(encoding="utf-8")
    try:
        parser = ug.USFMParser(usfm_string=text)
    except Exception as exc:  # pragma: no cover - parse failure at init
        return [str(exc)]
    errs = getattr(parser, "errors", None)
    out: list[str] = []
    if isinstance(errs, list):
        out.extend(str(e) for e in errs)
    # Walk USJ to ensure it's structurally ok; exceptions would bubble
    try:
        _ = parser.to_usj()
    except Exception as exc:  # pragma: no cover - conversion failure
        out.append(str(exc))
    return out


def _iter_usfm_files() -> Iterable[Path]:
    for root in (ULT_DIR, UST_DIR):
        if root.exists():
            yield from sorted(root.glob("*.usfm"))


def main() -> None:
    modified = 0
    total_added = 0
    problems_before: dict[str, int] = {}
    problems_after: dict[str, int] = {}

    for path in _iter_usfm_files():
        errs = try_parse(path)
        if errs:
            problems_before[str(path)] = len(errs)
        added = fix_file(path)
        if added:
            modified += 1
            total_added += added
            logger.info("Inserted %d \\p lines into %s", added, path)
        # Re-parse after potential fix
        errs2 = try_parse(path)
        if errs2:
            problems_after[str(path)] = len(errs2)

    logger.info(
        "USFM repair summary: files_modified=%d, paragraphs_added=%d, remaining_problem_files=%d",
        modified,
        total_added,
        len(problems_after),
    )
    if problems_before:
        logger.info("Files with problems before fix: %d", len(problems_before))
    if problems_after:
        logger.info("Files still with problems after fix: %d", len(problems_after))
        for k, v in list(problems_after.items())[:20]:
            logger.info("Remaining issues: %s (%d errors)", k, v)


if __name__ == "__main__":
    main()
