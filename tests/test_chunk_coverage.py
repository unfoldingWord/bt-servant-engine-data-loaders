from __future__ import annotations

import re
from types import SimpleNamespace

import load_bsb as lb

VERSE_LINE = "Sample verse text"


def _sample_bsb_text() -> str:
    # Minimal, deterministic subset spanning two books and multiple chapters.
    lines = [
        # James 1 (12 verses)
        *[f"James 1:{i}\t{VERSE_LINE}" for i in range(1, 13)],
        # James 2 (3 verses)
        *[f"James 2:{i}\t{VERSE_LINE}" for i in range(1, 4)],
        # Genesis 1 (5 verses)
        *[f"Genesis 1:{i}\t{VERSE_LINE}" for i in range(1, 6)],
    ]
    return "\n".join(lines) + "\n"


def _parse_ref(ref: str) -> tuple[str, int, int]:
    m = re.match(r"^(.+?) (\d+):(\d+)$", ref)
    assert m, f"Invalid ref format: {ref!r}"
    book, ch, vs = m.group(1), int(m.group(2)), int(m.group(3))
    return book, ch, vs


def test_every_verse_is_in_a_chunk(monkeypatch) -> None:
    # Mock requests.get used inside load_bsb.fetch_verses so no real network is hit.
    sample_text = _sample_bsb_text()

    def fake_get(url: str, timeout: int = 30):
        return SimpleNamespace(
            text=sample_text,
            raise_for_status=lambda: None,
        )

    monkeypatch.setattr(lb.requests, "get", fake_get)

    verses = lb.fetch_verses()
    assert verses, "Expected some verses from mocked BSB text"

    # Produce chunks (no LLM use). Keep defaults which enforce per-chapter chunks.
    chunks = lb.group_semantic_chunks(verses, include_text=False)
    assert chunks, "Expected at least one chunk for the provided verses"

    # Map verse ref -> index in original order for easy range expansion
    ref_to_idx: dict[str, int] = {v["ref"]: i for i, v in enumerate(verses)}

    covered: set[int] = set()
    for c in chunks:
        ref = c["ref"]
        if "–" in ref:  # en dash range
            start_ref, end_ref = ref.split("–", 1)
        else:
            start_ref = end_ref = ref

        # Verify we never cross chapters (by design of the algorithm)
        s_book, s_ch, s_vs = _parse_ref(start_ref)
        e_book, e_ch, e_vs = _parse_ref(end_ref)
        assert s_book == e_book and s_ch == e_ch, f"Chunk crosses chapter: {ref}"

        # Expand coverage by indices in the input verses list
        si = ref_to_idx[start_ref]
        ei = ref_to_idx[end_ref]
        assert si <= ei, f"Start after end in chunk: {ref}"
        covered.update(range(si, ei + 1))

    # Every verse index should be covered by at least one chunk
    assert covered == set(range(len(verses))), "Not all verses were covered by chunks"
