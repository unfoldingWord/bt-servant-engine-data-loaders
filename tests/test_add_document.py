from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import load_bsb as lb


def _sample_bsb_text() -> str:
    # Minimal sample that yields a couple of chunks in one chapter
    lines = [
        *[f"James 1:{i}	text {i}" for i in range(1, 7)],
        *[f"James 1:{i}	text {i}" for i in range(7, 13)],
    ]
    return "\n".join(lines) + "\n"


def test_post_chunks_calls_add_document(monkeypatch) -> None:
    sample_text = _sample_bsb_text()

    # Mock GET for BSB fetch
    def fake_get(url: str, timeout: int = 30):  # noqa: ARG001
        return SimpleNamespace(text=sample_text, raise_for_status=lambda: None)

    calls: list[dict[str, Any]] = []

    # Mock POST for add-document
    def fake_post(url: str, json: dict[str, Any], headers: dict[str, str], timeout: int = 30):  # noqa: ARG001
        calls.append({"url": url, "json": json, "headers": headers})
        return SimpleNamespace(status_code=200, text="ok")

    monkeypatch.setattr(lb.requests, "get", fake_get)
    monkeypatch.setattr(lb.requests, "post", fake_post)

    verses = lb.fetch_verses()
    chunks = lb.group_semantic_chunks(verses, include_text=True)

    ok, fail = lb.post_chunks_to_servant(
        chunks,
        base_url="https://example.test/api",
        token="TOKEN123",
        collection="bsb",
    )

    assert fail == 0
    assert ok == len(chunks) == len(calls)
    for c, ch in zip(calls, chunks, strict=False):
        assert c["url"].endswith("/add-document")
        assert c["headers"].get("Authorization") == "Bearer TOKEN123"
        payload = c["json"]
        assert payload["document_id"] == ch["ref"]
        assert payload["collection"] == "bsb"
        assert payload["name"] == ch["ref"]
        assert payload["text"].startswith(f"Reference: {ch['ref']}\nIncluded Verses: ")
        assert payload["metadata"].get("ref") == ch["ref"]
