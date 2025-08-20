"""Utilities to load the Berean Standard Bible and emit records.


This module can be executed directly (e.g., `python db_loaders/load_bsb.py`).
To support that, ensure the project root is on `sys.path` so top-level
imports like `logger` resolve correctly.
"""

from collections.abc import Iterable
import json
from pathlib import Path
import re
import sys
import uuid

from openai import OpenAI
import requests

try:
    from logger import get_logger
except ModuleNotFoundError:  # Allow running directly: python db_loaders/load_bsb.py
    PROJECT_ROOT = str(Path(__file__).resolve().parents[1])
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    from logger import get_logger

logger = get_logger(__name__)
VERSE_RE = re.compile(r"^(?P<book>[0-9A-Za-z ]+) (?P<chapter>\d+):(?P<verse>\d+) (?P<text>.+)$")


def fetch_verses() -> list[dict[str, str]]:
    """Fetch the BSB text and parse into verse records."""
    url = "https://bereanbible.com/bsb.txt"
    logger.info("Fetching %s", url)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

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
    return verses


def group_semantic_chunks(verses: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    """Use GPT-5 to group contiguous verses into semantic chunks.


    The request uses the Responses API with GPT‑5 and sets the
    reasoning effort to "medium" per OpenAI docs.
    """
    client = OpenAI()
    verse_lines = [f"{v['ref']} {v['text']}" for v in verses]
    instructions = (
        "You are a data processing assistant that segments contiguous Bible verses "
        "into coherent semantic chunks. Ensure boundaries are meaningful (topic, scene, or "
        "rhetorical unit). Output MUST be valid JSON matching the schema: { 'chunks': "
        "[ { 'ref': string, 'text': string } ] }."
    )
    response = client.responses.create(
        model="gpt-5",
        reasoning={"effort": "medium"},
        instructions=instructions,
        text={
            "format": {
                "type": "json_schema",
                "name": "BSBChunks",
                "schema": {
                    "type": "object",
                    "properties": {
                        "chunks": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "ref": {"type": "string"},
                                    "text": {"type": "string"},
                                },
                                "required": ["ref", "text"],
                                "additionalProperties": False,
                            },
                        }
                    },
                    "required": ["chunks"],
                    "additionalProperties": False,
                },
            }
        },
        input="\n".join(verse_lines),
    )
    content = response.output_text
    data = json.loads(content)
    chunks = data.get("chunks", data if isinstance(data, list) else [])
    results: list[dict[str, str]] = []
    for chunk in chunks:
        ref = chunk.get("ref")
        text = chunk.get("text")
        if not (ref and text):
            continue
        results.append({"id": str(uuid.uuid4()), "ref": ref, "text": text})
    logger.info("Generated %d semantic chunks", len(results))
    return results


def load():
    """Load verses and semantic chunks and print them out."""
    verses = fetch_verses()
    semantic_chunks = group_semantic_chunks(verses)

    # Print what would be inserted
    for v in verses:
        print(f"VERSE {v['ref']}: {v['text']}")

    for c in semantic_chunks:
        print(f"CHUNK {c['ref']}: {c['text']}")

    logger.info("Inserted %d verses and %d semantic chunks", len(verses), len(semantic_chunks))


def main():
    """Entrypoint to fetch, chunk, and emit records."""
    load()


if __name__ == "__main__":
    main()
