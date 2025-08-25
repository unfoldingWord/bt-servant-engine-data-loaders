import json
import re
from typing import Any

import requests

from config import config
from logger import get_logger
from servant_client import post_documents_to_servant

# Module-level logger should live near the top of the module
logger = get_logger(__name__)


def _transform_detail(detail: Any) -> dict[str, Any]:
    """Transform a raw resource detail into the standardized object.

    Extracts name, concatenates content with double newlines, adds collection,
    document_id, and metadata fields.
    """
    name_val = ""
    text_val = ""
    document_id_val = ""
    if isinstance(detail, dict):
        name_val = str(detail.get("name") or "")
        content = detail.get("content")
        if isinstance(content, list):
            text_val = "\n\n".join(str(x) for x in content)
        document_id_val = str(detail.get("id") or "")

    return {
        "name": name_val,
        "text": text_val,
        "collection": "tyndale_dictionary",
        "document_id": document_id_val,
        "metadata": {"source": name_val},
    }


try:  # Optional accurate token counting
    import tiktoken  # type: ignore

    _ENCODING = tiktoken.get_encoding("cl100k_base")

    def _count_tokens(text: str) -> int:
        return len(_ENCODING.encode(text))

except (ModuleNotFoundError, AttributeError):  # pragma: no cover - optional dependency

    def _count_tokens(text: str) -> int:  # fallback
        return max(1, int(len(text) / 4))


def _slugify(name: str, *, max_len: int = 40) -> str:
    s = name.lower().strip()
    s = re.sub(r"[\s_]+", "-", s)
    s = re.sub(r"[^a-z0-9\-]", "", s)
    s = re.sub(r"-+", "-", s).strip("-")
    if not s:
        s = "part"
    return s[:max_len]


def _split_markdown_sections(text: str) -> list[tuple[str | None, str]]:
    """Split markdown text into sections by ATX headers.

    Returns a list of (title, content-with-heading) tuples. Title may be None if no headers.
    """
    lines = text.splitlines()
    sections: list[tuple[str | None, str]] = []
    current_title: str | None = None
    current_buf: list[str] = []

    header_re = re.compile(r"^(#{1,6})\s+(.*)$")
    for line in lines:
        m = header_re.match(line)
        if m:
            if current_buf:
                sections.append((current_title, "\n".join(current_buf)))
                current_buf = []
            current_title = m.group(2).strip()
            current_buf.append(line)
        else:
            current_buf.append(line)

    if current_buf:
        sections.append((current_title, "\n".join(current_buf)))

    if not sections:
        return [(None, text)]
    return sections


def _chunk_tyndale_document_if_needed(
    doc: dict[str, Any], *, max_tokens: int = 6500
) -> list[dict[str, Any]]:
    """Chunk a Tyndale document by markdown headers if it exceeds size.

    - Uses a coarse token estimate. If the text is too large, split by headers,
      packing sequential sections up to the token limit into each chunk.
    - If no headers exist, split by paragraphs.
    - For each chunk, update name, metadata.source (to chunk name), and
      document_id by appending a slugified suffix.
    """
    text = doc.get("text", "")
    if not isinstance(text, str):
        return [doc]

    if _count_tokens(text) <= max_tokens:
        return [doc]

    original_id = str(doc.get("document_id", ""))
    sections = _split_markdown_sections(text)

    chunks: list[dict[str, Any]] = []
    buf: list[str] = []
    buf_titles: list[str] = []
    buf_tokens = 0
    part_counter = 1

    def flush_chunk() -> None:
        nonlocal part_counter
        if not buf:
            return
        chunk_text = "\n".join(buf).strip()
        first_title = next((t for t in buf_titles if t), None)
        if not first_title:
            first_line = chunk_text.splitlines()[0] if chunk_text else ""
            words = re.split(r"\s+", first_line.strip())
            first_title = " ".join(words[:8]) or f"Part {part_counter}"

        slug = _slugify(first_title) or f"part-{part_counter}"
        chunk_doc = {
            "name": first_title,
            "text": chunk_text,
            "collection": doc.get("collection", "tyndale_dictionary"),
            "document_id": f"{original_id}_{slug}",
            "metadata": {"source": first_title},
        }
        chunks.append(chunk_doc)
        part_counter += 1

    for title, content in sections:
        section_text = content.strip()
        sect_tokens = _count_tokens(section_text)
        if not buf:
            buf = [section_text]
            buf_titles = [title or ""]
            buf_tokens = sect_tokens
            continue
        if buf_tokens + sect_tokens > max_tokens:
            flush_chunk()
            buf = [section_text]
            buf_titles = [title or ""]
            buf_tokens = sect_tokens
        else:
            buf.append(section_text)
            buf_titles.append(title or "")
            buf_tokens += sect_tokens

    flush_chunk()

    # If no headers and still one huge chunk (very long paragraph), fallback to paragraph split
    if len(chunks) == 1 and _count_tokens(chunks[0]["text"]) > max_tokens:
        paragraphs = [p for p in text.split("\n\n") if p.strip()]
        chunks = []
        buf = []
        buf_tokens = 0
        part_counter = 1
        for p in paragraphs:
            t = _count_tokens(p)
            if not buf or buf_tokens + t <= max_tokens:
                buf.append(p)
                buf_tokens += t
            else:
                first_line = buf[0].splitlines()[0] if buf else ""
                words = re.split(r"\s+", first_line.strip())
                chunk_name = " ".join(words[:8]) or f"Part {part_counter}"
                slug = _slugify(chunk_name) or f"part-{part_counter}"
                chunks.append(
                    {
                        "name": chunk_name,
                        "text": "\n\n".join(buf),
                        "collection": doc.get("collection", "tyndale_dictionary"),
                        "document_id": f"{original_id}_{slug}",
                        "metadata": {"source": chunk_name},
                    }
                )
                part_counter += 1
                buf = [p]
                buf_tokens = t
        if buf:
            first_line = buf[0].splitlines()[0] if buf else ""
            words = re.split(r"\s+", first_line.strip())
            chunk_name = " ".join(words[:8]) or f"Part {part_counter}"
            slug = _slugify(chunk_name) or f"part-{part_counter}"
            chunks.append(
                {
                    "name": chunk_name,
                    "text": "\n\n".join(buf),
                    "collection": doc.get("collection", "tyndale_dictionary"),
                    "document_id": f"{original_id}_{slug}",
                    "metadata": {"source": chunk_name},
                }
            )

    # Final edge case: any chunk still too large (e.g., single mega-paragraph)
    def _split_by_halves(big_text: str) -> list[str]:
        # Split recursively near the middle, prefer whitespace boundaries
        stack = [big_text]
        out: list[str] = []
        while stack:
            cur = stack.pop()
            if _count_tokens(cur) <= max_tokens or len(cur) < 2:
                out.append(cur)
                continue
            mid = len(cur) // 2
            # find near whitespace break within +/- 200 chars
            window = 200
            left = cur.rfind(" ", max(0, mid - window), mid)
            right = cur.find(" ", mid, min(len(cur), mid + window))
            split_at = left if left != -1 else (right if right != -1 else mid)
            left_text = cur[:split_at].strip()
            right_text = cur[split_at:].strip()
            if not left_text or not right_text:
                # if we couldn't find a good split, force mid
                left_text = cur[:mid].strip()
                right_text = cur[mid:].strip()
            # push right first so left is processed first (optional)
            stack.append(right_text)
            stack.append(left_text)
        return out

    final_chunks: list[dict[str, Any]] = []
    idx = 1
    for ch in chunks or [{"name": doc.get("name", ""), "text": text}]:
        ch_text = ch["text"]
        texts = [ch_text]
        if _count_tokens(ch_text) > max_tokens:
            texts = _split_by_halves(ch_text)
        for seg in texts:
            # Derive a name from header if present, else first line words
            header_match = re.search(r"^(#{1,6})\s+(.*)$", seg, flags=re.MULTILINE)
            if header_match:
                chunk_name = header_match.group(2).strip()
            else:
                first_line = seg.splitlines()[0] if seg else ""
                words = re.split(r"\s+", first_line.strip())
                chunk_name = " ".join(words[:8]) or f"Part {idx}"
            slug = _slugify(chunk_name) or f"part-{idx}"
            final_chunks.append(
                {
                    "name": chunk_name,
                    "text": seg,
                    "collection": doc.get("collection", "tyndale_dictionary"),
                    "document_id": f"{original_id}_{slug}",
                    "metadata": {"source": chunk_name},
                }
            )
            idx += 1
    return final_chunks or [doc]


def fetch_aquifer_api_data(endpoint: str, params: dict[str, Any] | None = None) -> Any:
    """Fetch JSON data from Aquifer API with a safe timeout.

    Note: Aquifer endpoints may return a dict or a list depending on route.
    """
    url = f"{config.aquifer_base_url}/{endpoint}"
    response = requests.get(
        url,
        headers={"api-key": config.aquifer_api_key},
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def fetch_aquifer_resource_types() -> Any:
    return fetch_aquifer_api_data("resources/types")


# pylint: disable=too-many-branches,too-many-locals,too-many-statements,too-many-return-statements
def add_tyndale_dictionary_documents(
    collection_code: str = "TyndaleBibleDictionary",
    language_code: str = "eng",
    limit: int = 100,
) -> None:
    """Fetch Tyndale Dictionary resources, transform, and insert into servant.

    - Paginates with ``limit`` and accumulates ``items`` until no more pages
      or the reported ``totalItemCount`` is reached.
    - Logs compact progress at debug level after each page.
    """
    if not config.servant_api_base_url or not config.servant_api_token:
        logger.error("Missing SERVANT_API_BASE_URL or SERVANT_API_TOKEN. Skipping insertion.")
        return None

    detailed_items: list[dict[str, Any]] = []
    offset = 0
    total_count: int | None = None

    while True:
        params = {
            "ResourceCollectionCode": collection_code,
            "LanguageCode": language_code,
            "Limit": limit,
            "Offset": offset,
        }

        page = fetch_aquifer_api_data(endpoint="resources/search", params=params)

        if isinstance(page, dict):
            if total_count is None:
                total_count = page.get("totalItemCount")
            items = page.get("items", []) or []
        elif isinstance(page, list):
            items = page
        else:
            items = []
        if not items:
            break

        # For each search result, fetch the detailed resource by id.
        page_batch: list[dict[str, Any]] = []
        for item in items:
            resource_id = item.get("id") if isinstance(item, dict) else None
            if not resource_id:
                logger.debug("Skipping item without 'id': %s", item)
                continue

            detail = fetch_aquifer_api_data(
                endpoint=f"resources/{resource_id}",
                params={"ContentTextType": "Markdown"},
            )
            # Transform detail into standardized object, then chunk if needed
            transformed = _transform_detail(detail)
            chunked_docs = _chunk_tyndale_document_if_needed(transformed)
            for cdoc in chunked_docs:
                logger.info("Transformed resource:\n%s", json.dumps(cdoc, indent=3))
                page_batch.append(cdoc)
                detailed_items.append(cdoc)

        # Post this page's batch in one request loop for efficiency
        if page_batch:
            ok, fail = post_documents_to_servant(
                page_batch,
                base_url=config.servant_api_base_url,
                token=config.servant_api_token,
            )
            logger.info(
                "Posted %d docs this page: %d success, %d failed",
                len(page_batch),
                ok,
                fail,
            )
        offset += len(items)

        if total_count is not None:
            logger.debug("processed %s/%s", offset, total_count)
            print(f"processed {offset}/{total_count}")
        else:
            logger.debug("processed %s", offset)
            print(f"processed {offset}")

        # Stop if we've reached the reported total or the server returned a short page.
        if (total_count is not None and offset >= int(total_count)) or len(items) < limit:
            break

    logger.info("Inserted %d Tyndale dictionary documents", len(detailed_items))
    return None


def main():
    add_tyndale_dictionary_documents()


if __name__ == "__main__":
    main()
