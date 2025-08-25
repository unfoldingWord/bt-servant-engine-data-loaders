from __future__ import annotations

import time
from typing import Any

import requests

from logger import get_logger

logger = get_logger(__name__)


# pylint: disable=too-many-arguments
def post_documents_to_servant(
    documents: list[dict[str, Any]],
    *,
    base_url: str,
    token: str,
    timeout: int = 30,
    retries: int = 2,
    retry_backoff: float = 0.5,
) -> tuple[int, int]:
    """Send each document to the servant engine /chroma/add-document endpoint.

    Expects each document to include keys: document_id, collection, name, text, metadata.
    Returns (successes, failures).
    """
    if not base_url or not token:
        raise RuntimeError("SERVANT_API_BASE_URL and SERVANT_API_TOKEN must be configured")

    url = base_url.rstrip("/") + "/chroma/add-document"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    ok, fail = 0, 0
    logger.info("Posting %d documents to %s", len(documents), url)
    for doc in documents:
        attempt = 0
        while True:
            try:
                resp = requests.post(url, json=doc, headers=headers, timeout=timeout)
            except requests.RequestException as exc:  # pragma: no cover - network error path
                if attempt < retries:
                    attempt += 1
                    time.sleep(retry_backoff * attempt)
                    continue
                fail += 1
                logger.error("POST %s failed after retries: %s", url, exc)
                break

            if 200 <= resp.status_code < 300:
                ok += 1
                break

            # Retry on 5xx server errors
            if 500 <= resp.status_code < 600 and attempt < retries:
                attempt += 1
                time.sleep(retry_backoff * attempt)
                continue

            fail += 1
            logger.error(
                "POST %s returned %s; body=%s",
                url,
                resp.status_code,
                getattr(resp, "text", "<no body>"),
            )
            break
    logger.info("Document posting complete: %d success, %d failed", ok, fail)
    return ok, fail
