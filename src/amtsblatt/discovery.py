"""Source discovery for new Amtsblatt issues.

Discovers PDF URLs from three sources:
1. **berlin_live** -- the current listing page on berlin.de
2. **berlin_register** -- Sachwortregister (subject-index) PDFs on the same page
3. **wayback** -- historic snapshots via the Wayback Machine CDX API
"""

from __future__ import annotations

import asyncio
import logging
import re
from urllib.parse import urljoin

import httpx

from amtsblatt.config import REQUEST_TIMEOUT, USER_AGENT
from amtsblatt.downloader import canonicalize_url, extract_filename_from_url

logger = logging.getLogger(__name__)

LISTING_URL = (
    "https://www.berlin.de/landesverwaltungsamt/logistikservice/amtsblatt-fuer-berlin/"
)

# Matches regular issue PDFs  (abl_YYYY_NN_PPPP_PPPP_online.pdf)
_RE_ISSUE_PDF = re.compile(
    r'href="([^"]*abl_\d{4}_\d+_\d+_\d+_online[^"]*\.pdf[^"]*)"',
    re.IGNORECASE,
)

# Matches Sachwortregister PDFs  (abl_YYYY_sr_…)
_RE_REGISTER_PDF = re.compile(
    r'href="([^"]*abl_\d{4}_sr_[^"]*\.pdf[^"]*)"',
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Berlin.de live page
# ---------------------------------------------------------------------------


async def discover_berlin_live() -> list[dict]:
    """Scrape the Amtsblatt listing page for regular-issue PDF links."""
    try:
        async with httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        ) as client:
            resp = await client.get(LISTING_URL, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
    except (httpx.HTTPError, OSError) as exc:
        logger.error("Failed to fetch listing page: %s", exc)
        return []

    html = resp.text
    results: list[dict] = []

    for match in _RE_ISSUE_PDF.finditer(html):
        raw_href = match.group(1)
        absolute_url = urljoin(LISTING_URL, raw_href)
        filename = extract_filename_from_url(absolute_url)
        canonical = canonicalize_url(absolute_url)

        results.append(
            {
                "filename": filename,
                "original_url": absolute_url,
                "canonical_url": canonical,
                "source_kind": "berlin_live",
            }
        )

    logger.info("discover_berlin_live: found %d PDFs", len(results))
    return results


# ---------------------------------------------------------------------------
# Berlin.de register (Sachwortregister)
# ---------------------------------------------------------------------------


async def discover_berlin_register() -> list[dict]:
    """Scrape the Amtsblatt listing page for Sachwortregister PDF links."""
    try:
        async with httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        ) as client:
            resp = await client.get(LISTING_URL, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
    except (httpx.HTTPError, OSError) as exc:
        logger.error("Failed to fetch listing page: %s", exc)
        return []

    html = resp.text
    results: list[dict] = []

    for match in _RE_REGISTER_PDF.finditer(html):
        raw_href = match.group(1)
        absolute_url = urljoin(LISTING_URL, raw_href)
        filename = extract_filename_from_url(absolute_url)
        canonical = canonicalize_url(absolute_url)

        results.append(
            {
                "filename": filename,
                "original_url": absolute_url,
                "canonical_url": canonical,
                "source_kind": "berlin_register",
            }
        )

    logger.info("discover_berlin_register: found %d PDFs", len(results))
    return results


# ---------------------------------------------------------------------------
# Wayback Machine CDX API
# ---------------------------------------------------------------------------

_CDX_API = "https://web.archive.org/cdx/search/cdx"
_CDX_URL_PATTERN = (
    "www.berlin.de/landesverwaltungsamt/_assets/"
    "logistikservice/amtsblatt-fuer-berlin/abl_*.pdf"
)


async def discover_wayback(year: int | None = None) -> list[dict]:
    """Query the Wayback Machine CDX API for archived Amtsblatt PDFs."""
    params: dict[str, str] = {
        "url": _CDX_URL_PATTERN,
        "output": "json",
        "fl": "original,timestamp",
        "collapse": "urlkey",
    }

    if year is not None:
        params["from"] = f"{year}0101"
        params["to"] = f"{year}1231"

    try:
        async with httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        ) as client:
            resp = await client.get(_CDX_API, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
    except (httpx.HTTPError, OSError) as exc:
        logger.error("Wayback CDX query failed: %s", exc)
        return []

    try:
        rows = resp.json()
    except ValueError:
        logger.error("Wayback CDX returned non-JSON response")
        return []

    if not rows or len(rows) < 2:
        logger.info("discover_wayback: no results")
        return []

    # First row is the header: ["original", "timestamp"]
    results: list[dict] = []
    for row in rows[1:]:
        original, timestamp = row[0], row[1]
        wayback_raw = f"https://web.archive.org/web/{timestamp}id_/{original}"
        filename = extract_filename_from_url(original)
        canonical = canonicalize_url(original)

        results.append(
            {
                "filename": filename,
                "original_url": wayback_raw,
                "canonical_url": canonical,
                "source_kind": "wayback",
            }
        )

    logger.info("discover_wayback: found %d PDFs", len(results))
    return results


# ---------------------------------------------------------------------------
# Combined discovery
# ---------------------------------------------------------------------------


async def discover_all(year: int | None = None) -> list[dict]:
    """Run all discovery sources, merge, and deduplicate by filename.

    Individual source failures are caught and logged so that one broken
    source does not prevent the others from returning results.
    """
    tasks = {
        "berlin_live": asyncio.create_task(_safe(discover_berlin_live())),
        "berlin_register": asyncio.create_task(_safe(discover_berlin_register())),
        "wayback": asyncio.create_task(_safe(discover_wayback(year))),
    }

    await asyncio.gather(*tasks.values())

    seen_filenames: set[str] = set()
    merged: list[dict] = []

    # Prefer berlin_live over wayback when filenames collide.
    for source_name in ("berlin_live", "berlin_register", "wayback"):
        for item in tasks[source_name].result():
            if item["filename"] not in seen_filenames:
                seen_filenames.add(item["filename"])
                merged.append(item)

    logger.info("discover_all: %d unique PDFs after deduplication", len(merged))
    return merged


async def _safe(coro) -> list[dict]:  # noqa: ANN001
    """Wrap a coroutine so exceptions are logged instead of propagated."""
    try:
        return await coro
    except Exception:
        logger.exception("Discovery source failed")
        return []
