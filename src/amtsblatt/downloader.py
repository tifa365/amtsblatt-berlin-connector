"""Shared download utilities with URL canonicalization and streaming PDF downloads."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import httpx

from amtsblatt.config import BASE_DELAY, PDF_DIR, REQUEST_TIMEOUT, USER_AGENT

logger = logging.getLogger(__name__)


def canonicalize_url(url: str) -> str:
    """Strip query string and fragment from URL; normalize scheme to https.

    >>> canonicalize_url("http://www.berlin.de/path/file.pdf?ts=123#sec")
    'https://www.berlin.de/path/file.pdf'
    """
    parsed = urlparse(url)
    return urlunparse(("https", parsed.netloc, parsed.path, "", "", ""))


def extract_filename_from_url(url: str) -> str:
    """Extract the filename component from a URL path.

    >>> extract_filename_from_url(
    ...     "https://www.berlin.de/.../abl_2024_01_0001_0064_online.pdf?ts=123"
    ... )
    'abl_2024_01_0001_0064_online.pdf'
    """
    return urlparse(url).path.rsplit("/", 1)[-1]


def pdf_dest_path(year: int, filename: str) -> Path:
    """Return *PDF_DIR / year / filename*, creating the year directory if needed."""
    dest_dir = PDF_DIR / str(year)
    dest_dir.mkdir(parents=True, exist_ok=True)
    return dest_dir / filename


async def fetch_with_backoff(
    client: httpx.AsyncClient,
    url: str,
    timeout: int = REQUEST_TIMEOUT,
) -> httpx.Response | None:
    """Low-level GET with exponential backoff on 429 / 503.

    Returns the :class:`httpx.Response` on a 200, ``None`` on 404 or after
    exhausting all retry attempts.
    """
    backoff = 5
    max_attempts = 4

    for attempt in range(max_attempts):
        try:
            response = await client.get(url, timeout=timeout)

            if response.status_code == 200:
                return response

            if response.status_code == 404:
                logger.debug("404 for %s", url)
                return None

            if response.status_code in (429, 503):
                logger.info(
                    "Rate-limited (%s) on %s – backing off %ds (attempt %d/%d)",
                    response.status_code,
                    url,
                    backoff,
                    attempt + 1,
                    max_attempts,
                )
                await asyncio.sleep(backoff)
                backoff *= 2
                continue

            # Any other unexpected status code
            logger.warning("Unexpected status %s for %s", response.status_code, url)
            return None

        except (httpx.HTTPError, OSError) as exc:
            logger.warning(
                "Connection error for %s (attempt %d/%d): %s",
                url,
                attempt + 1,
                max_attempts,
                exc,
            )
            if attempt < max_attempts - 1:
                await asyncio.sleep(backoff)
                backoff *= 2
                continue

    logger.error("All %d attempts exhausted for %s", max_attempts, url)
    return None


async def download_pdf(
    url: str,
    dest_path: Path,
    timeout: int = REQUEST_TIMEOUT,
) -> bool:
    """Download a PDF from *url* to *dest_path*.

    * Validates the first 4 bytes are ``%PDF``.
    * Returns ``True`` on success, ``False`` otherwise.
    * Applies :data:`BASE_DELAY` after each request to be polite.
    """
    try:
        async with httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        ) as client:
            response = await fetch_with_backoff(client, url, timeout=timeout)

            if response is None:
                return False

            data = response.content

            if data[:4] != b"%PDF":
                logger.warning("Not a valid PDF (bad magic) from %s", url)
                return False

            dest_path.parent.mkdir(parents=True, exist_ok=True)
            dest_path.write_bytes(data)
            logger.info("Saved %s (%d KB)", dest_path.name, len(data) // 1024)
            return True

    except (httpx.HTTPError, OSError) as exc:
        logger.error("Download failed for %s: %s", url, exc)
        return False

    finally:
        await asyncio.sleep(BASE_DELAY)
