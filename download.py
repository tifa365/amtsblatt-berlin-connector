#!/usr/bin/env python3
"""
Download Amtsblatt PDFs from berlin.de with Wayback Machine fallback.

Respects rate limits with exponential backoff.
"""
import csv
import time
from pathlib import Path
from urllib.parse import urlparse

import requests

UA = {"User-Agent": "amtsblatt-downloader/0.1"}
OUTPUT_DIR = Path("pdfs")
BASE_DELAY = 1.5


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    return f"https://{parsed.netloc}{parsed.path}"


def extract_filename(url: str) -> str:
    return urlparse(url).path.split("/")[-1]


def load_pdf_list(csv_path: str) -> list[dict]:
    seen = set()
    results = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            normalized = normalize_url(row["original"])
            if normalized not in seen:
                seen.add(normalized)
                row["normalized"] = normalized
                row["filename"] = extract_filename(row["original"])
                results.append(row)
    return results


def fetch(url: str, timeout: int = 60) -> requests.Response | None:
    """Fetch URL with rate limit handling. Returns None if blocked/failed."""
    backoff = 5
    for attempt in range(4):
        try:
            r = requests.get(url, headers=UA, timeout=timeout, stream=True)
            if r.status_code == 200:
                return r
            if r.status_code == 404:
                return None
            if r.status_code in (429, 503):  # Rate limited
                print(f"[rate limited, waiting {backoff}s]", end=" ", flush=True)
                time.sleep(backoff)
                backoff *= 2
                continue
            return None
        except (requests.RequestException, ConnectionError):
            if attempt < 3:
                print(f"[connection error, waiting {backoff}s]", end=" ", flush=True)
                time.sleep(backoff)
                backoff *= 2
            continue
    return None


def download_pdf(url: str, wayback_url: str, output_path: Path) -> str | None:
    """Try berlin.de first, then Wayback. Returns source name or None."""
    # Try berlin.de
    r = fetch(url)
    if r and r.content[:4] == b'%PDF':
        output_path.write_bytes(r.content)
        return "berlin.de"

    # Try Wayback (if URL provided)
    if wayback_url:
        r = fetch(wayback_url)
        if r and r.content[:4] == b'%PDF':
            output_path.write_bytes(r.content)
            return "wayback"

    return None


def main():
    pdfs = load_pdf_list("urls.csv")
    print(f"Loaded {len(pdfs)} PDFs\n")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stats = {"berlin.de": 0, "wayback": 0, "failed": 0, "skipped": 0}

    for pdf in pdfs:
        year_dir = OUTPUT_DIR / pdf["year"]
        year_dir.mkdir(exist_ok=True)
        output_path = year_dir / pdf["filename"]

        if output_path.exists() and output_path.stat().st_size > 0:
            stats["skipped"] += 1
            continue

        print(f"  {pdf['filename']}...", end=" ", flush=True)

        source = download_pdf(pdf["normalized"], pdf["wayback_raw"], output_path)
        if source:
            size_kb = output_path.stat().st_size / 1024
            print(f"✓ {source} ({size_kb:.0f} KB)")
            stats[source] += 1
        else:
            print("✗ failed")
            stats["failed"] += 1

        time.sleep(BASE_DELAY)

    print(f"\nDone: {stats}")


if __name__ == "__main__":
    main()
