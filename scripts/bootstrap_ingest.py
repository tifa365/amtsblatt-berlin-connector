#!/usr/bin/env python3
"""One-time import of all existing Amtsblatt PDFs into the database."""

from __future__ import annotations

import asyncio
import csv
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure project src is on the import path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from amtsblatt.config import CSV_PATH, DB_PATH, PDF_DIR, PROJECT_ROOT
from amtsblatt.db import (
    close_db,
    init_db,
    insert_issue_with_pages,
    issue_exists,
    record_ingest_run,
    update_ingest_run,
    upsert_issue_source,
)
from amtsblatt.extract import compute_file_hash, extract_text_from_pdf, parse_filename
from amtsblatt.models import IngestStats
from amtsblatt.quality import flag_pages


def load_url_mapping(csv_path: Path) -> dict[str, dict]:
    """Read urls.csv and build a lookup dict keyed by filename.

    Each value has keys:
        url_berlin   -- the "original" column (berlin.de URL)
        url_wayback  -- the "wayback_raw" column (Wayback Machine URL)
        year         -- year from the CSV row
    """
    mapping: dict[str, dict] = {}
    if not csv_path.exists():
        return mapping

    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            url = row.get("original", "")
            if not url:
                continue
            # Extract filename from the URL (last path segment)
            filename = url.rstrip("/").rsplit("/", 1)[-1]
            mapping[filename] = {
                "url_berlin": url,
                "url_wayback": row.get("wayback_raw", "").strip(),
                "year": row.get("year", ""),
            }
    return mapping


async def main() -> None:
    print("=" * 60)
    print("  Amtsblatt Bootstrap Ingest")
    print("=" * 60)
    print()

    # Ensure the database directory exists and initialise the schema
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    await init_db()

    # Load URL mapping from CSV
    url_mapping = load_url_mapping(CSV_PATH)
    print(f"URL mapping loaded: {len(url_mapping)} entries from {CSV_PATH.name}")

    # Record the start of the ingest run
    stats = IngestStats()
    run_id = await record_ingest_run("bootstrap", "running", stats)

    # Scan for all PDF files
    pdf_files = sorted(PDF_DIR.rglob("*.pdf"))
    total = len(pdf_files)
    print(f"PDF files found: {total}")
    print()

    t0 = time.monotonic()

    for i, pdf_path in enumerate(pdf_files, start=1):
        filename = pdf_path.name

        # --- Parse filename ---
        try:
            parsed = parse_filename(filename)
        except ValueError as exc:
            print(f"  WARNING: {exc}")
            stats.errors += 1
            continue

        # --- Check if already in DB by filename ---
        if await issue_exists(filename=parsed.filename):
            stats.skipped += 1
            continue

        stats.discovered += 1

        # --- Compute SHA-256 hash ---
        file_hash = compute_file_hash(pdf_path)

        # --- Check if already in DB by hash ---
        if await issue_exists(pdf_sha256=file_hash):
            stats.skipped += 1
            continue

        # --- Extract text from all pages ---
        pages = extract_text_from_pdf(pdf_path)

        # --- Flag pages with quality assessment ---
        flag_pages(pages)

        # --- Build issue data dict ---
        pdf_rel_path = str(pdf_path.relative_to(PROJECT_ROOT))
        issue_data = {
            "year": parsed.year,
            "issue_code": parsed.issue_code,
            "issue_kind": parsed.issue_kind,
            "publication_date": None,
            "page_label_start": parsed.page_label_start,
            "page_label_end": parsed.page_label_end,
            "page_start_num": parsed.page_start_num,
            "page_end_num": parsed.page_end_num,
            "filename": parsed.filename,
            "pdf_rel_path": pdf_rel_path,
            "pdf_sha256": file_hash,
            "pdf_size_bytes": pdf_path.stat().st_size,
            "page_count": len(pages),
            "source_preferred_url": None,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }

        # --- Set preferred URL from mapping if available ---
        if filename in url_mapping:
            issue_data["source_preferred_url"] = url_mapping[filename]["url_berlin"]

        # --- Insert into database ---
        issue_id = await insert_issue_with_pages(issue_data, pages)

        # --- Upsert source URLs ---
        if filename in url_mapping:
            url_info = url_mapping[filename]
            await upsert_issue_source(
                issue_id=issue_id,
                source_kind="manual_seed",
                original_url=url_info["url_berlin"],
                canonical_url=url_info["url_berlin"],
            )
            if url_info["url_wayback"]:
                await upsert_issue_source(
                    issue_id=issue_id,
                    source_kind="wayback",
                    original_url=url_info["url_wayback"],
                    canonical_url=url_info["url_wayback"],
                )

        stats.inserted += 1
        print(f"  [{i}/{total}] {filename} -- {len(pages)} pages")

    elapsed = time.monotonic() - t0

    # --- Finalise ingest run ---
    final_status = "success" if stats.errors == 0 else "partial"
    await update_ingest_run(run_id, final_status, stats)

    # --- Summary ---
    print()
    print("-" * 60)
    print("  Bootstrap Ingest Complete")
    print("-" * 60)
    print(f"  Discovered : {stats.discovered}")
    print(f"  Inserted   : {stats.inserted}")
    print(f"  Skipped    : {stats.skipped}")
    print(f"  Errors     : {stats.errors}")
    print(f"  Status     : {final_status}")
    print(f"  Duration   : {elapsed:.1f}s")
    print("-" * 60)

    await close_db()


if __name__ == "__main__":
    asyncio.run(main())
