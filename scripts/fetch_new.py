#!/usr/bin/env python3
"""Idempotent incremental ingestion of new Amtsblatt issues.

Discovers new issues from berlin.de and Wayback Machine,
downloads PDFs, extracts text, and inserts into the database.
Safe to run repeatedly — duplicates are detected by filename and hash.
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from amtsblatt.config import DB_PATH, PDF_DIR  # noqa: E402
from amtsblatt.db import (  # noqa: E402
    close_db,
    init_db,
    insert_issue_with_pages,
    issue_exists,
    record_ingest_run,
    update_ingest_run,
    upsert_issue_source,
)
from amtsblatt.discovery import discover_all  # noqa: E402
from amtsblatt.downloader import download_pdf, pdf_dest_path  # noqa: E402
from amtsblatt.extract import compute_file_hash, extract_text_from_pdf, parse_filename  # noqa: E402
from amtsblatt.models import IngestStats  # noqa: E402
from amtsblatt.quality import flag_pages  # noqa: E402

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    logger.info("Starting incremental ingestion")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    await init_db()

    stats = IngestStats()
    run_id = await record_ingest_run("incremental", "running", stats)

    try:
        # ── Discover phase ──────────────────────────────────────────
        discovered = await discover_all()
        stats.discovered = len(discovered)
        logger.info("Discovered %d URLs", len(discovered))

        # ── Filter known ────────────────────────────────────────────
        new_items: list[dict] = []
        for item in discovered:
            if not await issue_exists(filename=item["filename"]):
                new_items.append(item)

        logger.info("%d new items after filtering", len(new_items))

        # ── Download + extract phase ────────────────────────────────
        for item in new_items:
            # Parse filename
            try:
                parsed = parse_filename(item["filename"])
            except ValueError:
                logger.warning("Skipping unparseable filename: %s", item["filename"])
                stats.errors += 1
                continue

            dest = pdf_dest_path(parsed.year, parsed.filename)

            # Download to temp file first
            fd, tmp_path_str = tempfile.mkstemp(suffix=".pdf")
            tmp_path = Path(tmp_path_str)
            try:
                os.close(fd)

                success = await download_pdf(item["original_url"], tmp_path)
                if not success:
                    logger.warning(
                        "Download failed for %s from %s",
                        parsed.filename,
                        item["original_url"],
                    )
                    stats.errors += 1
                    continue

                stats.downloaded += 1

                # Duplicate check by hash
                file_hash = compute_file_hash(tmp_path)
                if await issue_exists(pdf_sha256=file_hash):
                    logger.info("Duplicate by hash, skipping: %s", parsed.filename)
                    stats.skipped += 1
                    continue

                # Move temp file to final destination
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(tmp_path), str(dest))

            finally:
                # Clean up temp file if it still exists
                if tmp_path.exists():
                    tmp_path.unlink()

            # Extract text
            pages = extract_text_from_pdf(dest)
            pages = flag_pages(pages)

            # Build issue_data dict
            pdf_rel_path = str(dest.relative_to(PDF_DIR))
            pdf_size = dest.stat().st_size

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
                "pdf_size_bytes": pdf_size,
                "page_count": len(pages),
                "source_preferred_url": item["canonical_url"],
                "extracted_at": datetime.now(timezone.utc).isoformat(),
            }

            issue_id = await insert_issue_with_pages(issue_data, pages)

            await upsert_issue_source(
                issue_id=issue_id,
                source_kind=item["source_kind"],
                original_url=item["original_url"],
                canonical_url=item["canonical_url"],
            )

            stats.inserted += 1
            logger.info("Ingested %s — %d pages", parsed.filename, len(pages))

        # ── Finalize ────────────────────────────────────────────────
        if stats.errors == 0:
            status = "success"
        elif stats.inserted > 0:
            status = "partial"
        else:
            status = "failed"

        await update_ingest_run(run_id, status, stats)

        logger.info(
            "Ingestion complete: discovered=%d downloaded=%d inserted=%d skipped=%d errors=%d status=%s",
            stats.discovered,
            stats.downloaded,
            stats.inserted,
            stats.skipped,
            stats.errors,
            status,
        )

    except Exception:
        await update_ingest_run(run_id, "failed", stats)
        raise

    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
