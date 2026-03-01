#!/usr/bin/env python3
"""Verify archive integrity and completeness."""

from __future__ import annotations

import asyncio
import sys
from collections import defaultdict
from pathlib import Path

# Ensure project src is on the import path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from amtsblatt.config import DB_PATH, PDF_DIR, PROJECT_ROOT
from amtsblatt.db import close_db, get_db, init_db, list_issues
from amtsblatt.extract import compute_file_hash


async def main() -> None:
    print("=" * 60)
    print("  Amtsblatt Archive Verification")
    print("=" * 60)
    print()

    # --- Initialise DB ---
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at {DB_PATH}")
        print("Run bootstrap_ingest.py first.")
        sys.exit(1)

    await init_db()

    # --- Load all issues from DB ---
    issues = await list_issues(limit=10000)
    print(f"Issues in database: {len(issues)}")

    # Build lookup sets
    db_filenames: dict[str, object] = {iss.filename: iss for iss in issues}
    db_hashes: dict[str, str] = {iss.pdf_sha256: iss.filename for iss in issues}

    # --- Scan all PDFs on disk ---
    disk_pdfs = sorted(PDF_DIR.rglob("*.pdf"))
    disk_filenames: set[str] = {p.name for p in disk_pdfs}
    print(f"PDF files on disk : {len(disk_pdfs)}")
    print()

    # ---------------------------------------------------------------
    # Check 1: Files on disk not in DB
    # ---------------------------------------------------------------
    not_in_db = disk_filenames - set(db_filenames.keys())
    print(f"[1] Files on disk but NOT in database: {len(not_in_db)}")
    for fn in sorted(not_in_db):
        print(f"     - {fn}")
    print()

    # ---------------------------------------------------------------
    # Check 2: DB entries whose files don't exist on disk
    # ---------------------------------------------------------------
    missing_on_disk: list[str] = []
    for iss in issues:
        full_path = PROJECT_ROOT / iss.pdf_rel_path
        if not full_path.exists():
            missing_on_disk.append(iss.filename)

    print(f"[2] DB entries with missing file on disk: {len(missing_on_disk)}")
    for fn in sorted(missing_on_disk):
        print(f"     - {fn}")
    print()

    # ---------------------------------------------------------------
    # Check 3: Duplicate SHA-256 hashes among files on disk
    # ---------------------------------------------------------------
    print("[3] Checking for duplicate SHA-256 hashes on disk...")
    hash_to_files: dict[str, list[str]] = defaultdict(list)
    for pdf_path in disk_pdfs:
        file_hash = compute_file_hash(pdf_path)
        hash_to_files[file_hash].append(pdf_path.name)

    duplicates = {h: files for h, files in hash_to_files.items() if len(files) > 1}
    if duplicates:
        print(f"    Duplicate hashes found: {len(duplicates)}")
        for h, files in duplicates.items():
            print(f"     Hash {h[:16]}...  ->  {', '.join(files)}")
    else:
        print("    No duplicate hashes found.")
    print()

    # ---------------------------------------------------------------
    # Check 4: Per-year completeness
    # ---------------------------------------------------------------
    print("[4] Per-year completeness analysis:")
    issues_by_year: dict[int, list[object]] = defaultdict(list)
    for iss in issues:
        issues_by_year[iss.year].append(iss)

    for year in sorted(issues_by_year.keys()):
        year_issues = issues_by_year[year]

        # Separate regular issues from register/special
        regular_codes: list[int] = []
        other_codes: list[str] = []
        for iss in year_issues:
            if iss.issue_kind == "regular":
                try:
                    regular_codes.append(int(iss.issue_code))
                except ValueError:
                    other_codes.append(iss.issue_code)
            else:
                other_codes.append(iss.issue_code)

        if regular_codes:
            max_issue = max(regular_codes)
            expected = set(range(1, max_issue + 1))
            present = set(regular_codes)
            missing = sorted(expected - present)
            status = f"regular 1-{max_issue}, missing: {missing if missing else 'none'}"
        else:
            status = "no regular issues"

        extra = f" + {', '.join(sorted(other_codes))}" if other_codes else ""
        print(f"    {year}: {len(year_issues)} issues ({status}{extra})")

    print()

    # ---------------------------------------------------------------
    # Check 5: Page quality summary
    # ---------------------------------------------------------------
    print("[5] Page quality summary:")
    db = await get_db()

    cursor = await db.execute("SELECT COUNT(*) FROM pages")
    total_pages = (await cursor.fetchone())[0]

    cursor = await db.execute("SELECT COUNT(*) FROM pages WHERE is_low_text = 1")
    low_text_pages = (await cursor.fetchone())[0]

    cursor = await db.execute("SELECT COUNT(*) FROM pages WHERE is_image_like = 1")
    image_like_pages = (await cursor.fetchone())[0]

    print(f"    Total pages    : {total_pages}")
    print(f"    Low-text pages : {low_text_pages}")
    print(f"    Image-like     : {image_like_pages}")
    print()

    # ---------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------
    all_ok = (
        len(not_in_db) == 0
        and len(missing_on_disk) == 0
        and len(duplicates) == 0
    )
    print("=" * 60)
    if all_ok:
        print("  RESULT: Archive is consistent.")
    else:
        print("  RESULT: Issues detected -- see above for details.")
    print("=" * 60)

    await close_db()


if __name__ == "__main__":
    asyncio.run(main())
