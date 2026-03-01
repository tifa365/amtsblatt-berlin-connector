#!/usr/bin/env python3
"""Rebuild the FTS5 full-text search index."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# Ensure project src is on the import path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from amtsblatt.config import DB_PATH
from amtsblatt.db import close_db, get_db, init_db


async def main() -> None:
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at {DB_PATH}")
        print("Run bootstrap_ingest.py first.")
        sys.exit(1)

    await init_db()
    db = await get_db()

    print("Rebuilding FTS5 index...")
    await db.execute("INSERT INTO pages_fts(pages_fts) VALUES('rebuild')")
    await db.commit()

    cursor = await db.execute("SELECT COUNT(*) FROM pages_fts")
    count = (await cursor.fetchone())[0]

    print(f"FTS index rebuilt successfully. Rows: {count}")

    await close_db()


if __name__ == "__main__":
    asyncio.run(main())
