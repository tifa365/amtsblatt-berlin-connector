"""Async SQLite access layer for the Amtsblatt archive."""

from __future__ import annotations

import os
from pathlib import Path

import aiosqlite

from amtsblatt.config import DB_PATH
from amtsblatt.models import ArchiveStats, IngestStats, IssueMeta, PageResult

_db: aiosqlite.Connection | None = None


async def get_db() -> aiosqlite.Connection:
    """Lazily open and return the shared database connection."""
    global _db
    if _db is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _db = await aiosqlite.connect(DB_PATH)
        await _db.execute("PRAGMA journal_mode = WAL")
        await _db.execute("PRAGMA busy_timeout = 5000")
        await _db.execute("PRAGMA foreign_keys = ON")
        _db.row_factory = aiosqlite.Row
    return _db


async def close_db() -> None:
    """Close the database connection if open."""
    global _db
    if _db is not None:
        await _db.close()
        _db = None


async def init_db() -> None:
    """Read schema.sql and execute it to initialise the database."""
    db = await get_db()
    schema_path = Path(__file__).resolve().parent / "schema.sql"
    schema_sql = schema_path.read_text(encoding="utf-8")
    await db.executescript(schema_sql)


async def insert_issue_with_pages(issue_data: dict, page_texts: list[dict]) -> int:
    """Insert one issue row and all its page rows in a single transaction.

    Returns the new issue id.
    """
    db = await get_db()
    async with db.execute("BEGIN"):
        cursor = await db.execute(
            """
            INSERT INTO issues (
                year, issue_code, issue_kind, publication_date,
                page_label_start, page_label_end, page_start_num, page_end_num,
                filename, pdf_rel_path, pdf_sha256, pdf_size_bytes,
                page_count, source_preferred_url, extracted_at
            ) VALUES (
                :year, :issue_code, :issue_kind, :publication_date,
                :page_label_start, :page_label_end, :page_start_num, :page_end_num,
                :filename, :pdf_rel_path, :pdf_sha256, :pdf_size_bytes,
                :page_count, :source_preferred_url, :extracted_at
            )
            """,
            issue_data,
        )
        issue_id = cursor.lastrowid

        await db.executemany(
            """
            INSERT INTO pages (
                issue_id, pdf_page_num, gazette_page_label, gazette_page_num,
                text, text_length, is_low_text, is_image_like, extraction_method
            ) VALUES (
                :issue_id, :pdf_page_num, :gazette_page_label, :gazette_page_num,
                :text, :text_length, :is_low_text, :is_image_like, :extraction_method
            )
            """,
            [{"issue_id": issue_id, **p} for p in page_texts],
        )

        await db.commit()
    return issue_id


async def upsert_issue_source(
    issue_id: int, source_kind: str, original_url: str, canonical_url: str
) -> None:
    """Insert or update (last_seen_at) an issue_sources row."""
    db = await get_db()
    await db.execute(
        """
        INSERT INTO issue_sources (issue_id, source_kind, original_url, canonical_url)
        VALUES (:issue_id, :source_kind, :original_url, :canonical_url)
        ON CONFLICT (source_kind, canonical_url) DO UPDATE SET
            last_seen_at = datetime('now')
        """,
        {
            "issue_id": issue_id,
            "source_kind": source_kind,
            "original_url": original_url,
            "canonical_url": canonical_url,
        },
    )
    await db.commit()


async def issue_exists(
    filename: str | None = None, pdf_sha256: str | None = None
) -> bool:
    """Check whether an issue already exists by filename or SHA-256 hash."""
    db = await get_db()
    if filename is not None:
        cursor = await db.execute(
            "SELECT 1 FROM issues WHERE filename = ?", (filename,)
        )
        if await cursor.fetchone():
            return True
    if pdf_sha256 is not None:
        cursor = await db.execute(
            "SELECT 1 FROM issues WHERE pdf_sha256 = ?", (pdf_sha256,)
        )
        if await cursor.fetchone():
            return True
    return False


async def search_pages(
    query: str,
    year: int | None = None,
    issue_kind: str | None = None,
    limit: int = 10,
    offset: int = 0,
) -> list[PageResult]:
    """Full-text search across page content using FTS5 MATCH.

    Returns PageResult objects with a highlighted snippet.
    """
    db = await get_db()

    # Quote each word to prevent FTS5 operator interpretation
    # (hyphens as NOT, spaces as implicit AND on raw tokens)
    safe_query = " ".join(f'"{word}"' for word in query.split())

    where_clauses = ["pages_fts MATCH :query"]
    params: dict = {"query": safe_query, "limit": limit, "offset": offset}

    if year is not None:
        where_clauses.append("i.year = :year")
        params["year"] = year
    if issue_kind is not None:
        where_clauses.append("i.issue_kind = :issue_kind")
        params["issue_kind"] = issue_kind

    where = " AND ".join(where_clauses)

    sql = f"""
        SELECT
            p.issue_id,
            i.year,
            i.issue_code,
            i.issue_kind,
            p.pdf_page_num,
            p.gazette_page_label,
            p.gazette_page_num,
            p.text,
            snippet(pages_fts, 0, '**', '**', '...', 32) AS snippet
        FROM pages_fts
        JOIN pages p ON p.id = pages_fts.rowid
        JOIN issues i ON i.id = p.issue_id
        WHERE {where}
        ORDER BY rank
        LIMIT :limit OFFSET :offset
    """

    results: list[PageResult] = []
    async with db.execute(sql, params) as cursor:
        async for row in cursor:
            results.append(
                PageResult(
                    issue_id=row["issue_id"],
                    year=row["year"],
                    issue_code=row["issue_code"],
                    issue_kind=row["issue_kind"],
                    pdf_page_num=row["pdf_page_num"],
                    gazette_page_label=row["gazette_page_label"],
                    gazette_page_num=row["gazette_page_num"],
                    text=row["text"],
                    snippet=row["snippet"],
                )
            )
    return results


async def get_issue_meta(year: int, issue_code: str) -> IssueMeta | None:
    """Get issue metadata by year and issue_code."""
    db = await get_db()
    cursor = await db.execute(
        """
        SELECT
            id, year, issue_code, issue_kind, publication_date,
            page_label_start, page_label_end, page_start_num, page_end_num,
            filename, pdf_rel_path, pdf_sha256, pdf_size_bytes,
            page_count, source_preferred_url, extracted_at
        FROM issues
        WHERE year = ? AND issue_code = ?
        """,
        (year, issue_code),
    )
    row = await cursor.fetchone()
    if row is None:
        return None
    return IssueMeta(
        id=row["id"],
        year=row["year"],
        issue_code=row["issue_code"],
        issue_kind=row["issue_kind"],
        publication_date=row["publication_date"],
        page_label_start=row["page_label_start"],
        page_label_end=row["page_label_end"],
        page_start_num=row["page_start_num"],
        page_end_num=row["page_end_num"],
        filename=row["filename"],
        pdf_rel_path=row["pdf_rel_path"],
        pdf_sha256=row["pdf_sha256"],
        pdf_size_bytes=row["pdf_size_bytes"],
        page_count=row["page_count"],
        source_preferred_url=row["source_preferred_url"],
        extracted_at=row["extracted_at"],
    )


async def get_issue_pages(
    year: int,
    issue_code: str,
    page_from: int = 1,
    page_to: int | None = None,
    limit: int = 20,
) -> list[PageResult]:
    """Get pages for a specific issue, optionally filtered by page range."""
    db = await get_db()

    params: dict = {
        "year": year,
        "issue_code": issue_code,
        "page_from": page_from,
    }

    if page_to is not None:
        range_clause = "AND p.pdf_page_num BETWEEN :page_from AND :page_to"
        params["page_to"] = page_to
    else:
        range_clause = "AND p.pdf_page_num >= :page_from"

    params["limit"] = limit

    sql = f"""
        SELECT
            p.issue_id,
            i.year,
            i.issue_code,
            i.issue_kind,
            p.pdf_page_num,
            p.gazette_page_label,
            p.gazette_page_num,
            p.text
        FROM pages p
        JOIN issues i ON i.id = p.issue_id
        WHERE i.year = :year AND i.issue_code = :issue_code
            {range_clause}
        ORDER BY p.pdf_page_num
        LIMIT :limit
    """

    results: list[PageResult] = []
    async with db.execute(sql, params) as cursor:
        async for row in cursor:
            results.append(
                PageResult(
                    issue_id=row["issue_id"],
                    year=row["year"],
                    issue_code=row["issue_code"],
                    issue_kind=row["issue_kind"],
                    pdf_page_num=row["pdf_page_num"],
                    gazette_page_label=row["gazette_page_label"],
                    gazette_page_num=row["gazette_page_num"],
                    text=row["text"],
                )
            )
    return results


async def list_issues(
    year: int | None = None,
    issue_kind: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[IssueMeta]:
    """List issues with optional filters, ordered by year DESC, issue_code ASC."""
    db = await get_db()

    where_clauses: list[str] = []
    params: dict = {"limit": limit, "offset": offset}

    if year is not None:
        where_clauses.append("year = :year")
        params["year"] = year
    if issue_kind is not None:
        where_clauses.append("issue_kind = :issue_kind")
        params["issue_kind"] = issue_kind

    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    sql = f"""
        SELECT
            id, year, issue_code, issue_kind, publication_date,
            page_label_start, page_label_end, page_start_num, page_end_num,
            filename, pdf_rel_path, pdf_sha256, pdf_size_bytes,
            page_count, source_preferred_url, extracted_at
        FROM issues
        {where}
        ORDER BY year DESC, issue_code ASC
        LIMIT :limit OFFSET :offset
    """

    results: list[IssueMeta] = []
    async with db.execute(sql, params) as cursor:
        async for row in cursor:
            results.append(
                IssueMeta(
                    id=row["id"],
                    year=row["year"],
                    issue_code=row["issue_code"],
                    issue_kind=row["issue_kind"],
                    publication_date=row["publication_date"],
                    page_label_start=row["page_label_start"],
                    page_label_end=row["page_label_end"],
                    page_start_num=row["page_start_num"],
                    page_end_num=row["page_end_num"],
                    filename=row["filename"],
                    pdf_rel_path=row["pdf_rel_path"],
                    pdf_sha256=row["pdf_sha256"],
                    pdf_size_bytes=row["pdf_size_bytes"],
                    page_count=row["page_count"],
                    source_preferred_url=row["source_preferred_url"],
                    extracted_at=row["extracted_at"],
                )
            )
    return results


async def get_stats() -> ArchiveStats:
    """Aggregate archive statistics."""
    db = await get_db()

    cursor = await db.execute("SELECT COUNT(*) FROM issues")
    total_issues = (await cursor.fetchone())[0]

    cursor = await db.execute("SELECT COUNT(*) FROM pages")
    total_pages = (await cursor.fetchone())[0]

    cursor = await db.execute(
        "SELECT year, COUNT(*) as cnt FROM issues GROUP BY year ORDER BY year"
    )
    issues_by_year: dict[int, int] = {}
    years: list[int] = []
    async for row in cursor:
        years.append(row["year"])
        issues_by_year[row["year"]] = row["cnt"]

    cursor = await db.execute("SELECT COUNT(*) FROM pages WHERE is_low_text = 1")
    low_text_pages = (await cursor.fetchone())[0]

    cursor = await db.execute("SELECT COUNT(*) FROM pages WHERE is_image_like = 1")
    image_like_pages = (await cursor.fetchone())[0]

    try:
        db_size = os.path.getsize(DB_PATH)
    except OSError:
        db_size = 0

    return ArchiveStats(
        total_issues=total_issues,
        total_pages=total_pages,
        years=years,
        issues_by_year=issues_by_year,
        low_text_pages=low_text_pages,
        image_like_pages=image_like_pages,
        db_size_bytes=db_size,
    )


async def record_ingest_run(
    run_type: str, status: str, stats: IngestStats, notes: str | None = None
) -> int:
    """Insert a new ingest_runs row. Returns the new row id."""
    db = await get_db()
    cursor = await db.execute(
        """
        INSERT INTO ingest_runs (
            run_type, status,
            discovered_count, downloaded_count, inserted_count,
            skipped_count, error_count, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_type,
            status,
            stats.discovered,
            stats.downloaded,
            stats.inserted,
            stats.skipped,
            stats.errors,
            notes,
        ),
    )
    await db.commit()
    return cursor.lastrowid


async def update_ingest_run(
    run_id: int, status: str, stats: IngestStats, notes: str | None = None
) -> None:
    """Update an existing ingest_runs row with final stats."""
    db = await get_db()
    await db.execute(
        """
        UPDATE ingest_runs SET
            status = ?,
            finished_at = datetime('now'),
            discovered_count = ?,
            downloaded_count = ?,
            inserted_count = ?,
            skipped_count = ?,
            error_count = ?,
            notes = ?
        WHERE id = ?
        """,
        (
            status,
            stats.discovered,
            stats.downloaded,
            stats.inserted,
            stats.skipped,
            stats.errors,
            notes,
            run_id,
        ),
    )
    await db.commit()
