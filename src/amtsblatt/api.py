"""FastAPI REST API for the Amtsblatt für Berlin archive."""

from __future__ import annotations

import dataclasses
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Path, Query
from fastapi.responses import FileResponse, RedirectResponse

from amtsblatt.config import PDF_DIR
from amtsblatt.db import (
    close_db,
    get_issue_meta,
    get_issue_pages,
    get_stats,
    init_db,
    list_issues,
    search_pages,
)
from amtsblatt.models import ArchiveStats, IssueMeta, PageResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _issue_to_dict(issue: IssueMeta) -> dict:
    """Convert an IssueMeta dataclass to a plain dict."""
    return dataclasses.asdict(issue)


def _page_to_dict(page: PageResult) -> dict:
    """Convert a PageResult dataclass to a plain dict."""
    return dataclasses.asdict(page)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield
    await close_db()


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Amtsblatt für Berlin API",
    description="Volltextsuche und Zugriff auf das Amtsblatt für Berlin",
    version="0.1.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/api/search")
async def api_search(
    q: str = Query(..., min_length=1, max_length=500, description="Suchbegriff(e)"),
    year: int | None = Query(default=None, description="Nach Jahr filtern"),
    issue_kind: str | None = Query(default=None, description="Art der Ausgabe: regular, register, special"),
    limit: int = Query(default=10, ge=1, le=50, description="Maximale Anzahl Ergebnisse"),
    offset: int = Query(default=0, ge=0, description="Offset für Paginierung"),
):
    """Full-text search across all gazette pages."""
    results = await search_pages(
        query=q, year=year, issue_kind=issue_kind, limit=limit, offset=offset
    )
    return {
        "query": q,
        "count": len(results),
        "results": [
            {
                "year": r.year,
                "issue_code": r.issue_code,
                "issue_kind": r.issue_kind,
                "pdf_page_num": r.pdf_page_num,
                "gazette_page_label": r.gazette_page_label,
                "snippet": r.snippet,
            }
            for r in results
        ],
    }


@app.get("/api/issues")
async def api_list_issues(
    year: int | None = Query(default=None, description="Nach Jahr filtern"),
    issue_kind: str | None = Query(default=None, description="Art der Ausgabe: regular, register, special"),
    limit: int = Query(default=100, ge=1, le=500, description="Maximale Anzahl"),
    offset: int = Query(default=0, ge=0, description="Offset für Paginierung"),
):
    """List gazette issues with optional filters."""
    issues = await list_issues(
        year=year, issue_kind=issue_kind, limit=limit, offset=offset
    )
    return {
        "count": len(issues),
        "issues": [_issue_to_dict(i) for i in issues],
    }


@app.get("/api/issues/{year}/{issue_code}")
async def api_get_issue(
    year: int = Path(..., description="Erscheinungsjahr"),
    issue_code: str = Path(..., description="Ausgabennummer, z.B. '09' oder 'sr'"),
):
    """Get metadata for a single gazette issue."""
    issue = await get_issue_meta(year, issue_code)
    if issue is None:
        raise HTTPException(status_code=404, detail="Issue not found")
    return _issue_to_dict(issue)


@app.get("/api/issues/{year}/{issue_code}/pages")
async def api_get_issue_pages(
    year: int = Path(..., description="Erscheinungsjahr"),
    issue_code: str = Path(..., description="Ausgabennummer"),
    page_from: int = Query(default=1, ge=1, description="Erste Seite (PDF-Seitennummer)"),
    page_to: int | None = Query(default=None, ge=1, description="Letzte Seite (PDF-Seitennummer)"),
    limit: int = Query(default=20, ge=1, le=100, description="Maximale Anzahl Seiten"),
):
    """Get page content for a specific gazette issue."""
    pages = await get_issue_pages(
        year=year,
        issue_code=issue_code,
        page_from=page_from,
        page_to=page_to,
        limit=limit,
    )
    return {
        "count": len(pages),
        "pages": [
            {
                "pdf_page_num": p.pdf_page_num,
                "gazette_page_label": p.gazette_page_label,
                "gazette_page_num": p.gazette_page_num,
                "text": p.text,
                "text_length": len(p.text) if p.text else 0,
            }
            for p in pages
        ],
    }


@app.get("/api/issues/{year}/{issue_code}/pdf")
async def api_get_issue_pdf(
    year: int = Path(..., description="Erscheinungsjahr"),
    issue_code: str = Path(..., description="Ausgabennummer"),
):
    """Download or redirect to the PDF file for a gazette issue."""
    issue = await get_issue_meta(year, issue_code)
    if issue is None:
        raise HTTPException(status_code=404, detail="Issue not found")

    pdf_path = PDF_DIR.parent / issue.pdf_rel_path
    if pdf_path.is_file():
        return FileResponse(
            path=str(pdf_path),
            media_type="application/pdf",
            filename=issue.filename,
        )

    if issue.source_preferred_url:
        return RedirectResponse(url=issue.source_preferred_url)

    raise HTTPException(status_code=404, detail="PDF file not found")


@app.get("/api/stats")
async def api_stats():
    """Get aggregate archive statistics."""
    stats = await get_stats()
    return dataclasses.asdict(stats)


@app.get("/healthz")
async def healthz():
    """Health check endpoint."""
    return {"status": "ok"}
