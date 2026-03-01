"""
Amtsblatt fuer Berlin – MCP Server
====================================
MCP Server fuer Volltextsuche und Zugriff auf das Amtsblatt fuer Berlin.
Stellt Ausgaben (2020–heute) mit extrahiertem Seitentext bereit.
"""

import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from mcp.server.fastmcp import FastMCP

from amtsblatt.db import (
    close_db,
    get_issue_meta,
    get_issue_pages,
    get_stats,
    init_db,
    list_issues,
    search_pages,
)

# ─── Lifecycle ────────────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[None]:
    """Initialise the database on startup, close it on shutdown."""
    await init_db()
    try:
        yield
    finally:
        await close_db()


# ─── Server Setup ─────────────────────────────────────────────────────────────

mcp = FastMCP(
    "Amtsblatt für Berlin",
    instructions="Volltextsuche und Zugriff auf das Amtsblatt für Berlin (2020–heute)",
    lifespan=lifespan,
    host=os.environ.get("MCP_HOST", "0.0.0.0"),
    port=int(os.environ.get("PORT", "8000")),
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TOOLS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@mcp.tool(
    name="amtsblatt_search_pages",
    annotations={
        "title": "Volltextsuche im Amtsblatt",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    },
)
async def amtsblatt_search_pages(
    query: str,
    year: int | None = None,
    issue_kind: str | None = None,
    limit: int = 10,
) -> str:
    """Durchsucht den Volltext aller Amtsblatt-Seiten per FTS5.

    Findet Stellen in Amtsblaettern anhand von Suchbegriffen und gibt
    Treffer mit hervorgehobenen Textausschnitten zurueck.

    Args:
        query: Suchbegriff(e) fuer Volltextsuche
        year: Nur in einem bestimmten Jahr suchen
        issue_kind: Art der Ausgabe: regular, register, special
        limit: Maximale Anzahl Ergebnisse (1-50)
    """
    try:
        results = await search_pages(
            query=query,
            year=year,
            issue_kind=issue_kind,
            limit=min(max(limit, 1), 50),
        )

        if not results:
            return f"Keine Treffer fuer '{query}'."

        lines = [f"### {len(results)} Treffer fuer '{query}'\n"]
        for hit in results:
            ref = f"{hit.year}/{hit.issue_code}"
            label = hit.gazette_page_label or str(hit.pdf_page_num)
            snippet = hit.snippet or "(kein Snippet)"
            lines.append(f"- **{ref}** – Seite {label}\n  {snippet}\n")

        return "\n".join(lines)

    except Exception as e:
        return f"**Fehler** bei der Suche: {e}"


@mcp.tool(
    name="amtsblatt_get_issue_meta",
    annotations={
        "title": "Ausgaben-Metadaten abrufen",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    },
)
async def amtsblatt_get_issue_meta(year: int, issue_code: str) -> str:
    """Ruft die Metadaten einer bestimmten Amtsblatt-Ausgabe ab.

    Args:
        year: Erscheinungsjahr
        issue_code: Ausgabennummer, z.B. '09' oder 'sr' (Sonderregister)
    """
    try:
        meta = await get_issue_meta(year=year, issue_code=issue_code)

        if meta is None:
            return f"Ausgabe {year}/{issue_code} nicht gefunden."

        size_kb = round(meta.pdf_size_bytes / 1024, 1) if meta.pdf_size_bytes else "?"
        lines = [
            f"## Amtsblatt {meta.year}/{meta.issue_code}",
            "",
            f"- **Art**: {meta.issue_kind}",
            f"- **Erscheinungsdatum**: {meta.publication_date or 'unbekannt'}",
            f"- **Seitenbereich (Amtsblatt)**: {meta.page_label_start} – {meta.page_label_end}",
            f"- **PDF-Seiten**: {meta.page_count or '?'}",
            f"- **Dateiname**: `{meta.filename}`",
            f"- **Dateipfad**: `{meta.pdf_rel_path}`",
            f"- **Dateigroesse**: {size_kb} KB",
            f"- **SHA-256**: `{meta.pdf_sha256}`",
            f"- **Download-URL**: {meta.source_preferred_url or 'nicht verfuegbar'}",
            f"- **Extrahiert am**: {meta.extracted_at or '?'}",
        ]
        return "\n".join(lines)

    except Exception as e:
        return f"**Fehler** beim Abruf der Metadaten: {e}"


@mcp.tool(
    name="amtsblatt_get_pages",
    annotations={
        "title": "Seiten einer Ausgabe lesen",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    },
)
async def amtsblatt_get_pages(
    year: int,
    issue_code: str,
    page_from: int = 1,
    page_to: int = 5,
) -> str:
    """Gibt den extrahierten Text einzelner Seiten einer Amtsblatt-Ausgabe zurueck.

    Args:
        year: Erscheinungsjahr
        issue_code: Ausgabennummer
        page_from: Erste Seite (PDF-Seitennummer, ab 1)
        page_to: Letzte Seite (PDF-Seitennummer)
    """
    try:
        pages = await get_issue_pages(
            year=year,
            issue_code=issue_code,
            page_from=page_from,
            page_to=page_to,
        )

        if not pages:
            return (
                f"Keine Seiten gefunden fuer Ausgabe {year}/{issue_code} "
                f"(Seiten {page_from}–{page_to})."
            )

        # Get total page count from issue metadata for context
        meta = await get_issue_meta(year=year, issue_code=issue_code)
        total_info = f" (Ausgabe hat {meta.page_count} Seiten)" if meta and meta.page_count else ""

        lines = [
            f"## Amtsblatt {year}/{issue_code} – "
            f"Seiten {page_from}–{page_to}{total_info}\n"
        ]

        for page in pages:
            label = page.gazette_page_label or "?"
            lines.append(f"#### Seite {page.pdf_page_num} (Amtsblatt S. {label})")
            lines.append("")
            lines.append(page.text or "(kein Text extrahiert)")
            lines.append("")

        return "\n".join(lines)

    except Exception as e:
        return f"**Fehler** beim Seitenabruf: {e}"


@mcp.tool(
    name="amtsblatt_list_issues",
    annotations={
        "title": "Ausgaben auflisten",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    },
)
async def amtsblatt_list_issues(
    year: int | None = None,
    issue_kind: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> str:
    """Listet verfuegbare Amtsblatt-Ausgaben auf, optional nach Jahr oder Art gefiltert.

    Args:
        year: Nach Jahr filtern
        issue_kind: Art der Ausgabe: regular, register, special
        limit: Maximale Anzahl (1-500)
        offset: Offset fuer Paginierung
    """
    try:
        limit = min(max(limit, 1), 500)
        offset = max(offset, 0)
        issues = await list_issues(
            year=year,
            issue_kind=issue_kind,
            limit=limit,
            offset=offset,
        )

        if not issues:
            filter_info = ""
            if year:
                filter_info += f" Jahr={year}"
            if issue_kind:
                filter_info += f" Art={issue_kind}"
            return f"Keine Ausgaben gefunden{filter_info}."

        lines = [
            f"### {len(issues)} Ausgaben"
            + (f" (Offset {offset})" if offset else "")
            + "\n",
            "| Jahr | Nr. | Art | Seiten | Dateiname |",
            "|------|-----|-----|--------|-----------|",
        ]

        for issue in issues:
            pages = issue.page_count or "?"
            lines.append(
                f"| {issue.year} | {issue.issue_code} | {issue.issue_kind} "
                f"| {pages} | `{issue.filename}` |"
            )

        if len(issues) == limit:
            lines.append(
                f"\n*Weitere Ergebnisse mit offset={offset + limit}*"
            )

        return "\n".join(lines)

    except Exception as e:
        return f"**Fehler** beim Auflisten der Ausgaben: {e}"


@mcp.tool(
    name="amtsblatt_stats",
    annotations={
        "title": "Archiv-Statistiken",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    },
)
async def amtsblatt_stats() -> str:
    """Gibt Gesamtstatistiken des Amtsblatt-Archivs zurueck.

    Zeigt Anzahl der Ausgaben und Seiten insgesamt, aufgeschluesselt
    nach Jahr, sowie Informationen zur Textqualitaet.
    """
    try:
        stats = await get_stats()

        db_mb = round(stats.db_size_bytes / (1024 * 1024), 1)

        lines = [
            "## Amtsblatt-Archiv – Statistiken\n",
            f"- **Ausgaben gesamt**: {stats.total_issues}",
            f"- **Seiten gesamt**: {stats.total_pages}",
            f"- **Jahrgaenge**: {min(stats.years)}–{max(stats.years)}" if stats.years else "- **Jahrgaenge**: keine",
            f"- **Datenbankgroesse**: {db_mb} MB",
            "",
            "### Textqualitaet\n",
            f"- **Seiten mit wenig Text** (< Schwellenwert): {stats.low_text_pages}",
            f"- **Bild-aehnliche Seiten** (kaum extrahierbarer Text): {stats.image_like_pages}",
            "",
            "### Ausgaben pro Jahr\n",
            "| Jahr | Ausgaben |",
            "|------|----------|",
        ]

        for year in sorted(stats.issues_by_year):
            lines.append(f"| {year} | {stats.issues_by_year[year]} |")

        return "\n".join(lines)

    except Exception as e:
        return f"**Fehler** beim Abruf der Statistiken: {e}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# RESOURCES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@mcp.resource("amtsblatt://issues/{year}/{issue_code}/meta")
async def get_issue_meta_resource(year: int, issue_code: str) -> str:
    """Ausgaben-Metadaten als JSON-Resource."""
    meta = await get_issue_meta(year=year, issue_code=issue_code)
    if meta is None:
        return json.dumps({"error": f"Ausgabe {year}/{issue_code} nicht gefunden."})
    return json.dumps(
        {
            "id": meta.id,
            "year": meta.year,
            "issue_code": meta.issue_code,
            "issue_kind": meta.issue_kind,
            "publication_date": meta.publication_date,
            "page_label_start": meta.page_label_start,
            "page_label_end": meta.page_label_end,
            "page_start_num": meta.page_start_num,
            "page_end_num": meta.page_end_num,
            "filename": meta.filename,
            "pdf_rel_path": meta.pdf_rel_path,
            "pdf_sha256": meta.pdf_sha256,
            "pdf_size_bytes": meta.pdf_size_bytes,
            "page_count": meta.page_count,
            "source_preferred_url": meta.source_preferred_url,
            "extracted_at": meta.extracted_at,
        },
        indent=2,
        ensure_ascii=False,
        default=str,
    )


@mcp.resource("amtsblatt://issues/{year}/{issue_code}/pages/{page_num}")
async def get_page_resource(year: int, issue_code: str, page_num: int) -> str:
    """Einzelne Seite als Klartext-Resource."""
    pages = await get_issue_pages(
        year=year,
        issue_code=issue_code,
        page_from=page_num,
        page_to=page_num,
    )
    if not pages:
        return f"Seite {page_num} der Ausgabe {year}/{issue_code} nicht gefunden."
    return pages[0].text or ""


@mcp.resource("amtsblatt://issues/{year}/{issue_code}/range/{range_str}")
async def get_page_range_resource(year: int, issue_code: str, range_str: str) -> str:
    """Seitenbereich als zusammenhaengender Klartext-Resource.

    range_str hat das Format '{from}-{to}', z.B. '3-7'.
    """
    try:
        parts = range_str.split("-", 1)
        page_from = int(parts[0])
        page_to = int(parts[1])
    except (ValueError, IndexError):
        return f"Ungueltiges Seitenformat: '{range_str}'. Erwartet: '{{von}}-{{bis}}', z.B. '3-7'."

    pages = await get_issue_pages(
        year=year,
        issue_code=issue_code,
        page_from=page_from,
        page_to=page_to,
    )
    if not pages:
        return (
            f"Keine Seiten {page_from}–{page_to} in Ausgabe {year}/{issue_code} gefunden."
        )

    parts_text = []
    for page in pages:
        label = page.gazette_page_label or str(page.pdf_page_num)
        parts_text.append(f"--- Seite {page.pdf_page_num} (Amtsblatt S. {label}) ---")
        parts_text.append(page.text or "")
        parts_text.append("")

    return "\n".join(parts_text)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ENTRYPOINT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def main():
    """Start the Amtsblatt MCP server."""
    transport = os.environ.get("MCP_TRANSPORT", "stdio")
    mcp.run(transport=transport)


if __name__ == "__main__":
    main()
