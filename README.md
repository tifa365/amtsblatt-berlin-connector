# Amtsblatt für Berlin

[Amtsblatt für Berlin](https://de.wikipedia.org/wiki/Amtsblatt_f%C3%BCr_Berlin) — Berlin's official government gazette, published every Friday. It contains laws, regulations, job postings, public tenders, and court notices.

**Problem:** Berlin only keeps the last 6 issues online. Older issues disappear.

**Solution:** This project archives all issues as PDFs, extracts their full text into a searchable database, and provides both an MCP server and a REST API for access.

## What's in the archive

233 issues (2020–2025), 19,866 pages of extracted text in a SQLite database with full-text search.

| Year | Issues | Of total | Completeness | Missing |
|------|--------|----------|--------------|---------|
| 2020 | 10 | 55 | 18% | 1-13, 16-39, 46-50, 52-54 |
| 2021 | 28 | 58 | 48% | 1-14, 17-19, 21-22, 25-28, 30, 38-40, 42-44 |
| 2022 | 48 | 54 | 89% | 28-29, 34-35, 37, 39 |
| 2023 | 49 | 57 | 86% | 30-31, 33-38 |
| 2024 | 55 | 55 | **100%** | — |
| 2025 | 39 | 51 | 76% | 7, 24-25, 33-34, 36-37, 44-45, 47-48, 50 |

2020 and 2021 are largely incomplete because the Wayback Machine only started crawling these PDFs partway through those years. 2024 is the only fully complete year.

## Setup

```bash
uv sync
```

### Bootstrap the database

Download the PDFs first (if not already present), then extract text into SQLite:

```bash
python3 download.py                     # download PDFs to pdfs/
uv run python scripts/bootstrap_ingest.py  # extract text → data/amtsblatt.db
```

## MCP Server

Exposes 5 tools for AI assistants to search and read the gazette:

| Tool | Description |
|------|-------------|
| `amtsblatt_search_pages` | Full-text search with highlighted snippets |
| `amtsblatt_get_issue_meta` | Metadata for a specific issue |
| `amtsblatt_get_pages` | Read extracted text of specific pages |
| `amtsblatt_list_issues` | List available issues (filterable by year) |
| `amtsblatt_stats` | Archive statistics and completeness |

```bash
# Add to Claude Code
claude mcp add amtsblatt -- uv run amtsblatt-mcp

# Or run standalone (stdio)
uv run amtsblatt-mcp

# Or run with HTTP transport
MCP_TRANSPORT=streamable-http uv run amtsblatt-mcp
```

## REST API

Same capabilities as the MCP server, exposed as JSON endpoints with auto-generated OpenAPI docs at `/docs`.

```bash
uv run uvicorn amtsblatt.api:app
```

| Endpoint | Description |
|----------|-------------|
| `GET /api/search?q=Bebauungsplan` | Full-text search |
| `GET /api/issues?year=2024` | List issues |
| `GET /api/issues/2024/01` | Issue metadata |
| `GET /api/issues/2024/01/pages?page_from=1&page_to=5` | Page text |
| `GET /api/issues/2024/01/pdf` | Serve/redirect to PDF |
| `GET /api/stats` | Archive statistics |

## Automated ingestion

New issues are discovered from berlin.de and the Wayback Machine, downloaded, and extracted:

```bash
uv run python scripts/fetch_new.py
```

This is idempotent — safe to run on a daily cron:

```cron
30 08 * * * cd /path/to/amtsblatt && uv run python scripts/fetch_new.py >> data/logs/fetch.log 2>&1
```

## How it works

1. **Discovery** — scrapes berlin.de for current issues + queries Wayback CDX API for historical ones
2. **Download** — fetches PDFs with rate limiting and exponential backoff, validates `%PDF` magic bytes
3. **Extraction** — PyMuPDF extracts text page-by-page, with quality flags for low-text/image-heavy pages
4. **Storage** — SQLite with FTS5 full-text search, deduplicated by filename and SHA-256 hash
5. **Access** — MCP server for AI assistants, REST API for apps, raw PDFs as cold archive

## Data sources

- **Primary:** [berlin.de/landesverwaltungsamt](https://www.berlin.de/landesverwaltungsamt/logistikservice/amtsblatt-fuer-berlin/) (current + last 5 issues)
- **Fallback:** [Wayback Machine](https://web.archive.org/) (historical issues berlin.de has removed)
- **URL discovery:** Wayback Machine [CDX API](https://web.archive.org/cdx/search/cdx)

## Project structure

```
download.py          # Legacy PDF downloader (reads urls.csv)
urls.csv             # Historical URL manifest
src/amtsblatt/
  mcp_server.py      # MCP server (5 tools, 3 resources)
  api.py             # FastAPI REST API
  db.py              # SQLite + FTS5 access layer
  extract.py         # PDF text extraction (PyMuPDF)
  discovery.py       # Source discovery (berlin.de + Wayback)
  downloader.py      # PDF download with backoff
  quality.py         # Page quality assessment
  schema.sql         # Database schema
scripts/
  bootstrap_ingest.py   # One-time historical import
  fetch_new.py          # Incremental ingestion (for cron)
  verify_archive.py     # Integrity + completeness checks
  rebuild_fts.py        # FTS index maintenance
```
