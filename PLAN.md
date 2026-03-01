# Amtsblatt MCP + REST API — Revised Implementation Plan (v2)

## 1. Goal

Build a durable ingestion and access layer for the **Amtsblatt für Berlin** that:

* backfills historical issues from existing local PDFs, the live Berlin site, and Wayback,
* ingests new issues automatically and idempotently,
* makes the archive searchable at **page level**,
* exposes the data via both:

  * an **MCP server** for assistants/agents,
  * a **REST API** for apps and developers.

This version treats the database—not `urls.csv`—as the real source of truth.

---

## 2. Core Design Decisions

### A. `urls.csv` becomes legacy input, not the system of record

Keep `urls.csv` as:

* a bootstrap manifest,
* an audit artifact,
* a manual seed file.

But move all authoritative tracking into SQLite tables (`issues`, `issue_sources`, `ingest_runs`).

### B. Deduplicate by filename + file hash, not raw URL

Do **not** use the full PDF URL as the uniqueness key.

Use:

* `filename` (e.g. `abl_2026_09_0521_0564_online.pdf`)
* `pdf_sha256`
* canonicalized URL (query string stripped) only as metadata

This avoids false "new issue" detections when cache-busting query params change. The current Berlin PDF links include a `?ts=...` query suffix, so raw URL comparison is unsafe.

### C. Model page labels as text, not just integers

Use:

* `gazette_page_label TEXT` (e.g. `"521"`, `"522"`, `"I"`, `"III"`)
* optional `gazette_page_num INTEGER` when a numeric parse is possible

This handles both regular issues and yearly **Sachwortregister**, whose visible page labels use Roman numerals.

### D. Page-level indexing is phase 1; notice-level segmentation is phase 2

For MVP:

* store and search **pages**.

Later:

* optionally segment into **Bekanntmachungen / notices** for better retrieval quality.

Pages are the right first indexing unit; they are stable, simple, and map cleanly to PDFs.

### E. MCP should return bounded chunks, not whole issues by default

Avoid "dump full issue text" as the primary tool.

Instead:

* search pages,
* fetch metadata,
* fetch specific pages or ranges,
* optionally expose issue/page content as **resources**.

---

## 3. Architecture Overview

```text
Existing sources
  ├─ urls.csv                 (legacy seed)
  ├─ pdfs/{year}/*.pdf        (local archive)
  ├─ berlin.de listing pages  (live discovery)
  └─ Wayback CDX             (backfill / recovery)

                │
                ▼
        discover + canonicalize
                │
                ▼
          download / verify hash
                │
                ▼
         extract + normalize text
                │
                ▼
        SQLite (metadata + pages + FTS)
          ├─ issues
          ├─ issue_sources
          ├─ pages
          ├─ pages_fts
          └─ ingest_runs
                │
        ┌───────┴────────┐
        ▼                ▼
   MCP server         REST API
```

---

## 4. Project Structure

```text
amtsblatt/
  urls.csv                      # legacy seed / optional manual append log
  pdfs/                         # local archive (gitignored)
  data/
    amtsblatt.db                # SQLite DB (gitignored)
    logs/                       # ingestion logs
  pyproject.toml
  src/amtsblatt/
    __init__.py
    config.py                   # paths, settings
    models.py                   # pydantic / typed structs
    db.py                       # SQLite layer
    schema.sql                  # schema + indexes + triggers
    discovery.py                # berlin.de + register + wayback discovery
    downloader.py               # shared download + hash + canonicalization
    extract.py                  # PDF metadata + text extraction
    ingest.py                   # ingestion orchestration
    quality.py                  # low-text / image-like detection
    mcp_server.py               # MCP resources + tools
    api.py                      # FastAPI app
  scripts/
    bootstrap_ingest.py         # one-time historical import
    fetch_new.py                # idempotent incremental ingestion
    rebuild_fts.py              # maintenance helper
    verify_archive.py           # completeness + integrity checks
  tests/
    test_filename_parsing.py
    test_url_canonicalization.py
    test_extraction.py
    test_ingest_idempotency.py
```

---

## 5. Source Discovery Strategy

### Live discovery

Poll three source classes:

1. **Main Amtsblatt page** — current issue + last five issues
2. **Sachwortregister page** — yearly register PDFs
3. **Wayback CDX** — only for historical backfill, gaps, and recovery when Berlin removes old links

### Canonicalization rules

For every discovered URL:

* strip query string for canonical comparison,
* extract filename from path,
* keep: `original_url`, `canonical_url`, `source_kind` (`berlin_live`, `berlin_register`, `wayback`, `manual_seed`).

### Matching rules

A discovered file is "already known" if **any** of these match:

* same `filename`,
* same `pdf_sha256`,
* same `canonical_url`.

Do not rely on `urls.csv` diffing alone.

---

## 6. Database Schema

Use **SQLite + FTS5**.

### `issues`

```sql
CREATE TABLE issues (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    year                INTEGER NOT NULL,
    issue_code          TEXT NOT NULL,                 -- "09", "sr", etc.
    issue_kind          TEXT NOT NULL,                 -- "regular", "register", "special"
    publication_date    TEXT,                          -- ISO date if known
    page_label_start    TEXT,
    page_label_end      TEXT,
    page_start_num      INTEGER,
    page_end_num        INTEGER,
    filename            TEXT NOT NULL UNIQUE,
    pdf_rel_path        TEXT NOT NULL,
    pdf_sha256          TEXT NOT NULL UNIQUE,
    pdf_size_bytes      INTEGER,
    page_count          INTEGER,
    source_preferred_url TEXT,
    extracted_at        TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);
```

### `issue_sources`

```sql
CREATE TABLE issue_sources (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id            INTEGER REFERENCES issues(id) ON DELETE CASCADE,
    source_kind         TEXT NOT NULL,                 -- berlin_live, berlin_register, wayback, manual_seed
    original_url        TEXT NOT NULL,
    canonical_url       TEXT NOT NULL,
    first_seen_at       TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen_at        TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(source_kind, canonical_url)
);
```

### `pages`

```sql
CREATE TABLE pages (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id            INTEGER NOT NULL REFERENCES issues(id) ON DELETE CASCADE,
    pdf_page_num        INTEGER NOT NULL,              -- 1-based PDF page index
    gazette_page_label  TEXT,                          -- "521", "III", etc.
    gazette_page_num    INTEGER,                       -- nullable normalized numeric form
    text                TEXT NOT NULL,
    text_length         INTEGER NOT NULL,
    is_low_text         INTEGER NOT NULL DEFAULT 0,
    is_image_like       INTEGER NOT NULL DEFAULT 0,
    extraction_method   TEXT NOT NULL DEFAULT 'pymupdf',
    UNIQUE(issue_id, pdf_page_num)
);
```

### `pages_fts`

```sql
CREATE VIRTUAL TABLE pages_fts USING fts5(
    text,
    content='pages',
    content_rowid='id'
);
```

Add the usual FTS sync triggers on `pages`.

### `ingest_runs`

```sql
CREATE TABLE ingest_runs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    run_type            TEXT NOT NULL,                 -- bootstrap, incremental, rebuild
    started_at          TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at         TEXT,
    status              TEXT NOT NULL,                 -- running, success, partial, failed
    discovered_count    INTEGER NOT NULL DEFAULT 0,
    downloaded_count    INTEGER NOT NULL DEFAULT 0,
    inserted_count      INTEGER NOT NULL DEFAULT 0,
    skipped_count       INTEGER NOT NULL DEFAULT 0,
    error_count         INTEGER NOT NULL DEFAULT 0,
    notes               TEXT
);
```

### Recommended indexes

* `issues(year, issue_code)`
* `issues(publication_date)`
* `pages(issue_id, pdf_page_num)`
* `pages(issue_id, gazette_page_label)`

---

## 7. Extraction Pipeline

### Filename parsing

Support at least:

* regular issues: `abl_2026_09_0521_0564_online.pdf`
* register issues: `abl_2025_sr_0001_0024_online.pdf`

Return: `year`, `issue_code`, `issue_kind`, filename-based range hints.

### Text extraction

Primary extractor: **PyMuPDF** (`fitz`)

For each page:

1. extract text
2. normalize whitespace
3. compute `text_length`
4. try to parse visible page label from page header
5. store page text even if imperfect

### Quality flags

Mark a page as suspicious if:

* text length is below a threshold,
* text is mostly isolated symbols,
* very low alphanumeric ratio,
* repeated header fragments dominate page content.

Set `is_low_text = 1` and `is_image_like = 1` when likely map/image-heavy.

### OCR fallback

Not phase 1. Store poor pages, flag them, optionally add targeted OCR later for pages marked `is_image_like`.

---

## 8. Ingestion Workflows

### A. Bootstrap import (`bootstrap_ingest.py`)

1. Initialize DB/schema
2. Scan local `pdfs/`
3. Parse filename metadata
4. Look up matching URLs from `urls.csv`, live site, Wayback
5. Hash file
6. Skip if `pdf_sha256` already exists
7. Extract pages
8. Insert issue + sources + pages in one transaction
9. Record run in `ingest_runs`

Safe to rerun at any time.

### B. Incremental import (`fetch_new.py`)

1. Create `ingest_runs` row
2. Discover live source URLs
3. Canonicalize + filter already known items
4. Download unknown files to temp path
5. Hash file
6. Dedupe again by hash
7. Move into archive path (`pdfs/{year}/...`)
8. Extract + insert
9. Update `issue_sources`
10. Finalize run

**Must be idempotent:** running it twice produces zero duplicates.

---

## 9. Database Access Layer (`db.py`)

Key functions:

* `init_db()`
* `insert_issue_with_pages(...)`
* `upsert_issue_source(...)`
* `search_pages(query, year=None, issue_kind=None, limit=20, offset=0)`
* `get_issue_meta(year, issue_code)`
* `get_issue_pages(year, issue_code, page_from=None, page_to=None, limit=20)`
* `list_issues(year=None, issue_kind=None, limit=100, offset=0)`
* `get_stats()`
* `record_ingest_run(...)`

Operational settings on startup:

* enable **WAL mode**
* set `busy_timeout`
* wrap inserts in transactions
* prefer one writer at a time

---

## 10. MCP Server (`mcp_server.py`)

### Resources

* `amtsblatt://issues/{year}/{issue_code}/meta`
* `amtsblatt://issues/{year}/{issue_code}/pages/{page}`
* `amtsblatt://issues/{year}/{issue_code}/range/{from}-{to}`

### Tools

| Tool | Purpose | Key params |
|------|---------|------------|
| `amtsblatt_search_pages` | Full-text search with snippets | `query`, `year?`, `issue_kind?`, `limit?` |
| `amtsblatt_get_issue_meta` | Metadata for one issue | `year`, `issue_code` |
| `amtsblatt_get_pages` | Selected pages or range from one issue | `year`, `issue_code`, `page_from`, `page_to` |
| `amtsblatt_list_issues` | List issues with filters | `year?`, `issue_kind?`, `limit?` |
| `amtsblatt_stats` | Coverage, counts, ingestion status, gaps | — |

### Transport

* **stdio** for local / Claude Code
* **HTTP transport** for remote hosting

---

## 11. REST API (`api.py`)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/search?q=...&year=...&issue_kind=...&limit=...&offset=...` | GET | Full-text search with snippets |
| `/api/issues?year=...&issue_kind=...&limit=...&offset=...` | GET | List issues |
| `/api/issues/{year}/{issue_code}` | GET | Issue metadata |
| `/api/issues/{year}/{issue_code}/pages?from=...&to=...&limit=...` | GET | Page text |
| `/api/issues/{year}/{issue_code}/pdf` | GET | Redirect to or stream PDF |
| `/api/stats` | GET | Archive statistics |
| `/healthz` | GET | Liveness check |

FastAPI auto-generates `/docs` (OpenAPI).

---

## 12. Automation

### Scheduling philosophy

Do **not** rely on one single Friday cron run. Poll regularly and let "nothing new" be a valid result. The Berlin page says the regular issue appears **generally** on Fridays.

### Recommended: simple daily poll

```cron
30 08 * * * cd /path/to/amtsblatt && uv run python scripts/fetch_new.py >> data/logs/fetch.log 2>&1
```

### Each run logs

* start/end timestamps
* discovered/downloaded/inserted/skipped/error counts
* filenames processed
* exceptions

---

## 13. Observability and Safety

### Archive integrity (`verify_archive.py`)

* detect missing issue numbers within a year
* detect duplicate filenames
* detect mismatched page counts
* detect files on disk not registered in DB
* detect DB rows whose files no longer exist

### Search quality reporting

* total pages, low-text pages, image-like pages
* percent of pages with parsed `gazette_page_label`

### Access policy (if publicly hosted)

* rate limiting
* clear user-agent / contact info
* no bulk full-text dump endpoint
* log abusive patterns

---

## 14. Implementation Order

### Phase 1 — Solid archive + searchable pages

1. Scaffold project structure + `pyproject.toml`
2. Write `schema.sql`
3. Implement URL canonicalization + file hashing (`downloader.py`)
4. Implement filename parsing (`extract.py`)
5. Implement PyMuPDF extraction + quality flags (`extract.py`, `quality.py`)
6. Implement DB layer + FTS triggers (`db.py`)
7. Build `bootstrap_ingest.py`
8. Import existing archive
9. Build `verify_archive.py`
10. Build REST API (`api.py`)
11. Build MCP server (`mcp_server.py`)
12. Build `fetch_new.py` (incremental ingestion)
13. Add cron

### Phase 2 — Better retrieval

14. Parse publication dates more reliably
15. Improve page-label extraction
16. Add notice segmentation
17. Add notice-level search and retrieval
18. Optional targeted OCR for flagged pages

---

## 15. Verification

### After bootstrap

* all local PDFs represented in `issues`
* no duplicate `pdf_sha256`
* page counts match extracted pages
* FTS returns hits for known terms
* register issues correctly typed as `issue_kind = 'register'`

### After incremental runs

* rerun `fetch_new.py` twice → second run inserts nothing
* new live issue appears → downloads once, inserts once
* changed `?ts=` URL alone does not create a duplicate

### API tests

* search known term, fetch issue metadata, fetch specific page ranges
* `/docs` loads, `/healthz` returns OK

### MCP tests

* list tools/resources
* search for known term
* fetch a page range (not whole issue)
* verify outputs remain bounded and LLM-friendly

---

## 16. Dependencies

```toml
[project]
name = "amtsblatt-mcp"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "mcp[cli]>=1.0.0",
    "pydantic>=2.0.0",
    "aiosqlite>=0.20.0",
    "pymupdf>=1.24.0",
    "httpx>=0.27.0",
    "fastapi>=0.115.0",
    "uvicorn>=0.34.0",
]
```

---

## 17. What Changed from v1

* **raw URL dedupe** → now filename/hash based
* **integer-only page model** → now text labels + optional normalized numeric field
* **full-issue MCP dump** → now bounded page/range retrieval
* **Friday-only cron assumption** → now idempotent daily polling
* **Wayback as normal discovery path** → now fallback/backfill only
* **`urls.csv` as authority** → now DB-backed source tracking
* **no ingest observability** → now `ingest_runs` + verification scripts
* **no handling for low-text/image-like pages** → now quality flags + optional OCR later
