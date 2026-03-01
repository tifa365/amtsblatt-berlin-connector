-- Amtsblatt für Berlin — Database Schema

PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS issues (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    year                INTEGER NOT NULL,
    issue_code          TEXT NOT NULL,
    issue_kind          TEXT NOT NULL,
    publication_date    TEXT,
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

CREATE INDEX IF NOT EXISTS idx_issues_year_code ON issues(year, issue_code);
CREATE INDEX IF NOT EXISTS idx_issues_pub_date ON issues(publication_date);

CREATE TABLE IF NOT EXISTS issue_sources (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id            INTEGER REFERENCES issues(id) ON DELETE CASCADE,
    source_kind         TEXT NOT NULL,
    original_url        TEXT NOT NULL,
    canonical_url       TEXT NOT NULL,
    first_seen_at       TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen_at        TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(source_kind, canonical_url)
);

CREATE TABLE IF NOT EXISTS pages (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id            INTEGER NOT NULL REFERENCES issues(id) ON DELETE CASCADE,
    pdf_page_num        INTEGER NOT NULL,
    gazette_page_label  TEXT,
    gazette_page_num    INTEGER,
    text                TEXT NOT NULL,
    text_length         INTEGER NOT NULL,
    is_low_text         INTEGER NOT NULL DEFAULT 0,
    is_image_like       INTEGER NOT NULL DEFAULT 0,
    extraction_method   TEXT NOT NULL DEFAULT 'pymupdf',
    UNIQUE(issue_id, pdf_page_num)
);

CREATE INDEX IF NOT EXISTS idx_pages_issue ON pages(issue_id, pdf_page_num);
CREATE INDEX IF NOT EXISTS idx_pages_gazette ON pages(issue_id, gazette_page_label);

CREATE VIRTUAL TABLE IF NOT EXISTS pages_fts USING fts5(
    text,
    content='pages',
    content_rowid='id'
);

-- FTS sync triggers
CREATE TRIGGER IF NOT EXISTS pages_ai AFTER INSERT ON pages BEGIN
    INSERT INTO pages_fts(rowid, text) VALUES (new.id, new.text);
END;
CREATE TRIGGER IF NOT EXISTS pages_ad AFTER DELETE ON pages BEGIN
    INSERT INTO pages_fts(pages_fts, rowid, text) VALUES('delete', old.id, old.text);
END;
CREATE TRIGGER IF NOT EXISTS pages_au AFTER UPDATE ON pages BEGIN
    INSERT INTO pages_fts(pages_fts, rowid, text) VALUES('delete', old.id, old.text);
    INSERT INTO pages_fts(rowid, text) VALUES (new.id, new.text);
END;

CREATE TABLE IF NOT EXISTS ingest_runs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    run_type            TEXT NOT NULL,
    started_at          TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at         TEXT,
    status              TEXT NOT NULL,
    discovered_count    INTEGER NOT NULL DEFAULT 0,
    downloaded_count    INTEGER NOT NULL DEFAULT 0,
    inserted_count      INTEGER NOT NULL DEFAULT 0,
    skipped_count       INTEGER NOT NULL DEFAULT 0,
    error_count         INTEGER NOT NULL DEFAULT 0,
    notes               TEXT
);
