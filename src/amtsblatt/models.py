"""Pydantic models and typed dataclasses used across the Amtsblatt project."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


@dataclass
class ParsedFilename:
    """Metadata extracted from an Amtsblatt PDF filename."""

    year: int
    issue_code: str  # "09", "sr", etc.
    issue_kind: str  # "regular", "register", "special"
    page_label_start: str  # "0521"
    page_label_end: str  # "0564"
    page_start_num: int | None  # 521 or None if not numeric
    page_end_num: int | None  # 564 or None if not numeric
    filename: str  # full filename


@dataclass
class IssueMeta:
    """Issue metadata from the database."""

    id: int
    year: int
    issue_code: str
    issue_kind: str
    publication_date: str | None
    page_label_start: str | None
    page_label_end: str | None
    page_start_num: int | None
    page_end_num: int | None
    filename: str
    pdf_rel_path: str
    pdf_sha256: str
    pdf_size_bytes: int | None
    page_count: int | None
    source_preferred_url: str | None
    extracted_at: str | None


@dataclass
class PageResult:
    """A single page from search or retrieval."""

    issue_id: int
    year: int
    issue_code: str
    issue_kind: str
    pdf_page_num: int
    gazette_page_label: str | None
    gazette_page_num: int | None
    text: str
    snippet: str | None = None  # populated by search


@dataclass
class IngestStats:
    """Statistics from an ingestion run."""

    discovered: int = 0
    downloaded: int = 0
    inserted: int = 0
    skipped: int = 0
    errors: int = 0


@dataclass
class ArchiveStats:
    """Overall archive statistics."""

    total_issues: int
    total_pages: int
    years: list[int]
    issues_by_year: dict[int, int]
    low_text_pages: int
    image_like_pages: int
    db_size_bytes: int


# Pydantic input models for MCP tools


class SearchPagesInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    query: str = Field(..., description="Suchbegriff(e) fuer Volltextsuche", min_length=1, max_length=500)
    year: Optional[int] = Field(default=None, description="Nur in einem bestimmten Jahr suchen")
    issue_kind: Optional[str] = Field(default=None, description="Art der Ausgabe: regular, register, special")
    limit: int = Field(default=10, description="Maximale Anzahl Ergebnisse", ge=1, le=50)


class GetIssueMetaInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    year: int = Field(..., description="Erscheinungsjahr")
    issue_code: str = Field(..., description="Ausgabennummer, z.B. '09' oder 'sr'", min_length=1)


class GetPagesInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    year: int = Field(..., description="Erscheinungsjahr")
    issue_code: str = Field(..., description="Ausgabennummer", min_length=1)
    page_from: int = Field(default=1, description="Erste Seite (PDF-Seitennummer)", ge=1)
    page_to: int = Field(default=5, description="Letzte Seite (PDF-Seitennummer)", ge=1)


class ListIssuesInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    year: Optional[int] = Field(default=None, description="Nach Jahr filtern")
    issue_kind: Optional[str] = Field(default=None, description="Art der Ausgabe: regular, register, special")
    limit: int = Field(default=100, description="Maximale Anzahl", ge=1, le=500)
    offset: int = Field(default=0, description="Offset fuer Paginierung", ge=0)
