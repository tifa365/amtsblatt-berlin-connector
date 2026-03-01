"""PDF filename parsing and text extraction using PyMuPDF."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import fitz  # PyMuPDF

from amtsblatt.models import ParsedFilename

# Matches filenames like abl_2024_01_0001_0064_online.pdf
# or abl_2024_01_0001_0064_online_vbb-tarif.pdf
# or abl_2025_sr_0001_0028_online.pdf
_FILENAME_RE = re.compile(r'abl_(\d{4})_(\w+)_(\d{4})_(\d{4})_online')


def parse_filename(filename: str) -> ParsedFilename:
    """Parse an Amtsblatt PDF filename into structured metadata.

    Supported patterns:
        abl_2024_01_0001_0064_online.pdf
        abl_2024_01_0001_0064_online_vbb-tarif.pdf
        abl_2025_sr_0001_0028_online.pdf

    Raises:
        ValueError: If the filename does not match the expected pattern.
    """
    m = _FILENAME_RE.search(filename)
    if not m:
        raise ValueError(f"Filename does not match expected Amtsblatt pattern: {filename}")

    year_str, issue_code, page_start_str, page_end_str = m.groups()

    issue_kind = "register" if issue_code == "sr" else "regular"

    # Try to parse page numbers as ints (stripping leading zeros).
    try:
        page_start_num: int | None = int(page_start_str)
    except ValueError:
        page_start_num = None

    try:
        page_end_num: int | None = int(page_end_str)
    except ValueError:
        page_end_num = None

    return ParsedFilename(
        year=int(year_str),
        issue_code=issue_code,
        issue_kind=issue_kind,
        page_label_start=page_start_str,
        page_label_end=page_end_str,
        page_start_num=page_start_num,
        page_end_num=page_end_num,
        filename=filename,
    )


def _normalize_whitespace(text: str) -> str:
    """Collapse runs of whitespace while preserving paragraph breaks."""
    # Normalise line endings
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    # Collapse 3+ newlines into 2 (paragraph break)
    text = re.sub(r'\n{3,}', '\n\n', text)
    # Within each line, collapse multiple spaces/tabs into one
    text = re.sub(r'[^\S\n]+', ' ', text)
    # Strip leading/trailing whitespace on each line
    text = '\n'.join(line.strip() for line in text.split('\n'))
    # Strip overall leading/trailing whitespace
    return text.strip()


def _extract_gazette_page_label(text: str) -> str | None:
    """Try to find a gazette page number in the header area of the page.

    Heuristic: if the first non-empty line is a bare number (possibly with
    surrounding whitespace), treat it as the gazette page label.  Returns
    the string form (e.g. "521") or None.
    """
    for line in text.split('\n'):
        stripped = line.strip()
        if not stripped:
            continue
        # Check if the first non-empty line is a standalone number
        if re.fullmatch(r'\d+', stripped):
            return stripped
        # Not a bare number -- stop looking
        return None
    return None


def extract_text_from_pdf(pdf_path: Path) -> list[dict]:
    """Extract text from each page of a PDF using PyMuPDF.

    Returns a list of dicts, one per page, with keys:
        pdf_page_num       (int, 1-based)
        gazette_page_label (str | None)
        gazette_page_num   (int | None)
        text               (str)
        text_length        (int)
    """
    pages: list[dict] = []

    with fitz.open(pdf_path) as doc:
        for page_index, page in enumerate(doc):
            raw_text = page.get_text()
            text = _normalize_whitespace(raw_text)

            gazette_page_label = _extract_gazette_page_label(text)

            # Derive numeric gazette page number if possible
            gazette_page_num: int | None = None
            if gazette_page_label is not None:
                try:
                    gazette_page_num = int(gazette_page_label)
                except ValueError:
                    pass

            text_length = len(text)

            pages.append({
                "pdf_page_num": page_index + 1,
                "gazette_page_label": gazette_page_label,
                "gazette_page_num": gazette_page_num,
                "text": text,
                "text_length": text_length,
            })

    return pages


def compute_file_hash(file_path: Path) -> str:
    """Compute SHA-256 hash of a file, reading in 64 KB chunks.

    Returns the hex digest string.
    """
    h = hashlib.sha256()
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()
