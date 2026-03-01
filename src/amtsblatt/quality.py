"""Quality assessment for extracted Amtsblatt pages."""

from __future__ import annotations

# Thresholds
_LOW_TEXT_THRESHOLD = 50
_IMAGE_LIKE_TEXT_THRESHOLD = 200
_IMAGE_LIKE_ALPHA_RATIO = 0.3


def assess_page_quality(text: str, text_length: int) -> dict:
    """Assess whether a page has quality issues.

    Returns a dict with:
        is_low_text  -- True if text_length < 50 (likely blank/separator page)
        is_image_like -- True if text_length < 200 AND alphanumeric ratio < 0.3
                         (likely image/map/diagram)
    """
    is_low_text = text_length < _LOW_TEXT_THRESHOLD

    # Compute alphanumeric ratio
    if text_length > 0:
        alpha_count = sum(1 for ch in text if ch.isalnum())
        alpha_ratio = alpha_count / len(text)
    else:
        alpha_ratio = 0.0

    is_image_like = text_length < _IMAGE_LIKE_TEXT_THRESHOLD and alpha_ratio < _IMAGE_LIKE_ALPHA_RATIO

    return {
        "is_low_text": is_low_text,
        "is_image_like": is_image_like,
    }


def flag_pages(pages: list[dict]) -> list[dict]:
    """Add quality flags to a list of page dicts from extract_text_from_pdf.

    For each page dict, adds:
        is_low_text       -- 0 or 1
        is_image_like     -- 0 or 1
        extraction_method -- "pymupdf"

    Returns the modified list (pages are modified in place).
    """
    for page in pages:
        quality = assess_page_quality(page["text"], page["text_length"])
        page["is_low_text"] = int(quality["is_low_text"])
        page["is_image_like"] = int(quality["is_image_like"])
        page["extraction_method"] = "pymupdf"
    return pages
