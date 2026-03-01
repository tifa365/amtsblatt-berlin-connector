# Amtsblatt für Berlin - PDF Archive

https://de.wikipedia.org/wiki/Amtsblatt_f%C3%BCr_Berlin

The Amtsblatt für Berlin is Berlin's official government gazette, published every Friday. It contains laws, regulations, job postings, public tenders, and court notices.

**Problem:** Berlin only keeps the last 6 issues online. Older issues disappear.

**Solution:** This archive preserves PDFs using the Wayback Machine as a fallback source.

## How it works

`download.py` downloads PDFs listed in a CSV file:

1. Try berlin.de first (faster, current source)
2. Fall back to Wayback Machine if berlin.de returns 404
3. Skip files already downloaded
4. Rate-limited to avoid Wayback blocks

## The CSV

The CSV contains PDF URLs discovered by querying the Wayback Machine's CDX API - a database of everything the Internet Archive has ever crawled. Each row has:

- `year` - Publication year
- `original` - The berlin.de URL
- `wayback_raw` - Direct Wayback Machine download URL

This lets us find PDFs that berlin.de has removed but the Wayback Machine preserved.

## Completeness

How many issues we have vs. the highest issue number published that year (our best estimate of total issues). Each year may also have a Sonderregister (special index issue).

| Year | Issues | Of total | Completeness | Missing |
|------|--------|----------|--------------|---------|
| 2020 | 10 | 55 | 18% | 1-13, 16-39, 46-50, 52-54 |
| 2021 | 28 | 58 | 48% | 1-14, 17-19, 21-22, 25-28, 30, 38-40, 42-44 |
| 2022 | 48 | 54 | 89% | 28-29, 34-35, 37, 39 |
| 2023 | 49 | 57 | 86% | 30-31, 33-38 |
| 2024 | 55 | 55 | **100%** | — |
| 2025 | 39 | 51 | 76% | 7, 24-25, 33-34, 36-37, 44-45, 47-48, 50 |

2020 and 2021 are largely incomplete because the Wayback Machine only started crawling these PDFs partway through those years. 2024 is the only fully complete year.

## Usage

```bash
python3 download.py
```
