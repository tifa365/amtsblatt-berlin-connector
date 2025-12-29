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

## Usage

```bash
python3 download.py
```
