"""Project paths and settings for the Amtsblatt MCP server."""

from pathlib import Path

# config.py is at src/amtsblatt/config.py -> go up 3 levels to reach project root
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent.parent

DB_PATH: Path = PROJECT_ROOT / "data" / "amtsblatt.db"
PDF_DIR: Path = PROJECT_ROOT / "pdfs"
CSV_PATH: Path = PROJECT_ROOT / "urls.csv"
LOG_DIR: Path = PROJECT_ROOT / "data" / "logs"

USER_AGENT: str = "amtsblatt-downloader/0.2"
REQUEST_TIMEOUT: int = 60
BASE_DELAY: float = 1.5
