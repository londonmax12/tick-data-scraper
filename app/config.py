"""Central configuration for the download manager."""
import os

# Root directory of the project (one level up from this package).
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Where the SQLite queue/state database lives.
DB_PATH = os.path.join(BASE_DIR, "manager.db")

# Output directories (kept consistent with the original scripts).
RESULTS_DIR = os.path.join(BASE_DIR, "results")          # raw .bi5 files
CSV_DIR = os.path.join(BASE_DIR, "results_csv")          # decoded CSVs
COMPANY_DIR = os.path.join(BASE_DIR, "company_data")     # yfinance prices + metadata
CRYPTO_DIR = os.path.join(BASE_DIR, "crypto_data")       # Binance OHLCV klines
SEC_DIR = os.path.join(BASE_DIR, "sec_data")             # SEC EDGAR by ticker (facts/filings/insider)
SEC_13F_DIR = os.path.join(BASE_DIR, "sec_13f")          # SEC 13F institutional holdings by CIK

# How many download tasks run at once across the whole manager.
MAX_CONCURRENCY = int(os.environ.get("DM_CONCURRENCY", "12"))

# Per-request network timeout (seconds).
REQUEST_TIMEOUT = float(os.environ.get("DM_TIMEOUT", "30"))

# Retry policy for network failures (dropped / paused connections).
# Tasks that fail on a transient error are retried automatically with
# exponential backoff, capped at RETRY_BACKOFF_MAX seconds, up to
# MAX_ATTEMPTS times before being marked permanently failed.
MAX_ATTEMPTS = int(os.environ.get("DM_MAX_ATTEMPTS", "50"))
RETRY_BACKOFF_BASE = 1.5
RETRY_BACKOFF_MAX = 60.0

# SEC EDGAR access. The SEC *requires* a descriptive User-Agent carrying a
# contact address on every request — anonymous/botty UAs get a 403. It also
# rate-limits to 10 requests/second per IP, so all SEC traffic funnels through a
# shared limiter (see app/sec.py) set a touch under the ceiling for safety.
SEC_USER_AGENT = os.environ.get(
    "SEC_UA", "quant_data/1.0 (londonhmball@gmail.com)")
SEC_HEADERS = {
    "User-Agent": SEC_USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json, text/xml, */*",
}
SEC_RATE_LIMIT = float(os.environ.get("SEC_RATE_LIMIT", "8"))  # requests/second

# HTTP headers used for Dukascopy requests (from the original scrape.py).
DUKAS_HEADERS = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-GB,en;q=0.9,en-US;q=0.8",
    "origin": "https://freeserv.dukascopy.com",
    "referer": "https://freeserv.dukascopy.com/",
    "sec-ch-ua": '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
}
