"""Central configuration for the download manager.

Every setting has a safe default, so the app runs with no environment at all.
Secrets (e.g. FRED_API_KEY) and overrides can be supplied either as real
environment variables or via a local ``.env`` file (git-ignored; copy
``.env.example`` to ``.env``). Numeric overrides fall back to their default if
unset, blank, or unparseable, so a typo in one variable can never crash startup.
"""
import os

# Root directory of the project (one level up from this package).
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Load a local .env (KEY=value lines) if one exists, so secrets and overrides
# don't have to be exported by hand each session. Optional: if python-dotenv
# isn't installed the app still runs entirely off real environment variables.
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(BASE_DIR, ".env"))
except ModuleNotFoundError:
    pass


def _env_int(name: str, default: int) -> int:
    """Read an int env var, falling back to ``default`` if missing/blank/invalid."""
    try:
        return int(os.environ.get(name, "").strip())
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    """Read a float env var, falling back to ``default`` if missing/blank/invalid."""
    try:
        return float(os.environ.get(name, "").strip())
    except (TypeError, ValueError):
        return default


def _env_str(name: str, default: str) -> str:
    """Read a string env var, falling back to ``default`` if missing or blank."""
    val = os.environ.get(name, "")
    return val.strip() if val.strip() else default


# Where the SQLite queue/state database lives.
DB_PATH = os.path.join(BASE_DIR, "manager.db")

# Output directories (kept consistent with the original scripts).
RESULTS_DIR = os.path.join(BASE_DIR, "results")          # raw .bi5 files
CSV_DIR = os.path.join(BASE_DIR, "results_csv")          # decoded CSVs
COMPANY_DIR = os.path.join(BASE_DIR, "company_data")     # yfinance prices + metadata
CRYPTO_DIR = os.path.join(BASE_DIR, "crypto_data")       # Binance OHLCV klines
SEC_DIR = os.path.join(BASE_DIR, "sec_data")             # SEC EDGAR by ticker (facts/filings/insider)
SEC_13F_DIR = os.path.join(BASE_DIR, "sec_13f")          # SEC 13F institutional holdings by CIK
FRED_DIR = os.path.join(BASE_DIR, "fred_data")           # FRED macro series by series id
CFTC_DIR = os.path.join(BASE_DIR, "cftc_data")           # CFTC Commitments of Traders by contract market code

# How many download tasks run at once across the whole manager.
MAX_CONCURRENCY = _env_int("DM_CONCURRENCY", 12)

# Per-request network timeout (seconds).
REQUEST_TIMEOUT = _env_float("DM_TIMEOUT", 30)

# Retry policy for network failures (dropped / paused connections).
# Tasks that fail on a transient error are retried automatically with
# exponential backoff, capped at RETRY_BACKOFF_MAX seconds, up to
# MAX_ATTEMPTS times before being marked permanently failed.
MAX_ATTEMPTS = _env_int("DM_MAX_ATTEMPTS", 50)
RETRY_BACKOFF_BASE = 1.5
RETRY_BACKOFF_MAX = 60.0

# SEC EDGAR access. The SEC *requires* a descriptive User-Agent carrying a
# contact address on every request — anonymous/botty UAs get a 403. It also
# rate-limits to 10 requests/second per IP, so all SEC traffic funnels through a
# shared limiter (see app/sec.py) set a touch under the ceiling for safety.
SEC_USER_AGENT = _env_str("SEC_UA", "quant_data/1.0 (londonhmball@gmail.com)")
SEC_HEADERS = {
    "User-Agent": SEC_USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json, text/xml, */*",
}
SEC_RATE_LIMIT = _env_float("SEC_RATE_LIMIT", 8)  # requests/second

# FRED (Federal Reserve Economic Data) access. Unlike SEC, the FRED API *requires*
# a free API key (https://fred.stlouisfed.org/docs/api/api_key.html) passed on
# every request — set it via the FRED_API_KEY environment variable. FRED rate
# limits to 120 requests/minute per key, so all FRED traffic funnels through a
# shared limiter (see app/fred.py) kept a touch under 2 req/s for safety.
FRED_API_KEY = _env_str("FRED_API_KEY", "")
FRED_RATE_LIMIT = _env_float("FRED_RATE_LIMIT", 2)  # requests/second
FRED_HEADERS = {
    "User-Agent": _env_str("FRED_UA", "quant_data/1.0 (londonhmball@gmail.com)"),
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json",
}

# CFTC Commitments of Traders access (Socrata, https://publicreporting.cftc.gov).
# Keyless, but the host throttles anonymous traffic — an optional Socrata app
# token (free; https://publicreporting.cftc.gov/profile/app_tokens) is sent as
# an X-App-Token header to lift the ceiling. All CFTC traffic funnels through a
# shared limiter (see app/cftc.py) kept polite at a few requests/second.
CFTC_APP_TOKEN = _env_str("CFTC_APP_TOKEN", "")
CFTC_RATE_LIMIT = _env_float("CFTC_RATE_LIMIT", 5)  # requests/second
CFTC_HEADERS = {
    "User-Agent": _env_str("CFTC_UA", "quant_data/1.0 (londonhmball@gmail.com)"),
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json",
}

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
