"""SEC EDGAR job types: fundamentals, filing index, insider trades, 13F holdings.

This plugs four new data products into the same plan/run task model the rest of
the manager uses (see :mod:`app.jobs`). Everything here speaks to the free,
key-less SEC endpoints:

* ``sec_facts``       — one task per ticker. Pulls the full XBRL ``companyfacts``
                        JSON (every financial line item a company ever reported)
                        and flattens it to a long CSV. Complements yfinance:
                        yfinance gives prices + corporate actions, this gives the
                        actual financial statements.
* ``sec_submissions`` — one task per ticker. The company's complete filing index
                        (every form, date, accession) plus a one-row profile.
* ``sec_insider``     — one task per Form 3/4/5 filing. Parses the ownership XML
                        into insider buy/sell transactions.
* ``sec_13f``         — one task per 13F-HR filing for an institution (by CIK).
                        Parses the holdings information table.

Three things make SEC different from the other sources and are handled centrally
here: a mandatory descriptive User-Agent, a hard 10 req/s rate limit (shared
across every worker thread via :data:`_LIMITER`), and ticker→CIK resolution via
the official mapping file (cached on disk under ``sec_data/_meta``).
"""
import csv
import json
import os
import threading
import time
import xml.etree.ElementTree as ET

import requests

from . import config
from .jobs import RetryableError

SEC_DATA_BASE = "https://data.sec.gov"
SEC_ARCHIVES = "https://www.sec.gov/Archives/edgar/data"
COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

# Ownership (Form 3/4/5) and 13F transaction codes are stored verbatim; callers
# can map them downstream. These are the forms each job type cares about.
OWNERSHIP_FORMS = {"3", "4", "5", "3/A", "4/A", "5/A"}
THIRTEEN_F_FORMS = {"13F-HR", "13F-HR/A"}


# ---------------------------------------------------------------------------
# Shared rate limiter — every SEC HTTP request goes through this so the whole
# process stays under the SEC's 10 req/s ceiling regardless of how many worker
# threads are downloading at once.
# ---------------------------------------------------------------------------

class _RateLimiter:
    """Hands each caller a monotonically spaced time slot, then sleeps until it.

    The slot bookkeeping happens under a lock but the sleep does not, so threads
    queue up without serialising the actual work — they just can't *issue*
    requests faster than the configured rate.
    """

    def __init__(self, rate_per_sec: float):
        self._interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.0
        self._lock = threading.Lock()
        self._next = 0.0

    def acquire(self):
        if self._interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            slot = max(now, self._next)
            self._next = slot + self._interval
        wait = slot - time.monotonic()
        if wait > 0:
            time.sleep(wait)


_LIMITER = _RateLimiter(config.SEC_RATE_LIMIT)

_thread_local = threading.local()


def _session() -> requests.Session:
    s = getattr(_thread_local, "sec_session", None)
    if s is None:
        s = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=4)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _thread_local.sec_session = s
    return s


def _get(url: str, *, params: dict | None = None, expect: str = "json"):
    """Rate-limited GET against SEC. Returns parsed JSON, text, or the Response.

    Maps the SEC's transient conditions onto :class:`RetryableError` so the
    manager's existing backoff/retry machinery handles them: 429 (rate limited),
    403 (often a transient throttle when bursting), and 5xx. A 404 is returned
    to the caller (it usually means "no data for this entity", not an error).
    """
    _LIMITER.acquire()
    try:
        resp = _session().get(url, params=params, headers=config.SEC_HEADERS,
                              timeout=config.REQUEST_TIMEOUT)
    except requests.exceptions.RequestException as e:
        raise RetryableError(f"network error: {e}") from e

    if resp.status_code == 404:
        return None
    if resp.status_code in (429, 403):
        raise RetryableError(f"throttled ({resp.status_code})")
    if resp.status_code >= 500:
        raise RetryableError(f"server error {resp.status_code}")
    if resp.status_code != 200:
        raise ValueError(f"sec {resp.status_code}: {resp.text[:160]}")

    if expect == "json":
        try:
            return resp.json()
        except ValueError as e:
            raise RetryableError(f"bad json: {e}") from e
    if expect == "bytes":
        return resp.content
    return resp.text


# ---------------------------------------------------------------------------
# Ticker -> CIK resolution (cached in memory + on disk).
# ---------------------------------------------------------------------------

_cik_lock = threading.Lock()
_cik_map: dict[str, str] | None = None


def _meta_path() -> str:
    d = os.path.join(config.SEC_DIR, "_meta")
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, "company_tickers.json")


def _load_cik_map() -> dict[str, str]:
    """Return {TICKER: 10-digit-zero-padded CIK}. Fetched once, cached on disk."""
    global _cik_map
    with _cik_lock:
        if _cik_map is not None:
            return _cik_map

        raw = None
        cache = _meta_path()
        # Prefer the on-disk cache (the mapping changes rarely); fall back to a
        # fresh download, and write it back for next time.
        if os.path.exists(cache):
            try:
                with open(cache, encoding="utf-8") as f:
                    raw = json.load(f)
            except (OSError, ValueError):
                raw = None
        if raw is None:
            raw = _get(COMPANY_TICKERS_URL)
            if raw is None:
                raise RetryableError("could not fetch company_tickers.json")
            try:
                with open(cache, "w", encoding="utf-8") as f:
                    json.dump(raw, f)
            except OSError:
                pass

        mapping: dict[str, str] = {}
        # The file is {"0": {"cik_str": 320193, "ticker": "AAPL", "title": ...}}
        rows = raw.values() if isinstance(raw, dict) else raw
        for row in rows:
            try:
                tic = str(row["ticker"]).upper()
                cik = str(int(row["cik_str"])).zfill(10)
            except (KeyError, TypeError, ValueError):
                continue
            mapping[tic] = cik
        _cik_map = mapping
        return _cik_map


def resolve_cik(ticker: str) -> str | None:
    """Map a ticker to its zero-padded CIK, tolerating '.'/'-' class suffixes."""
    t = ticker.strip().upper()
    m = _load_cik_map()
    if t in m:
        return m[t]
    # BRK.B vs BRK-B etc. — SEC uses '-'; callers may pass either.
    for variant in (t.replace(".", "-"), t.replace("-", "."), t.replace(".", ""), t.replace("-", "")):
        if variant in m:
            return m[variant]
    return None


def normalize_cik(value: str) -> str | None:
    """Accept a raw CIK ('320193', 'CIK0000320193', '0000320193') -> 10 digits."""
    s = str(value).strip().upper().replace("CIK", "")
    s = "".join(ch for ch in s if ch.isdigit())
    return s.zfill(10) if s else None


# ---------------------------------------------------------------------------
# Submissions (filing history) — shared by submissions/insider/13F planners.
# ---------------------------------------------------------------------------

def _fetch_submissions(cik: str) -> dict | None:
    """Full submissions doc for a CIK with the older filing pages merged in.

    ``filings.recent`` holds the latest ~1000 filings inline; anything older is
    split into extra JSON files referenced under ``filings.files``. We stitch
    them all back into one ``recent``-shaped dict of parallel arrays.
    """
    data = _get(f"{SEC_DATA_BASE}/submissions/CIK{cik}.json")
    if data is None:
        return None

    filings = data.get("filings", {})
    merged = {k: list(v) for k, v in filings.get("recent", {}).items()}
    for extra in filings.get("files", []):
        name = extra.get("name")
        if not name:
            continue
        page = _get(f"{SEC_DATA_BASE}/submissions/{name}")
        if not page:
            continue
        for k, v in page.items():
            merged.setdefault(k, []).extend(v)
    data["_merged"] = merged
    return data


def _iter_filings(merged: dict):
    """Yield each filing as a dict, zipping the parallel arrays together."""
    n = len(merged.get("accessionNumber", []))
    fields = list(merged.keys())
    for i in range(n):
        yield {f: (merged[f][i] if i < len(merged[f]) else "") for f in fields}


def _in_range(date_str: str, start: str | None, end: str | None) -> bool:
    if start and date_str < start:
        return False
    if end and date_str > end:
        return False
    return True


# ---------------------------------------------------------------------------
# Namespace-agnostic XML helpers (ownership + 13F docs use varying namespaces).
# ---------------------------------------------------------------------------

def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _find(elem, *names):
    """First descendant whose local tag name matches any of ``names``."""
    want = set(names)
    for e in elem.iter():
        if _local(e.tag) in want:
            return e
    return None


def _text(elem, *names, default: str = "") -> str:
    e = _find(elem, *names) if elem is not None else None
    if e is None:
        return default
    # Ownership docs wrap many scalars as <foo><value>X</value></foo>.
    val = _find(e, "value")
    txt = (val.text if val is not None else e.text) or ""
    return txt.strip()


def _iter_local(elem, name: str):
    for e in elem.iter():
        if _local(e.tag) == name:
            yield e


def _discover_doc(cik_int: int, acc_nodash: str, *, root_names: set[str],
                  fallback: str = "") -> tuple[str, bytes] | None:
    """Find and fetch the primary XML in a filing folder by its root tag.

    Tries the submissions-declared ``fallback`` document first (one fewer
    request when it's already the right XML), then falls back to listing the
    filing directory and picking the .xml whose root local-name is in
    ``root_names``.
    """
    base = f"{SEC_ARCHIVES}/{cik_int}/{acc_nodash}"

    def _matches(content: bytes) -> bool:
        try:
            return _local(ET.fromstring(content).tag) in root_names
        except ET.ParseError:
            return False

    if fallback and fallback.lower().endswith(".xml"):
        content = _get(f"{base}/{fallback}", expect="bytes")
        if content and _matches(content):
            return f"{base}/{fallback}", content

    listing = _get(f"{base}/index.json")
    if not listing:
        return None
    items = listing.get("directory", {}).get("item", [])
    xmls = [it["name"] for it in items
            if str(it.get("name", "")).lower().endswith(".xml")]
    # Skip the rendered/XSLT helper variants; check the plain documents first.
    xmls.sort(key=lambda n: (n.lower().startswith("r"), len(n)))
    for name in xmls:
        content = _get(f"{base}/{name}", expect="bytes")
        if content and _matches(content):
            return f"{base}/{name}", content
    return None


def _atomic_write_csv(path: str, header: list[str], rows: list[list]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".part"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    os.replace(tmp, path)


# ===========================================================================
# 1. Fundamentals — XBRL companyfacts
# ===========================================================================

def sec_facts_plan(params: dict) -> list[tuple[str, str]]:
    return _ticker_tasks(params)


def sec_facts_run(params: dict, key: str) -> tuple[str, str]:
    ticker = key
    out_path = os.path.join(config.SEC_DIR, ticker, f"{ticker}_facts.csv")
    if os.path.exists(out_path):
        return ("done", out_path)

    cik = resolve_cik(ticker)
    if not cik:
        return ("skipped", "")  # not in the SEC ticker map (e.g. delisted/foreign)

    data = _get(f"{SEC_DATA_BASE}/api/xbrl/companyfacts/CIK{cik}.json")
    if data is None:
        return ("skipped", "")  # filer has no XBRL facts

    rows = []
    concepts = []
    for taxonomy, tags in (data.get("facts") or {}).items():
        for tag, body in tags.items():
            label = body.get("label") or ""
            desc = (body.get("description") or "").replace("\n", " ")
            units = body.get("units") or {}
            n_pts = 0
            for unit, points in units.items():
                for p in points:
                    rows.append([
                        taxonomy, tag, label, unit,
                        p.get("start", ""), p.get("end", ""), p.get("val", ""),
                        p.get("fy", ""), p.get("fp", ""), p.get("form", ""),
                        p.get("filed", ""), p.get("frame", ""), p.get("accn", ""),
                    ])
                    n_pts += 1
            concepts.append([taxonomy, tag, label, ";".join(units.keys()),
                             n_pts, desc])

    if not rows:
        return ("skipped", "")

    # Long, faithful dump of every reported data point...
    rows.sort(key=lambda r: (r[0], r[1], str(r[5])))
    _atomic_write_csv(out_path,
                      ["taxonomy", "tag", "label", "unit", "start", "end",
                       "value", "fy", "fp", "form", "filed", "frame", "accn"],
                      rows)
    # ...plus a compact index of which concepts exist, for discoverability.
    concepts.sort(key=lambda r: (r[0], r[1]))
    _atomic_write_csv(
        os.path.join(config.SEC_DIR, ticker, f"{ticker}_concepts.csv"),
        ["taxonomy", "tag", "label", "units", "points", "description"], concepts)

    # One-row entity header straight from the facts doc.
    _atomic_write_csv(
        os.path.join(config.SEC_DIR, ticker, f"{ticker}_entity.csv"),
        ["cik", "entityName", "ticker"],
        [[cik, data.get("entityName", ""), ticker]])
    return ("done", out_path)


# ===========================================================================
# 2. Submissions — complete filing index + company profile
# ===========================================================================

def sec_submissions_plan(params: dict) -> list[tuple[str, str]]:
    return _ticker_tasks(params)


_FILING_COLS = ["accessionNumber", "filingDate", "reportDate", "acceptanceDateTime",
                "act", "form", "fileNumber", "items", "size", "isXBRL",
                "isInlineXBRL", "primaryDocument", "primaryDocDescription"]


def sec_submissions_run(params: dict, key: str) -> tuple[str, str]:
    ticker = key
    out_path = os.path.join(config.SEC_DIR, ticker, f"{ticker}_filings.csv")
    if os.path.exists(out_path):
        return ("done", out_path)

    cik = resolve_cik(ticker)
    if not cik:
        return ("skipped", "")
    data = _fetch_submissions(cik)
    if data is None:
        return ("skipped", "")

    merged = data.get("_merged", {})
    rows = []
    for fil in _iter_filings(merged):
        rows.append([fil.get(c, "") for c in _FILING_COLS])
    if not rows:
        return ("skipped", "")
    rows.sort(key=lambda r: r[1], reverse=True)  # newest first
    _atomic_write_csv(out_path, _FILING_COLS, rows)

    # One-row company profile from the submissions header.
    prof_cols = ["cik", "name", "sic", "sicDescription", "ein", "category",
                 "fiscalYearEnd", "stateOfIncorporation", "tickers", "exchanges",
                 "phone", "website", "investorWebsite"]
    profile = [
        cik, data.get("name", ""), data.get("sic", ""),
        data.get("sicDescription", ""), data.get("ein", ""),
        data.get("category", ""), data.get("fiscalYearEnd", ""),
        data.get("stateOfIncorporation", ""),
        ";".join(data.get("tickers", []) or []),
        ";".join(data.get("exchanges", []) or []),
        data.get("phone", ""), data.get("website", ""),
        data.get("investorWebsite", ""),
    ]
    _atomic_write_csv(
        os.path.join(config.SEC_DIR, ticker, f"{ticker}_profile.csv"),
        prof_cols, [profile])
    return ("done", out_path)


# ===========================================================================
# 3. Insider transactions — Form 3/4/5 ownership documents
# ===========================================================================

def sec_insider_plan(params: dict) -> list[tuple[str, str]]:
    start = params.get("start_date") or None
    end = params.get("end_date") or None
    tasks = []
    seen = set()
    for ticker in _clean_tickers(params.get("tickers")):
        if ticker in seen:
            continue
        seen.add(ticker)
        cik = resolve_cik(ticker)
        if not cik:
            continue
        data = _fetch_submissions(cik)
        if data is None:
            continue
        for fil in _iter_filings(data.get("_merged", {})):
            if fil.get("form") not in OWNERSHIP_FORMS:
                continue
            if not _in_range(fil.get("filingDate", ""), start, end):
                continue
            acc = fil.get("accessionNumber", "")
            if not acc:
                continue
            acc_nodash = acc.replace("-", "")
            doc = fil.get("primaryDocument", "")
            # key carries everything run() needs without another submissions hit.
            tkey = f"{ticker}|{cik}|{acc_nodash}|{doc}"
            label = f"{ticker} {fil.get('form')} {fil.get('filingDate')}"
            tasks.append((tkey, label))
    return tasks


def sec_insider_run(params: dict, key: str) -> tuple[str, str]:
    ticker, cik, acc_nodash, doc = key.split("|", 3)
    out_path = os.path.join(config.SEC_DIR, ticker, "insider", f"{acc_nodash}.csv")
    if os.path.exists(out_path):
        return ("done", out_path)

    found = _discover_doc(int(cik), acc_nodash,
                          root_names={"ownershipDocument"}, fallback=doc)
    if not found:
        return ("skipped", "")  # no parseable ownership XML (rare legacy filings)
    _, content = found
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return ("skipped", "")

    issuer_name = _text(root, "issuerName")
    issuer_symbol = _text(root, "issuerTradingSymbol")
    owner = _find(root, "reportingOwner")
    owner_name = _text(owner, "rptOwnerName") if owner is not None else ""
    owner_cik = _text(owner, "rptOwnerCik") if owner is not None else ""
    rel = _find(owner, "reportingOwnerRelationship") if owner is not None else None
    is_dir = _text(rel, "isDirector") if rel is not None else ""
    is_off = _text(rel, "isOfficer") if rel is not None else ""
    is_ten = _text(rel, "isTenPercentOwner") if rel is not None else ""
    officer_title = _text(rel, "officerTitle") if rel is not None else ""
    period = _text(root, "periodOfReport")
    doc_type = _text(root, "documentType")

    cols = ["filingType", "periodOfReport", "issuer", "issuerSymbol", "issuerCik",
            "owner", "ownerCik", "isDirector", "isOfficer", "isTenPercentOwner",
            "officerTitle", "derivative", "securityTitle", "transactionDate",
            "transactionCode", "shares", "pricePerShare", "acquiredDisposed",
            "sharesOwnedAfter", "directOrIndirect", "underlyingSecurity",
            "underlyingShares", "conversionPrice", "exercisableDate",
            "expirationDate", "accession"]
    rows = []

    def _emit(txn, derivative: bool):
        amt = _find(txn, "transactionAmounts")
        post = _find(txn, "postTransactionAmounts")
        rows.append([
            doc_type, period, issuer_name, issuer_symbol, _text(root, "issuerCik"),
            owner_name, owner_cik, is_dir, is_off, is_ten, officer_title,
            "Y" if derivative else "N",
            _text(txn, "securityTitle"),
            _text(txn, "transactionDate"),
            _text(amt, "transactionCode") or _text(txn, "transactionCode"),
            _text(amt, "transactionShares"),
            _text(amt, "transactionPricePerShare"),
            _text(amt, "transactionAcquiredDisposedCode"),
            _text(post, "sharesOwnedFollowingTransaction"),
            _text(txn, "directOrIndirectOwnership"),
            _text(txn, "underlyingSecurityTitle") if derivative else "",
            _text(txn, "underlyingSecurityShares") if derivative else "",
            _text(txn, "conversionOrExercisePrice") if derivative else "",
            _text(txn, "exerciseDate") if derivative else "",
            _text(txn, "expirationDate") if derivative else "",
            acc_nodash,
        ])

    for txn in _iter_local(root, "nonDerivativeTransaction"):
        _emit(txn, False)
    for txn in _iter_local(root, "derivativeTransaction"):
        _emit(txn, True)
    # Form 3 (initial ownership) often has holdings but no transactions — record
    # the position rows so a holdings snapshot is still captured.
    if not rows:
        for h in _iter_local(root, "nonDerivativeHolding"):
            _emit(h, False)
        for h in _iter_local(root, "derivativeHolding"):
            _emit(h, True)

    if not rows:
        return ("skipped", "")
    _atomic_write_csv(out_path, cols, rows)
    return ("done", out_path)


# ===========================================================================
# 4. 13F institutional holdings — by institution CIK
# ===========================================================================

def sec_13f_plan(params: dict) -> list[tuple[str, str]]:
    start = params.get("start_date") or None
    end = params.get("end_date") or None
    tasks = []
    seen = set()
    for raw in (params.get("ciks") or []):
        cik = normalize_cik(raw)
        if not cik or cik in seen:
            continue
        seen.add(cik)
        data = _fetch_submissions(cik)
        if data is None:
            continue
        manager_name = data.get("name", cik)
        for fil in _iter_filings(data.get("_merged", {})):
            if fil.get("form") not in THIRTEEN_F_FORMS:
                continue
            if not _in_range(fil.get("filingDate", ""), start, end):
                continue
            acc = fil.get("accessionNumber", "")
            if not acc:
                continue
            acc_nodash = acc.replace("-", "")
            period = fil.get("reportDate", "") or fil.get("filingDate", "")
            doc = fil.get("primaryDocument", "")
            tkey = f"{cik}|{acc_nodash}|{period}|{doc}"
            label = f"{manager_name[:30]} 13F {period}"
            tasks.append((tkey, label))
    return tasks


def sec_13f_run(params: dict, key: str) -> tuple[str, str]:
    cik, acc_nodash, period, doc = key.split("|", 3)
    fname = f"{period or 'unknown'}_{acc_nodash}.csv"
    out_path = os.path.join(config.SEC_13F_DIR, cik, fname)
    if os.path.exists(out_path):
        return ("done", out_path)

    found = _discover_doc(int(cik), acc_nodash,
                          root_names={"informationTable"}, fallback=doc)
    if not found:
        return ("skipped", "")  # cover-page-only / paper filing — no info table
    _, content = found
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return ("skipped", "")

    cols = ["cik", "period", "nameOfIssuer", "titleOfClass", "cusip", "value",
            "sshPrnamt", "sshPrnamtType", "putCall", "investmentDiscretion",
            "otherManager", "votingSole", "votingShared", "votingNone", "accession"]
    rows = []
    for info in _iter_local(root, "infoTable"):
        shrs = _find(info, "shrsOrPrnAmt")
        vote = _find(info, "votingAuthority")
        rows.append([
            cik, period,
            _text(info, "nameOfIssuer"),
            _text(info, "titleOfClass"),
            _text(info, "cusip"),
            _text(info, "value"),
            _text(shrs, "sshPrnamt"),
            _text(shrs, "sshPrnamtType"),
            _text(info, "putCall"),
            _text(info, "investmentDiscretion"),
            _text(info, "otherManager"),
            _text(vote, "Sole"),
            _text(vote, "Shared"),
            _text(vote, "None"),
            acc_nodash,
        ])
    if not rows:
        return ("skipped", "")
    _atomic_write_csv(out_path, cols, rows)
    return ("done", out_path)


# ---------------------------------------------------------------------------
# Shared planners
# ---------------------------------------------------------------------------

def _valid_ticker(t: str) -> bool:
    return bool(t) and all(c.isalnum() or c in ".-" for c in t)


def _clean_tickers(tickers) -> list[str]:
    out, seen = [], set()
    for t in (tickers or []):
        t = str(t).strip().upper()
        if _valid_ticker(t) and t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _ticker_tasks(params: dict) -> list[tuple[str, str]]:
    return [(t, t) for t in _clean_tickers(params.get("tickers"))]


# Registry merged into jobs.JOB_TYPES.
SEC_JOB_TYPES = {
    "sec_facts": (sec_facts_plan, sec_facts_run, "SEC XBRL fundamentals"),
    "sec_submissions": (sec_submissions_plan, sec_submissions_run, "SEC filing index"),
    "sec_insider": (sec_insider_plan, sec_insider_run, "SEC insider (Form 3/4/5)"),
    "sec_13f": (sec_13f_plan, sec_13f_run, "SEC 13F holdings"),
}
