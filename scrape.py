"""Download raw Dukascopy .bi5 tick files for a set of tickers and date range.

Files are saved to ``results/<ticker>/<YYYY_MM_DD>/<HH>h.bi5``. Hours with no
data on the server (weekends, holidays, before a symbol was listed) return HTTP
404 and are skipped silently. Transient network errors are retried with
exponential backoff.

This is the original standalone script; the web app under ``app/`` reimplements
the same logic sliced per-task for resumability.
"""
import os
import queue
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from threading import RLock, Thread

import requests
from tqdm import tqdm

# --- configuration ---------------------------------------------------------

TICKERS = ["GBPJPY"]
START_DATE = "2003/08/01"
END_DATE = "2025/01/01"
HOUR_MIN = 0
HOUR_MAX = 23

MAX_WORKERS = 12
REQUEST_TIMEOUT = 30          # seconds
MAX_ATTEMPTS = 5              # retries per hour on transient errors
RETRY_BACKOFF_BASE = 1.5      # backoff = BASE ** attempt, capped below
RETRY_BACKOFF_MAX = 30.0      # seconds

HEADERS = {
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

tqdm.set_lock(RLock())


def file_saver(save_queue):
    """Drain (filepath, data) items from the queue and write them to disk.

    Writing happens on a single thread so the download workers never block on
    disk I/O. ``None`` is the sentinel that stops the saver.
    """
    while True:
        item = save_queue.get()
        if item is None:
            break
        filepath, data = item
        with open(filepath, "wb") as f:
            f.write(data)
        save_queue.task_done()


def fetch_hour(session, security, date, hour, save_queue, progress_bar):
    """Download one hour's .bi5 file, retrying transient failures.

    Returns silently on a 404 (no data for this hour) so weekends/holidays do
    not spam the log or stall the run.
    """
    url = (f"https://datafeed.dukascopy.com/datafeed/{security}/"
           f"{date.strftime('%Y/%m/%d')}/{hour:02d}h_ticks.bi5")
    results_dir = f"results/{security}/{date.strftime('%Y_%m_%d')}"
    filepath = f"{results_dir}/{hour:02d}h.bi5"

    try:
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                resp = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            except requests.exceptions.RequestException as e:
                if attempt == MAX_ATTEMPTS:
                    print(f"Giving up on {url}: {e}")
                    return
                time.sleep(min(RETRY_BACKOFF_MAX, RETRY_BACKOFF_BASE ** attempt))
                continue

            if resp.status_code == 404:
                return  # no data for this hour (market closed / pre-listing)
            if resp.status_code >= 500:
                if attempt == MAX_ATTEMPTS:
                    print(f"Giving up on {url}: server error {resp.status_code}")
                    return
                time.sleep(min(RETRY_BACKOFF_MAX, RETRY_BACKOFF_BASE ** attempt))
                continue
            if resp.status_code != 200:
                print(f"Unexpected status {resp.status_code} for {url}")
                return

            save_queue.put((filepath, resp.content))
            return
    finally:
        progress_bar.update(1)


def download_for_date(session, security, date, save_queue):
    """Download every hour of one day for one security, in parallel."""
    os.makedirs(f"results/{security}/{date.strftime('%Y_%m_%d')}", exist_ok=True)
    hours = range(HOUR_MIN, HOUR_MAX + 1)
    with tqdm(total=len(hours), desc=f"{security} {date.strftime('%Y-%m-%d')}",
              leave=False) as hour_bar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for hour in hours:
                executor.submit(fetch_hour, session, security, date, hour,
                                save_queue, hour_bar)


def main():
    start = datetime.strptime(START_DATE, "%Y/%m/%d")
    end = datetime.strptime(END_DATE, "%Y/%m/%d")
    dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]

    save_queue = queue.Queue()
    saver_thread = Thread(target=file_saver, args=(save_queue,))
    saver_thread.start()

    session = requests.Session()
    # Size the connection pool to the worker count so keep-alive connections are
    # reused across hours rather than discarded once 10 are in flight.
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount("https://", adapter)
    try:
        with tqdm(total=len(TICKERS), desc="Tickers done", position=0) as tickers_bar:
            for ticker in TICKERS:
                with tqdm(total=len(dates), desc=ticker, position=1,
                          leave=False) as dates_bar:
                    for date in dates:
                        download_for_date(session, ticker, date, save_queue)
                        dates_bar.update(1)
                tickers_bar.update(1)
    finally:
        save_queue.join()
        save_queue.put(None)  # stop the saver
        saver_thread.join()
        session.close()


if __name__ == "__main__":
    main()
