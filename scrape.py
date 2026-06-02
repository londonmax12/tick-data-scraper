from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import requests
import os
import time
from tqdm import tqdm
from threading import RLock, Thread
import queue

tqdm.set_lock(RLock())

hour_range_min = 0
hour_range_max = 23

tickers = ["GBPJPY"]

headers = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-GB,en;q=0.9,en-US;q=0.8",
    "origin": "https://freeserv.dukascopy.com",
    "priority": "u=1, i",
    "referer": "https://freeserv.dukascopy.com/",
    "sec-ch-ua": '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
}

def file_saver(save_queue):
    while True:
        item = save_queue.get()
        if item is None:
            break
        filepath, data = item
        with open(filepath, 'wb') as f:
            f.write(data)
        save_queue.task_done()

def download_bi5_file(date, security, progress_bar, save_queue):
    results_dir = f"results/{security}/{date.strftime('%Y_%m_%d')}"
    os.makedirs(results_dir, exist_ok=True)

    def fetch_hour(i):
        url = f"https://datafeed.dukascopy.com/datafeed/{security}/{date.strftime('%Y/%m/%d')}/{i:02d}h_ticks.bi5"
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            filepath = f"{results_dir}/{i:02d}h.bi5"

            save_queue.put((filepath, response.content))
        except requests.exceptions.RequestException as e:
            print(f"Failed to download {url}: {e}")
            time.sleep(5)
        progress_bar.update(1)

    with ThreadPoolExecutor(max_workers=12) as executor:
        executor.map(fetch_hour, range(hour_range_min, hour_range_max + 1))

def download_for_ticker(ticker, dates, progress_bar_for_dates, progress_bar_for_tickers, save_queue):
    for date in dates:
        per_hour_bar = tqdm(total=hour_range_max - hour_range_min + 1,
                            desc=f"{ticker} {date.strftime('%Y-%m-%d')}", leave=False)
        download_bi5_file(date, ticker, per_hour_bar, save_queue)
        progress_bar_for_dates.update(1)
    progress_bar_for_tickers.update(1)

def main():
    start_date = datetime.strptime("2003/08/01", "%Y/%m/%d")
    end_date = datetime.strptime("2025/01/01", "%Y/%m/%d")
    date_list = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

    save_queue = queue.Queue()
    saver_thread = Thread(target=file_saver, args=(save_queue,))
    saver_thread.start()

    with tqdm(total=len(tickers), desc="Tickers done", position=0) as tickers_bar:
        for ticker in tickers:
            per_ticker_bar = tqdm(total=len(date_list), desc=f"{ticker}", position=1, leave=False)
            download_for_ticker(ticker, date_list, per_ticker_bar, tickers_bar, save_queue)

    save_queue.join()

    save_queue.put(None)
    saver_thread.join()

if __name__ == "__main__":
    main()
