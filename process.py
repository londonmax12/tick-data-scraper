"""Decode raw Dukascopy .bi5 tick files into per-day CSVs.

Reads ``results/<ticker>/<YYYY_MM_DD>/<HH>h.bi5`` and writes one sorted CSV per
day to ``results_csv/<ticker>/<YYYY_MM_DD>.csv``.

This is the original standalone script; the web app under ``app/`` reimplements
the same logic sliced per-task for resumability.
"""
import csv
import lzma
import os
import struct
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

TICKER = "GBPJPY"


def price_scale(ticker):
    """Divisor that turns Dukascopy's integer price points into real prices.

    Dukascopy stores prices as integers scaled by ``10**digits``, and ``digits``
    depends on the instrument: 3 for JPY-quoted FX pairs (e.g. GBPJPY, USDJPY)
    and 5 for the other FX pairs. Using the wrong scale shifts every price by a
    factor of 100, so JPY pairs must use 1e3, not 1e5. Exotic (non-FX)
    instruments may need their own entry.
    """
    return 1e3 if ticker.upper().endswith("JPY") else 1e5


def decode_bi5(file_path, scale):
    """Unpack a .bi5 file into (timestamp_ms, ask, bid, ask_vol, bid_vol) rows."""
    if os.stat(file_path).st_size == 0:
        return []
    with lzma.open(file_path, "rb") as f:
        data = f.read()
    ticks = []
    for i in range(0, len(data) - len(data) % 20, 20):
        timestamp, ask, bid, ask_vol, bid_vol = struct.unpack(">IIIff", data[i:i + 20])
        ticks.append((timestamp, ask / scale, bid / scale, ask_vol, bid_vol))
    return ticks


def process_file(file_path, day):
    """Decode one hour file, resolving each tick's millisecond offset to a datetime."""
    hour_str = os.path.splitext(os.path.basename(file_path))[0]
    if not hour_str.endswith("h"):
        return []
    try:
        hour = int(hour_str[:-1])
    except ValueError:
        return []

    # Dukascopy tick timestamps are UTC; carry that through explicitly so the
    # output is unambiguous downstream.
    start_time = day.replace(hour=hour, tzinfo=timezone.utc)
    scale = price_scale(TICKER)
    return [
        (timestamp, start_time + timedelta(milliseconds=timestamp),
         ask, bid, ask_vol, bid_vol)
        for timestamp, ask, bid, ask_vol, bid_vol in decode_bi5(file_path, scale)
    ]


def main():
    root = f"results/{TICKER}/"
    output_root = f"results_csv/{TICKER}/"
    os.makedirs(output_root, exist_ok=True)

    for folder in sorted(os.listdir(root)):
        src_dir = os.path.join(root, folder)
        if not os.path.isdir(src_dir):
            continue
        try:
            day = datetime.strptime(folder, "%Y_%m_%d")
        except ValueError:
            continue

        bi5_files = [os.path.join(src_dir, name)
                     for name in sorted(os.listdir(src_dir))
                     if name.endswith(".bi5")]
        if not bi5_files:
            continue

        all_ticks = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(process_file, f, day) for f in bi5_files]
            for future in as_completed(futures):
                all_ticks.extend(future.result())
        all_ticks.sort(key=lambda row: row[1])

        output_path = os.path.join(output_root, f"{folder}.csv")
        with open(output_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["timestamp", "datetime", "ask", "bid",
                             "ask_volume", "bid_volume"])
            for timestamp, dt, ask, bid, ask_vol, bid_vol in all_ticks:
                # ISO-8601 with millisecond precision and an explicit +00:00
                # (UTC) offset, e.g. "2023-01-02 14:30:00.123+00:00".
                writer.writerow([timestamp, dt.isoformat(sep=" ", timespec="milliseconds"),
                                 ask, bid, ask_vol, bid_vol])


if __name__ == "__main__":
    main()
