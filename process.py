import lzma
import struct
import csv
from datetime import datetime, timedelta
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

def decode_bi5(file_path):
    if os.stat(file_path).st_size == 0:
        return []
    with lzma.open(file_path, 'rb') as f:
        data = f.read()
    ticks = []
    for i in range(0, len(data), 20):
        if i + 20 > len(data):
            break
        chunk = data[i:i+20]
        timestamp, ask, bid, ask_vol, bid_vol = struct.unpack('>IIIff', chunk)
        ticks.append((timestamp, ask / 1e5, bid / 1e5, ask_vol, bid_vol))
    return ticks

def convert_timestamp_to_datetime(timestamp, start_time):
    return start_time + timedelta(milliseconds=timestamp)

def process_file(file, day, folder):
    all_ticks = []
    hour_str = os.path.splitext(os.path.basename(file))[0]
    if not hour_str.endswith("h"):
        return []
    try:
        hour = int(hour_str[:-1])
    except ValueError:
        return []
    start_time = day.replace(hour=hour)
    ticks = decode_bi5(file)
    for tick in ticks:
        timestamp, ask, bid, ask_vol, bid_vol = tick
        dt = convert_timestamp_to_datetime(timestamp, start_time)
        all_ticks.append((timestamp, dt, ask, bid, ask_vol, bid_vol))
    return all_ticks

if __name__ == "__main__":
    root = "results/GBPJPY/"
    output_root = "results_csv/GBPJPY/"
    os.makedirs(output_root, exist_ok=True)

    bi5_files_by_folder = {}

    subdirs = [d for d in os.listdir(root) if os.path.isdir(os.path.join(root, d))]
    for subdir in subdirs:
        full_subdir = os.path.join(root, subdir)
        if os.path.isdir(full_subdir):
            bi5_files = []
            for dirpath, _, filenames in os.walk(full_subdir):
                for file in sorted(filenames):
                    if file.endswith(".bi5"):
                        bi5_files.append(os.path.join(dirpath, file))
            if bi5_files:
                bi5_files_by_folder[subdir] = bi5_files

    for folder, files in bi5_files_by_folder.items():
        try:
            day = datetime.strptime(folder, "%Y_%m_%d")
        except ValueError:
            continue

        output_path = os.path.join(output_root, f"{folder}.csv")
        with open(output_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['timestamp', 'datetime', 'ask', 'bid', 'ask_volume', 'bid_volume'])

            all_ticks = []
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [executor.submit(process_file, file, day, folder) for file in files]
                for future in as_completed(futures):
                    all_ticks.extend(future.result())

            all_ticks.sort(key=lambda x: x[1])

            for tick in all_ticks:
                timestamp, dt, ask, bid, ask_vol, bid_vol = tick
                writer.writerow([timestamp, dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], ask, bid, ask_vol, bid_vol])
