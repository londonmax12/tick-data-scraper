"""Fetch company metadata (info, dividends, stock splits) from yfinance.

Reads tickers from ``tickers/company_tickers_raw.csv`` and writes one folder per
ticker under ``company_data/<ticker>/``. Tickers already downloaded are skipped,
so the script is safe to re-run.

This is the original standalone script; the web app under ``app/`` reimplements
the same logic sliced per-task for resumability.
"""
import os
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import yfinance as yf
from tqdm import tqdm

RAW_TICKERS_CSV = "tickers/company_tickers_raw.csv"
BASE_DIR = "company_data/"
MAX_WORKERS = 15


def _dedup_series(series, value_label):
    """Collapse consecutive equal values, keeping the date each value changed."""
    rows, last = [], None
    for date in series.index:
        value = series[date]
        if value != last:
            rows.append({"Date": date, value_label: value})
            last = value
    return rows


def fetch_ticker_data(ticker):
    """Return (ticker, data) for one ticker, or None if it has no usable data.

    Network/parse errors are reported and treated as a skip so one bad ticker
    never aborts the whole run.
    """
    ticker = str(ticker)
    if not ticker.isalpha():
        return None
    try:
        tk = yf.Ticker(ticker)
        hist = tk.history(period="max")
        if hist.empty:
            return None
        data = {
            "company_info": {
                "Ticker": ticker,
                "Name": tk.info.get("longName", ticker),
                "First Date": hist.index[0],
            },
            "dividends": _dedup_series(tk.dividends, "Dividend"),
            "stock_splits": _dedup_series(tk.splits, "Stock Split"),
        }
        return ticker, data
    except Exception as e:  # noqa: BLE001 - yfinance raises many error types
        print(f"Skipping {ticker}: {e}")
        return None


def save_ticker_data(ticker, data):
    company_dir = os.path.join(BASE_DIR, ticker)
    os.makedirs(company_dir, exist_ok=True)
    pd.DataFrame([data["company_info"]]).to_csv(
        os.path.join(company_dir, f"{ticker}_company_info.csv"), index=False)
    if data["dividends"]:
        pd.DataFrame(data["dividends"]).to_csv(
            os.path.join(company_dir, f"{ticker}_dividends.csv"), index=False)
    if data["stock_splits"]:
        pd.DataFrame(data["stock_splits"]).to_csv(
            os.path.join(company_dir, f"{ticker}_stock_splits.csv"), index=False)


def main():
    df = pd.read_csv(RAW_TICKERS_CSV)
    os.makedirs(BASE_DIR, exist_ok=True)

    existing = {
        name for name in os.listdir(BASE_DIR)
        if os.path.exists(os.path.join(BASE_DIR, name, f"{name}_company_info.csv"))
    }
    todo = [t for t in df["ticker"] if str(t) not in existing]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = executor.map(fetch_ticker_data, todo)
        for res in tqdm(results, total=len(todo), desc="Processing Tickers"):
            if res:
                save_ticker_data(*res)

    print("Data processing complete!")


if __name__ == "__main__":
    main()
