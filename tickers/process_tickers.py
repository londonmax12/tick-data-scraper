import os
import pandas as pd
import yfinance as yf
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

def fetch_ticker_data(ticker):
    ticker = str(ticker)
    if ticker.isalpha():
        try:
            tk = yf.Ticker(ticker)
            hist = tk.history(period="max")
            if hist.empty:
                return None
            first_date = hist.index[0]
            name = tk.info.get('longName', ticker)
            divs = tk.dividends
            splits = tk.splits

            data = {
                'company_info': {
                    'Ticker': ticker,
                    'Name': name,
                    'First Date': first_date
                },
                'dividends': [],
                'stock_splits': []
            }

            last_div = None
            for date in divs.index:
                cur_div = divs[date]
                if cur_div != last_div:
                    data['dividends'].append({'Date': date, 'Dividend': cur_div})
                    last_div = cur_div

            last_split = None
            for date in splits.index:
                cur_split = splits[date]
                if cur_split != last_split:
                    data['stock_splits'].append({'Date': date, 'Stock Split': cur_split})
                    last_split = cur_split

            return ticker, data
        except Exception:
            return None

df = pd.read_csv('tickers/company_tickers_raw.csv')
base_dir = 'company_data/'
os.makedirs(base_dir, exist_ok=True)

existing_tickers = {
    name for name in os.listdir(base_dir)
    if os.path.exists(os.path.join(base_dir, name, f'{name}_company_info.csv'))
}

tickers_to_process = [t for t in df['ticker'] if str(t) not in existing_tickers]

with ThreadPoolExecutor(max_workers=15) as executor:
    for res in tqdm(executor.map(fetch_ticker_data, tickers_to_process), total=len(tickers_to_process), desc="Processing Tickers"):
        if res:
            tkr, d = res
            company_dir = os.path.join(base_dir, tkr)
            os.makedirs(company_dir, exist_ok=True)
            pd.DataFrame([d['company_info']]).to_csv(os.path.join(company_dir, f'{tkr}_company_info.csv'), index=False)
            if d['dividends']:
                pd.DataFrame(d['dividends']).to_csv(os.path.join(company_dir, f'{tkr}_dividends.csv'), index=False)
            if d['stock_splits']:
                pd.DataFrame(d['stock_splits']).to_csv(os.path.join(company_dir, f'{tkr}_stock_splits.csv'), index=False)

print("Data processing complete!")
