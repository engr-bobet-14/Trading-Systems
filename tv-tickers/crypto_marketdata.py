import requests
import pandas as pd
import re
import concurrent.futures
import time
from requests.adapters import HTTPAdapter, Retry
import logging
from utils import tv_pricedata
import numpy as np

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("progress.log"),
        logging.StreamHandler()
    ]
)

#load previous fetch data
df_prev = pd.read_csv("./data/crypto_marketdata_prev.csv")

#load exchange map
df_exchange = pd.read_csv("./data/tv-crypto-exchange-info.csv")

def stable_coins(df, excluded_sc):
    """
    Filters the given DataFrame for stablecoins and removes unwanted symbols.

    Args:
        df (pd.DataFrame): The input crypto DataFrame.
        excluded_sc (list): List of symbol keywords to exclude.

    Returns:
        pd.DataFrame: Cleaned DataFrame with only desired stablecoins.
    """
    #Filter rows where 'categories' contains 'stable'
    df_sc = df[df['categories'].str.contains('stable', case=False, na=False)][['id', 'symbol', 'name']]
    df_sc['id'] = df['id'].str.upper()

    # Build pattern from excluded list
    pattern = '|'.join(excluded_sc)

    # Remove rows where 'symbol' matches unwanted patterns
    df_sc = df_sc[~df_sc['symbol'].str.contains(pattern, case=False, na=False)]

    return df_sc.set_index(['id'])

def stablecoins_exist(stable_coin, df_sc):
  if stable_coin.upper() in stablecoins_df.index:
    return True
  else:
      return False
  
def cryptoexchange_map(df, exchange):
    if exchange is None:
        return ""
    else:
        df['exch_name'] = df['exch_name'].str.upper()
        df_exch = df.set_index('exch_name')
        try:
            # Try to access 'Gate.io'
            return df_exch.loc[exchange.upper()].exch_id
        except KeyError:
            # If 'Gate.io' is not found, return the passed exchange
            return exchange

# List of excluded stablecoins
excluded_sc = ['AG', 'AU', 'BUIDL', 'CHF', 'DRT', 'EUR', 'FRAX', 'gold', 'INDR', 'KRW', 'SGD', 'TRY']

#list of stablecoins
stablecoins_df = stable_coins(df_prev, excluded_sc)

# Configure requests session with retries and exponential back-off
session = requests.Session()
retries = Retry(
    total=10,
    backoff_factor=5,
    status_forcelist=[429, 502, 503, 504],
    respect_retry_after_header=False
)
session.mount('https://', HTTPAdapter(max_retries=retries))

# Precompile regex for filtering wrapped and bridged assets
cross_chain_regex = re.compile(r"\b(bridged|wrapped)\b", re.IGNORECASE)

def is_cross_chain(name):
    return bool(cross_chain_regex.search(name))

# Retrieve list of cryptocurrencies
def crypto_ticker_list():
    url = "https://api.coingecko.com/api/v3/coins/list"
    response = session.get(url, headers={"accept": "application/json"})
    response.raise_for_status()
    return response.json()

# Retrieve market data for each cryptocurrency with robust filtering
def crypto_market_data(crypt_dict, marketcap_min=5000000):
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{crypt_dict['id']}"
        response = session.get(url, headers={"accept": "application/json"})
        response.raise_for_status()
        data = response.json()

        market_cap = data.get("market_data", {}).get("market_cap", {}).get("usd") or 0

        if market_cap < marketcap_min or is_cross_chain(crypt_dict['name']):
            return None

        ticker_exchange_data = data['tickers'][0] if data.get('tickers') else {}
        target_coin=ticker_exchange_data.get('target_coin_id', '')

        #Map stablecoins
        if stablecoins_exist(target_coin, stablecoins_df):
            target_coin_symbol = (stablecoins_df.loc[target_coin.upper()].symbol).upper()
            crypto_symbol=crypt_dict['symbol'].upper()
        else:
            target_coin_symbol = ""
            crypto_symbol= ""
        
        #Check /map correct exchange as per previous data
        if crypt_dict['id'] in df_prev['id']:
            CX = df_prev.loc[crypt_dict['id']]['exchange']
        else:
            CX = cryptoexchange_map(df_exchange, ticker_exchange_data.get('market', {}).get('name', None))
            
        return {
            'id': crypt_dict['id'],
            'symbol': crypt_dict['symbol'],
            'name': crypt_dict['name'],
            'categories': data.get('categories', []),
            'circulation_supply': data.get('market_data', {}).get('circulating_supply', None),
            'total_supply': data.get('market_data', {}).get('total_supply', None),
            'market_cap_rank': data.get('market_cap_rank', None),
            'exchange': CX,
            'target_coin': target_coin,
            'crypto_pair(usd)': f"{crypto_symbol}{target_coin_symbol}"
            }

    except requests.RequestException as e:
        print(f"Error fetching data for {crypt_dict['id']}: {e}")
    return None

# Main execution logic with optimized concurrency, batching, progress bar, and interim saving

if __name__ == "__main__":

    start_time = time.time()
    crypto_tickers = crypto_ticker_list()[8745:]
    filtered_crypto_list = []

    MAX_WORKERS = 8
    BATCH_SIZE = 30 
    SLEEP_TIME = 45

    total_batches = (len(crypto_tickers) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(crypto_tickers), BATCH_SIZE):
        batch = crypto_tickers[i:i + BATCH_SIZE]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(crypto_market_data, crypto): crypto for crypto in batch}
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    filtered_crypto_list.append(result)

        pd.DataFrame(filtered_crypto_list).to_csv("crypto_marketdata_interim.csv", index=False)

        # Logging the progress
        batch_number = (i // BATCH_SIZE) + 1
        progress_pct = (batch_number / total_batches) * 100
        logging.info(f"Processed batch {batch_number}/{total_batches} ({progress_pct:.2f}%)")

        if batch_number < total_batches:
            time.sleep(SLEEP_TIME)

    df = pd.DataFrame(filtered_crypto_list)
    df.to_csv("crypto_marketdata_final.csv", index=False)

    end_time = time.time()
    print(f"\nData scraping completed in {(end_time - start_time)/60:.2f} minutes. Total valid cryptocurrencies: {len(filtered_crypto_list)}")
