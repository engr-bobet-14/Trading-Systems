import requests
import pandas as pd
import re
import concurrent.futures
import time
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm

# Configure requests session with retries and exponential back-off
session = requests.Session()
retries = Retry(
    total=10,
    backoff_factor=5,
    status_forcelist=[429, 502, 503, 504],
    respect_retry_after_header=True
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

        market_cap = data.get("market_data", {}).get("market_cap", {}).get("usd")
        if market_cap is None:
            market_cap = 0

        if market_cap < marketcap_min or is_cross_chain(crypt_dict['name']):
            return None

        return {
            'id': crypt_dict['id'],
            'symbol': crypt_dict['symbol'],
            'name': crypt_dict['name'],
            'categories': data.get('categories', []),
            'market_cap (usd)': market_cap,
            'market_cap_rank': data.get('market_cap_rank', None),
            'fully_diluted_valuation (usd)': data.get('market_data', {}).get('fully_diluted_valuation', {}).get('usd', None)
        }
    except requests.RequestException as e:
        print(f"Error fetching data for {crypt_dict['id']}: {e}")
    return None

# Main execution logic with optimized concurrency, batching, progress bar, and interim saving
if __name__ == "__main__":
    start_time = time.time()
    crypto_tickers = crypto_ticker_list()
    filtered_crypto_list = []

    # Optimized parameters balancing speed and reliability
    MAX_WORKERS = 8
    BATCH_SIZE = 30
    SLEEP_TIME = 45

    total_batches = (len(crypto_tickers) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in tqdm(range(0, len(crypto_tickers), BATCH_SIZE), desc="Processing Batches"):
        batch = crypto_tickers[i:i + BATCH_SIZE]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(crypto_market_data, crypto): crypto for crypto in batch}
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    filtered_crypto_list.append(result)

        # Save interim results after each batch
        pd.DataFrame(filtered_crypto_list).to_csv("categories_interim.csv", index=False)

        if (i // BATCH_SIZE) + 1 < total_batches:
            time.sleep(SLEEP_TIME)

    # Final save to CSV
    df = pd.DataFrame(filtered_crypto_list)
    df.to_csv("categories_final.csv", index=False)

    end_time = time.time()
    print(f"\nData scraping completed in {(end_time - start_time)/60:.2f} minutes. Total valid cryptocurrencies: {len(filtered_crypto_list)}")
