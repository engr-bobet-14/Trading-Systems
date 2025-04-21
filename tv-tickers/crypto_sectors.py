import requests
import pandas as pd
import concurrent.futures
import time
from requests.adapters import HTTPAdapter, Retry

# Configure requests session with retries and exponential back-off
session = requests.Session()
retries = Retry(
    total=10,
    backoff_factor=5,
    status_forcelist=[429, 502, 503, 504],
    respect_retry_after_header=True
)
session.mount('https://', HTTPAdapter(max_retries=retries))

# Retrieve closing price with fallback method
def crypto_closing_price(coin_id):
    ohlc_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc?days=1"
    fallback_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"

    try:
        response = session.get(ohlc_url, headers={"accept": "application/json"})
        if response.status_code == 200:
            data = response.json()
            if data:
                closing_price = data[-1][4]  # Closing price
                return {'id': coin_id, 'closing_price_usd': closing_price}

        # Fallback if OHLC endpoint fails
        response = session.get(fallback_url, headers={"accept": "application/json"})
        response.raise_for_status()
        data = response.json()
        closing_price = data.get('market_data', {}).get('current_price', {}).get('usd')

        return {'id': coin_id, 'closing_price_usd': closing_price}

    except requests.RequestException as e:
        print(f"Error fetching data for {coin_id}: {e}")

    return {'id': coin_id, 'closing_price_usd': None}


if __name__ == "__main__":
    df_coin = pd.read_csv("categories_final.csv").dropna(subset=['crypto_ticker'])
    coin_ids = df_coin['id'].tolist()

    results = []

    MAX_WORKERS = 12
    BATCH_SIZE = 100
    SLEEP_TIME = 30

    total_batches = (len(coin_ids) + BATCH_SIZE - 1) // BATCH_SIZE

    start_time = time.time()

    for batch_num, i in enumerate(range(0, len(coin_ids), BATCH_SIZE), start=1):
        batch = coin_ids[i:i + BATCH_SIZE]
        print(f"Processing batch {batch_num}/{total_batches}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(crypto_closing_price, coin_id): coin_id for coin_id in batch}
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    results.append(result)

        pd.DataFrame(results).to_csv("crypto_prices_interim.csv", index=False)

        if batch_num < total_batches:
            time.sleep(SLEEP_TIME)

    df_final = pd.DataFrame(results)
    df_final.to_csv("crypto_closing_prices_final.csv", index=False)

    end_time = time.time()
    print(f"\nData scraping completed in {(end_time - start_time)/60:.2f} minutes. Total cryptocurrencies fetched: {len(results)}")
