import requests
import pandas as pd
import concurrent.futures
import time
from random import uniform

# API Endpoints
URL_TICKERS = "https://api.coingecko.com/api/v3/coins/list"
URL_MARKET = "https://api.coingecko.com/api/v3/coins/markets"

# API Constraints
COINS_PER_PAGE = 250  # Max allowed per request
MAX_RETRIES = 5       # Number of retries for failed requests
INITIAL_DELAY = 2     # Start retry delay (in seconds)
MAX_WORKERS = 5       # Limits parallel API calls to avoid rate limits

# Headers
HEADERS = {"accept": "application/json"}

def fetch_tickers():
    """Fetch all cryptocurrency tickers from CoinGecko API."""
    response = requests.get(URL_TICKERS, headers=HEADERS)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch tickers: {response.status_code}")
        return []

def fetch_market_data(page, session):
    """Fetch market capitalization data for a page, with retry on failure."""
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": COINS_PER_PAGE,
        "page": page,
        "sparkline": "false"
    }

    delay = INITIAL_DELAY
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(URL_MARKET, params=params, headers=HEADERS)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                print(f"⚠️ Rate limit hit on page {page}. Retrying in {delay:.1f}s...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                print(f"❌ Error fetching page {page}: {response.status_code}")
                return []
        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed for page {page}: {e}")
            time.sleep(delay)
            delay *= 2

    print(f"❌ Failed to fetch page {page} after {MAX_RETRIES} retries.")
    return []

def fetch_all_market_data(total_coins):
    """Fetch all cryptocurrency market data in parallel using ThreadPoolExecutor with session pooling."""
    num_pages = (total_coins // COINS_PER_PAGE) + 1
    all_data = []

    with requests.Session() as session, concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_market_data, page, session): page for page in range(1, num_pages + 1)}

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                all_data.extend(result)
            
            # Randomized short sleep to further reduce API throttling
            time.sleep(uniform(0.2, 0.5))

    return all_data

def main():
    start_time = time.time()

    # Step 1: Fetch tickers
    crypto_tickers = fetch_tickers()
    total_coins = len(crypto_tickers)
    print(f"✅ Total cryptocurrencies available: {total_coins}")

    # Step 2: Fetch market cap data with retries & rate-limit handling
    all_crypto_data = fetch_all_market_data(total_coins)

    # Step 3: Convert to DataFrame
    df = pd.DataFrame(all_crypto_data, columns=["id", "symbol", "name", "market_cap", "current_price"])
    
    # Step 4: Save results
    df.to_csv("crypto_market_data.csv", index=False)
    print(df.head())  # Preview first 5 rows

    print(f"✅ Data fetching completed in {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    main()