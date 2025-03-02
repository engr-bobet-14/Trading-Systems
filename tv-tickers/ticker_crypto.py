import asyncio
import aiohttp
import time
import pandas as pd

COINGECKO_API_URL = "https://api.coingecko.com/api/v3/exchanges"

async def fetch_exchange_data(session, exchange):
    url = f"{COINGECKO_API_URL}/{exchange['id']}/tickers"
    try:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                tickers = {}
                for ticker in data.get("tickers", []):
                    symbol = ticker.get("base", "")
                    if symbol:
                        tickers[symbol] = {
                            "category": ticker.get("coin_id", "unknown"),
                            "exchange": exchange.get("name", "unknown")
                        }
                return tickers
            else:
                print(f"Failed to fetch {exchange['id']}: {response.status}")
                return {}
    except Exception as e:
        print(f"Error fetching {exchange['id']}: {e}")
        return {}

async def fetch_all_exchanges():
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(COINGECKO_API_URL) as response:
                if response.status == 200:
                    exchanges = await response.json()
                    tasks = [fetch_exchange_data(session, exchange) for exchange in exchanges]
                    results = await asyncio.gather(*tasks)
                    combined_tickers = {k: v for result in results for k, v in result.items()}
                    return combined_tickers
                else:
                    print("Failed to fetch exchange list.")
                    return {}
        except Exception as e:
            print(f"Error fetching exchanges: {e}")
            return {}

if __name__ == "__main__":
    start_time = time.time()
    ticker_data = asyncio.run(fetch_all_exchanges())
    end_time = time.time()
    
    print(f"Fetched data in {end_time - start_time:.2f} seconds")

    # Convert dictionary to DataFrame
    df = pd.DataFrame.from_dict(ticker_data, orient="index")
    df.index.name = "Ticker"
    df.reset_index(inplace=True)
    print(df)