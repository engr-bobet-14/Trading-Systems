import pandas as pd
from utils import *
import time
import numpy as np
import logging

# -------------------- Logging Setup --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("./output/crypto_pricedata.log"),
        logging.StreamHandler()
    ]
)

# -------------------- Functions --------------------

def tv_get_exchange_list(df_exchanges, default_exch):
    """
    Combines top CEX and DEX exchanges with the default exchange for a given crypto.

    Args:
        df_exchanges (DataFrame): CoinGecko exchange volume data.
        default_exch (str): Default exchange for the crypto asset.

    Returns:
        list: Combined list of exchanges to attempt.
    """
    cex = tv_get_top_exchanges_by_volume(df_exchanges, type='CEX', n=10)
    dex = tv_get_top_exchanges_by_volume(df_exchanges, type='DEX', n=10)
    exchange_list = cex + dex
    if default_exch not in exchange_list:
        exchange_list.append(default_exch)
    return exchange_list

# -------------------- Main Script --------------------

if __name__ == "__main__":
    """
    Main script to update cryptocurrency price data and calculate market cap.

    Steps:
        1. Loads existing crypto market data.
        2. Fetches latest exchange trade volume data from CoinGecko.
        3. For each crypto asset:
            - Attempts to fetch the latest closing price across top exchanges.
            - Updates the closing price and exchange used.
        4. Calculates market cap (price * circulating supply).
        5. Saves updated data to CSV.
    """

    start_time = time.time()

    # Load crypto market data
    df_crypto_prices = pd.read_csv("./output/crypto_marketdata.csv").set_index('id')

    # Fetch latest CoinGecko exchange volume data
    df_exchanges = CG_exchanges_trade_volume()

    logging.info(f"Processing {len(df_crypto_prices)} cryptocurrencies...")

    # Iterate through all cryptocurrencies
    for count, crypto_id in enumerate(df_crypto_prices.index, 1):
        ticker = df_crypto_prices.loc[crypto_id, 'crypto_pair']
        default_exch = cryptoexchange_map(df_crypto_prices.loc[crypto_id, 'exchange'])

        logging.info(f"\n[{count}/{len(df_crypto_prices)}] Fetching price for: {ticker} | Default Exchange: {default_exch}")

        exchange_list = tv_get_exchange_list(df_exchanges, default_exch)

        # Try fetching closing price
        price_found = fetch_closing_price(crypto_id, ticker, exchange_list, df_crypto_prices)

        if not price_found:
            df_crypto_prices.loc[crypto_id, 'closing_price'] = np.nan
            logging.warning(f"Could not find price for {ticker} on any exchange.")

    # -------------------- Vectorized Market Cap Calculation --------------------
    df_crypto_prices['market_cap'] = df_crypto_prices['closing_price'] * df_crypto_prices['circulation_supply']

    # Save results
    output_path = "./output/crypto_marketdata_pricedata.csv"
    df_crypto_prices.to_csv(output_path)
    logging.info(f"\n✅ Data saved to {output_path}")

    end_time = time.time()
    logging.info(f"\nData scraping completed in {(end_time - start_time)/60:.2f} minutes. "
                 f"Total cryptocurrencies processed: {len(df_crypto_prices)}")