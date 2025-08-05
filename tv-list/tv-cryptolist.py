import requests
import pandas as pd
from tvDatafeed import TvDatafeedLive, Interval
import logging
import os
import time

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

def load_data(filepath="categories_final.csv"):
    df = pd.read_csv(filepath)
    df = df.sort_values(by="market_cap_rank")
    return df

def filter_stablecoins(df):
    stablecoins = df[df['categories'].str.contains("Stablecoins", na=False)]
    return stablecoins.head(10)['symbol'].to_list()

def filter_non_stablecoins(df, stablecoins):
    return df[~df['symbol'].isin(stablecoins)].copy()

def filter_category(df, category):
    filtered_df = df[df['categories'].str.contains(category, na=False)]
    return filtered_df if not filtered_df.empty else None

def fetch_top_exchanges():
    url = "https://api.coingecko.com/api/v3/exchanges"
    response = requests.get(url)
    data = response.json()

    exchanges = [{
        'Name': e['name'],
        'Trust Score': e.get('trust_score', None),
        'Trust Score Rank': e.get('trust_score_rank', None),
        'Normalized 24h Volume (BTC)': e.get('trade_volume_24h_btc_normalized', 0)
    } for e in data]

    df_exchanges = pd.DataFrame(exchanges)
    df_exchanges.sort_values(by='Normalized 24h Volume (BTC)', ascending=False, inplace=True)
    df_exchanges.to_csv("Top_Crypto_Exchanges.csv", index=False)

    return df_exchanges['Name'].to_list()

def check_pair_on_tv(tv, crypto, stablecoin, exchange):
    symbol = f"{crypto}{stablecoin}".lower()
    logging.info(f"Checking {symbol.upper()} on {exchange}")
    try:
        data = tv.get_hist(symbol=symbol, exchange=exchange, interval=Interval.in_1_hour, n_bars=1)
        if data is not None and not data.empty:
            return symbol
    except Exception as e:
        logging.info(f"Error retrieving {symbol.upper()} on {exchange}: {e}")
    finally:
        time.sleep(0.2)  # prevent rate limiting
    return None

def find_valid_pairs(tv, cryptos, stablecoins, exchanges):
    valid_pairs = []

    for crypto in cryptos:
        pair_found = False
        for stablecoin in stablecoins:
            if pair_found:
                break
            for exchange in exchanges:
                pair_symbol = check_pair_on_tv(tv, crypto, stablecoin, exchange)
                if pair_symbol:
                    logging.info(f"Pair supported: {pair_symbol.upper()} on {exchange}")
                    valid_pairs.append((crypto, pair_symbol, exchange))
                    pair_found = True
                    break

    return valid_pairs

def update_dataframe(df, valid_pairs):
    df['crypto_pair'] = None
    df['tv_exchange'] = None

    for symbol, pair, exchange in valid_pairs:
        df.loc[df['symbol'] == symbol, 'crypto_pair'] = pair
        df.loc[df['symbol'] == symbol, 'tv_exchange'] = exchange

    return df

def save_to_tradingview_watchlist(filename, categorized_data):
    directory = os.path.dirname(filename)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)

    with open(filename, 'w') as file:
        for category_name, tickers in categorized_data.items():
            file.write(f"## {category_name}\n")
            for ticker in tickers:
                file.write(f"{ticker}\n")
            file.write("\n")

def main():
    start_time = time.time()

    df = load_data()
    stablecoins = filter_stablecoins(df)
    crypto_df = filter_non_stablecoins(df, stablecoins)
    exchanges = fetch_top_exchanges()

    tv = TvDatafeedLive()
    cryptos = crypto_df['symbol'].to_list()

    valid_pairs = find_valid_pairs(tv, cryptos, stablecoins, exchanges)

    updated_df = update_dataframe(crypto_df, valid_pairs)
    updated_df.to_csv('crypto_ticker_updated.csv', index=False)

    categorized_pairs = {}
    all_categories = updated_df['categories'].dropna().unique().tolist()
    for category in all_categories:
        category_df = filter_category(updated_df, category)
        if category_df is not None:
            tickers = [
                f"{row['tv_exchange'].upper()}:{row['crypto_pair'].upper()}"
                for _, row in category_df.dropna(subset=['crypto_pair']).iterrows()
            ]
            if tickers:
                categorized_pairs[category] = tickers

    uncategorized_df = updated_df[updated_df['categories'].isna()]
    uncategorized_tickers = [
        f"{row['tv_exchange'].upper()}:{row['crypto_pair'].upper()}"
        for _, row in uncategorized_df.dropna(subset=['crypto_pair']).iterrows()
    ]
    if uncategorized_tickers:
        categorized_pairs['Uncategorized'] = uncategorized_tickers

    save_to_tradingview_watchlist("valid_crypto_pairs.txt", categorized_pairs)

    elapsed_time = time.time() - start_time
    logging.info(f"\nExecution Time: {elapsed_time:.2f} seconds")
    logging.info("\nScript completed successfully. Valid pairs saved.")

if __name__ == "__main__":
    main()