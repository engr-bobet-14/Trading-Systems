import pandas as pd
import numpy as np
import time
from utils import *

if __name__ == "__main__":

    start_time = time.time()
    
    df_marketdata = pd.read_csv("./output/crypto_marketdata.csv").set_index('id')
    df_crypto_prices = df_marketdata.copy()
    df_crypto_prices = df_crypto_prices.dropna(subset=["crypto_pair"])
    df_crypto_prices['closing_price'] = np.nan

    df_exchanges = CG_exchanges_trade_volume()

    cex = tv_get_top_exchanges_by_volume(df_exchanges, type='CEX', n=10)
    dex = tv_get_top_exchanges_by_volume(df_exchanges, type='DEX', n=10)

    base_exchangelist = cex + dex

    for crypto in df_crypto_prices.index:

        crypto_ticker = df_crypto_prices.loc[crypto, 'crypto_pair']

        exch = cryptoexchange_map(df_crypto_prices.loc[crypto, 'exchange'])
        
        exchange_list = base_exchangelist.copy()

        if exch not in exchange_list:
            exchange_list.append(exch)

        price_found = False

        print(f"\nTrying to fetch price for: {crypto_ticker} | Default Exchange: {exch}")

        for CX in exchange_list:
            print(f"  Attempting exchange: {CX}")

            try:
                data = tv_pricedata(ticker=crypto_ticker, exchange=CX, timeframe='daily', num_candles=1)

                if data is not None and not data.empty:
                    last_price = data['close'].iloc[-1]
                    df_crypto_prices.loc[crypto, 'closing_price'] = last_price
                    df_crypto_prices.loc[crypto, 'exchange'] = CX

                    print(f"    ✅ Found price: {last_price} on {CX}")
                    price_found = True
                    break

                else:
                    print(f"    ⚠️ No data returned for {crypto_ticker} on {CX}")

            except Exception as e:
                print(f"    ❌ Error for {crypto_ticker} on {CX}: {e}")
                continue

        if not price_found:
            df_crypto_prices.loc[crypto, 'closing_price'] = np.nan
            print(f"    ❌ Could not find price for {crypto_ticker} on any exchange.")
    
    #exchange of market data based from df_crypto_prices
    df_crypto_prices_ref = df_crypto_prices.copy()
    df_crypto_prices_ref = df_crypto_prices_ref.dropna(subset=["closing_price"])

    df_marketdata['in_tv'] = ""

    for id in df_crypto_prices_ref.index:
        df_marketdata.loc[id, 'exchange'] = df_crypto_prices_ref.loc[id, 'exchange']
        df_marketdata[id, 'in_tv'] ='y'

    df_crypto_prices.to_csv("./output/crypto_marketdata_pricedata.csv")
    df_marketdata.to_csv("./output/crypto_marketdata_copy.csv")

    end_time = time.time()

    elapsed = end_time - start_time

if elapsed < 60:
    print(f"\nData scraping completed in {elapsed:.2f} seconds. ")
elif elapsed >= 3600:
    print(f"\nData scraping completed in {elapsed / 3600:.2f} hrs. ")
else:
    print(f"\nData scraping completed in {elapsed / 60:.2f} minutes. ")

print(f"Total cryptocurrencies processed: {len(df_crypto_prices_ref)}")