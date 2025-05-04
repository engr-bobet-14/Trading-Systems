import pandas as pd
from utils import *
import time
import numpy as np


if __name__ == "__main__":

    start_time = time.time()

    df_crypto_prices = pd.read_csv("./output/crypto_marketdata.csv").set_index('id')

    df_exchages = CG_exchanges_trade_volume()
    cex = tv_get_top_exchanges_by_volume(df_exchages, type='CEX', n=10)
    dex = tv_get_top_exchanges_by_volume(df_exchages, type='DEX', n=10)
    
    for crypto in df_crypto_prices.index:
        crypto_ticker = df_crypto_prices.loc[crypto, 'crypto_pair']
        exch = df_crypto_prices.loc[crypto, 'exchange']
        exchange_list = cex+dex

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
                    df_crypto_prices.loc[crypto,'exchange'] = CX
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
        
        #market cap
        prices = df_crypto_prices.loc[crypto, 'closing_price']
        circulating_supply = df_crypto_prices.loc[crypto,'circulation_supply']
        
        df_crypto_prices.loc[crypto,'market_cap']= prices*circulating_supply

    df_crypto_prices.to_csv("./output/crypto_marketdata_pricedata.csv")

    end_time = time.time()
    print(f"\nData scraping completed in {(end_time - start_time)/60:.2f} minutes. Total cryptocurrencies fetched: {len(df_crypto_prices)}")
