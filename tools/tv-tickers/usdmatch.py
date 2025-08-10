import pandas as pd
from utils import tv_tradingpair, extract_quote_currency
import time

def find_usd_quote_matches(df):
    """
    For each cryptocurrency in the DataFrame index, attempts to find a USD-based trading pair
    using TradingView's native aggregate data. If found, extracts the quote currency and 
    saves both the trading pair and quote currency back to the DataFrame.

    Parameters:
        df (pd.DataFrame): DataFrame indexed by cryptocurrency symbols (e.g., BTC, ETH).

    Returns:
        pd.DataFrame: Updated DataFrame with 'cryptopair' and 'quote_currency' columns.
    """
    for coin in df.index:
        cryptopair = tv_tradingpair(coin)
        if cryptopair is not None:
            stablecoin = extract_quote_currency(cryptopair, coin)
            df.loc[coin, 'cryptopair'] = cryptopair
            df.loc[coin, 'quote_currency'] = stablecoin
            print(f"    ✅ Matched {coin} to stablecoin {stablecoin} via ticker {cryptopair}")
        else:
            df.loc[coin, 'cryptopair'] = ""
            df.loc[coin, 'quote_currency'] = ""
            print(f"    ❌ No USD match found for {coin}")

    return df

if __name__ == "__main__":
    start_time = time.time()

    # Load the initial DataFrame
    df = pd.read_csv("./data/tv_cryptopair_database.csv")
    df.set_index("Coin", inplace=True)

    # Process and match USD quotes
    df = find_usd_quote_matches(df)

    # Save updated results
    df.to_csv("./data/tv_cryptopair_database.csv")

    # Count valid USD matches (non-empty quote currencies)
    valid_matches = df['quote_currency'].str.strip().astype(bool).sum()

    end_time = time.time()
    print(f"\nUSD quote matching completed in {(end_time - start_time)/60:.2f} minutes. "
          f"Valid matches found: {valid_matches} out of {len(df)}")