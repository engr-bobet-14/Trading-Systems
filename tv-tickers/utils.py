import pandas as pd
from tvDatafeed import TvDatafeed, Interval
import configparser

def tv_pricedata(ticker: str, exchange: str, timeframe: str = 'daily', num_candles: int = 5) -> pd.DataFrame:
    """
    Fetch historical price data from TradingView via tvDatafeed.

    Args:
        ticker (str): 
            Trading symbol to fetch (Example: 'BTCUSDT', 'AAPL', 'ETHUSD').
        exchange (str): 
            Exchange name where the ticker is traded (Example: 'BINANCE', 'NASDAQ').
        timeframe (str, optional): 
            Timeframe for candlestick data. Defaults to 'daily'.
            Supported timeframes: '1m', '3m', '5m', '15m', '30m', '45m', '1h', '2h', '3h', '4h', 'daily', 'weekly', 'monthly'.
        num_candles (int, optional): 
            Number of recent candles to retrieve. Defaults to 5.

    Returns:
        pd.DataFrame: 
            A DataFrame containing historical OHLCV data (Open, High, Low, Close, Volume) with timestamps as the index.
            Returns None if data is unavailable or an error occurs.

    Example:
        >>> data = tv_pricedata(ticker='BTCUSDT', exchange='BINANCE', timeframe='daily', num_candles=5)
        >>> print(data)

    Notes:
        - You must have a TradingView account to use tvDatafeed.
        - Ensure the exchange and ticker are spelled correctly as per TradingView's naming.
        - Some assets might require a premium subscription.
    """
    config = configparser.ConfigParser()
    config.read('config.ini')

    USERNAME = config['tradingview']['username']
    PASSWORD = config['tradingview']['password']

    # Create a single connection session
    tv = TvDatafeed(username=USERNAME, password=PASSWORD)

    # Mapping timeframes to tvDatafeed Interval objects
    TIMEFRAME_MAPPING = {
        '1m': Interval.in_1_minute,
        '3m': Interval.in_3_minute,
        '5m': Interval.in_5_minute,
        '15m': Interval.in_15_minute,
        '30m': Interval.in_30_minute,
        '45m': Interval.in_45_minute,
        '1h': Interval.in_1_hour,
        '2h': Interval.in_2_hour,
        '3h': Interval.in_3_hour,
        '4h': Interval.in_4_hour,
        'daily': Interval.in_daily,
        'weekly': Interval.in_weekly,
        'monthly': Interval.in_monthly
    }
    print(f"  Attempting to fetch data from exchange: {exchange}")
    
    tf = TIMEFRAME_MAPPING.get(timeframe.lower())
    if tf is None:
        raise ValueError(f"Unsupported timeframe provided: {timeframe}")

    try:
        data = tv.get_hist(symbol=ticker, exchange=exchange, interval=tf, n_bars=num_candles)
        if data is not None and not data.empty:
            return data
        else:
            print(f"    ⚠️ No data returned for {ticker} on {exchange}.")
            return None
    except Exception as e:
        print(f"    ❌ Error fetching data for {ticker} on {exchange}: {e}")
        return None


def crypto_category(df):
    """
    Extracts and returns a sorted list of unique category tags from a pandas Series
    where each entry contains a comma-separated string of tags.

    Parameters:
    -----------
    df : pd.Series
        A pandas Series containing comma-separated category strings (e.g., 'Layer 1, Smart contracts').

    Returns:
    --------
    list
        A sorted list of unique category tags as strings.

    Example:
    --------
    >>> df = pd.Series(['Layer 1, Smart contracts', 'DeFi, Layer 1'])
    >>> crypto_category(df)
    ['DeFi', 'Layer 1', 'Smart contracts']
    """
    unique_tags = (
        df
        .str.split(',')       # split by comma
        .explode()            # flatten to rows
        .str.strip()          # remove leading/trailing whitespace
        .dropna()             # remove any NaNs
        .unique()             # get unique values
    )

    unique_tags = sorted(unique_tags)

    category_list = []
    for tag in unique_tags:
        category_list.append(tag)
    
    return category_list


def explode_column(df, column='Category'):
    """
    Explodes a comma-separated category column in a DataFrame into multiple rows.

    Parameters:
    -----------
    df : pd.DataFrame
        The input DataFrame containing a column with comma-separated category strings.

    column : str, default='Category'
        The name of the column to explode.

    Returns:
    --------
    pd.DataFrame
        A new DataFrame with the specified column exploded into individual category rows.

    Example:
    --------
    >>> df_exploded = explode_category_column(df_tvcoins, column='Category')
    """
    df = df.copy()

    # Step 1: Split comma-separated strings
    df[column] = df[column].apply(
        lambda x: x.split(',') if isinstance(x, str) else x
    )

    # Step 2: Strip whitespace
    df[column] = df[column].apply(
        lambda x: [i.strip() for i in x] if isinstance(x, list) else x
    )

    # Step 3: Explode the list into rows
    df_exploded = df.explode(column).reset_index(drop=True)

    return df_exploded

def tv_tradingpair(coin):
    """
    Attempts to find a stablecoin trading pair for the given coin using available price data.

    It checks the coin paired with each stablecoin in a predefined list. Returns the first
    available pair with valid price data. Defaults to coin + 'USD' if none are found.

    Parameters:
        coin (str): Ticker symbol of the cryptocurrency (e.g., 'BTC', 'ETH').

    Returns:
        str: A valid trading pair symbol (e.g., 'BTCUSD'), or defaults to coin + 'USD'.
    """
    usd_list = [
        'USD', 'HUSD', 'BUSD', 'AVUSD', 'FUSD', 'DUSD',
        'IUSD', '2USD', 'BIUSD', 'RUSD', 'OUSD', 'VUUSD',
        'GUSD', 'AUSD', 'LUSD', 'SUSD', 'WUSD', 'TUSD',
        'PUDUSD', 'CUSD', 'ZUSD'
    ]
    
    pair_set = [("BTC", "BTCUSD")]

    # Check if already cached
    for crypto, pair in pair_set:
        if crypto == coin:
            return pair

    # Try each stablecoin until one returns data
    for usd in usd_list:
        cryptopair = coin + usd
        pricedata = tv_pricedata(ticker=cryptopair, exchange="CRYPTO", timeframe='daily', num_candles=1)
        if pricedata is not None:
            pair_set.append((coin, cryptopair))
            return cryptopair

    # Fallback if none found
    return coin + "USD"

def marketcap_index(tv_CryptoScreener, category):
    """
    Constructs a TradingView-compatible index string based on the market capitalization 
    of the top cryptocurrencies within a specific category.

    Parameters:
    -----------
    tv_CryptoScreener : pd.DataFrame
        A DataFrame containing at least the following columns:
        - 'Coin': the symbol or identifier of the cryptocurrency
        - 'Market capitalization': the numerical market cap value
        - 'Category': a comma-separated string of sector tags

    category : str
        The category to filter coins by (e.g., 'Layer 1', 'DeFi').

    Returns:
    --------
    str
        A TradingView index string that aggregates the top 10 coins (by market capitalization)
        within the specified category. Each entry is formatted as:
        `'CRYPTO:COINUSD*weight'`, and entries are joined with `'+'`.

    Notes:
    ------
    - Only the top 10 coins by market capitalization in the selected category are included.
    - The weight of each coin is proportional to its share of the category's total market cap.
    - The function uses `explode_column()` to normalize comma-separated tags into individual rows.

    Example:
    --------
    >>> index_str = marketcap_index(df_tvcoins, 'Smart contract platforms')
    >>> print(index_str)
    'CRYPTO:ETHUSD*0.42+CRYPTO:ADAUSD*0.28+CRYPTO:SOLUSD*0.30'
    """
    # Explode category column
    df = explode_column(tv_CryptoScreener, column='Category')
    df.set_index('Coin', inplace=True)

    # Filter by category and compute % of market cap
    df_cat = df[df['Category'] == category].sort_values(by='Market capitalization', ascending=False)
    df_cat['%part'] = round(df_cat['Market capitalization'] / df_cat['Market capitalization'].sum(), 2)

    # Limit to top 10 coins by market cap
    df_cat = df_cat.head(10)

    # Construct TradingView expression
    coin_list = []
    for coin in df_cat.index:
        factor = df_cat.loc[coin, '%part']
        if factor > 0:
            ticker = tv_tradingpair(coin)
            coin_list.append(f'CRYPTO:{ticker}*{factor}')
    
    result = '+'.join(coin_list)
    return result

def capped_marketcap_index(tv_CryptoScreener, category, cap=0.25):
    """
    Constructs a TradingView-compatible index string based on the market capitalization
    of the top cryptocurrencies in a specified category, with an optional weight cap.

    Parameters:
    -----------
    tv_CryptoScreener : pd.DataFrame
        A DataFrame containing at least the following columns:
        - 'Coin': the symbol of each cryptocurrency (e.g., 'BTC', 'ETH')
        - 'Market capitalization': the market cap value for each coin
        - 'Category': a comma-separated string or list of categories per coin

    category : str
        The category to filter coins by (e.g., 'Layer 1', 'DeFi').
        The function will include only those coins tagged with this category.

    cap : float, default=0.25
        The maximum allowed weight (as a decimal) for any single coin in the index.
        For example, a cap of 0.25 limits any one coin to 25% of the total index weight.

    Returns:
    --------
    str
        A TradingView-compatible index expression, where each entry is formatted as:
        'CRYPTO:COINUSD*weight', and all entries are joined by '+'.
        Only the top 10 coins (by market cap) within the selected category are included.

    Example:
    --------
    >>> capped_marketcap_index(df, category='Smart contract platforms', cap=0.25)
    'CRYPTO:ETHUSD*0.25+CRYPTO:SOLUSD*0.22+CRYPTO:ADAUSD*0.18'

    Notes:
    ------
    - The weights are capped but not redistributed.
    - The 'explode_column' helper function is used to normalize multi-category rows.
    - The function assumes weights are relative to the total market cap of the filtered category.
    - Rounding is applied to 2 decimal places for TradingView formatting compatibility.
    """
    # Explode category column
    df = explode_column(tv_CryptoScreener, column='Category')
    df.set_index('Coin', inplace=True)

    # Filter by category and compute market share
    df_cat = df[df['Category'] == category].sort_values(by='Market capitalization', ascending=False)
    df_cat['%part'] = df_cat['Market capitalization'] / df_cat['Market capitalization'].sum()

    # Limit to top 10 coins
    df_cat = df_cat.head(10)

    # Construct TradingView expression with capped weights
    coin_list = []
    for coin in df_cat.index:
        factor = min((df_cat.loc[coin, '%part']).max(), cap)
        factor = round(factor, 2)
        if factor > 0:
            ticker = tv_tradingpair(coin)
            coin_list.append(f'CRYPTO:{ticker}*{factor}')

    return '+'.join(coin_list)

def crytoindex_bycatergoy(df,file_name, cryptocat_list):
  with open(f"{file_name}.txt", "w") as f:
    for category in cryptocat_list:
      i = capped_marketcap_index(df, category, cap=0.25)
      if len(i) > 0:
        f.write(f"###{category}\n")
        f.write(f"{i}\n")

# Optional test block
if __name__ == "__main__":
    df_sample = tv_pricedata(ticker="GATHUSD", exchange = "CRYPTO", timeframe='daily', num_candles=1)
    print(df_sample)

    