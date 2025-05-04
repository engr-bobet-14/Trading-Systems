import pandas as pd
from tvDatafeed import TvDatafeed, Interval
import configparser
import logging

headers = {"accept": "application/json"}

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

def exchange_type(exchange_id):
    """
    Check whether the given exchange is centralized (CEX) or decentralized (DEX).

    Parameters:
    exchange_id (str): The exchange ID as used by CoinGecko API.

    Returns:
    str: 
        - 'CEX' if the exchange is centralized.
        - 'DEX' if the exchange is decentralized.
        - "" (empty string) if any error occurs.
    """

    url = f"https://api.coingecko.com/api/v3/exchanges/{exchange_id}"

    try:
        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code != 200:
            print(f"API request failed with status code {response.status_code}")
            return ""

        res = response.json()

        if 'centralized' not in res:
            print("The response does not contain 'centralized' field.")
            return ""

        if res['centralized']:
            print(f"{exchange_id} is a Centralized Exchange (CEX).")
            return 'CEX'
        else:
            print(f"{exchange_id} is a Decentralized Exchange (DEX).")
            return 'DEX'

    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return ""

    except Exception as e:
        print(f"Unexpected error: {e}")
        return ""
    
import requests
import pandas as pd

headers = {"accept": "application/json"}

def CG_exchanges_trade_volume():
    """
    Fetches exchange data from CoinGecko and returns a DataFrame 
    containing exchange IDs, names, and normalized 24-hour trade volumes in BTC.

    Returns:
        pd.DataFrame: A DataFrame indexed by 'coin_gecko_id' with the following columns:
            - name: Name of the exchange.
            - trade_volume_24h_btc_normalized: 24-hour trade volume normalized in BTC.

    Example:
        df = CG_exchanges_trade_volume()
        print(df.head())

    Notes:
        - Uses the CoinGecko public API endpoint: /api/v3/exchanges
        - Results are sorted in descending order of trade volume.
        - Requires an active internet connection.
    """
    url = "https://api.coingecko.com/api/v3/exchanges"
    response = requests.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data from CoinGecko. Status code: {response.status_code}")

    df = pd.DataFrame(response.json())
    df = df.loc[:, ['id', 'name', 'trade_volume_24h_btc_normalized']]
    df = df.sort_values(by='trade_volume_24h_btc_normalized', ascending=False)
    df = df.rename(columns={'id': 'coin_gecko_id'})
    df.reset_index(drop=True, inplace=True)
    df.set_index('coin_gecko_id', inplace=True)
    return df

def tv_get_top_exchanges_by_volume(df_CG_exch_trade_vol, type='CEX', n=10):
    """
    Returns the top 'n' exchange IDs based on 24-hour BTC trading volume, 
    filtered by exchange type (CEX/DEX).

    Args:
        df_CG_exch_trade_vol (pd.DataFrame): 
            A DataFrame containing CoinGecko exchange trade volume data. 
            Must include 'trade_volume_24h_btc_normalized' and use 'coin_gecko_id' as the index.
        type (str, optional): 
            The exchange type to filter by (e.g., 'CEX' or 'DEX'). Default is 'CEX'.
        n (int, optional): 
            The number of top exchanges to return. Default is 10.

    Returns:
        List[str]: 
            A list of the top 'n' exchange IDs (tv_exch_id) ordered by 24-hour BTC trading volume descending.

    Notes:
        - Reads exchange metadata from 'tv-crypto-exchange-info.csv'.
        - Only includes exchanges with status 'Active' and a valid CoinGecko ID.
        - If an exchange in the CSV does not exist in the CoinGecko trade volume data, 
          its volume is set to 0.
    
    Example:
        df_CG_exch = CG_exchanges_trade_volume()
        top_exchanges = tv_top_exch_byvolume(df_CG_exch, type='CEX', n=5)
        print(top_exchanges)
    """
    # Read exchange data
    tv_exch = pd.read_csv('./data/tv-crypto-exchange-info.csv')

    # Filter active exchanges only
    tv_exch = tv_exch.loc[tv_exch['status'] == 'Active']

    # Drop rows without coin_gecko_id
    tv_exch = tv_exch.dropna(subset=['coin_gecko_id'])

    # Set index for easy lookup
    tv_exch.set_index('coin_gecko_id', inplace=True)

    # Add the volume data from the CoinGecko API
    for id in tv_exch.index:
        try:
            tv_exch.loc[id, 'volume_24h_btc'] = df_CG_exch_trade_vol.loc[id, 'trade_volume_24h_btc_normalized']
        except KeyError:
            tv_exch.loc[id, 'volume_24h_btc'] = 0

    # Filter exchanges by type and sort
    filtered_exch = tv_exch.loc[tv_exch['type'] == type].sort_values('volume_24h_btc', ascending=False)

    # Get top 'n' exchange IDs
    top_exchanges = filtered_exch.head(n)['tv_exch_id'].to_list()

    return top_exchanges

def fetch_closing_price(crypto_id, ticker, exchanges, df_crypto_prices):
    """
    Attempts to fetch the latest closing price for a crypto asset across multiple exchanges.

    Args:
        crypto_id (str): The ID of the cryptocurrency.
        ticker (str): The trading pair symbol.
        exchanges (list): List of exchanges to attempt.
        df_crypto_prices (DataFrame): DataFrame to update with price and exchange.

    Returns:
        bool: True if a price was found and updated, False otherwise.
    """
    for exch in exchanges:
        logging.info(f"Trying {ticker} on {exch}...")
        try:
            data = tv_pricedata(ticker=ticker, exchange=exch, timeframe='daily', num_candles=1)
            if data is not None and not data.empty:
                last_price = data['close'].iloc[-1]
                df_crypto_prices.loc[crypto_id, 'closing_price'] = last_price
                df_crypto_prices.loc[crypto_id, 'exchange'] = exch
                logging.info(f"✅ Found price: {last_price} on {exch}")
                return True
            else:
                logging.warning(f"No data for {ticker} on {exch}")
        except Exception as e:
            logging.error(f"Error fetching {ticker} on {exch}: {e}")
    return False

 
def cryptoexchange_map(exchange):
    """
    Maps a given exchange name to its standardized TradingView exchange ID.

    Args:
        exchange (str or None): The name of the exchange to map. 
            If None is provided, returns an empty string.

    Returns:
        str: The corresponding TradingView exchange ID ('tv_exch_id') if found.
             If the exchange name is not found in the mapping file, returns the original input.

    Notes:
        - Uses './data/tv-crypto-exchange-info.csv' as the mapping source.
        - The CSV must contain a 'Name' column and a 'tv_exch_id' column.
        - Matching is case-insensitive.

    Example:
        >>> cryptoexchange_map("Gate.io")
        'GATEIO'

        >>> cryptoexchange_map("Binance")
        'BINANCE'

        >>> cryptoexchange_map("UnknownExchange")
        'UnknownExchange'
    """
    df = pd.read_csv("./data/tv-crypto-exchange-info.csv").set_index('Name')

    if exchange is None:
        return ""

    # Standardize names for matching
    df['Name'] = df.index.str.upper()
    df_exch = df.set_index('Name')

    try:
        return df_exch.loc[exchange.upper()].tv_exch_id
    except KeyError:
        return exchange

if __name__ == "__main__":
    # Example usage
    # Fetch 5 daily candles for BTCUSDT from Binance
    result = tv_pricedata(ticker='BTCUSDT', exchange='BINANCE', timeframe='daily', num_candles=1)
    
    if result is not None:
        print(result)