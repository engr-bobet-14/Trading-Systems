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

if __name__ == "__main__":
    # Example usage
    # Fetch 5 daily candles for BTCUSDT from Binance
    result = tv_pricedata(ticker='BTCUSDT', exchange='BINANCE', timeframe='daily', num_candles=5)
    
    if result is not None:
        print(result)