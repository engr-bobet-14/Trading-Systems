# Crypto Data Logger

Raw Binance spot and futures ingestion pipeline that stores only normalized raw events in Parquet. The logger intentionally does not calculate OHLC, VWAP, spreads, imbalance, basis, bars, or any other derived feature.

## Key Features

- Continuous token buckets, per-endpoint min spacing
- Endpoint-scoped, half-open circuits with adaptive decay
- Unified timeout policy + latency buckets
- Strong trades pagination with `fromId` continuation + dedupe
- Arrow Parquet writer with batched writes, periodic fsync, atomic swap
- Persisted dedupe seeding per shard (reads last few `ts_ms`)
- Cross-thread stats aggregation and graceful shutdown (no `os._exit`)
- Multi-threaded execution across websocket ingestion, REST catch-up, and table writers

## What It Fetches

Spot websocket streams:
- `<symbol>@trade`
- `<symbol>@bookTicker`
- `<symbol>@depth@100ms`

Futures websocket streams:
- `<symbol>@trade`
- `<symbol>@bookTicker`
- `<symbol>@depth@100ms`
- `<symbol>@markPrice@1s`

Futures REST:
- funding rate history

Spot and futures REST catch-up:
- `historicalTrades` with `fromId` continuation when `BINANCE_API_KEY` is present

## What It Does Not Fetch

- `kline`
- `aggTrade`
- `miniTicker`
- `ticker`

## What It Does Not Calculate

- OHLC
- VWAP
- imbalance
- basis
- spreads
- mid
- microprice
- slopes
- event bars
- run bars
- any derived feature

## Output Tables

The logger writes immutable Parquet shards under:

```text
parquet_binance-datasets/
  <table>/
    date=YYYY-MM-DD/
      instrument=SYMBOL/
        hour=HH/
          part-*.parquet
```

Tables:
- `spot_trades`
- `futures_trades`
- `spot_l1`
- `futures_l1`
- `spot_depth_updates`
- `futures_depth_updates`
- `futures_mark_price`
- `futures_funding_history`

## Concurrency Model

- One writer thread per output table.
- One websocket thread per stream group chunk.
- One spot trade REST worker per spot symbol when `BINANCE_API_KEY` is set.
- One futures trade REST worker per futures symbol when `BINANCE_API_KEY` is set.
- One futures funding history worker per futures symbol.
- One stats thread for periodic aggregation and logging.

This means the pipeline is explicitly multi-threaded and parallelized across symbols, markets, and storage paths.

## Default Symbols

Default spot and futures symbol inputs are:

```text
BTCUSDT,ETHUSDT,ADAUSDT,BNBUSDT,DOGEUSDT,MATICUDST,SOLUSDT,TRXUSDT,XRPUSDT,MONERO
```

The logger normalizes the following aliases automatically:
- `MATICUDST` -> `MATICUSDT`
- `MONERO` -> `XMRUSDT`

Effective default trading symbols are therefore:

```text
BTCUSDT,ETHUSDT,ADAUSDT,BNBUSDT,DOGEUSDT,MATICUSDT,SOLUSDT,TRXUSDT,XRPUSDT,XMRUSDT
```

## Requirements

- Python 3.11+
- `pyarrow`
- `requests`
- `websockets`

Install:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

Websocket ingestion works without API keys. Trade REST catch-up requires `BINANCE_API_KEY` because Binance `historicalTrades` needs it.

```bash
export BINANCE_API_KEY="your_api_key_here"

python crypto-data-logger.py \
  --spot-symbols BTCUSDT,ETHUSDT,ADAUSDT,BNBUSDT,DOGEUSDT,MATICUDST,SOLUSDT,TRXUSDT,XRPUSDT,MONERO \
  --futures-symbols BTCUSDT,ETHUSDT,ADAUSDT,BNBUSDT,DOGEUSDT,MATICUDST,SOLUSDT,TRXUSDT,XRPUSDT,MONERO \
  --out-dir parquet_binance-datasets \
  --state-dir state
```

## Docker

Build the image:

```bash
docker build -t crypto-data-logger .
```

Run it with mounted output directories so Parquet data and state survive container restarts:

```bash
docker run --rm \
  -e BINANCE_API_KEY="your_api_key_here" \
  -v "$(pwd)/parquet_binance-datasets:/app/parquet_binance-datasets" \
  -v "$(pwd)/state:/app/state" \
  crypto-data-logger
```

Override symbols if needed:

```bash
docker run --rm \
  -e BINANCE_API_KEY="your_api_key_here" \
  -e SPOT_SYMBOLS="BTCUSDT,ETHUSDT,ADAUSDT,BNBUSDT,DOGEUSDT,MATICUSDT,SOLUSDT,TRXUSDT,XRPUSDT,XMRUSDT" \
  -e FUTURES_SYMBOLS="BTCUSDT,ETHUSDT,ADAUSDT,BNBUSDT,DOGEUSDT,MATICUSDT,SOLUSDT,TRXUSDT,XRPUSDT,XMRUSDT" \
  -v "$(pwd)/parquet_binance-datasets:/app/parquet_binance-datasets" \
  -v "$(pwd)/state:/app/state" \
  crypto-data-logger
```

## Important CLI Options

- `--spot-symbols`
- `--futures-symbols`
- `--out-dir`
- `--state-dir`
- `--queue-maxsize`
- `--parquet-batch-size`
- `--segment-max-rows`
- `--segment-max-age-s`
- `--flush-interval-s`
- `--fsync-interval-s`
- `--ws-max-streams-per-connection`
- `--initial-trade-backfill-trades`
- `--funding-lookback-hours`
- `--stats-interval-s`

## Operational Notes

- If `BINANCE_API_KEY` is missing, live trade websocket ingestion still runs, but REST `historicalTrades` catch-up is skipped.
- Dedupe state is persisted in two ways: short-term key seeding from recent Parquet shard tails and cursor state files under `state/`.
- Numeric exchange values are stored as raw strings to preserve exchange precision. Downstream Python or SQL jobs can cast them later.
- Depth updates are stored one row per websocket event with raw bid and ask level arrays.

## Main Files

- `crypto-data-logger.py`: runtime, schemas, workers, and CLI
- `requirements.txt`: minimal runtime dependencies
- `dockerfile`: simple container entrypoint
