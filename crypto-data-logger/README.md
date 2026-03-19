# Binance Raw Async Data Logger

`crypto-data-logger.py` is a raw-data-only Binance ingestion service for spot and USD-M futures. It fetches only the required live streams and REST backfill endpoints, normalizes them into Parquet tables, and leaves all feature engineering to downstream Python or SQL jobs.

## Key features

- Continuous token buckets, per-endpoint min spacing
- Endpoint-scoped, half-open circuits with adaptive decay
- Unified timeout policy + latency buckets
- Strong trades pagination with `fromId` continuation + dedupe
- Arrow Parquet writer with batched writes, periodic fsync, atomic swap
- Persisted dedupe seeding per shard (reads last few `ts_ms`)
- Cross-task stats aggregation and graceful shutdown (no `os._exit`)
- Fully async ingestion with bounded queues and strict write backpressure
- Prometheus `/metrics` and liveness `/health`
- Structured JSON logging with `structlog`

## Fetch scope

### Spot

- `<symbol>@trade`
- `<symbol>@bookTicker`
- `<symbol>@depth@100ms`

### Futures

- `<symbol>@trade`
- `<symbol>@bookTicker`
- `<symbol>@depth@100ms`
- `<symbol>@markPrice@1s`
- funding rate history via REST

## Non-goals

The logger does not fetch:

- kline
- aggTrade
- miniTicker
- ticker

The logger does not calculate:

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

## Output tables

- `spot_trades`
- `futures_trades`
- `spot_l1`
- `futures_l1`
- `spot_depth_updates`
- `futures_depth_updates`
- `futures_mark_price`
- `futures_funding_history`

Downstream canonical tables produced by `canonicalize-trades.py`:

- `canonical_spot_trades`
- `canonical_futures_trades`

Partition layout:

```text
parquet_binance-datasets/
  <table_name>/
    date=YYYY-MM-DD/
      instrument=SYMBOL/
        hour=HH/
          part-<opened_ms>-<session>-<segment>.parquet
```

## Architecture summary

The service is split into four layers:

1. `asyncio` producers
   Spot/futures websocket tasks ingest live raw events. Async REST tasks backfill trades with `fromId` continuation and fetch futures funding history.

2. Bounded async routing
   Each normalized row is pushed into an `asyncio.Queue(maxsize=...)` for its target table. If a writer falls behind, producers block on queue put and memory stays bounded.

3. Async table writers
   One async writer task runs per table. It batches rows, manages partition-local dedupe windows, and schedules blocking Arrow Parquet work on a bounded executor.

4. Observability/control plane
   A lightweight HTTP server exposes `/health` and `/metrics`. Logs are emitted as structured JSON via `structlog`.

5. Canonical reconciliation layer
   A separate downstream job reads raw trades, preserves both websocket and REST variants, and writes one canonical row per `(symbol, trade_id)` with REST-preferred fields where available.

## Configuration

Configuration is loaded via `pydantic-settings` from environment variables and optional CLI overrides.

Core environment variables:

- `ENABLE_SPOT=true|false`
- `ENABLE_FUTURES=true|false`
- `SPOT_SYMBOLS=BTCUSDT,ETHUSDT,ADAUSDT,BNBUSDT,DOGEUSDT,MATICUSDT,SOLUSDT,TRXUSDT,XRPUSDT,XMRUSDT`
- `FUTURES_SYMBOLS=BTCUSDT,ETHUSDT,ADAUSDT,BNBUSDT,DOGEUSDT,MATICUSDT,SOLUSDT,TRXUSDT,XRPUSDT,XMRUSDT`
- `QUOTE_ASSETS=USDT`
- `DISCOVERY_LIMIT=0`
- `OUT_DIR=parquet_binance-datasets`
- `STATE_DIR=state`
- `LOG_LEVEL=INFO`
- `QUEUE_MAXSIZE=20000`
- `PARQUET_BATCH_SIZE=512`
- `SEGMENT_MAX_ROWS=8192`
- `SEGMENT_MAX_AGE_SECONDS=30`
- `FLUSH_INTERVAL_SECONDS=2`
- `FSYNC_INTERVAL_SECONDS=15`
- `MAX_OPEN_PARTITION_WRITERS=16`
- `PARTITION_WRITER_IDLE_CLOSE_SECONDS=5`
- `DEDUPE_WINDOW_SECONDS=900`
- `SEEN_CAPACITY=<optional override>`
- `WS_MAX_STREAMS_PER_CONNECTION=120`
- `INITIAL_TRADE_BACKFILL_TRADES=1000`
- `FUNDING_LOOKBACK_HOURS=168`
- `HEALTH_PORT=8080`
- `BINANCE_API_KEY` or `BINANCE_API_KEY_FILE`

### Symbol discovery

If you explicitly set `SPOT_SYMBOLS=auto` or `FUTURES_SYMBOLS=auto`, the logger discovers active pairs at startup:

- spot: `/api/v3/exchangeInfo`
- futures: `/fapi/v1/exchangeInfo`

If discovery fails, the fallback basket is:

- `BTCUSDT`
- `ETHUSDT`
- `SOLUSDT`
- `BNBUSDT`
- `XRPUSDT`
- `ADAUSDT`

### Symbol typo handling

`MATICUSDT` is the correct symbol. If an older config still uses `MATICUDST`, it is normalized to `MATICUSDT`. `MONERO` is normalized to `XMRUSDT`.

### Dedupe window sizing

If `SEEN_CAPACITY` is not set, dedupe capacity is derived automatically:

```text
symbols × expected_rate × window_seconds
```

This keeps dedupe state proportional to the configured basket and stream intensity instead of relying on a fixed heuristic.

### Trade completeness strategy

For trade tables, writer dedupe now keeps websocket and REST variants separately for the same trade ID. That means:

- websocket remains your low-latency live source
- REST backfill can still land later for the same trade
- the downstream canonical job can merge both views without losing the richer REST row

This is the recommended pattern if you want both fast live capture and better post-trade completeness.

### File descriptor safety

Wide-universe ingestion can create many active partitions in the same hour. To prevent `Too many open files` failures, the writer layer now:

- caps concurrently open Parquet partition writers with `MAX_OPEN_PARTITION_WRITERS`
- checkpoints and closes idle partition writers after `PARTITION_WRITER_IDLE_CLOSE_SECONDS`
- still preserves atomic shard swaps and writer-level dedupe

## Running locally

### 1. Create a virtual environment

```bash
cd "/Users/bobet/Documents/Code Repository/Trading-Systems/crypto-data-logger"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Run with the default focused basket

```bash
python crypto-data-logger.py
```

### 3. Run with auto-discovered symbols

```bash
SPOT_SYMBOLS=auto FUTURES_SYMBOLS=auto python crypto-data-logger.py
```

### 4. Run with an explicit basket

```bash
export SPOT_SYMBOLS="BTCUSDT,ETHUSDT,ADAUSDT,BNBUSDT,DOGEUSDT,MATICUSDT,SOLUSDT,TRXUSDT,XRPUSDT,XMRUSDT"
export FUTURES_SYMBOLS="BTCUSDT,ETHUSDT,ADAUSDT,BNBUSDT,DOGEUSDT,MATICUSDT,SOLUSDT,TRXUSDT,XRPUSDT,XMRUSDT"
python crypto-data-logger.py --print-config
```

### 5. Enable historical trade backfill

Trade backfill needs an API key because Binance `historicalTrades` requires it:

```bash
export BINANCE_API_KEY="your_api_key_here"
python crypto-data-logger.py
```

Without an API key, the logger still captures:

- spot websocket `trade`, `bookTicker`, `depth@100ms`
- futures websocket `trade`, `bookTicker`, `depth@100ms`, `markPrice@1s`
- futures funding rate history

Without an API key, the logger does not backfill:

- spot `historicalTrades`
- futures `historicalTrades`

## Canonical trades

`canonicalize-trades.py` builds a cleaner trade layer from the raw trade lake. It is intentionally downstream of ingestion, so the live logger stays raw and low-latency.

Canonical merge rules:

- group by `(symbol, trade_id)`
- preserve raw parquet as-is
- prefer REST values when REST has richer trade fields
- keep websocket lineage flags
- optionally derive `quote_quantity = price * quantity` when Binance did not provide `quoteQty`
- leave impossible public-market fields nullable when Binance does not expose them

Canonical output fields include:

- `preferred_source`
- `has_ws`
- `has_rest`
- `ws_ingest_ts_ms`
- `rest_ingest_ts_ms`
- `quote_quantity_is_derived`
- `raw_row_count`

### Build canonical trades locally

```bash
python canonicalize-trades.py
```

### Rebuild only a subset of symbols

```bash
python canonicalize-trades.py --symbols BTCUSDT,ETHUSDT,SOLUSDT
```

### Disable quote quantity derivation

```bash
python canonicalize-trades.py --no-derive-quote-quantity
```

### Use canonical trades in live systems

Recommended usage:

- live trading hot path: websocket market data
- background data quality path: REST backfill plus canonical reconciliation
- research/audit path: canonical tables

Do not block trade execution on REST completion. REST is for completeness, recovery, and reconciliation, not for your lowest-latency decision path.

### Scheduling canonicalization

Recommended deployment shape:

- same `docker-compose.yml`
- separate `canonicalize-trades` service
- same image
- same mounted parquet/state volumes
- separate lifecycle from the live logger

This is better than running both jobs inside one container because:

- the logger and reconciler can restart independently
- logs stay easier to reason about
- reconciliation cadence can change without touching live ingestion
- heavier canonical rebuilds do not get coupled to the logger process

## Observability

The service exposes:

- `GET /health`
- `GET /metrics`

Default address:

```text
http://127.0.0.1:8080
```

Examples:

```bash
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/metrics
```

Prometheus metrics include:

- REST latency histograms and coarse latency buckets
- queue sizes and queue backpressure wait time
- open partition writer count per table
- token bucket balance gauges
- circuit breaker state gauges
- disk write latency, bytes, rows, throughput, and fsync counts
- last activity age for websocket, REST, and disk

### What healthy looks like

- `/health` returns `status=ok`
- `spot_symbol_count` and `futures_symbol_count` match your intended basket
- `binance_table_queue_size` fluctuates but does not climb forever
- `binance_table_queue_backpressure_total` stays low or grows slowly
- `binance_open_partition_writers` stays near or below your configured cap
- `binance_circuit_state` stays at `0`
- `binance_last_activity_age_seconds` stays low for `ws`, `rest`, and `disk`
- logs continue to emit `runtime_stats` without repeated task failures

### Quick health checks

```bash
curl http://127.0.0.1:8080/health
curl -s http://127.0.0.1:8080/metrics | rg '^binance_'
docker logs crypto-data-logger --tail 50
docker stats crypto-data-logger
```

### Suggested alerts

- queue size remains near `QUEUE_MAXSIZE`
- backpressure counter accelerates quickly
- open partition writers stay pinned near `MAX_OPEN_PARTITION_WRITERS`
- disk write throughput drops to zero while websocket traffic is still flowing
- repeated websocket reconnect spikes
- circuit breaker opens repeatedly
- `/health` returns non-200

## Production readiness

Recommended production practices:

- keep the raw logger and canonical job separate
- run the logger with Docker health checks enabled
- set a high enough `nofile` limit for Parquet shards
- keep symbol scope explicit unless you intentionally want `auto`
- mount persistent volumes for both parquet output and state cursors
- monitor queue growth, open writers, reconnects, and disk write speed
- use API keys for trade completeness, but not as a dependency for low-latency execution
- test restart/recovery regularly so REST backfill can prove it closes gaps

### Production smoke test

1. Start the logger and wait for `/health` to return `ok`.
2. Confirm parquet files are appearing only for your intended symbols.
3. Restart the container and confirm ingestion resumes without queue explosion.
4. Start the canonical service or run `canonicalize-trades.py` and verify canonical output appears.
5. Confirm `has_ws` and `has_rest` appear as expected in canonical trade rows.

## Docker

### Build the image locally

```bash
cd "/Users/bobet/Documents/Code Repository/Trading-Systems/crypto-data-logger"
docker build -t engr-bobet-14/crypto-data-logger:latest -f dockerfile .
```

### Run the image directly

```bash
mkdir -p parquet_binance-datasets state secrets
printf '%s' "your_api_key_here" > secrets/binance_api_key.txt

docker run --rm \
  --ulimit nofile=65536:65536 \
  -p 8080:8080 \
  -e BINANCE_API_KEY_FILE=/run/secrets/binance_api_key \
  -e SPOT_SYMBOLS=BTCUSDT,ETHUSDT,ADAUSDT,BNBUSDT,DOGEUSDT,MATICUSDT,SOLUSDT,TRXUSDT,XRPUSDT,XMRUSDT \
  -e FUTURES_SYMBOLS=BTCUSDT,ETHUSDT,ADAUSDT,BNBUSDT,DOGEUSDT,MATICUSDT,SOLUSDT,TRXUSDT,XRPUSDT,XMRUSDT \
  -v "$(pwd)/parquet_binance-datasets:/app/parquet_binance-datasets" \
  -v "$(pwd)/state:/app/state" \
  -v "$(pwd)/secrets/binance_api_key.txt:/run/secrets/binance_api_key:ro" \
  engr-bobet-14/crypto-data-logger:latest
```

### Run canonical reconciliation from the image

```bash
docker run --rm \
  -v "$(pwd)/parquet_binance-datasets:/app/parquet_binance-datasets" \
  engr-bobet-14/crypto-data-logger:latest \
  python canonicalize-trades.py
```

### Push to Docker Hub

If your Docker Hub repository is `engr-bobet-14/crypto-data-logger`, the standard publish flow is:

```bash
docker login
docker build -t engr-bobet-14/crypto-data-logger:latest -f dockerfile .
docker push engr-bobet-14/crypto-data-logger:latest
```

If you use a different Docker Hub repo or tag, change the image reference everywhere consistently.

## Docker Compose

`docker-compose.yml` is image-based and expects a prebuilt/pushed image. It can run both:

- `crypto-data-logger` for live raw ingestion
- `canonicalize-trades` for scheduled canonical reconciliation

The canonical service lives in the same compose file, but behind a dedicated profile so you can enable it when you want it.

- image: `engr-bobet-14/crypto-data-logger:latest`
- secret file: `./secrets/binance_api_key.txt`
- data mount: `./parquet_binance-datasets`
- state mount: `./state`
- default basket: `BTCUSDT, ETHUSDT, ADAUSDT, BNBUSDT, DOGEUSDT, MATICUSDT, SOLUSDT, TRXUSDT, XRPUSDT, XMRUSDT`

### 1. Prepare directories

```bash
mkdir -p parquet_binance-datasets state secrets
printf '%s' "your_api_key_here" > secrets/binance_api_key.txt
```

### 2. Start the service

```bash
docker compose up -d
```

That starts only the live raw logger by default.

### 3. Override the image or symbol basket

```bash
CRYPTO_DATA_LOGGER_IMAGE=engr-bobet-14/crypto-data-logger:latest \
SPOT_SYMBOLS="BTCUSDT,ETHUSDT,MATICUSDT" \
FUTURES_SYMBOLS="BTCUSDT,ETHUSDT,MATICUSDT" \
docker compose up -d
```

### 4. Start scheduled canonicalization too

```bash
docker compose --profile canonicalize up -d
```

That starts:

- `crypto-data-logger`
- `canonicalize-trades`

Default canonical schedule settings:

- every `300` seconds
- markets: `spot,futures`
- symbols: your focused 10-symbol basket
- `quote_quantity` derivation enabled

Override example:

```bash
CANONICALIZE_INTERVAL_SECONDS=600 \
CANONICALIZE_SYMBOLS="BTCUSDT,ETHUSDT,SOLUSDT" \
docker compose --profile canonicalize up -d
```

### 5. Switch to full auto-discovery intentionally

If you want the full active Binance universe instead of the default focused basket:

```bash
SPOT_SYMBOLS=auto \
FUTURES_SYMBOLS=auto \
DISCOVERY_LIMIT=0 \
MAX_OPEN_PARTITION_WRITERS=16 \
docker compose up -d
```

## Operational notes

- Each output table has its own bounded queue and writer task.
- Disk I/O is intentionally offloaded to a bounded executor instead of blocking the main event loop.
- The logger uses atomic Parquet shard swaps so partially written files do not become visible as final data files.
- State cursors live under `state/` and are fsynced on update.
- Existing Parquet shards are scanned on startup to seed dedupe windows for recent partitions.
