#!/bin/sh
set -eu

ROOT_DIR="${OUT_DIR:-parquet_binance-datasets}"
MARKETS="${CANONICALIZE_MARKETS:-spot,futures}"
SYMBOLS="${CANONICALIZE_SYMBOLS:-}"
INTERVAL_SECONDS="${CANONICALIZE_INTERVAL_SECONDS:-300}"
LOG_LEVEL="${CANONICALIZE_LOG_LEVEL:-INFO}"
DERIVE_QUOTE_QUANTITY="${CANONICALIZE_DERIVE_QUOTE_QUANTITY:-true}"

while true; do
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Starting canonical trade reconciliation"

  set -- \
    --root-dir "$ROOT_DIR" \
    --markets "$MARKETS" \
    --log-level "$LOG_LEVEL"

  if [ -n "$SYMBOLS" ]; then
    set -- "$@" --symbols "$SYMBOLS"
  fi

  case "$(printf '%s' "$DERIVE_QUOTE_QUANTITY" | tr '[:upper:]' '[:lower:]')" in
    true|1|yes|y)
      set -- "$@" --derive-quote-quantity
      ;;
    false|0|no|n)
      set -- "$@" --no-derive-quote-quantity
      ;;
  esac

  python canonicalize-trades.py "$@"

  if [ "$INTERVAL_SECONDS" -le 0 ] 2>/dev/null; then
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Canonical reconciliation finished once"
    exit 0
  fi

  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Sleeping ${INTERVAL_SECONDS}s before next canonical run"
  sleep "$INTERVAL_SECONDS"
done
