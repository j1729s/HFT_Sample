# HFT_Sample

A clean-room sample repository showcasing market-data ingestion, state building, and market-making research implementations. This repo is intentionally NDA-safe and focuses on clarity and reproducibility over breadth.

## Current Status (Implemented)

### Bybit (Python)

**Located in:** `Bybit/`

#### What's included

- WebSocket market-data ingest for:
  - `orderbook.1.<symbol>` (top-of-book stream)
  - `orderbook.50.<symbol>` (snapshot + delta)
- Opens 3 WebSocket connections to the same topic (staggered) to reduce latency spikes
- Uses `orjson` for fast JSON decoding
- Logs to per-run files using a user-provided `run_tag`

#### Latency testing (Bybit `orderbook.1`)

Latency was measured in two ways:

1. **Local in-process latency**
   - Time from local receipt of a WebSocket message to successful state application
   - Measured with a monotonic clock to avoid wall-clock skew

2. **Exchange-referenced latency**
   - Time from Bybit’s matching-engine timestamp (`cts`) to local state application
   - Useful as a diagnostic, but not the same as pure local processing time

#### What we found

On a sample run from a **Vultr VPS in Singapore**:

- **Local end-to-end apply latency**
  - p50: **~81 µs**
  - p95: **~116 µs**
  - p99: **~137 µs**
  - max: **~292 µs**

- **Queue delay inside the process**
  - p50: **~69 µs**

- **State update cost**
  - p50: **~11 µs**

- **Exchange `cts` to local apply**
  - p50: **~26 ms**
  - p95: **~29 ms**

These results suggest that the Python hot path itself is lightweight once a message is already inside the process. The larger timing component comes from the exchange/network/socket side rather than from local state maintenance.

> Note: older “tick-to-log” measurements in the ~6–8 ms range were based on a different measurement path and are not directly comparable to the monotonic local in-process timings reported here.

#### Graphs

**Local end-to-end latency over time**

![Local end-to-end latency](docs/local_e2e_latency_over_time.png)

**Distribution of local end-to-end latency**

![Local end-to-end latency distribution](docs/local_e2e_latency_distribution.png)

**Queue delay vs state update cost**

![Queue delay vs state update](docs/queue_delay_vs_state_update.png)

**Exchange `cts` to local apply**

![Exchange cts to local apply](docs/exchange_cts_to_apply_over_time.png)

**Winning connection share**

![Winning connection share](docs/winning_connection_share.png)

## Roadmap

### 1) Bybit (Python)

- Add trades stream ingestion and processing
- Support trade prints and basic feature hooks

### 2) Binance (Rust)

Build a Rust market-data connector focused on practical low-latency patterns:

- WebSocket connector for key streams such as `bookTicker` and `aggTrade`
- Bounded ring buffer / channels with lock-free sharing where possible
- Busy-spin consumer with core isolation for hot-path processing
- Micro-benchmark covering parse → enqueue cost and throughput, without exaggerated claims

### 3) Market-making paper (Python)

Implement a market-making bot based on:

- Avellaneda–Stoikov
- Olivier Guéant extensions to optimal market making

## How to run (current Bybit code)

From the repo root:

```bash
python3 Sample_main.py
