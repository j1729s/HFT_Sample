# HFT_Sample

A clean-room sample repository showcasing market-data ingestion, state building, and market-making research implementations. This repo is intentionally **NDA-safe** and focuses on **clarity** and **reproducibility** over breadth.

---

## Current Status (Implemented)

### Bybit (Python)
Located in: `Bybit/`

**What’s included**
- **WebSocket market-data ingest** for:
  - `orderbook.1.<symbol>` (top-of-book / “ticker-like” stream)
  - `orderbook.50.<symbol>` (snapshot + delta)
- Opens **3 WebSocket connections** to the same topic (staggered) to reduce latency spikes.
- Uses `orjson` for fast JSON decoding.
- Logs to per-run files using a user-provided `run_tag`.

### Latency testing (Bybit orderbook.1)
We measured feed delay using local receive time vs Bybit timestamps.

- Typical observed latency: **~6–8 ms**
- Test environment: **Vultr VPS (Singapore)** (same country as Bybit servers)

> Note: this measures *feed delay* as observed by the client, not exchange round-trip latency.

**Plot**
- `images/latency_hist.png`

---


---

## Roadmap

### 1) Bybit (Python)
- Add **trades stream** ingestion + processing (trade prints, basic feature hooks)

### 2) Binance (Rust)
Build a Rust market-data connector focused on practical low-latency patterns:
- WebSocket connector for key streams (e.g., `bookTicker`, `aggTrade`)
- **Bounded ring buffer / channels** with **lock-free sharing** (single-producer / single-consumer where possible)
- **Busy-Spin** consumer with **core isolation** for hot-path processing (documented and clearly scoped)
- Micro-benchmark: parse → enqueue cost and throughput (no exaggerated claims)

### 3) Market making paper (Python)
Implement a market-making bot based on:
- **Avellaneda–Stoikov**
- **Olivier Guéant** extensions to optimal market making

---

## How to run (current Bybit code)

From the repo root:

```
bash python3 Sample_main.py
