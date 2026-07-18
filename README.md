<p align="center">
  <a href="https://moonbot.pro">
    <img src="assets/moonbot-logo-full.svg" alt="Moonbot" width="199">
  </a>
</p>

<h1 align="center">TradesLag</h1>

<p align="center">
  <b>Real-time trade-stream latency monitor for exchange WebSocket feeds</b><br>
  How far behind real time is each trade when it reaches you?
</p>

<p align="center">
  <a href="https://latency.moonbot.pro/"><img src="https://img.shields.io/badge/live-latency.moonbot.pro-4C6EF5" alt="Live demo"></a>
  <img src="https://img.shields.io/badge/Rust-1.96%2B-DEA584?logo=rust&logoColor=white" alt="Rust 1.96+">
  <img src="https://img.shields.io/badge/feeds-Binance%20%C2%B7%20Bybit-8B5CF6" alt="Feeds: Binance · Bybit">
  <img src="https://img.shields.io/badge/build-single%20native%20binary-16A34A" alt="Single native binary">
</p>

<p align="center">
  <a href="#features">Features</a> ·
  <a href="#how-trade-lag-is-measured">Measurement</a> ·
  <a href="#streams">Streams</a> ·
  <a href="#chart-colors">Colors</a> ·
  <a href="#build">Build</a> ·
  <a href="#configuration">Configuration</a>
</p>

TradesLag (binary `lagwatch`) measures **Trade Lag** — the delay between when a trade happens on an exchange and when its message actually reaches a client over the public WebSocket feed. It draws every received trade as a single point on rolling bitmap charts and serves them as a live web page and PNG snapshots.

**▶ See it live: [latency.moonbot.pro](https://latency.moonbot.pro/)**

<p align="center">
  <a href="https://latency.moonbot.pro/">
    <img src="https://latency.moonbot.pro/chart/hour.png" alt="Live 1-hour trade-lag chart" width="900">
  </a>
</p>
<p align="center"><sub>Live 1-hour trade-lag chart — magenta Bybit · amber Binance crypto · cyan Binance TradFi</sub></p>

## Features

- **Trade Lag measurement** — `exchange-clock-corrected receive time − exchange trade time`, so a slow local clock cannot fake a low reading.
- **Rolling bitmap charts** — the last **1 hour**, **24 hours**, and **7 days**, with every received trade drawn as one point.
- **Excess-lag hunting** — the top two non-overlapping hours of excess lag in the last 7 days, scored by integrating only the lag **above `250 ms`** (`max(0, lag_ms − 250)` per second), recomputed once a minute on a low-priority worker.
- **Survives restarts** — chart state is persisted to disk and restored on startup, so a deploy or restart does not reset the picture.
- **Resilient feeds** — WebSocket reconnects use backoff plus liveness/idle checks; exchange clock offsets are sampled through public time endpoints.
- **HTTP surface** — a web UI, a status JSON API, a health check, and live PNG chart snapshots.
- **Lean runtime** — a single native Rust binary with a lock-free ingest queue (`crossbeam-queue`) and plain OS threads — no async runtime.

## How Trade Lag is measured

Trade Lag is computed as:

```text
exchange-clock-corrected receive time − exchange trade time
```

The service samples each exchange's clock offset through its public time endpoint, so the receive timestamp is corrected to the exchange's own clock before subtracting the trade time.

**Excess-lag score.** The two "worst" windows shown on the page ignore the normal baseline: when a lag pixel first appears in a one-second bucket, `max(0, lag_ms − 250)` is added to that second's score. Only lag visibly above `250 ms` counts, and the top two non-overlapping hours over the last 7 days are recalculated once per minute in a low-priority worker thread.

## Streams

| Feed | Symbols |
|---|---|
| **Binance Futures — crypto** | `BTCUSDT`, `ETHUSDT`, `XRPUSDT` |
| **Binance Futures — TradFi** | top 20 TradFi perpetuals, selected at startup by 24h quote volume |
| **Bybit — crypto** | `BTCUSDT`, `ETHUSDT`, `XRPUSDT` |

## Chart Colors

Series are drawn as stacked layers so overlaps stay readable:

| Series | Color | Layer |
|---|---|---|
| Bybit crypto | 🟣 magenta (`#ff2bd6`) | front |
| Binance crypto | 🟡 amber (`#ffb000`) | middle |
| Binance TradFi | 🔵 cyan (`#00d7ff`) | back |

## Build

Requires **Rust 1.96 or newer**.

```bash
cargo build --release
```

Run locally:

```bash
LAGWATCH_PORT=8080 LAGWATCH_STATE=lagwatch-charts.bin ./target/release/lagwatch
```

Then open `http://localhost:8080/`.

## Configuration

| Variable | Purpose |
|---|---|
| `LAGWATCH_PORT` | HTTP listen port. |
| `LAGWATCH_STATE` | Path to the persisted chart state. Defaults to `/var/lib/lagwatch/charts.bin` on Linux. |

### HTTP endpoints

| Path | Response |
|---|---|
| `/` | Web UI |
| `/api/status` | Status JSON (connections, counters, clock offsets) |
| `/health` | `ok` health check |
| `/chart/hour.png` · `/chart/day.png` · `/chart/week.png` | 1h / 24h / 7d chart snapshots |
| `/chart/top1.png` · `/chart/top2.png` | The two worst excess-lag windows |

## Runtime Notes

- WebSocket reconnects use backoff and liveness checks; idle feeds are detected and re-established.
- Exchange clock offsets are sampled through public time endpoints and applied to every receive timestamp.
- HTTP serves the UI, status JSON, health check, and PNG chart snapshots from the same process.
- Chart state is saved periodically and on shutdown, and restored on the next start.

---

<p align="center">
  <strong>Moonbot</strong> · trade-stream latency monitoring · <a href="https://moonbot.pro">moonbot.pro</a>
</p>
