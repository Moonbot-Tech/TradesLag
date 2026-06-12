<p align="center">
  <a href="https://moonbot.pro">
    <img src="assets/moonbot-logo-full.svg" alt="Moonbot" width="199">
  </a>
</p>

# TradesLag

Real-time trade stream latency monitor for exchange WebSocket feeds.

Live instance: [latency.moonbot.pro](https://latency.moonbot.pro/).

TradesLag measures **Trade Lag** as:

```text
exchange-clock-corrected receive time - exchange trade time
```

The monitor keeps rolling bitmap charts in memory and draws every received trade
as one point. The current service shows the last hour, 24 hours, and 7 days.
Chart state is periodically persisted to disk and restored on restart, so the
picture does not reset on deploy or service restart.

The page also shows the top two non-overlapping excess-lag hours found inside
the last 7 days. Their score ignores the normal baseline and only integrates
visible lag above `250 ms`: when a lag pixel first appears in a second bucket,
`max(0, lag_ms - 250)` is added to that second's score. Top windows are
recalculated once per minute in a low-priority worker thread.

## Streams

- Binance Futures crypto: `BTCUSDT`, `ETHUSDT`, `XRPUSDT`.
- Binance Futures TradFi: top 20 TradFi perpetuals selected at startup by 24h quote volume.
- Bybit crypto: `BTCUSDT`, `ETHUSDT`, `XRPUSDT`.

## Chart Colors

- Bybit crypto: magenta, front layer.
- Binance crypto: amber, middle layer.
- Binance TradFi: cyan, back layer.

## Runtime Notes

- WebSocket reconnects use backoff and liveness checks.
- Exchange clock offsets are sampled through public time endpoints.
- HTTP serves the UI, status JSON, health check, and PNG chart snapshots.
- Default state path on Linux is `/var/lib/lagwatch/charts.bin`.

## Build

Requires Rust 1.96 or newer.

```bash
cargo build --release
```

Run locally:

```bash
LAGWATCH_PORT=8080 LAGWATCH_STATE=lagwatch-charts.bin ./target/release/lagwatch
```

---

<p align="center">
  <strong>Moonbot</strong><br>
  Advanced terminal for cryptocurrency trading<br>
  <a href="https://moonbot.pro">moonbot.pro</a>
</p>
