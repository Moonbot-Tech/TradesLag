use anyhow::{anyhow, Context, Result};
use crossbeam_queue::ArrayQueue;
use png::{BitDepth, ColorType, Encoder};
use serde_json::{json, Value};
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;
use std::collections::{HashMap, VecDeque};
use std::fs::{self, File};
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tiny_http::{Header, Request, Response, Server, StatusCode};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{connect, Error as WsError, Message, WebSocket};

const BINANCE_EXCHANGE_INFO_URL: &str = "https://fapi.binance.com/fapi/v1/exchangeInfo";
const BINANCE_24H_URL: &str = "https://fapi.binance.com/fapi/v1/ticker/24hr";
const BINANCE_TIME_URL: &str = "https://fapi.binance.com/fapi/v1/time";
const BINANCE_WS_BASE: &str = "wss://fstream.binance.com/market/stream?streams=";
const BYBIT_TIME_URL: &str = "https://api.bybit.com/v5/market/time";
const BYBIT_WS_URL: &str = "wss://stream.bybit.com/v5/public/linear";
const CRYPTO_SYMBOLS: [&str; 3] = ["BTCUSDT", "ETHUSDT", "XRPUSDT"];
const TRADFI_CONTRACT: &str = "TRADIFI_PERPETUAL";
const TOP_TRADFI_SYMBOLS: usize = 20;
const MAX_LAG_MS: f64 = 10_000.0;
const CHART_HEIGHT: usize = 260;
const CHART_ROW_MAX: usize = CHART_HEIGHT - 1;
const CHART_ROW_MAX_F: f64 = CHART_ROW_MAX as f64;
const HOUR_SECONDS: usize = 3_600;
const WEEK_HOURS: usize = 24 * 7;
const WEEK_SECONDS: usize = WEEK_HOURS * HOUR_SECONDS;
const TOP_LAG_HOURS: usize = 2;
const TOP_LAG_BASELINE_MS: i64 = 250;
const CPU_HZ_WINDOW: usize = 5;
const CHART_STATE_MAGIC: &[u8; 8] = b"LAGWCH03";
const HOUR_ARCHIVE_MAGIC: &[u8; 8] = b"LAGHAR01";
const CHART_STATE_VERSION: u32 = 1;
const CHART_SAVE_INTERVAL: Duration = Duration::from_secs(30);
const WS_READ_TIMEOUT: Duration = Duration::from_secs(5);
const WS_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const WS_PING_INTERVAL: Duration = Duration::from_secs(10);
const WS_IDLE_TIMEOUT: Duration = Duration::from_secs(25);
const WS_TRADE_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const CHART_QUEUE_CAPACITY: usize = 1 << 20;
const CHART_WRITE_BATCH: usize = 8_192;
const COLOR_BINANCE_CRYPTO: u32 = 0xffb000;
const COLOR_BYBIT_CRYPTO: u32 = 0xff2bd6;
const COLOR_BINANCE_TRADFI: u32 = 0x00d7ff;

#[derive(Clone, Copy)]
enum Series {
    BinanceCrypto,
    BinanceTradFi,
    BybitCrypto,
}

struct AppState {
    charts: Vec<Mutex<Chart>>,
    hour_archive: Mutex<HourArchive>,
    top_lag: Mutex<TopLagState>,
    chart_queue: Arc<ArrayQueue<TradePoint>>,
    stats: Stats,
    status: Mutex<String>,
    persist_path: PathBuf,
    started_ms: i64,
}

struct Stats {
    binance_connected: AtomicBool,
    bybit_connected: AtomicBool,
    reconnects: AtomicU64,
    messages: AtomicU64,
    bad_messages: AtomicU64,
    binance_crypto_trades: AtomicU64,
    tradfi_trades: AtomicU64,
    bybit_trades: AtomicU64,
    last_rx_ms: AtomicI64,
    last_trade_lag_ms: AtomicI64,
    min_trade_lag_ms: AtomicI64,
    msg_per_sec_x100: AtomicU64,
    cpu_vps_x100: AtomicU64,
    clock_offset_ms: AtomicI64,
    clock_rtt_ms: AtomicI64,
    bybit_clock_offset_ms: AtomicI64,
    bybit_clock_rtt_ms: AtomicI64,
    symbols_total: AtomicUsize,
    tradfi_symbols: AtomicUsize,
    bybit_symbols: AtomicUsize,
    dropped_chart_points: AtomicU64,
}

struct SymbolSet {
    streams: Vec<String>,
    tradfi_count: usize,
}

struct Chart {
    name: &'static str,
    width: usize,
    height: usize,
    bucket_ms: i64,
    last_bucket: i64,
    layers: Vec<u8>,
}

#[derive(Clone, Copy)]
struct TradePoint {
    rx_ms: i64,
    row: u16,
    layer: u8,
}

#[derive(Clone, Copy)]
struct TopLagWindow {
    start_sec: i64,
    score: u64,
}

struct TopLagState {
    windows: [Option<TopLagWindow>; TOP_LAG_HOURS],
    updated_ms: i64,
}

struct HourArchive {
    height: usize,
    last_second: i64,
    layers: Vec<u8>,
    sec_scores: Vec<u64>,
}

impl Stats {
    fn new() -> Self {
        Self {
            binance_connected: AtomicBool::new(false),
            bybit_connected: AtomicBool::new(false),
            reconnects: AtomicU64::new(0),
            messages: AtomicU64::new(0),
            bad_messages: AtomicU64::new(0),
            binance_crypto_trades: AtomicU64::new(0),
            tradfi_trades: AtomicU64::new(0),
            bybit_trades: AtomicU64::new(0),
            last_rx_ms: AtomicI64::new(0),
            last_trade_lag_ms: AtomicI64::new(0),
            min_trade_lag_ms: AtomicI64::new(i64::MAX),
            msg_per_sec_x100: AtomicU64::new(0),
            cpu_vps_x100: AtomicU64::new(0),
            clock_offset_ms: AtomicI64::new(0),
            clock_rtt_ms: AtomicI64::new(0),
            bybit_clock_offset_ms: AtomicI64::new(0),
            bybit_clock_rtt_ms: AtomicI64::new(0),
            symbols_total: AtomicUsize::new(0),
            tradfi_symbols: AtomicUsize::new(0),
            bybit_symbols: AtomicUsize::new(0),
            dropped_chart_points: AtomicU64::new(0),
        }
    }
}

impl TopLagState {
    fn new() -> Self {
        Self {
            windows: [None; TOP_LAG_HOURS],
            updated_ms: 0,
        }
    }
}

impl Chart {
    fn new(name: &'static str, width: usize, height: usize, bucket_ms: i64) -> Self {
        Self {
            name,
            width,
            height,
            bucket_ms,
            last_bucket: i64::MIN,
            layers: vec![0; width * height],
        }
    }

    fn draw_point(&mut self, point: TradePoint) {
        let bucket = point.rx_ms.div_euclid(self.bucket_ms);
        self.advance(bucket);

        let col = bucket.rem_euclid(self.width as i64) as usize;
        let row = usize::from(point.row).min(self.height - 1);
        let idx = row * self.width + col;
        if point.layer >= self.layers[idx] {
            self.layers[idx] = point.layer;
        }
    }

    fn render_png(&self, target_width: Option<usize>) -> Result<Vec<u8>> {
        let render_width = target_width.unwrap_or(self.width).clamp(64, self.width);
        let mut rgba = vec![0u8; render_width * self.height * 4];
        let grid_rows = [
            self.lag_to_row(5_000),
            self.lag_to_row(2_000),
            self.lag_to_row(1_000),
            self.lag_to_row(500),
            self.lag_to_row(250),
        ];

        for y in 0..self.height {
            for x in 0..render_width {
                let dst = (y * render_width + x) * 4;
                let grid = x % (render_width / 12).max(1) == 0 || grid_rows.contains(&y);
                let bg = if grid {
                    [30u8, 38u8, 50u8, 255u8]
                } else {
                    [8u8, 12u8, 18u8, 255u8]
                };
                rgba[dst..dst + 4].copy_from_slice(&bg);
            }
        }

        for x in 0..render_width {
            let src_start = x * self.width / render_width;
            let mut src_end = ((x + 1) * self.width + render_width - 1) / render_width;
            if src_end <= src_start {
                src_end = src_start + 1;
            }
            src_end = src_end.min(self.width);

            for y in 0..self.height {
                let mut layer = 0u8;
                for src_x in src_start..src_end {
                    let idx = y * self.width + src_x;
                    let next_layer = self.layers[idx];
                    if next_layer > layer {
                        layer = next_layer;
                    }
                }
                if layer == 0 {
                    continue;
                }

                let color = layer_color(layer);
                let dst = (y * render_width + x) * 4;
                rgba[dst] = ((color >> 16) & 0xff) as u8;
                rgba[dst + 1] = ((color >> 8) & 0xff) as u8;
                rgba[dst + 2] = (color & 0xff) as u8;
                rgba[dst + 3] = 255;
            }
        }

        let mut png_bytes = Vec::new();
        {
            let mut encoder = Encoder::new(&mut png_bytes, render_width as u32, self.height as u32);
            encoder.set_color(ColorType::Rgba);
            encoder.set_depth(BitDepth::Eight);
            encoder.set_compression(png::Compression::Fast);
            let mut writer = encoder.write_header()?;
            writer.write_image_data(&rgba)?;
        }
        Ok(png_bytes)
    }

    fn advance(&mut self, bucket: i64) {
        if self.last_bucket == i64::MIN {
            self.clear_all();
            self.last_bucket = bucket;
            return;
        }

        let delta = bucket - self.last_bucket;
        if delta <= 0 {
            return;
        }

        if delta >= self.width as i64 {
            self.clear_all();
        } else {
            for b in self.last_bucket + 1..=bucket {
                self.clear_col(b.rem_euclid(self.width as i64) as usize);
            }
        }
        self.last_bucket = bucket;
    }

    fn clear_all(&mut self) {
        self.layers.fill(0);
    }

    fn clear_col(&mut self, col: usize) {
        for y in 0..self.height {
            self.layers[y * self.width + col] = 0;
        }
    }

    fn max_lag_ms(&self) -> i64 {
        for row in 0..self.height {
            let start = row * self.width;
            let end = start + self.width;
            if self.layers[start..end].iter().any(|layer| *layer != 0) {
                return self.row_to_lag_ms(row);
            }
        }
        0
    }

    fn lag_to_row(&self, lag_ms: i64) -> usize {
        lag_to_row(self.height, lag_ms)
    }

    fn row_to_lag_ms(&self, row: usize) -> i64 {
        row_to_lag_ms(self.height, row)
    }
}

impl HourArchive {
    fn new(height: usize) -> Self {
        Self {
            height,
            last_second: i64::MIN,
            layers: vec![0; packed_len(WEEK_SECONDS * height)],
            sec_scores: vec![0; WEEK_SECONDS],
        }
    }

    fn draw_point(&mut self, point: TradePoint) {
        let second = point.rx_ms.div_euclid(1_000);
        self.advance(second);

        let sec_slot = second.rem_euclid(WEEK_SECONDS as i64) as usize;
        let row = usize::from(point.row).min(self.height - 1);
        let pixel = row * WEEK_SECONDS + sec_slot;
        let old_layer = packed_get(&self.layers, pixel);

        if old_layer == 0 {
            self.sec_scores[sec_slot] =
                self.sec_scores[sec_slot].saturating_add(self.row_score(row));
            packed_set(&mut self.layers, pixel, point.layer);
        } else if point.layer > old_layer {
            packed_set(&mut self.layers, pixel, point.layer);
        }
    }

    fn advance(&mut self, second: i64) {
        if self.last_second == i64::MIN {
            self.clear_all();
            self.last_second = second;
            return;
        }

        let delta = second - self.last_second;
        if delta <= 0 {
            return;
        }

        if delta >= WEEK_SECONDS as i64 {
            self.clear_all();
        } else {
            for s in self.last_second + 1..=second {
                self.clear_second(s.rem_euclid(WEEK_SECONDS as i64) as usize);
            }
        }
        self.last_second = second;
    }

    fn top_windows(&mut self, now_ms: i64, count: usize) -> [Option<TopLagWindow>; TOP_LAG_HOURS] {
        let now_sec = now_ms.div_euclid(1_000);
        self.advance(now_sec);

        let mut windows = [None; TOP_LAG_HOURS];
        let first = self.best_window(now_sec, None);
        if count > 0 {
            windows[0] = first;
        }
        if count > 1 {
            if let Some(window) = first {
                windows[1] = self.best_window(now_sec, Some((window.start_sec, window.end_sec())));
            }
        }
        windows
    }

    fn best_window(&self, now_sec: i64, exclude: Option<(i64, i64)>) -> Option<TopLagWindow> {
        let oldest_start = now_sec - WEEK_SECONDS as i64 + 1;
        let newest_start = now_sec - HOUR_SECONDS as i64 + 1;
        if newest_start < oldest_start {
            return None;
        }

        let mut score = 0u64;
        for offset in 0..HOUR_SECONDS as i64 {
            score = score.saturating_add(self.score_at_second(oldest_start + offset));
        }

        let mut best: Option<TopLagWindow> = None;
        let mut start = oldest_start;
        loop {
            let end = start + HOUR_SECONDS as i64;
            let overlaps = exclude
                .map(|(ex_start, ex_end)| start < ex_end && end > ex_start)
                .unwrap_or(false);
            if !overlaps && score > 0 {
                let replace = best
                    .map(|current| {
                        score > current.score
                            || (score == current.score && start > current.start_sec)
                    })
                    .unwrap_or(true);
                if replace {
                    best = Some(TopLagWindow {
                        start_sec: start,
                        score,
                    });
                }
            }

            if start >= newest_start {
                break;
            }
            score = score
                .saturating_add(self.score_at_second(start + HOUR_SECONDS as i64))
                .saturating_sub(self.score_at_second(start));
            start += 1;
        }

        best
    }

    fn render_window_png(
        &self,
        start_sec: Option<i64>,
        target_width: Option<usize>,
    ) -> Result<Vec<u8>> {
        let render_width = target_width.unwrap_or(HOUR_SECONDS).clamp(64, HOUR_SECONDS);
        let mut rgba = chart_background(render_width, self.height);

        if let Some(start_sec) = start_sec {
            for x in 0..render_width {
                let src_start = start_sec + (x * HOUR_SECONDS / render_width) as i64;
                let mut src_end =
                    start_sec + (((x + 1) * HOUR_SECONDS + render_width - 1) / render_width) as i64;
                if src_end <= src_start {
                    src_end = src_start + 1;
                }
                src_end = src_end.min(start_sec + HOUR_SECONDS as i64);

                for y in 0..self.height {
                    let mut layer = 0u8;
                    for second in src_start..src_end {
                        let sec_slot = second.rem_euclid(WEEK_SECONDS as i64) as usize;
                        let pixel = y * WEEK_SECONDS + sec_slot;
                        let next_layer = packed_get(&self.layers, pixel);
                        if next_layer > layer {
                            layer = next_layer;
                        }
                    }
                    if layer == 0 {
                        continue;
                    }

                    let color = layer_color(layer);
                    let dst = (y * render_width + x) * 4;
                    rgba[dst] = ((color >> 16) & 0xff) as u8;
                    rgba[dst + 1] = ((color >> 8) & 0xff) as u8;
                    rgba[dst + 2] = (color & 0xff) as u8;
                    rgba[dst + 3] = 255;
                }
            }
        }

        encode_png(render_width, self.height, &rgba)
    }

    fn load(&mut self, last_second: i64, height: usize, scores: &[u64], layers: &[u8]) {
        let expected_len = packed_len(WEEK_SECONDS * self.height);
        if height != self.height || scores.len() != WEEK_SECONDS || layers.len() != expected_len {
            return;
        }
        self.last_second = last_second;
        self.layers.copy_from_slice(layers);
        self.rebuild_scores();
        self.advance(unix_ms().div_euclid(1_000));
    }

    fn clear_all(&mut self) {
        self.layers.fill(0);
        self.sec_scores.fill(0);
    }

    fn clear_second(&mut self, sec_slot: usize) {
        self.sec_scores[sec_slot] = 0;
        for row in 0..self.height {
            let pixel = row * WEEK_SECONDS + sec_slot;
            packed_set(&mut self.layers, pixel, 0);
        }
    }

    fn rebuild_scores(&mut self) {
        self.sec_scores.fill(0);
        for row in 0..self.height {
            let score = self.row_score(row);
            if score == 0 {
                continue;
            }
            let row_start = row * WEEK_SECONDS;
            for sec_slot in 0..WEEK_SECONDS {
                if packed_get(&self.layers, row_start + sec_slot) != 0 {
                    self.sec_scores[sec_slot] = self.sec_scores[sec_slot].saturating_add(score);
                }
            }
        }
    }

    fn row_score(&self, row: usize) -> u64 {
        row_to_lag_ms(self.height, row)
            .saturating_sub(TOP_LAG_BASELINE_MS)
            .max(0) as u64
    }
    fn score_at_second(&self, second: i64) -> u64 {
        let sec_slot = second.rem_euclid(WEEK_SECONDS as i64) as usize;
        self.sec_scores[sec_slot]
    }
}

impl TopLagWindow {
    fn end_sec(self) -> i64 {
        self.start_sec + HOUR_SECONDS as i64
    }

    fn start_ms(self) -> i64 {
        self.start_sec.saturating_mul(1_000)
    }

    fn end_ms(self) -> i64 {
        self.end_sec().saturating_mul(1_000)
    }
}

fn packed_len(pixel_count: usize) -> usize {
    (pixel_count + 3) / 4
}

fn packed_get(data: &[u8], pixel: usize) -> u8 {
    let shift = ((pixel & 3) * 2) as u32;
    (data[pixel / 4] >> shift) & 0b11
}

fn packed_set(data: &mut [u8], pixel: usize, value: u8) {
    let shift = ((pixel & 3) * 2) as u32;
    let mask = !(0b11u8 << shift);
    data[pixel / 4] = (data[pixel / 4] & mask) | ((value & 0b11) << shift);
}

fn series_layer(series: Series) -> u8 {
    match series {
        Series::BinanceTradFi => 1,
        Series::BinanceCrypto => 2,
        Series::BybitCrypto => 3,
    }
}

fn layer_color(layer: u8) -> u32 {
    match layer {
        1 => COLOR_BINANCE_TRADFI,
        2 => COLOR_BINANCE_CRYPTO,
        3 => COLOR_BYBIT_CRYPTO,
        _ => 0,
    }
}

fn lag_to_row(height: usize, lag_ms: i64) -> usize {
    let normalized = lag_to_trader_scale(lag_ms);
    let y = (height - 1) as f64 - normalized * (height - 1) as f64;
    y.round() as usize
}

fn trade_lag_to_row(lag_ms: i64) -> u16 {
    let lag = lag_ms.clamp(0, MAX_LAG_MS as i64) as f64;
    let normalized = if lag <= 250.0 {
        lag * 0.000_88
    } else if lag <= 500.0 {
        0.22 + (lag - 250.0) * 0.000_88
    } else if lag <= 1_000.0 {
        0.44 + (lag - 500.0) * 0.000_4
    } else if lag <= 2_000.0 {
        0.64 + (lag - 1_000.0) * 0.000_18
    } else if lag <= 5_000.0 {
        0.82 + (lag - 2_000.0) * 0.000_05
    } else {
        0.97 + (lag - 5_000.0) * 0.000_006
    };
    (CHART_ROW_MAX_F - normalized * CHART_ROW_MAX_F)
        .round()
        .clamp(0.0, CHART_ROW_MAX_F) as u16
}

fn row_to_lag_ms(height: usize, row: usize) -> i64 {
    let denom = (height - 1).max(1) as f64;
    let normalized = 1.0 - row as f64 / denom;
    trader_scale_to_lag(normalized).round() as i64
}

fn chart_background(width: usize, height: usize) -> Vec<u8> {
    let mut rgba = vec![0u8; width * height * 4];
    let grid_rows = [
        lag_to_row(height, 5_000),
        lag_to_row(height, 2_000),
        lag_to_row(height, 1_000),
        lag_to_row(height, 500),
        lag_to_row(height, 250),
    ];

    for y in 0..height {
        for x in 0..width {
            let dst = (y * width + x) * 4;
            let grid = x % (width / 12).max(1) == 0 || grid_rows.contains(&y);
            let bg = if grid {
                [30u8, 38u8, 50u8, 255u8]
            } else {
                [8u8, 12u8, 18u8, 255u8]
            };
            rgba[dst..dst + 4].copy_from_slice(&bg);
        }
    }

    rgba
}

fn encode_png(width: usize, height: usize, rgba: &[u8]) -> Result<Vec<u8>> {
    let mut png_bytes = Vec::new();
    {
        let mut encoder = Encoder::new(&mut png_bytes, width as u32, height as u32);
        encoder.set_color(ColorType::Rgba);
        encoder.set_depth(BitDepth::Eight);
        encoder.set_compression(png::Compression::Fast);
        let mut writer = encoder.write_header()?;
        writer.write_image_data(rgba)?;
    }
    Ok(png_bytes)
}

fn lag_to_trader_scale(lag_ms: i64) -> f64 {
    let lag = lag_ms.max(0) as f64;
    const POINTS: &[(f64, f64)] = &[
        (0.0, 0.00),
        (250.0, 0.22),
        (500.0, 0.44),
        (1_000.0, 0.64),
        (2_000.0, 0.82),
        (5_000.0, 0.97),
        (MAX_LAG_MS, 1.00),
    ];

    for pair in POINTS.windows(2) {
        let (x0, y0) = pair[0];
        let (x1, y1) = pair[1];
        if lag <= x1 {
            let t = ((lag - x0) / (x1 - x0)).clamp(0.0, 1.0);
            return y0 + (y1 - y0) * t;
        }
    }

    1.0
}

fn trader_scale_to_lag(normalized: f64) -> f64 {
    const POINTS: &[(f64, f64)] = &[
        (0.0, 0.00),
        (250.0, 0.22),
        (500.0, 0.44),
        (1_000.0, 0.64),
        (2_000.0, 0.82),
        (5_000.0, 0.97),
        (MAX_LAG_MS, 1.00),
    ];

    let normalized = normalized.clamp(0.0, 1.0);
    for pair in POINTS.windows(2) {
        let (lag0, y0) = pair[0];
        let (lag1, y1) = pair[1];
        if normalized <= y1 {
            let t = ((normalized - y0) / (y1 - y0)).clamp(0.0, 1.0);
            return lag0 + (lag1 - lag0) * t;
        }
    }

    MAX_LAG_MS
}

fn main() -> Result<()> {
    let port = std::env::var("LAGWATCH_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(8080);
    let persist_path = chart_state_path();
    let mut charts = vec![
        Chart::new("hour", 3600, CHART_HEIGHT, 1_000),
        Chart::new("day", 2880, CHART_HEIGHT, 30_000),
        Chart::new("week", 2016, CHART_HEIGHT, 300_000),
    ];
    let mut hour_archive = HourArchive::new(CHART_HEIGHT);
    if let Err(err) = load_chart_state(&persist_path, &mut charts, &mut hour_archive) {
        eprintln!("chart state load failed: {err:#}");
    }

    let state = Arc::new(AppState {
        charts: charts.into_iter().map(Mutex::new).collect(),
        hour_archive: Mutex::new(hour_archive),
        top_lag: Mutex::new(TopLagState::new()),
        chart_queue: Arc::new(ArrayQueue::new(CHART_QUEUE_CAPACITY)),
        stats: Stats::new(),
        status: Mutex::new("booting".to_string()),
        persist_path,
        started_ms: unix_ms(),
    });

    install_shutdown_handler(Arc::clone(&state)).context("install shutdown handler")?;

    {
        let state = Arc::clone(&state);
        thread::Builder::new()
            .name("chart-writer".to_string())
            .spawn(move || chart_writer_loop(state))
            .context("spawn chart writer")?;
    }

    {
        let state = Arc::clone(&state);
        thread::Builder::new()
            .name("binance-ingest".to_string())
            .spawn(move || binance_ingest_loop(state))
            .context("spawn binance ingest thread")?;
    }

    {
        let state = Arc::clone(&state);
        thread::Builder::new()
            .name("bybit-ingest".to_string())
            .spawn(move || bybit_ingest_loop(state))
            .context("spawn bybit ingest thread")?;
    }

    {
        let state = Arc::clone(&state);
        thread::Builder::new()
            .name("binance-clock".to_string())
            .spawn(move || binance_clock_loop(state))
            .context("spawn binance clock thread")?;
    }

    {
        let state = Arc::clone(&state);
        thread::Builder::new()
            .name("bybit-clock".to_string())
            .spawn(move || bybit_clock_loop(state))
            .context("spawn bybit clock thread")?;
    }

    {
        let state = Arc::clone(&state);
        thread::Builder::new()
            .name("vps-metrics".to_string())
            .spawn(move || metrics_loop(state))
            .context("spawn metrics thread")?;
    }

    {
        let state = Arc::clone(&state);
        thread::Builder::new()
            .name("chart-persist".to_string())
            .spawn(move || persist_loop(state))
            .context("spawn persist thread")?;
    }

    {
        let state = Arc::clone(&state);
        thread::Builder::new()
            .name("top-lag-worker".to_string())
            .spawn(move || top_lag_loop(state))
            .context("spawn top lag worker")?;
    }

    run_http(state, port)
}

fn chart_state_path() -> PathBuf {
    if let Ok(path) = std::env::var("LAGWATCH_STATE") {
        return PathBuf::from(path);
    }
    #[cfg(unix)]
    {
        PathBuf::from("/var/lib/lagwatch/charts.bin")
    }
    #[cfg(not(unix))]
    {
        PathBuf::from("lagwatch-charts.bin")
    }
}

fn install_shutdown_handler(state: Arc<AppState>) -> Result<()> {
    let mut signals = Signals::new([SIGTERM, SIGINT]).context("create signal iterator")?;
    thread::Builder::new()
        .name("shutdown-save".to_string())
        .spawn(move || {
            if let Some(signal) = signals.forever().next() {
                set_status(&state, "saving chart state");
                if let Err(err) = save_chart_state(&state) {
                    eprintln!("chart state save failed on shutdown: {err:#}");
                }
                std::process::exit(if signal == SIGINT { 130 } else { 0 });
            }
        })
        .context("spawn shutdown handler")?;
    Ok(())
}

fn persist_loop(state: Arc<AppState>) {
    loop {
        thread::sleep(CHART_SAVE_INTERVAL);
        if let Err(err) = save_chart_state(&state) {
            set_status(&state, &format!("chart persist error: {err:#}"));
        }
    }
}

fn top_lag_loop(state: Arc<AppState>) {
    lower_current_thread_priority();
    loop {
        recompute_top_lag(&state);
        thread::sleep(Duration::from_secs(60));
    }
}

fn recompute_top_lag(state: &Arc<AppState>) {
    let now = unix_ms();
    let windows = {
        let Ok(mut archive) = state.hour_archive.lock() else {
            return;
        };
        archive.top_windows(now, TOP_LAG_HOURS)
    };

    if let Ok(mut top_lag) = state.top_lag.lock() {
        top_lag.windows = windows;
        top_lag.updated_ms = now;
    }
}

fn chart_writer_loop(state: Arc<AppState>) {
    let mut batch = Vec::with_capacity(CHART_WRITE_BATCH);
    loop {
        batch.clear();
        while batch.len() < CHART_WRITE_BATCH {
            let Some(point) = state.chart_queue.pop() else {
                break;
            };
            batch.push(point);
        }

        if batch.is_empty() {
            thread::sleep(Duration::from_millis(1));
            continue;
        }

        for chart in &state.charts {
            if let Ok(mut chart) = chart.lock() {
                for point in &batch {
                    chart.draw_point(*point);
                }
            }
        }

        if let Ok(mut archive) = state.hour_archive.lock() {
            for point in &batch {
                archive.draw_point(*point);
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn lower_current_thread_priority() {
    unsafe {
        let tid = libc::syscall(libc::SYS_gettid) as libc::id_t;
        let _ = libc::setpriority(libc::PRIO_PROCESS, tid, 10);
    }
}

#[cfg(not(target_os = "linux"))]
fn lower_current_thread_priority() {}

fn save_chart_state(state: &Arc<AppState>) -> Result<()> {
    save_charts_to_path(&state.persist_path, &state.charts, &state.hour_archive)
}

fn save_charts_to_path(
    path: &Path,
    charts: &[Mutex<Chart>],
    hour_archive: &Mutex<HourArchive>,
) -> Result<()> {
    let mut data = Vec::new();
    data.extend_from_slice(CHART_STATE_MAGIC);
    push_u32(&mut data, CHART_STATE_VERSION);
    push_u32(&mut data, charts.len() as u32);

    for chart in charts {
        let chart = chart
            .lock()
            .map_err(|_| anyhow!("chart lock poisoned during save"))?;
        let name = chart.name.as_bytes();
        if name.len() > u16::MAX as usize {
            return Err(anyhow!("chart name too long: {}", chart.name));
        }
        push_u16(&mut data, name.len() as u16);
        data.extend_from_slice(name);
        push_u32(&mut data, chart.width as u32);
        push_u32(&mut data, chart.height as u32);
        push_i64(&mut data, chart.bucket_ms);
        push_i64(&mut data, chart.last_bucket);
        push_u32(&mut data, chart.layers.len() as u32);
        data.extend_from_slice(&chart.layers);
    }

    {
        let archive = hour_archive
            .lock()
            .map_err(|_| anyhow!("hour archive lock poisoned during save"))?;
        data.extend_from_slice(HOUR_ARCHIVE_MAGIC);
        push_i64(&mut data, archive.last_second);
        push_u32(&mut data, archive.height as u32);
        push_u32(&mut data, archive.sec_scores.len() as u32);
        for score in &archive.sec_scores {
            push_u64(&mut data, *score);
        }
        push_u32(&mut data, archive.layers.len() as u32);
        data.extend_from_slice(&archive.layers);
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let tmp = path.with_extension("tmp");
    {
        let mut file = File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
        file.write_all(&data)
            .with_context(|| format!("write {}", tmp.display()))?;
        file.sync_all()
            .with_context(|| format!("sync {}", tmp.display()))?;
    }
    #[cfg(windows)]
    {
        let _ = fs::remove_file(path);
    }
    fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

fn load_chart_state(
    path: &Path,
    charts: &mut [Chart],
    hour_archive: &mut HourArchive,
) -> Result<()> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err).with_context(|| format!("open {}", path.display())),
    };
    let mut data = Vec::new();
    file.read_to_end(&mut data)
        .with_context(|| format!("read {}", path.display()))?;

    let mut pos = 0usize;
    let magic = read_bytes(&data, &mut pos, CHART_STATE_MAGIC.len())?;
    if magic != CHART_STATE_MAGIC {
        return Err(anyhow!("bad chart state magic"));
    }
    let version = read_u32(&data, &mut pos)?;
    if version != CHART_STATE_VERSION {
        return Err(anyhow!("unsupported chart state version: {version}"));
    }

    let count = read_u32(&data, &mut pos)? as usize;
    for _ in 0..count {
        let name_len = read_u16(&data, &mut pos)? as usize;
        let name_bytes = read_bytes(&data, &mut pos, name_len)?;
        let name = std::str::from_utf8(name_bytes).context("chart state name is not utf8")?;
        let width = read_u32(&data, &mut pos)? as usize;
        let height = read_u32(&data, &mut pos)? as usize;
        let bucket_ms = read_i64(&data, &mut pos)?;
        let last_bucket = read_i64(&data, &mut pos)?;
        let layer_len = read_u32(&data, &mut pos)? as usize;
        let layers = read_bytes(&data, &mut pos, layer_len)?;

        if let Some(chart) = charts.iter_mut().find(|chart| chart.name == name) {
            let expected_len = chart.width * chart.height;
            if chart.width == width
                && chart.height == height
                && chart.bucket_ms == bucket_ms
                && layer_len == expected_len
            {
                chart.last_bucket = last_bucket;
                chart.layers.copy_from_slice(layers);
                chart.advance(unix_ms().div_euclid(chart.bucket_ms));
            }
        }
    }

    if pos < data.len() {
        let magic = read_bytes(&data, &mut pos, HOUR_ARCHIVE_MAGIC.len())?;
        if magic == HOUR_ARCHIVE_MAGIC {
            let last_second = read_i64(&data, &mut pos)?;
            let height = read_u32(&data, &mut pos)? as usize;
            let score_len = read_u32(&data, &mut pos)? as usize;
            let mut scores = Vec::with_capacity(score_len);
            for _ in 0..score_len {
                scores.push(read_u64(&data, &mut pos)?);
            }
            let layer_len = read_u32(&data, &mut pos)? as usize;
            let layers = read_bytes(&data, &mut pos, layer_len)?;
            hour_archive.load(last_second, height, &scores, layers);
        }
    }
    Ok(())
}

fn push_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_bytes<'a>(data: &'a [u8], pos: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = pos
        .checked_add(len)
        .ok_or_else(|| anyhow!("chart state offset overflow"))?;
    if end > data.len() {
        return Err(anyhow!("truncated chart state"));
    }
    let result = &data[*pos..end];
    *pos = end;
    Ok(result)
}

fn read_u16(data: &[u8], pos: &mut usize) -> Result<u16> {
    let bytes = read_bytes(data, pos, 2)?;
    Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
}

fn read_u32(data: &[u8], pos: &mut usize) -> Result<u32> {
    let bytes = read_bytes(data, pos, 4)?;
    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_i64(data: &[u8], pos: &mut usize) -> Result<i64> {
    let bytes = read_bytes(data, pos, 8)?;
    Ok(i64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

fn read_u64(data: &[u8], pos: &mut usize) -> Result<u64> {
    let bytes = read_bytes(data, pos, 8)?;
    Ok(u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

fn binance_ingest_loop(state: Arc<AppState>) {
    let mut reconnect_delay = Duration::from_secs(2);
    loop {
        let started = Instant::now();
        match run_binance_ingest_once(&state) {
            Ok(()) => set_status(&state, "binance reconnecting"),
            Err(err) => set_status(&state, &format!("binance ingest error: {err:#}")),
        }
        state
            .stats
            .binance_connected
            .store(false, Ordering::Relaxed);
        thread::sleep(reconnect_delay);
        reconnect_delay = if started.elapsed() > Duration::from_secs(60) {
            Duration::from_secs(2)
        } else {
            (reconnect_delay * 2).min(Duration::from_secs(60))
        };
    }
}

fn bybit_ingest_loop(state: Arc<AppState>) {
    let mut reconnect_delay = Duration::from_secs(2);
    loop {
        let started = Instant::now();
        match run_bybit_ingest_once(&state) {
            Ok(()) => set_status(&state, "bybit reconnecting"),
            Err(err) => set_status(&state, &format!("bybit ingest error: {err:#}")),
        }
        state.stats.bybit_connected.store(false, Ordering::Relaxed);
        thread::sleep(reconnect_delay);
        reconnect_delay = if started.elapsed() > Duration::from_secs(60) {
            Duration::from_secs(2)
        } else {
            (reconnect_delay * 2).min(Duration::from_secs(60))
        };
    }
}

fn run_binance_ingest_once(state: &Arc<AppState>) -> Result<()> {
    let symbols = fetch_binance_symbols().context("fetch binance symbols")?;
    if symbols.streams.is_empty() {
        return Err(anyhow!("no streams selected"));
    }

    let bybit_streams = state.stats.bybit_symbols.load(Ordering::Relaxed);
    state
        .stats
        .symbols_total
        .store(symbols.streams.len() + bybit_streams, Ordering::Relaxed);
    state
        .stats
        .tradfi_symbols
        .store(symbols.tradfi_count, Ordering::Relaxed);

    let url = format!("{}{}", BINANCE_WS_BASE, symbols.streams.join("/"));
    set_status(
        state,
        &format!("binance connecting {} streams", symbols.streams.len()),
    );

    let (mut socket, _) = connect(url.as_str()).context("connect binance websocket")?;
    set_ws_timeouts(&mut socket).context("set binance websocket timeouts")?;
    state.stats.binance_connected.store(true, Ordering::Relaxed);
    state.stats.reconnects.fetch_add(1, Ordering::Relaxed);
    set_status(state, "binance connected");

    let connected_at = Instant::now();
    let mut last_frame_at = Instant::now();
    let mut last_trade_at = Instant::now();
    let mut last_ping_at = Instant::now() - WS_PING_INTERVAL;
    loop {
        if connected_at.elapsed() > Duration::from_secs(23 * 60 * 60 + 50 * 60) {
            return Ok(());
        }

        let msg = match socket.read() {
            Ok(msg) => msg,
            Err(err) if is_ws_timeout(&err) => {
                handle_ws_idle(
                    &mut socket,
                    &mut last_frame_at,
                    &last_trade_at,
                    &mut last_ping_at,
                    "binance",
                )?;
                continue;
            }
            Err(err) if err.to_string().contains("Connection reset") => return Ok(()),
            Err(err) => return Err(err).context("read binance websocket"),
        };
        last_frame_at = Instant::now();
        let rx_ms = unix_ms();

        match msg {
            Message::Text(text) => {
                state.stats.messages.fetch_add(1, Ordering::Relaxed);
                if handle_binance_trade_message(text.as_bytes(), rx_ms, state) > 0 {
                    last_trade_at = Instant::now();
                }
            }
            Message::Binary(bytes) => {
                state.stats.messages.fetch_add(1, Ordering::Relaxed);
                if handle_binance_trade_message(&bytes, rx_ms, state) > 0 {
                    last_trade_at = Instant::now();
                }
            }
            Message::Ping(payload) => {
                let _ = socket.send(Message::Pong(payload));
            }
            Message::Close(_) => return Ok(()),
            Message::Pong(_) | Message::Frame(_) => {}
        }
    }
}

fn run_bybit_ingest_once(state: &Arc<AppState>) -> Result<()> {
    let args: Vec<String> = CRYPTO_SYMBOLS
        .iter()
        .map(|symbol| format!("publicTrade.{symbol}"))
        .collect();
    state
        .stats
        .bybit_symbols
        .store(args.len(), Ordering::Relaxed);
    let binance_streams = fetch_binance_stream_count(state);
    state
        .stats
        .symbols_total
        .store(binance_streams + args.len(), Ordering::Relaxed);

    set_status(state, &format!("bybit connecting {} streams", args.len()));
    let (mut socket, _) = connect(BYBIT_WS_URL).context("connect bybit websocket")?;
    set_ws_timeouts(&mut socket).context("set bybit websocket timeouts")?;
    let sub = json!({
        "op": "subscribe",
        "args": args,
    })
    .to_string();
    socket
        .send(Message::Text(sub))
        .context("send bybit subscribe")?;

    state.stats.bybit_connected.store(true, Ordering::Relaxed);
    state.stats.reconnects.fetch_add(1, Ordering::Relaxed);
    set_status(state, "bybit connected");

    let connected_at = Instant::now();
    let mut last_frame_at = Instant::now();
    let mut last_trade_at = Instant::now();
    let mut last_ping_at = Instant::now() - WS_PING_INTERVAL;
    loop {
        if connected_at.elapsed() > Duration::from_secs(23 * 60 * 60 + 50 * 60) {
            return Ok(());
        }

        let msg = match socket.read() {
            Ok(msg) => msg,
            Err(err) if is_ws_timeout(&err) => {
                handle_ws_idle(
                    &mut socket,
                    &mut last_frame_at,
                    &last_trade_at,
                    &mut last_ping_at,
                    "bybit",
                )?;
                continue;
            }
            Err(err) if err.to_string().contains("Connection reset") => return Ok(()),
            Err(err) => return Err(err).context("read bybit websocket"),
        };
        last_frame_at = Instant::now();
        let rx_ms = unix_ms();

        match msg {
            Message::Text(text) => {
                state.stats.messages.fetch_add(1, Ordering::Relaxed);
                if handle_bybit_trade_message(text.as_bytes(), rx_ms, state) > 0 {
                    last_trade_at = Instant::now();
                }
            }
            Message::Binary(bytes) => {
                state.stats.messages.fetch_add(1, Ordering::Relaxed);
                if handle_bybit_trade_message(&bytes, rx_ms, state) > 0 {
                    last_trade_at = Instant::now();
                }
            }
            Message::Ping(payload) => {
                let _ = socket.send(Message::Pong(payload));
            }
            Message::Close(_) => return Ok(()),
            Message::Pong(_) | Message::Frame(_) => {}
        }
    }
}

fn set_ws_timeouts(socket: &mut WebSocket<MaybeTlsStream<TcpStream>>) -> Result<()> {
    match socket.get_mut() {
        MaybeTlsStream::Plain(stream) => {
            stream
                .set_read_timeout(Some(WS_READ_TIMEOUT))
                .context("set plain read timeout")?;
            stream
                .set_write_timeout(Some(WS_WRITE_TIMEOUT))
                .context("set plain write timeout")?;
        }
        MaybeTlsStream::NativeTls(stream) => {
            stream
                .get_ref()
                .set_read_timeout(Some(WS_READ_TIMEOUT))
                .context("set tls read timeout")?;
            stream
                .get_ref()
                .set_write_timeout(Some(WS_WRITE_TIMEOUT))
                .context("set tls write timeout")?;
        }
        _ => return Err(anyhow!("unsupported websocket TLS backend")),
    }
    Ok(())
}

fn is_ws_timeout(err: &WsError) -> bool {
    matches!(
        err,
        WsError::Io(io_err)
            if matches!(io_err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut)
    )
}

fn handle_ws_idle(
    socket: &mut WebSocket<MaybeTlsStream<TcpStream>>,
    last_frame_at: &mut Instant,
    last_trade_at: &Instant,
    last_ping_at: &mut Instant,
    label: &str,
) -> Result<()> {
    let now = Instant::now();
    if now.duration_since(*last_frame_at) >= WS_IDLE_TIMEOUT {
        return Err(anyhow!("{label} websocket idle for {:?}", WS_IDLE_TIMEOUT));
    }
    if now.duration_since(*last_trade_at) >= WS_TRADE_IDLE_TIMEOUT {
        return Err(anyhow!(
            "{label} trade stream idle for {:?}",
            WS_TRADE_IDLE_TIMEOUT
        ));
    }
    if now.duration_since(*last_ping_at) >= WS_PING_INTERVAL {
        socket
            .send(Message::Ping(Vec::new().into()))
            .with_context(|| format!("send {label} websocket ping"))?;
        *last_ping_at = now;
    }
    Ok(())
}

fn fetch_binance_stream_count(state: &Arc<AppState>) -> usize {
    state
        .stats
        .symbols_total
        .load(Ordering::Relaxed)
        .saturating_sub(state.stats.bybit_symbols.load(Ordering::Relaxed))
}

fn fetch_binance_symbols() -> Result<SymbolSet> {
    let value: Value = ureq::get(BINANCE_EXCHANGE_INFO_URL)
        .timeout(Duration::from_secs(10))
        .call()
        .context("GET exchangeInfo")?
        .into_json()
        .context("parse exchangeInfo")?;

    let symbols = value
        .get("symbols")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("exchangeInfo has no symbols array"))?;

    let volumes = fetch_binance_24h_quote_volumes().unwrap_or_default();
    let mut tradfi_symbols = Vec::new();
    let mut has_crypto = HashMap::new();

    for symbol in symbols {
        let Some(name) = symbol.get("symbol").and_then(Value::as_str) else {
            continue;
        };
        let status = symbol
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if status != "TRADING" {
            continue;
        }

        let contract_type = symbol
            .get("contractType")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if CRYPTO_SYMBOLS.contains(&name) {
            has_crypto.insert(name.to_string(), true);
        }

        let is_tradfi = contract_type == TRADFI_CONTRACT
            || symbol
                .get("underlyingSubType")
                .and_then(Value::as_array)
                .map(|items| items.iter().any(|item| item.as_str() == Some("TradFi")))
                .unwrap_or(false);
        if is_tradfi {
            let volume = volumes.get(name).copied().unwrap_or(0.0);
            tradfi_symbols.push((name.to_string(), volume));
        }
    }

    tradfi_symbols.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    tradfi_symbols.truncate(TOP_TRADFI_SYMBOLS);

    let mut streams = Vec::new();
    for symbol in CRYPTO_SYMBOLS {
        if has_crypto.contains_key(symbol) {
            streams.push(format!("{}@aggTrade", symbol.to_ascii_lowercase()));
        }
    }
    for (symbol, _) in &tradfi_symbols {
        streams.push(format!("{}@aggTrade", symbol.to_ascii_lowercase()));
    }

    Ok(SymbolSet {
        streams,
        tradfi_count: tradfi_symbols.len(),
    })
}

fn fetch_binance_24h_quote_volumes() -> Result<HashMap<String, f64>> {
    let value: Value = ureq::get(BINANCE_24H_URL)
        .timeout(Duration::from_secs(10))
        .call()
        .context("GET binance 24hr ticker")?
        .into_json()
        .context("parse binance 24hr ticker")?;
    let items = value
        .as_array()
        .ok_or_else(|| anyhow!("24hr ticker response is not array"))?;
    let mut volumes = HashMap::with_capacity(items.len());

    for item in items {
        let Some(symbol) = item.get("symbol").and_then(Value::as_str) else {
            continue;
        };
        let volume = item
            .get("quoteVolume")
            .and_then(Value::as_str)
            .and_then(|v| v.parse::<f64>().ok())
            .or_else(|| item.get("quoteVolume").and_then(Value::as_f64))
            .unwrap_or(0.0);
        volumes.insert(symbol.to_string(), volume);
    }

    Ok(volumes)
}

fn binance_clock_loop(state: Arc<AppState>) {
    loop {
        match fetch_binance_clock_offset() {
            Ok((offset, rtt)) => {
                state.stats.clock_offset_ms.store(offset, Ordering::Relaxed);
                state.stats.clock_rtt_ms.store(rtt, Ordering::Relaxed);
            }
            Err(err) => set_status(&state, &format!("binance clock error: {err:#}")),
        }
        thread::sleep(Duration::from_secs(30));
    }
}

fn bybit_clock_loop(state: Arc<AppState>) {
    loop {
        match fetch_bybit_clock_offset() {
            Ok((offset, rtt)) => {
                state
                    .stats
                    .bybit_clock_offset_ms
                    .store(offset, Ordering::Relaxed);
                state.stats.bybit_clock_rtt_ms.store(rtt, Ordering::Relaxed);
            }
            Err(err) => set_status(&state, &format!("bybit clock error: {err:#}")),
        }
        thread::sleep(Duration::from_secs(30));
    }
}

fn fetch_binance_clock_offset() -> Result<(i64, i64)> {
    let t0 = unix_ms();
    let value: Value = ureq::get(BINANCE_TIME_URL)
        .timeout(Duration::from_secs(5))
        .call()
        .context("GET server time")?
        .into_json()
        .context("parse server time")?;
    let t1 = unix_ms();

    let server_time = value
        .get("serverTime")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("serverTime missing"))?;
    let midpoint = t0 + (t1 - t0) / 2;
    Ok((server_time - midpoint, t1 - t0))
}

fn fetch_bybit_clock_offset() -> Result<(i64, i64)> {
    let t0 = unix_ms();
    let value: Value = ureq::get(BYBIT_TIME_URL)
        .timeout(Duration::from_secs(5))
        .call()
        .context("GET bybit server time")?
        .into_json()
        .context("parse bybit server time")?;
    let t1 = unix_ms();

    let result = value.get("result").unwrap_or(&Value::Null);
    let server_time = result
        .get("timeNano")
        .and_then(Value::as_str)
        .and_then(|v| v.parse::<i64>().ok())
        .map(|v| v / 1_000_000)
        .or_else(|| {
            result
                .get("timeSecond")
                .and_then(Value::as_str)
                .and_then(|v| v.parse::<i64>().ok())
                .map(|v| v * 1_000)
        })
        .or_else(|| value.get("time").and_then(Value::as_i64))
        .ok_or_else(|| anyhow!("bybit server time missing"))?;
    let midpoint = t0 + (t1 - t0) / 2;
    Ok((server_time - midpoint, t1 - t0))
}

fn metrics_loop(state: Arc<AppState>) {
    let mut last_messages = state.stats.messages.load(Ordering::Relaxed);
    let mut last_time = Instant::now();
    let mut msg_samples: VecDeque<f64> = VecDeque::with_capacity(CPU_HZ_WINDOW);
    let mut cpu_samples: VecDeque<f64> = VecDeque::with_capacity(CPU_HZ_WINDOW);
    let mut last_cpu = read_cpu_sample().ok();

    loop {
        thread::sleep(Duration::from_secs(1));

        let now = Instant::now();
        let messages = state.stats.messages.load(Ordering::Relaxed);
        let elapsed = now.duration_since(last_time).as_secs_f64().max(0.001);
        push_sample(
            &mut msg_samples,
            (messages.saturating_sub(last_messages)) as f64 / elapsed,
        );
        state.stats.msg_per_sec_x100.store(
            (avg_sample(&msg_samples) * 100.0).round() as u64,
            Ordering::Relaxed,
        );
        last_messages = messages;
        last_time = now;

        if let (Some(prev), Ok(next)) = (last_cpu, read_cpu_sample()) {
            let total_delta = next.total.saturating_sub(prev.total);
            let idle_delta = next.idle.saturating_sub(prev.idle);
            if total_delta > 0 {
                let busy =
                    total_delta.saturating_sub(idle_delta) as f64 * 100.0 / total_delta as f64;
                push_sample(&mut cpu_samples, busy.clamp(0.0, 100.0));
                state.stats.cpu_vps_x100.store(
                    (avg_sample(&cpu_samples) * 100.0).round() as u64,
                    Ordering::Relaxed,
                );
            }
            last_cpu = Some(next);
        }
    }
}

#[derive(Clone, Copy)]
struct CpuSample {
    idle: u64,
    total: u64,
}

fn read_cpu_sample() -> Result<CpuSample> {
    let stat = std::fs::read_to_string("/proc/stat").context("read /proc/stat")?;
    let line = stat
        .lines()
        .next()
        .ok_or_else(|| anyhow!("/proc/stat is empty"))?;
    let mut total = 0u64;
    let mut idle = 0u64;

    for (idx, part) in line.split_ascii_whitespace().skip(1).enumerate() {
        let value = part.parse::<u64>().unwrap_or(0);
        total = total.saturating_add(value);
        if idx == 3 || idx == 4 {
            idle = idle.saturating_add(value);
        }
    }

    Ok(CpuSample { idle, total })
}

fn push_sample(samples: &mut VecDeque<f64>, value: f64) {
    if samples.len() == CPU_HZ_WINDOW {
        samples.pop_front();
    }
    samples.push_back(value);
}

fn avg_sample(samples: &VecDeque<f64>) -> f64 {
    if samples.is_empty() {
        0.0
    } else {
        samples.iter().sum::<f64>() / samples.len() as f64
    }
}

fn handle_binance_trade_message(buf: &[u8], rx_ms: i64, state: &Arc<AppState>) -> usize {
    let Some(trade_time) = find_number_after(buf, b"\"T\":") else {
        state.stats.bad_messages.fetch_add(1, Ordering::Relaxed);
        return 0;
    };

    let offset = state.stats.clock_offset_ms.load(Ordering::Relaxed);
    let series = if is_crypto_payload(buf) {
        Series::BinanceCrypto
    } else {
        Series::BinanceTradFi
    };
    record_trade(state, rx_ms, trade_time, offset, series);
    1
}

fn handle_bybit_trade_message(buf: &[u8], rx_ms: i64, state: &Arc<AppState>) -> usize {
    if !contains(buf, b"\"topic\":\"publicTrade.") {
        return 0;
    }

    let offset = state.stats.bybit_clock_offset_ms.load(Ordering::Relaxed);
    let mut pos = 0usize;
    let mut trades = 0usize;
    while let Some((next_pos, trade_time)) = find_number_after_from(buf, b"\"T\":", pos) {
        record_trade(state, rx_ms, trade_time, offset, Series::BybitCrypto);
        trades += 1;
        pos = next_pos + 1;
    }

    if trades == 0 {
        state.stats.bad_messages.fetch_add(1, Ordering::Relaxed);
    }
    trades
}

fn record_trade(
    state: &Arc<AppState>,
    rx_ms: i64,
    trade_time: i64,
    clock_offset_ms: i64,
    series: Series,
) {
    let corrected_rx = rx_ms + clock_offset_ms;
    let trade_lag = (corrected_rx - trade_time).max(0);

    state.stats.last_rx_ms.store(rx_ms, Ordering::Relaxed);
    state
        .stats
        .last_trade_lag_ms
        .store(trade_lag, Ordering::Relaxed);
    atomic_min(&state.stats.min_trade_lag_ms, trade_lag);

    match series {
        Series::BinanceCrypto => {
            state
                .stats
                .binance_crypto_trades
                .fetch_add(1, Ordering::Relaxed);
        }
        Series::BinanceTradFi => {
            state.stats.tradfi_trades.fetch_add(1, Ordering::Relaxed);
        }
        Series::BybitCrypto => {
            state.stats.bybit_trades.fetch_add(1, Ordering::Relaxed);
        }
    }

    let point = TradePoint {
        rx_ms,
        row: trade_lag_to_row(trade_lag),
        layer: series_layer(series),
    };
    if let Err(point) = state.chart_queue.push(point) {
        let mut dropped = 0;
        if state.chart_queue.pop().is_some() {
            dropped += 1;
        }
        if state.chart_queue.push(point).is_err() {
            dropped += 1;
        }
        if dropped > 0 {
            state
                .stats
                .dropped_chart_points
                .fetch_add(dropped, Ordering::Relaxed);
        }
    }
}

fn find_number_after(buf: &[u8], key: &[u8]) -> Option<i64> {
    find_number_after_from(buf, key, 0).map(|(_, value)| value)
}

fn find_number_after_from(buf: &[u8], key: &[u8], start: usize) -> Option<(usize, i64)> {
    let rel = find_subslice(buf.get(start..)?, key)?;
    let key_pos = start + rel;
    let mut i = key_pos + key.len();
    while i < buf.len() && (buf[i] == b' ' || buf[i] == b'\t') {
        i += 1;
    }

    let mut sign = 1i64;
    if i < buf.len() && buf[i] == b'-' {
        sign = -1;
        i += 1;
    }

    let mut value = 0i64;
    let mut digits = 0usize;
    while i < buf.len() && buf[i].is_ascii_digit() {
        value = value
            .saturating_mul(10)
            .saturating_add((buf[i] - b'0') as i64);
        i += 1;
        digits += 1;
    }

    (digits > 0).then_some((i, value * sign))
}

fn is_crypto_payload(buf: &[u8]) -> bool {
    if let Some(pos) = find_subslice(buf, b"\"s\":\"") {
        return is_binance_crypto_symbol(&buf[pos + 5..]);
    }
    if let Some(pos) = find_subslice(buf, b"\"stream\":\"") {
        return is_binance_crypto_stream(&buf[pos + 10..]);
    }
    false
}

fn is_binance_crypto_symbol(value: &[u8]) -> bool {
    value.starts_with(b"BTCUSDT\"")
        || value.starts_with(b"ETHUSDT\"")
        || value.starts_with(b"XRPUSDT\"")
}

fn is_binance_crypto_stream(value: &[u8]) -> bool {
    value.starts_with(b"btcusdt@aggTrade\"")
        || value.starts_with(b"ethusdt@aggTrade\"")
        || value.starts_with(b"xrpusdt@aggTrade\"")
}

fn contains(buf: &[u8], needle: &[u8]) -> bool {
    find_subslice(buf, needle).is_some()
}

fn find_subslice(buf: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > buf.len() {
        return None;
    }

    let first = needle[0];
    let limit = buf.len() - needle.len();
    let mut i = 0usize;
    while i <= limit {
        if buf[i] == first && &buf[i..i + needle.len()] == needle {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn run_http(state: Arc<AppState>, port: u16) -> Result<()> {
    let addr = format!("0.0.0.0:{port}");
    let server = Server::http(&addr).map_err(|e| anyhow!("{e}"))?;
    set_status(&state, &format!("http listening on {addr}"));

    for request in server.incoming_requests() {
        let state = Arc::clone(&state);
        thread::spawn(move || {
            if let Err(err) = handle_request(request, &state) {
                eprintln!("http error: {err:#}");
            }
        });
    }

    Ok(())
}

fn handle_request(request: Request, state: &Arc<AppState>) -> Result<()> {
    let url = request.url().to_string();
    let path = url.split('?').next().unwrap_or("/");
    let target_width = query_usize(&url, "w");

    match path {
        "/" => respond_text(request, 200, "text/html; charset=utf-8", INDEX_HTML),
        "/health" => respond_text(request, 200, "text/plain; charset=utf-8", "ok\n"),
        "/api/status" => respond_json(request, &status_json(state)),
        "/chart/hour.png" => respond_chart(request, state, "hour", target_width),
        "/chart/day.png" => respond_chart(request, state, "day", target_width),
        "/chart/week.png" => respond_chart(request, state, "week", target_width),
        "/chart/top1.png" => respond_top_chart(request, state, 0, target_width),
        "/chart/top2.png" => respond_top_chart(request, state, 1, target_width),
        _ => respond_text(request, 404, "text/plain; charset=utf-8", "not found\n"),
    }
}

fn respond_chart(
    request: Request,
    state: &Arc<AppState>,
    name: &str,
    target_width: Option<usize>,
) -> Result<()> {
    let chart = state
        .charts
        .iter()
        .find_map(|chart| {
            let chart = chart.lock().ok()?;
            (chart.name == name).then(|| chart.render_png(target_width))
        })
        .ok_or_else(|| anyhow!("chart not found: {name}"))??;

    let response = Response::from_data(chart)
        .with_status_code(StatusCode(200))
        .with_header(header("Content-Type", "image/png")?)
        .with_header(header("Cache-Control", "no-store, max-age=0")?);
    request.respond(response)?;
    Ok(())
}

fn respond_top_chart(
    request: Request,
    state: &Arc<AppState>,
    index: usize,
    target_width: Option<usize>,
) -> Result<()> {
    let (_, windows) = top_lag_snapshot(state);
    let start_sec = windows
        .get(index)
        .and_then(|window| *window)
        .map(|w| w.start_sec);
    let chart = {
        let archive = state
            .hour_archive
            .lock()
            .map_err(|_| anyhow!("hour archive lock poisoned"))?;
        archive.render_window_png(start_sec, target_width)?
    };

    let response = Response::from_data(chart)
        .with_status_code(StatusCode(200))
        .with_header(header("Content-Type", "image/png")?)
        .with_header(header("Cache-Control", "no-store, max-age=0")?);
    request.respond(response)?;
    Ok(())
}

fn query_usize(url: &str, name: &str) -> Option<usize> {
    let (_, query) = url.split_once('?')?;
    for item in query.split('&') {
        let Some((key, value)) = item.split_once('=') else {
            continue;
        };
        if key == name {
            return value.parse::<usize>().ok();
        }
    }
    None
}

fn respond_json(request: Request, value: &Value) -> Result<()> {
    respond_text(
        request,
        200,
        "application/json; charset=utf-8",
        &value.to_string(),
    )
}

fn respond_text(request: Request, code: u16, content_type: &str, body: &str) -> Result<()> {
    let response = Response::from_string(body)
        .with_status_code(StatusCode(code))
        .with_header(header("Content-Type", content_type)?)
        .with_header(header("Cache-Control", "no-store, max-age=0")?);
    request.respond(response)?;
    Ok(())
}

fn status_json(state: &Arc<AppState>) -> Value {
    let now = unix_ms();
    let status = state
        .status
        .lock()
        .map(|v| v.clone())
        .unwrap_or_else(|_| "status lock poisoned".to_string());
    let min_lag = state.stats.min_trade_lag_ms.load(Ordering::Relaxed);
    let max_lag = chart_max_lag_ms(state, "week");
    let (top_updated_ms, top_windows) = top_lag_snapshot(state);
    let binance_connected = state.stats.binance_connected.load(Ordering::Relaxed);
    let bybit_connected = state.stats.bybit_connected.load(Ordering::Relaxed);

    json!({
        "status": status,
        "connected": binance_connected && bybit_connected,
        "binance_connected": binance_connected,
        "bybit_connected": bybit_connected,
        "uptime_sec": (now - state.started_ms) / 1000,
        "messages": state.stats.messages.load(Ordering::Relaxed),
        "bad_messages": state.stats.bad_messages.load(Ordering::Relaxed),
        "binance_crypto_trades": state.stats.binance_crypto_trades.load(Ordering::Relaxed),
        "binance_tradfi_trades": state.stats.tradfi_trades.load(Ordering::Relaxed),
        "bybit_crypto_trades": state.stats.bybit_trades.load(Ordering::Relaxed),
        "reconnects": state.stats.reconnects.load(Ordering::Relaxed),
        "symbols_total": state.stats.symbols_total.load(Ordering::Relaxed),
        "tradfi_symbols": state.stats.tradfi_symbols.load(Ordering::Relaxed),
        "bybit_symbols": state.stats.bybit_symbols.load(Ordering::Relaxed),
        "binance_clock_offset_ms": state.stats.clock_offset_ms.load(Ordering::Relaxed),
        "binance_clock_rtt_ms": state.stats.clock_rtt_ms.load(Ordering::Relaxed),
        "bybit_clock_offset_ms": state.stats.bybit_clock_offset_ms.load(Ordering::Relaxed),
        "bybit_clock_rtt_ms": state.stats.bybit_clock_rtt_ms.load(Ordering::Relaxed),
        "clock_offset_ms": state.stats.clock_offset_ms.load(Ordering::Relaxed),
        "clock_rtt_ms": state.stats.clock_rtt_ms.load(Ordering::Relaxed),
        "msg_per_sec": state.stats.msg_per_sec_x100.load(Ordering::Relaxed) as f64 * 0.01,
        "cpu_vps_5s": state.stats.cpu_vps_x100.load(Ordering::Relaxed) as f64 * 0.01,
        "chart_queue_len": state.chart_queue.len(),
        "chart_queue_capacity": CHART_QUEUE_CAPACITY,
        "dropped_chart_points": state.stats.dropped_chart_points.load(Ordering::Relaxed),
        "server_time_ms": now,
        "last_rx_age_ms": now - state.stats.last_rx_ms.load(Ordering::Relaxed),
        "last_trade_lag_ms": state.stats.last_trade_lag_ms.load(Ordering::Relaxed),
        "max_trade_lag_ms": max_lag,
        "min_trade_lag_ms": if min_lag == i64::MAX { 0 } else { min_lag },
        "charts": {
            "hour": chart_meta(now, 3600, 260, 1000),
            "day": chart_meta(now, 2880, 260, 30000),
            "week": chart_meta(now, 2016, 260, 300000),
            "top1": top_chart_meta(top_windows[0]),
            "top2": top_chart_meta(top_windows[1])
        },
        "top_lag_updated_ms": top_updated_ms
    })
}

fn top_lag_snapshot(state: &Arc<AppState>) -> (i64, [Option<TopLagWindow>; TOP_LAG_HOURS]) {
    state
        .top_lag
        .lock()
        .map(|top| (top.updated_ms, top.windows))
        .unwrap_or((0, [None; TOP_LAG_HOURS]))
}

fn top_chart_meta(window: Option<TopLagWindow>) -> Value {
    if let Some(window) = window {
        json!({
            "width": HOUR_SECONDS,
            "height": CHART_HEIGHT,
            "bucket_ms": 1000,
            "start_ms": window.start_ms(),
            "end_ms": window.end_ms(),
            "score_baseline_ms": TOP_LAG_BASELINE_MS,
            "score": window.score
        })
    } else {
        json!({
            "width": HOUR_SECONDS,
            "height": CHART_HEIGHT,
            "bucket_ms": 1000,
            "start_ms": 0,
            "end_ms": 0,
            "score_baseline_ms": TOP_LAG_BASELINE_MS,
            "score": 0
        })
    }
}

fn chart_max_lag_ms(state: &Arc<AppState>, name: &str) -> i64 {
    for chart in &state.charts {
        let Ok(chart) = chart.lock() else {
            continue;
        };
        if chart.name == name {
            return chart.max_lag_ms();
        }
    }
    0
}

fn chart_meta(now_ms: i64, width: usize, height: usize, bucket_ms: i64) -> Value {
    let cursor = now_ms.div_euclid(bucket_ms).rem_euclid(width as i64) as usize;
    json!({
        "width": width,
        "height": height,
        "bucket_ms": bucket_ms,
        "cursor": cursor
    })
}

fn set_status(state: &Arc<AppState>, status: &str) {
    if let Ok(mut value) = state.status.lock() {
        value.clear();
        value.push_str(status);
    }
}

fn header(name: &str, value: &str) -> Result<Header> {
    Header::from_bytes(name.as_bytes(), value.as_bytes()).map_err(|_| anyhow!("bad header"))
}

fn unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn atomic_min(target: &AtomicI64, value: i64) {
    let mut current = target.load(Ordering::Relaxed);
    while value < current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}

const INDEX_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Trade Lag Monitor</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #070b10;
      --panel: #0d141d;
      --line: #253244;
      --text: #e8eef7;
      --muted: #91a0b5;
      --bn-crypto: #ffb000;
      --bn-tradfi: #00d7ff;
      --bybit-crypto: #ff2bd6;
      --bad: #ff5b6e;
      --ok: #52e09a;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.4 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 14px 18px;
      border-bottom: 1px solid var(--line);
      background: #091019;
      position: sticky;
      top: 0;
      z-index: 2;
    }
    h1 {
      margin: 0;
      font-size: 18px;
      font-weight: 650;
      letter-spacing: 0;
    }
    main {
      display: grid;
      grid-template-columns: minmax(0, 1fr);
      gap: 14px;
      padding: 14px;
    }
    .title {
      display: flex;
      flex-direction: column;
      gap: 3px;
      min-width: 0;
    }
    .title-row {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
      min-width: 0;
    }
    .title span {
      color: var(--muted);
      font-size: 12px;
    }
    .connections {
      display: flex;
      align-items: center;
      gap: 10px;
      min-width: 0;
      color: var(--muted);
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .conn {
      display: inline-flex;
      align-items: center;
      gap: 7px;
      min-height: 30px;
      padding: 0 10px;
      border: 1px solid var(--line);
      background: #0d141d;
      white-space: nowrap;
    }
    .conn b {
      color: var(--text);
      font-weight: 650;
    }
    .conn span {
      font-size: 12px;
      font-weight: 700;
      color: var(--bad);
    }
    .conn.ok span { color: var(--ok); }
    .feed-status {
      color: var(--muted);
      font-size: 12px;
      white-space: nowrap;
    }
    .repo-link {
      display: inline-flex;
      align-items: center;
      gap: 7px;
      min-height: 28px;
      padding: 0 11px;
      border: 1px solid #f0f6fc;
      border-radius: 5px;
      background: #f0f6fc;
      color: #070b10;
      text-decoration: none;
      font-size: 12px;
      font-weight: 800;
      white-space: nowrap;
      box-shadow: 0 0 0 1px rgba(240, 246, 252, 0.10), 0 8px 20px rgba(0, 0, 0, 0.22);
    }
    .repo-link:hover {
      background: #ffffff;
      border-color: #ffffff;
      transform: translateY(-1px);
    }
    .repo-link svg {
      width: 15px;
      height: 15px;
      flex: 0 0 auto;
    }
    .dot {
      width: 9px;
      height: 9px;
      border-radius: 50%;
      background: var(--bad);
      box-shadow: 0 0 18px currentColor;
      flex: 0 0 auto;
    }
    .dot.ok { background: var(--ok); }
    .stats {
      display: grid;
      grid-template-columns: repeat(6, minmax(92px, 1fr));
      gap: 1px;
      background: var(--line);
      border: 1px solid var(--line);
    }
    .stat {
      background: var(--panel);
      padding: 10px 12px;
      min-width: 0;
    }
    .stat b {
      display: block;
      font-size: 17px;
      font-weight: 700;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .stat span {
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin-top: 2px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .legend {
      display: flex;
      gap: 10px;
      color: var(--muted);
      align-items: center;
      flex-wrap: wrap;
    }
    .key {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 6px 9px;
      border: 1px solid var(--line);
      background: #0b121b;
    }
    .key b {
      color: var(--text);
      font-weight: 650;
    }
    .key small {
      color: var(--muted);
      font-size: 11px;
      font-weight: 600;
      letter-spacing: 0;
    }
    .swatch {
      width: 28px;
      height: 8px;
      border-radius: 0;
      display: inline-block;
    }
    .scale-note {
      color: var(--muted);
      font-size: 12px;
      margin-left: 2px;
    }
    .chart-block {
      border: 1px solid var(--line);
      background: #0a1018;
    }
    .chart-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      flex-wrap: wrap;
      padding: 9px 12px;
      border-bottom: 1px solid var(--line);
    }
    .chart-head h2 {
      margin: 0;
      font-size: 14px;
      font-weight: 650;
      min-width: 180px;
    }
    .chart-head span {
      min-width: 0;
      text-align: right;
    }
    .chart-wrap {
      display: grid;
      grid-template-columns: 48px minmax(0, 1fr);
      align-items: stretch;
    }
    .chart-frame {
      position: relative;
      min-width: 0;
      cursor: crosshair;
    }
    .time-cursor {
      position: absolute;
      top: 0;
      bottom: 0;
      width: 1px;
      pointer-events: none;
      transform: translateX(-0.5px);
      opacity: 0.95;
    }
    .time-cursor::before,
    .time-cursor::after {
      content: "";
      position: absolute;
      left: 0;
      width: 2px;
      height: 16px;
      background: #f0f6fc;
      box-shadow: 0 0 10px rgba(240, 246, 252, 0.7);
    }
    .time-cursor::before {
      top: 0;
    }
    .time-cursor::after {
      bottom: 0;
    }
    .hover-cursor {
      display: none;
      position: absolute;
      top: 0;
      bottom: 0;
      width: 1px;
      pointer-events: none;
      background: rgba(240, 246, 252, 0.70);
      box-shadow: 0 0 8px rgba(240, 246, 252, 0.55);
      transform: translateX(-0.5px);
      z-index: 1;
    }
    .chart-frame.inspecting .hover-cursor {
      display: block;
    }
    .axis {
      position: relative;
      color: var(--muted);
      font-size: 11px;
      padding: 3px 8px 7px 8px;
      border-right: 1px solid var(--line);
      text-align: right;
    }
    .axis span {
      position: absolute;
      right: 8px;
      transform: translateY(-50%);
      white-space: nowrap;
    }
    img.chart {
      display: block;
      width: 100%;
      height: 260px;
      object-fit: fill;
      image-rendering: pixelated;
    }
    @media (max-width: 980px) {
      header { align-items: flex-start; flex-direction: column; }
      .connections { justify-content: flex-start; }
      .stats { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .chart-wrap { grid-template-columns: 42px minmax(0, 1fr); }
      img.chart { height: 220px; }
    }
  </style>
</head>
<body>
  <header>
    <div class="title">
      <div class="title-row">
        <h1>Trade Lag Monitor</h1>
        <a class="repo-link" href="https://github.com/Moonbot-Tech/TradesLag" target="_blank" rel="noopener noreferrer" aria-label="Open TradesLag on GitHub">
          <svg viewBox="0 0 16 16" aria-hidden="true"><path fill="currentColor" d="M8 0C3.58 0 0 3.67 0 8.2c0 3.62 2.29 6.69 5.47 7.78.4.08.55-.18.55-.39 0-.19-.01-.83-.01-1.51-2.01.38-2.53-.5-2.69-.97-.09-.24-.48-.97-.82-1.17-.28-.16-.68-.56-.01-.57.63-.01 1.08.59 1.23.83.72 1.24 1.87.89 2.33.68.07-.53.28-.89.51-1.09-1.78-.21-3.64-.91-3.64-4.04 0-.89.31-1.62.82-2.2-.08-.21-.36-1.04.08-2.17 0 0 .67-.22 2.2.84A7.4 7.4 0 0 1 8 4.15c.68 0 1.36.09 2 .27 1.53-1.06 2.2-.84 2.2-.84.44 1.13.16 1.96.08 2.17.51.58.82 1.31.82 2.2 0 3.14-1.87 3.83-3.65 4.04.29.25.54.75.54 1.52 0 1.09-.01 1.97-.01 2.24 0 .21.15.47.55.39A8.13 8.13 0 0 0 16 8.2C16 3.67 12.42 0 8 0Z"/></svg>
          GitHub
        </a>
      </div>
      <span>Binance + Bybit</span>
    </div>
    <div class="connections">
      <span id="bnConnBox" class="conn"><i id="bnDot" class="dot"></i><b>Binance</b> <span id="bnConn">WS ...</span></span>
      <span id="bbConnBox" class="conn"><i id="bbDot" class="dot"></i><b>Bybit</b> <span id="bbConn">WS ...</span></span>
      <span id="feedStatus" class="feed-status">booting</span>
    </div>
  </header>
  <main>
    <section class="stats">
      <div class="stat"><b id="lastLag">0 ms</b><span>last Trade Lag</span></div>
      <div class="stat"><b id="maxLag">0 ms</b><span>max 7d Trade Lag</span></div>
      <div class="stat"><b id="msgSec">0</b><span>msg/sec</span></div>
      <div class="stat"><b id="cpu">0%</b><span>VPS CPU 5s</span></div>
      <div class="stat"><b id="symbols">0</b><span>pairs / topics</span></div>
      <div class="stat"><b id="offset">0 ms</b><span>clock offset BN / BB</span></div>
    </section>
    <div class="legend">
      <span class="key"><i class="swatch" style="background: var(--bn-crypto)"></i><b>Binance crypto</b> <small>BTC ETH XRP</small></span>
      <span class="key"><i class="swatch" style="background: var(--bybit-crypto)"></i><b>Bybit crypto</b> <small>BTC ETH XRP</small></span>
      <span class="key"><i class="swatch" style="background: var(--bn-tradfi)"></i><b>Binance TradFi</b> <small>top 20 volume</small></span>
      <span class="scale-note">Y: 250 / 500 ms / 1s / 2s / 5s</span>
    </div>
    <section class="chart-block">
      <div class="chart-head"><h2>Trade Lag: 1 hour</h2><span id="hourMeta"></span></div>
      <div class="chart-wrap">
        <div class="axis"><span style="top:3%">5s</span><span style="top:18%">2s</span><span style="top:36%">1s</span><span style="top:56%">500</span><span style="top:78%">250</span><span style="top:98%">0ms</span></div>
        <div class="chart-frame"><img id="hour" class="chart" alt="Last hour lag chart"><i id="hourCursor" class="time-cursor"></i><i id="hourHover" class="hover-cursor"></i></div>
      </div>
    </section>
    <section class="chart-block">
      <div class="chart-head"><h2>Trade Lag: 24 hours</h2><span id="dayMeta"></span></div>
      <div class="chart-wrap">
        <div class="axis"><span style="top:3%">5s</span><span style="top:18%">2s</span><span style="top:36%">1s</span><span style="top:56%">500</span><span style="top:78%">250</span><span style="top:98%">0ms</span></div>
        <div class="chart-frame"><img id="day" class="chart" alt="Last day lag chart"><i id="dayCursor" class="time-cursor"></i><i id="dayHover" class="hover-cursor"></i></div>
      </div>
    </section>
    <section class="chart-block">
      <div class="chart-head"><h2>Trade Lag: 7 days</h2><span id="weekMeta"></span></div>
      <div class="chart-wrap">
        <div class="axis"><span style="top:3%">5s</span><span style="top:18%">2s</span><span style="top:36%">1s</span><span style="top:56%">500</span><span style="top:78%">250</span><span style="top:98%">0ms</span></div>
        <div class="chart-frame"><img id="week" class="chart" alt="Last week lag chart"><i id="weekCursor" class="time-cursor"></i><i id="weekHover" class="hover-cursor"></i></div>
      </div>
    </section>
    <section class="chart-block">
      <div class="chart-head"><h2>Top excess lag hour #1: 7 days</h2><span id="top1Meta"></span></div>
      <div class="chart-wrap">
        <div class="axis"><span style="top:3%">5s</span><span style="top:18%">2s</span><span style="top:36%">1s</span><span style="top:56%">500</span><span style="top:78%">250</span><span style="top:98%">0ms</span></div>
        <div class="chart-frame"><img id="top1" class="chart" alt="Top lag hour 1 chart"><i id="top1Hover" class="hover-cursor"></i></div>
      </div>
    </section>
    <section class="chart-block">
      <div class="chart-head"><h2>Top excess lag hour #2: 7 days</h2><span id="top2Meta"></span></div>
      <div class="chart-wrap">
        <div class="axis"><span style="top:3%">5s</span><span style="top:18%">2s</span><span style="top:36%">1s</span><span style="top:56%">500</span><span style="top:78%">250</span><span style="top:98%">0ms</span></div>
        <div class="chart-frame"><img id="top2" class="chart" alt="Top lag hour 2 chart"><i id="top2Hover" class="hover-cursor"></i></div>
      </div>
    </section>
  </main>
  <script>
    const $ = (id) => document.getElementById(id);
    const fmt = (n) => Number.isFinite(n) ? n.toLocaleString("en-US") : "0";
    const ms = (n) => `${fmt(n)} ms`;
    let lastStatus = null;
    let activeInspectChart = null;

    async function refreshStatus() {
      const r = await fetch("/api/status", { cache: "no-store" });
      const s = await r.json();
      lastStatus = s;
      $("bnDot").classList.toggle("ok", !!s.binance_connected);
      $("bbDot").classList.toggle("ok", !!s.bybit_connected);
      $("bnConnBox").classList.toggle("ok", !!s.binance_connected);
      $("bbConnBox").classList.toggle("ok", !!s.bybit_connected);
      $("bnConn").textContent = s.binance_connected ? "WS OK" : "WS DOWN";
      $("bbConn").textContent = s.bybit_connected ? "WS OK" : "WS DOWN";
      $("feedStatus").textContent = s.connected ? "feed live" : s.status;
      $("lastLag").textContent = ms(s.last_trade_lag_ms);
      $("maxLag").textContent = ms(s.max_trade_lag_ms);
      $("msgSec").textContent = Number(s.msg_per_sec || 0).toFixed(1);
      $("cpu").textContent = `${Number(s.cpu_vps_5s || 0).toFixed(1)}%`;
      $("symbols").textContent = `${s.symbols_total} (${s.tradfi_symbols} TF, ${s.bybit_symbols} BB)`;
      $("offset").textContent = `BN ${ms(s.binance_clock_offset_ms)} / BB ${ms(s.bybit_clock_offset_ms)}`;
      setMetaDefault("hour", s.charts.hour);
      setMetaDefault("day", s.charts.day);
      setMetaDefault("week", s.charts.week);
      setMetaDefault("top1", s.charts.top1);
      setMetaDefault("top2", s.charts.top2);
      setCursor("hourCursor", s.charts.hour);
      setCursor("dayCursor", s.charts.day);
      setCursor("weekCursor", s.charts.week);
    }

    function setMetaDefault(chartName, chart) {
      if (activeInspectChart === chartName) return;
      if (Object.prototype.hasOwnProperty.call(chart, "score")) {
        $(`${chartName}Meta`).textContent = formatTopMeta(chart);
        return;
      }
      const bucketMs = Number(chart.bucket_ms || 0);
      const bucketLabel = bucketMs >= 1000 ? `${bucketMs / 1000}s` : `${bucketMs}ms`;
      $(`${chartName}Meta`).textContent = `UTC · ${bucketLabel} buckets`;
    }

    function setCursor(id, chart) {
      const cursor = $(id);
      if (!cursor || !chart || !chart.width) return;
      const pct = ((Number(chart.cursor || 0) + 0.5) / chart.width) * 100;
      cursor.style.left = `${pct}%`;
    }

    function columnTimeMs(col, chart, serverTimeMs) {
      const width = Number(chart.width || 1);
      const bucketMs = Number(chart.bucket_ms || 1000);
      if (Number(chart.start_ms || 0) > 0) {
        return Number(chart.start_ms) + col * bucketMs;
      }
      const cursor = Number(chart.cursor || 0);
      const deltaBuckets = (cursor - col + width) % width;
      const currentBucketStart = Math.floor(Number(serverTimeMs || Date.now()) / bucketMs) * bucketMs;
      return currentBucketStart - deltaBuckets * bucketMs;
    }

    function utcLabel(timeMs, chart) {
      const spanMs = Number(chart.width || 0) * Number(chart.bucket_ms || 0);
      const d = new Date(timeMs);
      const p2 = (n) => String(n).padStart(2, "0");
      const hhmm = `${p2(d.getUTCHours())}:${p2(d.getUTCMinutes())}`;
      if (Number(chart.start_ms || 0) > 0) {
        const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
        return `${p2(d.getUTCDate())} ${months[d.getUTCMonth()]} ${hhmm}:${p2(d.getUTCSeconds())} UTC`;
      }
      if (spanMs > 24 * 60 * 60 * 1000) {
        const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
        return `${p2(d.getUTCDate())} ${months[d.getUTCMonth()]} ${hhmm} UTC`;
      }
      if (spanMs >= 24 * 60 * 60 * 1000) return `${hhmm} UTC`;
      return `${hhmm}:${p2(d.getUTCSeconds())} UTC`;
    }

    function utcRangeLabel(startMs, endMs) {
      const start = new Date(Number(startMs || 0));
      const end = new Date(Number(endMs || 0));
      const p2 = (n) => String(n).padStart(2, "0");
      const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
      const startTime = `${p2(start.getUTCHours())}:${p2(start.getUTCMinutes())}:${p2(start.getUTCSeconds())}`;
      const endTime = `${p2(end.getUTCHours())}:${p2(end.getUTCMinutes())}:${p2(end.getUTCSeconds())}`;
      return `${p2(start.getUTCDate())} ${months[start.getUTCMonth()]} ${startTime}-${endTime} UTC`;
    }

    function formatTopMeta(chart) {
      const score = Number(chart.score || 0);
      if (score <= 0 || Number(chart.start_ms || 0) <= 0) return "collecting";
      const baseline = Number(chart.score_baseline_ms || 250);
      return `${utcRangeLabel(chart.start_ms, chart.end_ms)} · excess >${baseline}ms ${fmt(score)}`;
    }

    function bindInspector(imgId, hoverId, chartName) {
      const img = $(imgId);
      const frame = img.closest(".chart-frame");
      const hover = $(hoverId);
      frame.addEventListener("mousemove", (event) => {
        const chart = lastStatus && lastStatus.charts && lastStatus.charts[chartName];
        if (!chart) return;

        const rect = img.getBoundingClientRect();
        const x = Math.max(0, Math.min(rect.width - 1, event.clientX - rect.left));
        const col = Math.max(0, Math.min(Number(chart.width || 1) - 1, Math.floor(x / rect.width * Number(chart.width || 1))));
        const timeMs = columnTimeMs(col, chart, lastStatus.server_time_ms);

        hover.style.left = `${x}px`;
        $(`${chartName}Meta`).textContent = utcLabel(timeMs, chart);
        activeInspectChart = chartName;
        frame.classList.add("inspecting");
      });
      frame.addEventListener("mouseleave", () => {
        frame.classList.remove("inspecting");
        activeInspectChart = null;
        const chart = lastStatus && lastStatus.charts && lastStatus.charts[chartName];
        if (chart) setMetaDefault(chartName, chart);
      });
    }

    function refreshImage(id, path) {
      const img = $(id);
      const w = Math.max(64, Math.ceil(img.getBoundingClientRect().width || img.clientWidth || 1200));
      img.src = `${path}?w=${w}&t=${Date.now()}`;
    }

    refreshStatus();
    refreshImage("hour", "/chart/hour.png");
    refreshImage("day", "/chart/day.png");
    refreshImage("week", "/chart/week.png");
    refreshImage("top1", "/chart/top1.png");
    refreshImage("top2", "/chart/top2.png");
    bindInspector("hour", "hourHover", "hour");
    bindInspector("day", "dayHover", "day");
    bindInspector("week", "weekHover", "week");
    bindInspector("top1", "top1Hover", "top1");
    bindInspector("top2", "top2Hover", "top2");
    setInterval(refreshStatus, 1000);
    setInterval(() => refreshImage("hour", "/chart/hour.png"), 1000);
    setInterval(() => refreshImage("day", "/chart/day.png"), 5000);
    setInterval(() => refreshImage("week", "/chart/week.png"), 15000);
    setInterval(() => refreshImage("top1", "/chart/top1.png"), 60000);
    setInterval(() => refreshImage("top2", "/chart/top2.png"), 60000);
  </script>
</body>
</html>
"#;
