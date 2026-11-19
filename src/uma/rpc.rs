use std::{
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};

use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use crate::{
    config::Config,
    pipeline::Processor,
    stats::Stats,
    storage::Storage,
    uma::events::{RpcLog, TOPIC_DISPUTE_PRICE, TOPIC_PROPOSE_PRICE, parse_hex_u64},
    wire::now_us,
};

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("WebSocket transport error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("HTTP transport error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("JSON decode error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("RPC error {code}: {message}")]
    Remote { code: i64, message: String },
    #[error("RPC response is missing result")]
    MissingResult,
    #[error("subscription stream closed")]
    Closed,
    #[error("invalid RPC number")]
    Number,
    #[error("timed out {0}")]
    Timeout(&'static str),
    #[error(
        "subscription silent for {idle_s}s while the chain had {logs} matching logs in blocks {from}..={to}"
    )]
    Stalled {
        idle_s: u64,
        from: u64,
        to: u64,
        logs: usize,
    },
}

/// Runs the live WSS racers and the one-shot HTTP backfill.
///
/// `config.wss_rpc_urls` is raced, not load-balanced: every configured
/// endpoint gets its own independent, always-reconnecting subscription, all
/// feeding the same `Processor`. Whichever endpoint delivers a given log
/// first wins; `EventHub`'s (transaction_hash, log_index) dedup — already
/// required for backfill/live overlap — silently drops the slower copies, so
/// this degrades to a single connection for free when only one URL is
/// configured. Backfill stays HTTP-only and runs exactly once, independent of
/// which racer is up.
pub async fn run_rpc_loop(
    config: Arc<Config>,
    storage: Storage,
    processor: Arc<Processor>,
    stats: Arc<Stats>,
    mut shutdown: watch::Receiver<bool>,
) {
    let http = match HttpRpc::new(config.polygon_rpc_url.clone()) {
        Ok(client) => Arc::new(client),
        Err(error) => {
            warn!(%error, "cannot initialize HTTP RPC client");
            return;
        }
    };

    let (any_connected_tx, mut any_connected_rx) = watch::channel(false);
    let workers: Vec<JoinHandle<()>> = config
        .wss_rpc_urls
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, url)| {
            tokio::spawn(live_worker(
                index,
                url,
                config.clone(),
                http.clone(),
                processor.clone(),
                stats.clone(),
                any_connected_tx.clone(),
                shutdown.clone(),
            ))
        })
        .collect();
    info!(racers = workers.len(), "Polygon live racers starting");

    // Wait for at least one racer to subscribe before backfilling, so the
    // backfill boundary (current head) doesn't leave a gap before live
    // coverage begins. If shutdown fires first, skip straight to teardown.
    tokio::select! {
        _ = shutdown.changed() => {}
        _ = any_connected_rx.wait_for(|connected| *connected) => {}
    }

    let mut backoff = Duration::from_secs(1);
    while !*shutdown.borrow() {
        match run_backfill(&config, &storage, &http, &processor).await {
            Ok(()) => break,
            Err(error) => {
                warn!(%error, "initial signal backfill failed; retrying");
                tokio::select! {
                    _ = shutdown.changed() => break,
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }

    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            break;
        }
    }
    for worker in workers {
        worker.abort();
    }
}

const RECONNECT_BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// A session that stayed up at least this long counts as healthy: the next
/// reconnect starts over at the initial delay instead of continuing the
/// exponential climb.
const HEALTHY_SESSION: Duration = Duration::from_secs(30);

/// Delay to sleep before the next reconnect attempt, and the escalated
/// value to carry into the attempt after that. Exponential only across
/// *consecutive* quick failures — a healthy session resets the climb, so a
/// provider's routine connection rotation weeks apart never accumulates
/// into a permanent 30s outage per drop (which, with the racers behind a
/// shared edge, is a permanent event gap when both drop together).
fn reconnect_backoff(current: Duration, session_lasted: Duration) -> (Duration, Duration) {
    if session_lasted >= HEALTHY_SESSION {
        // A healthy session that just dropped (the provider's routine
        // connection rotation, about once a day in production) reconnects
        // at once. Every event during the wait is only delivered late by the
        // gap-fill, and with a single racer nothing else covers that second.
        // A failed attempt is a short session, so the climb below still
        // starts at the initial delay — this cannot spin.
        return (Duration::ZERO, RECONNECT_BACKOFF_INITIAL);
    }
    (current, (current * 2).min(RECONNECT_BACKOFF_MAX))
}

/// One racer's forever-reconnecting live subscription.
#[allow(clippy::too_many_arguments)]
async fn live_worker(
    index: usize,
    url: String,
    config: Arc<Config>,
    http: Arc<HttpRpc>,
    processor: Arc<Processor>,
    stats: Arc<Stats>,
    any_connected: watch::Sender<bool>,
    mut shutdown: watch::Receiver<bool>,
) {
    let source = format!("wss[{index}]");
    let mut backoff = RECONNECT_BACKOFF_INITIAL;
    let mut sessions = 0_u64;
    loop {
        if *shutdown.borrow() {
            break;
        }
        let started = Instant::now();
        // Not the first session => a reconnect: fill whatever this racer
        // missed while it was down (see `run_live_session`).
        let gap_fill = (sessions > 0).then(|| GapFill {
            index,
            config: config.clone(),
            http: http.clone(),
            processor: processor.clone(),
            stats: stats.clone(),
        });
        let watchdog = StallWatchdog {
            idle: config.rpc_idle_check,
            http: http.clone(),
            max_window: config.backfill_batch_blocks.max(1),
        };
        match run_live_session(
            &source,
            &url,
            config.live_buffer,
            &processor,
            &stats,
            &watchdog,
            &any_connected,
            gap_fill,
            shutdown.clone(),
        )
        .await
        {
            Ok(()) if *shutdown.borrow() => break,
            Ok(()) => warn!(source, "Polygon live subscription ended; reconnecting"),
            Err(error @ RpcError::Stalled { .. }) => {
                Stats::increment(&stats.rpc_stall_reconnects);
                warn!(%error, source, "Polygon live subscription stalled; reconnecting");
            }
            Err(error) => warn!(%error, source, "Polygon live subscription failed; reconnecting"),
        }
        sessions += 1;
        mark_source_disconnected(&stats);
        Stats::increment(&stats.rpc_reconnects);
        let (delay, next) = reconnect_backoff(backoff, started.elapsed());
        backoff = next;
        tokio::select! {
            _ = shutdown.changed() => if *shutdown.borrow() { break; },
            _ = tokio::time::sleep(delay) => {}
        }
    }
}

/// Everything a reconnecting racer needs to catch up on the blocks that
/// went by while it was disconnected.
struct GapFill {
    index: usize,
    config: Arc<Config>,
    http: Arc<HttpRpc>,
    processor: Arc<Processor>,
    stats: Arc<Stats>,
}

impl GapFill {
    /// Spawned, never awaited by the live loop: the reconnected subscription
    /// must start draining immediately, not sit behind HTTP round trips —
    /// the catch-up runs beside it and `EventHub` dedup absorbs the
    /// overlap, exactly like the startup backfill. Only needed when *every*
    /// racer was down at once (otherwise the others covered the gap and
    /// this is a few cheap, fully-deduplicated `eth_getLogs`), but that is
    /// precisely the case that used to be a permanent hole: backfill ran
    /// once at startup and nothing ever re-walked a live outage.
    fn spawn(self) {
        let Some(from) = gap_fill_start(self.stats.latest_block.load(Ordering::Relaxed)) else {
            return; // nothing observed yet; the startup backfill owns the range
        };
        let source = format!("gapfill[{}]", self.index);
        tokio::spawn(async move {
            match gap_fill(
                &self.http,
                &self.processor,
                from,
                self.config.backfill_batch_blocks,
                &source,
                &GAP_FILL_RETRY_DELAYS,
            )
            .await
            {
                Ok(head) => info!(from, to = head, source, "gap-fill after reconnect complete"),
                Err(error) => {
                    warn!(%error, from, source, "gap-fill after reconnect failed; giving up")
                }
            }
        });
    }
}

/// Waits between gap-fill attempts; its length is the retry count. The HTTP
/// endpoint is usually the same provider as the WSS that just dropped, so a
/// failure right after a reconnect is likely and short-lived. Without a
/// retry one failed `eth_getLogs` left the outage window unfilled for good.
const GAP_FILL_RETRY_DELAYS: [Duration; 5] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(5),
    Duration::from_secs(10),
    Duration::from_secs(30),
];

/// First block a gap-fill must re-read, given the newest block any log was
/// seen in. That block itself is included: the connection can drop between
/// two logs of one block, and `latest_block + 1` would skip the rest of it
/// for good. Re-read logs are absorbed by `EventHub` dedup. `None` before
/// any log was seen.
fn gap_fill_start(latest_block: u64) -> Option<u64> {
    (latest_block > 0).then_some(latest_block)
}

/// Re-reads `from..=head` over HTTP, retrying the whole pass after each of
/// `retry_delays`. Returns the head it filled up to.
async fn gap_fill(
    http: &HttpRpc,
    processor: &Arc<Processor>,
    from: u64,
    batch_blocks: u64,
    source: &str,
    retry_delays: &[Duration],
) -> Result<u64, RpcError> {
    let mut attempt = 0;
    loop {
        let pass = async {
            let head = http.latest_block().await?;
            if from <= head {
                backfill_range(http, processor, from, head, batch_blocks, source).await?;
            }
            Ok::<u64, RpcError>(head)
        };
        match pass.await {
            Ok(head) => return Ok(head),
            Err(error) if attempt < retry_delays.len() => {
                warn!(%error, from, source, attempt, "gap-fill after reconnect failed; retrying");
                tokio::time::sleep(retry_delays[attempt]).await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_live_session(
    source: &str,
    url: &str,
    live_buffer: usize,
    processor: &Arc<Processor>,
    stats: &Arc<Stats>,
    watchdog: &StallWatchdog,
    any_connected: &watch::Sender<bool>,
    gap_fill: Option<GapFill>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), RpcError> {
    let (mut live, reader) = subscribe_live(
        url,
        live_buffer,
        stats.clone(),
        shutdown.clone(),
        SUBSCRIBE_STEP_TIMEOUT,
    )
    .await?;
    mark_source_connected(stats);
    any_connected.send_replace(true);
    info!(source, "Polygon signal subscription connected");
    // Subscribed first, then catch up: the new subscription bounds the gap
    // from above, so nothing between "last seen" and "now" can slip past.
    if let Some(gap_fill) = gap_fill {
        gap_fill.spawn();
    }

    // Stall watchdog state. `verified` is the highest block this session is
    // known to have covered: the newest log it delivered, or the upper end
    // of a window a check proved empty. `None` until either happens.
    let mut verified: Option<u64> = None;
    let mut received = 0_u64;
    let mut idle_since = tokio::time::Instant::now();
    let (check_tx, mut check_rx) = mpsc::channel::<(u64, Result<StallCheck, RpcError>)>(1);
    let mut checking = false;

    let result = loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() { break Ok(()); }
            }
            message = live.recv() => {
                let Some(log) = message else { break Err(RpcError::Closed); };
                let block = parse_hex_u64(&log.block_number, "blockNumber").unwrap_or_default();
                processor.process(log, now_us(), source).await;
                if block > 0 {
                    processor.checkpoint(block);
                    verified = Some(verified.map_or(block, |seen| seen.max(block)));
                }
                received += 1;
                idle_since = tokio::time::Instant::now();
            }
            // Never awaited inline: the check is spawned so the live reader
            // keeps draining while the HTTP round trips run.
            _ = tokio::time::sleep_until(idle_since + watchdog.idle),
                if !watchdog.idle.is_zero() && !checking =>
            {
                checking = true;
                let http = watchdog.http.clone();
                let tx = check_tx.clone();
                let max_window = watchdog.max_window;
                let snapshot = received;
                tokio::spawn(async move {
                    let outcome = check_for_stall(&http, verified, max_window).await;
                    let _ = tx.send((snapshot, outcome)).await;
                });
            }
            Some((snapshot, outcome)) = check_rx.recv() => {
                checking = false;
                idle_since = tokio::time::Instant::now();
                if snapshot != received {
                    // A log arrived while the check was in flight: alive.
                    continue;
                }
                match outcome {
                    Ok(StallCheck::Quiet { to }) => {
                        verified = Some(verified.map_or(to, |seen| seen.max(to)));
                    }
                    Ok(StallCheck::Missed { from, to, logs }) => {
                        break Err(RpcError::Stalled {
                            idle_s: watchdog.idle.as_secs(),
                            from,
                            to,
                            logs,
                        });
                    }
                    // The HTTP side being down says nothing about this
                    // subscription; keep it and try again next idle period.
                    Err(error) => warn!(%error, source, "stall check failed"),
                }
            }
        }
    };
    reader.abort();
    result
}

/// Per-session settings for the "connected but silent" watchdog. A WSS
/// subscription can stay open (pings answered) while the provider has
/// silently dropped it server-side; the reader then waits forever. Polygon
/// regularly goes several minutes without a single UMA log, so silence alone
/// is not proof — the check asks the chain over HTTP before reconnecting.
struct StallWatchdog {
    idle: Duration,
    http: Arc<HttpRpc>,
    max_window: u64,
}

/// Blocks a log must be behind the head before its absence from the
/// subscription counts against it — absorbs push latency and the HTTP and
/// WSS backends sitting on slightly different heads.
const STALL_CONFIRM_LAG_BLOCKS: u64 = 5;

#[derive(Debug, PartialEq, Eq)]
enum StallCheck {
    /// Nothing the session should have delivered: it covered up to `to`.
    Quiet { to: u64 },
    /// The chain has `logs` matching logs in `from..=to` that the session
    /// never pushed.
    Missed { from: u64, to: u64, logs: usize },
}

/// One stall probe. Runs in a spawned task, never on the live reader.
///
/// With no `verified` block yet (fresh session, quiet chain) there is no
/// lower bound to compare against, so the first probe only records the
/// current head; the next idle period checks from there.
async fn check_for_stall(
    http: &HttpRpc,
    verified: Option<u64>,
    max_window: u64,
) -> Result<StallCheck, RpcError> {
    let to = http
        .latest_block()
        .await?
        .saturating_sub(STALL_CONFIRM_LAG_BLOCKS);
    let Some(verified) = verified else {
        return Ok(StallCheck::Quiet { to });
    };
    if to <= verified {
        return Ok(StallCheck::Quiet { to: verified });
    }
    let from = verified
        .saturating_add(1)
        .max(to.saturating_sub(max_window.max(1) - 1));
    let logs = http.get_logs(from, to).await?.len();
    Ok(if logs == 0 {
        StallCheck::Quiet { to }
    } else {
        StallCheck::Missed { from, to, logs }
    })
}

fn mark_source_connected(stats: &Stats) {
    Stats::increment(&stats.rpc_sources_connected);
    stats.rpc_connected.store(true, Ordering::Relaxed);
}

fn mark_source_disconnected(stats: &Stats) {
    Stats::decrement_saturating(&stats.rpc_sources_connected);
    let remaining = stats.rpc_sources_connected.load(Ordering::Relaxed);
    stats.rpc_connected.store(remaining > 0, Ordering::Relaxed);
}

/// One-shot historical catch-up from the last checkpoint (or the configured
/// start / a 7-day-lookback estimate on a fresh data directory) up to the
/// head observed when backfill starts. Purely HTTP; independent of the WSS
/// racers.
async fn run_backfill(
    config: &Config,
    storage: &Storage,
    http: &HttpRpc,
    processor: &Arc<Processor>,
) -> Result<(), RpcError> {
    let subscribed_head = http.latest_block().await?;
    let checkpoint = storage.load_uma_cursor().ok().flatten();
    let from = if let Some(checkpoint) = checkpoint {
        // The checkpoint block itself is re-read, not skipped: the live path
        // checkpoints the block of each log as it arrives, so a stop between
        // two logs of one block leaves that block only partly processed.
        Some(checkpoint)
    } else if let Some(start) = config.start_block {
        Some(start)
    } else {
        let head_timestamp = http.block_timestamp(subscribed_head).await?;
        let target = head_timestamp
            .saturating_sub(config.initial_backfill_days.saturating_mul(24 * 60 * 60));
        // Polygon normally produces fewer than one block per second. This lower bound is
        // deliberately wider than the requested wall-clock window and avoids querying genesis
        // on full (non-archive) RPC nodes during the binary search.
        let search_low = subscribed_head
            .saturating_sub(config.initial_backfill_days.saturating_mul(24 * 60 * 60));
        Some(
            http.first_block_at_or_after(target, search_low, subscribed_head)
                .await?,
        )
    };

    if let Some(cursor) = from
        && cursor <= subscribed_head
    {
        info!(
            from = cursor,
            to = subscribed_head,
            "starting signal backfill after live subscription"
        );
        backfill_range(
            http,
            processor,
            cursor,
            subscribed_head,
            config.backfill_batch_blocks,
            "backfill",
        )
        .await?;
    }
    Ok(())
}

/// Walks `[from, to]` in `batch_blocks`-sized `eth_getLogs` calls, feeding
/// every log through the normal `Processor` path (dedup included) and
/// checkpointing each completed batch. Shared by the one-shot startup
/// backfill and the per-reconnect gap-fill.
async fn backfill_range(
    http: &HttpRpc,
    processor: &Arc<Processor>,
    from: u64,
    to: u64,
    batch_blocks: u64,
    source: &str,
) -> Result<(), RpcError> {
    let mut cursor = from;
    while cursor <= to {
        let end = cursor.saturating_add(batch_blocks.max(1) - 1).min(to);
        let mut logs = http.get_logs(cursor, end).await?;
        logs.sort_unstable_by_key(|log| {
            (
                parse_hex_u64(&log.block_number, "blockNumber").unwrap_or_default(),
                parse_hex_u64(&log.log_index, "logIndex").unwrap_or_default(),
            )
        });
        for log in logs {
            processor.process(log, now_us(), source).await;
        }
        processor.checkpoint(end);
        cursor = end.saturating_add(1);
    }
    Ok(())
}

/// Limit for each of the two steps of opening a subscription: the
/// TCP/TLS/WebSocket handshake, and the `eth_subscribe` acknowledgement.
/// Neither had one. An endpoint that accepted the connection and then went
/// quiet parked that racer in `connect_async` or the ack loop with no retry —
/// the stall watchdog only starts once a session exists — and with a single
/// racer that is a full outage with nothing in the journal.
const SUBSCRIBE_STEP_TIMEOUT: Duration = Duration::from_secs(10);

async fn subscribe_live(
    url: &str,
    live_buffer: usize,
    stats: Arc<Stats>,
    mut shutdown: watch::Receiver<bool>,
    step_timeout: Duration,
) -> Result<(mpsc::Receiver<RpcLog>, JoinHandle<()>), RpcError> {
    let (mut socket, _) = tokio::time::timeout(step_timeout, connect_async(url))
        .await
        .map_err(|_| RpcError::Timeout("connecting to the WSS endpoint"))??;
    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": ["logs", {
            "topics": [[TOPIC_PROPOSE_PRICE, TOPIC_DISPUTE_PRICE]]
        }]
    });
    let request_text = request.to_string();
    stats
        .rpc_bytes_sent
        .fetch_add(request_text.len() as u64, Ordering::Relaxed);
    let acknowledged = async {
        socket.send(Message::Text(request_text.into())).await?;
        loop {
            let message = socket.next().await.ok_or(RpcError::Closed)??;
            match message {
                Message::Text(text) => {
                    stats
                        .rpc_bytes_received
                        .fetch_add(text.len() as u64, Ordering::Relaxed);
                    let response: WsResponse = serde_json::from_str(text.as_ref())?;
                    if let Some(error) = response.error {
                        return Err(RpcError::Remote {
                            code: error.code,
                            message: error.message,
                        });
                    }
                    if response.id == Some(1) && response.result.is_some() {
                        return Ok(());
                    }
                }
                Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
                Message::Close(_) => return Err(RpcError::Closed),
                _ => {}
            }
        }
    };
    tokio::time::timeout(step_timeout, acknowledged)
        .await
        .map_err(|_| RpcError::Timeout("waiting for the eth_subscribe acknowledgement"))??;

    let (tx, rx) = mpsc::channel(live_buffer.max(1));
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.changed() => if *shutdown.borrow() { break; },
                message = socket.next() => {
                    let Some(message) = message else { break; };
                    match message {
                        Ok(Message::Text(text)) => {
                            stats
                                .rpc_bytes_received
                                .fetch_add(text.len() as u64, Ordering::Relaxed);
                            match serde_json::from_str::<SubscriptionNotification>(text.as_ref()) {
                                Ok(notification) if notification.method == "eth_subscription" => {
                                    if tx.send(notification.params.result).await.is_err() { break; }
                                }
                                Ok(_) => {}
                                Err(error) => warn!(%error, "invalid subscription notification"),
                            }
                        }
                        Ok(Message::Ping(payload)) => {
                            if socket.send(Message::Pong(payload)).await.is_err() { break; }
                        }
                        Ok(Message::Close(_)) | Err(_) => break,
                        _ => {}
                    }
                }
            }
        }
    });
    Ok((rx, task))
}

struct HttpRpc {
    client: Client,
    url: String,
}

impl HttpRpc {
    fn new(url: String) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_secs(20))
                .user_agent("rust_uma/0.1")
                .build()?,
            url,
        })
    }

    async fn latest_block(&self) -> Result<u64, RpcError> {
        let value: String = self.call("eth_blockNumber", json!([])).await?;
        parse_hex_u64(&value, "blockNumber").map_err(|_| RpcError::Number)
    }

    async fn block_timestamp(&self, block: u64) -> Result<u64, RpcError> {
        let value: RpcBlock = self
            .call(
                "eth_getBlockByNumber",
                json!([format!("0x{block:x}"), false]),
            )
            .await?;
        parse_hex_u64(&value.timestamp, "timestamp").map_err(|_| RpcError::Number)
    }

    async fn first_block_at_or_after(
        &self,
        target_timestamp: u64,
        mut low: u64,
        head: u64,
    ) -> Result<u64, RpcError> {
        let mut high = head;
        while low < high {
            let middle = low + (high - low) / 2;
            if self.block_timestamp(middle).await? < target_timestamp {
                low = middle.saturating_add(1);
            } else {
                high = middle;
            }
        }
        Ok(low)
    }

    async fn get_logs(&self, from: u64, to: u64) -> Result<Vec<RpcLog>, RpcError> {
        self.call(
            "eth_getLogs",
            json!([{
                "fromBlock": format!("0x{from:x}"),
                "toBlock": format!("0x{to:x}"),
                "topics": [[TOPIC_PROPOSE_PRICE, TOPIC_DISPUTE_PRICE]]
            }]),
        )
        .await
    }

    async fn call<T: DeserializeOwned>(&self, method: &str, params: Value) -> Result<T, RpcError> {
        let response = self
            .client
            .post(&self.url)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .send()
            .await
            .map_err(reqwest::Error::without_url)?;
        // `without_url`: reqwest prints the request URL in its errors, and
        // the provider's API key is part of this URL — it must not reach the
        // journal through a `warn!(%error)`.
        let body: RpcResponse<T> = response.json().await.map_err(reqwest::Error::without_url)?;
        if let Some(error) = body.error {
            return Err(RpcError::Remote {
                code: error.code,
                message: error.message,
            });
        }
        body.result.ok_or(RpcError::MissingResult)
    }
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcRemoteError>,
}

#[derive(Deserialize)]
struct RpcBlock {
    timestamp: String,
}

#[derive(Deserialize)]
struct WsResponse {
    id: Option<u64>,
    result: Option<String>,
    error: Option<RpcRemoteError>,
}

#[derive(Deserialize)]
struct RpcRemoteError {
    code: i64,
    message: String,
}

#[derive(Deserialize)]
struct SubscriptionNotification {
    method: String,
    params: SubscriptionParams,
}

#[derive(Deserialize)]
struct SubscriptionParams {
    result: RpcLog,
}

#[cfg(test)]
mod tests {
    use axum::{Json, Router, routing::post};
    use serde_json::{Value, json};

    use super::*;

    #[tokio::test]
    async fn finds_first_block_at_or_after_seven_day_boundary() {
        async fn rpc(Json(request): Json<Value>) -> Json<Value> {
            let block = request["params"][0]
                .as_str()
                .and_then(|value| u64::from_str_radix(value.trim_start_matches("0x"), 16).ok())
                .unwrap();
            Json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {"timestamp": format!("0x{:x}", block * 10)}
            }))
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route("/", post(rpc));
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let client = HttpRpc::new(format!("http://{address}")).unwrap();

        assert_eq!(
            client.first_block_at_or_after(555, 0, 100).await.unwrap(),
            56
        );
        server.abort();
    }

    #[test]
    fn backoff_escalates_across_quick_failures_and_resets_after_a_healthy_session() {
        let quick = Duration::from_millis(200);
        let (d1, next) = reconnect_backoff(RECONNECT_BACKOFF_INITIAL, quick);
        assert_eq!(d1, Duration::from_secs(1));
        let (d2, next) = reconnect_backoff(next, quick);
        assert_eq!(d2, Duration::from_secs(2));
        let (d3, next) = reconnect_backoff(next, quick);
        assert_eq!(d3, Duration::from_secs(4));
        // Keeps climbing but caps.
        let (_, capped) = (0..10).fold((d3, next), |(_, n), _| reconnect_backoff(n, quick));
        assert_eq!(capped, RECONNECT_BACKOFF_MAX);
        // A long healthy session reconnects at once, regardless of how high
        // the climb had gotten; if that attempt fails quickly the climb
        // starts over at the initial delay.
        let (delay, next) = reconnect_backoff(capped, HEALTHY_SESSION);
        assert_eq!(delay, Duration::ZERO);
        assert_eq!(next, RECONNECT_BACKOFF_INITIAL);
        let (delay, next) = reconnect_backoff(next, quick);
        assert_eq!(delay, RECONNECT_BACKOFF_INITIAL);
        assert_eq!(next, RECONNECT_BACKOFF_INITIAL * 2);
    }

    /// `backfill_range` must cover the whole inclusive range in
    /// `batch_blocks`-sized `eth_getLogs` windows with no gap and no
    /// overlap, and checkpoint the last block — this is what the
    /// per-reconnect gap-fill relies on to turn a live outage into a
    /// late delivery rather than a permanent hole.
    #[tokio::test]
    async fn backfill_range_walks_every_block_once_in_batches() {
        use std::sync::Mutex;

        use crate::{
            config::test_config, enrichment::Catalog, hub::EventHub, storage::StorageCommand,
        };

        type Windows = Arc<Mutex<Vec<(u64, u64)>>>;
        async fn rpc(
            axum::extract::State(windows): axum::extract::State<Windows>,
            Json(request): Json<Value>,
        ) -> Json<Value> {
            assert_eq!(request["method"], "eth_getLogs");
            let hex = |key: &str| {
                u64::from_str_radix(
                    request["params"][0][key]
                        .as_str()
                        .unwrap()
                        .trim_start_matches("0x"),
                    16,
                )
                .unwrap()
            };
            windows
                .lock()
                .unwrap()
                .push((hex("fromBlock"), hex("toBlock")));
            Json(json!({"jsonrpc": "2.0", "id": 1, "result": []}))
        }

        let windows: Windows = Arc::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/", post(rpc))
            .with_state(windows.clone());
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let client = HttpRpc::new(format!("http://{address}")).unwrap();

        let (batch_tx, _batch_rx) = mpsc::channel(4);
        let (storage_tx, mut storage_rx) = mpsc::channel(16);
        let processor = Arc::new(Processor::new(
            Arc::new(test_config()),
            Arc::new(Catalog::new(Vec::new())),
            Arc::new(EventHub::new(16)),
            batch_tx,
            storage_tx,
            Arc::new(Stats::default()),
            Arc::new(crate::forfeit::ForfeitBlocklist::disabled()),
            0,
        ));

        backfill_range(&client, &processor, 100, 125, 10, "gapfill[0]")
            .await
            .unwrap();

        assert_eq!(
            *windows.lock().unwrap(),
            vec![(100, 109), (110, 119), (120, 125)]
        );
        let mut checkpoints = Vec::new();
        while let Ok(StorageCommand::Checkpoint(block)) = storage_rx.try_recv() {
            checkpoints.push(block);
        }
        assert_eq!(checkpoints, vec![109, 119, 125]);
        server.abort();
    }

    /// Mock HTTP RPC for the stall watchdog: a head starting at `head` and
    /// advancing by `step` per `eth_blockNumber`, and `logs` copies of a
    /// minimal log for every `eth_getLogs`. Records each `eth_getLogs` window.
    type Calls = Arc<std::sync::Mutex<Vec<(u64, u64)>>>;

    async fn stall_rpc(
        head: u64,
        step: u64,
        logs: usize,
    ) -> (String, Calls, tokio::task::JoinHandle<()>) {
        use axum::extract::State;

        #[derive(Clone)]
        struct Mock {
            head: Arc<std::sync::atomic::AtomicU64>,
            step: u64,
            logs: usize,
            calls: Calls,
        }
        async fn rpc(State(mock): State<Mock>, Json(request): Json<Value>) -> Json<Value> {
            let result = match request["method"].as_str().unwrap() {
                "eth_blockNumber" => {
                    let head = mock.head.fetch_add(mock.step, Ordering::Relaxed);
                    json!(format!("0x{head:x}"))
                }
                "eth_getLogs" => {
                    let hex = |key: &str| {
                        u64::from_str_radix(
                            request["params"][0][key]
                                .as_str()
                                .unwrap()
                                .trim_start_matches("0x"),
                            16,
                        )
                        .unwrap()
                    };
                    mock.calls
                        .lock()
                        .unwrap()
                        .push((hex("fromBlock"), hex("toBlock")));
                    let log = json!({
                        "address": "0x0000000000000000000000000000000000000001",
                        "topics": [TOPIC_PROPOSE_PRICE],
                        "data": "0x",
                        "blockNumber": "0x1",
                        "transactionHash": format!("0x{:064x}", 1),
                        "logIndex": "0x0"
                    });
                    json!(vec![log; mock.logs])
                }
                other => panic!("unexpected method {other}"),
            };
            Json(json!({"jsonrpc": "2.0", "id": 1, "result": result}))
        }

        let calls: Calls = Arc::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route("/", post(rpc)).with_state(Mock {
            head: Arc::new(head.into()),
            step,
            logs,
            calls: calls.clone(),
        });
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{address}"), calls, server)
    }

    #[tokio::test]
    async fn stall_check_first_probe_only_records_the_head() {
        let (url, calls, server) = stall_rpc(1000, 0, 3).await;
        let http = HttpRpc::new(url).unwrap();
        assert_eq!(
            check_for_stall(&http, None, 1000).await.unwrap(),
            StallCheck::Quiet {
                to: 1000 - STALL_CONFIRM_LAG_BLOCKS
            }
        );
        assert!(
            calls.lock().unwrap().is_empty(),
            "no eth_getLogs without a lower bound"
        );
        server.abort();
    }

    #[tokio::test]
    async fn stall_check_flags_logs_the_session_never_pushed() {
        let (url, calls, server) = stall_rpc(1000, 0, 2).await;
        let http = HttpRpc::new(url).unwrap();
        assert_eq!(
            check_for_stall(&http, Some(900), 1000).await.unwrap(),
            StallCheck::Missed {
                from: 901,
                to: 995,
                logs: 2
            }
        );
        // The window is capped so a long-dead session costs one bounded call.
        check_for_stall(&http, Some(10), 100).await.unwrap();
        assert_eq!(*calls.lock().unwrap(), vec![(901, 995), (896, 995)]);
        server.abort();
    }

    #[tokio::test]
    async fn stall_check_quiet_chain_advances_the_verified_block() {
        let (url, _calls, server) = stall_rpc(1000, 0, 0).await;
        let http = HttpRpc::new(url).unwrap();
        assert_eq!(
            check_for_stall(&http, Some(900), 1000).await.unwrap(),
            StallCheck::Quiet { to: 995 }
        );
        // Already covered past the confirmed head: nothing to ask.
        assert_eq!(
            check_for_stall(&http, Some(999), 1000).await.unwrap(),
            StallCheck::Quiet { to: 999 }
        );
        server.abort();
    }

    /// A WSS endpoint that acknowledges `eth_subscribe` and then never
    /// pushes anything, while answering pings — the "connected but silent"
    /// failure seen in production on 2026-11-12.
    async fn silent_wss() -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                    while let Some(Ok(message)) = ws.next().await {
                        if let Message::Text(_) = message {
                            let ack = json!({"jsonrpc": "2.0", "id": 1, "result": "0xsub"});
                            let _ = ws.send(Message::Text(ack.to_string().into())).await;
                        }
                    }
                });
            }
        });
        (format!("ws://{address}"), server)
    }

    async fn run_silent_session(logs_on_chain: usize) -> (Result<(), RpcError>, Arc<Stats>) {
        use crate::{config::test_config, enrichment::Catalog, hub::EventHub};

        let (wss, wss_server) = silent_wss().await;
        let (http_url, _calls, http_server) = stall_rpc(1000, 10, logs_on_chain).await;
        let stats = Arc::new(Stats::default());
        let (batch_tx, _batch_rx) = mpsc::channel(4);
        let (storage_tx, _storage_rx) = mpsc::channel(16);
        let processor = Arc::new(Processor::new(
            Arc::new(test_config()),
            Arc::new(Catalog::new(Vec::new())),
            Arc::new(EventHub::new(16)),
            batch_tx,
            storage_tx,
            stats.clone(),
            Arc::new(crate::forfeit::ForfeitBlocklist::disabled()),
            0,
        ));
        let watchdog = StallWatchdog {
            idle: Duration::from_millis(50),
            http: Arc::new(HttpRpc::new(http_url).unwrap()),
            max_window: 1000,
        };
        let (any_connected, _) = watch::channel(false);
        let (_shutdown_tx, shutdown) = watch::channel(false);
        let result = tokio::time::timeout(
            Duration::from_millis(1500),
            run_live_session(
                "wss[0]",
                &wss,
                16,
                &processor,
                &stats,
                &watchdog,
                &any_connected,
                None,
                shutdown,
            ),
        )
        .await;
        wss_server.abort();
        http_server.abort();
        // Timing out means the session was (correctly) kept alive.
        (result.unwrap_or(Ok(())), stats)
    }

    /// An endpoint that accepts the TCP connection and never answers the
    /// WebSocket handshake, and one that completes the handshake but never
    /// acknowledges `eth_subscribe`. Both used to park the racer forever.
    #[tokio::test]
    async fn subscribe_times_out_on_a_mute_endpoint() {
        let step = Duration::from_millis(150);
        let (_tx, shutdown) = watch::channel(false);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mute_tcp = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream); // keep it open, say nothing
            }
        });
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            subscribe_live(
                &format!("ws://{address}"),
                16,
                Arc::new(Stats::default()),
                shutdown.clone(),
                step,
            ),
        )
        .await
        .expect("must not hang in the handshake");
        assert!(
            matches!(&result, Err(RpcError::Timeout(what)) if what.contains("connecting")),
            "{:?}",
            result.err()
        );
        mute_tcp.abort();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mute_ws = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                    while ws.next().await.is_some() {} // read, never acknowledge
                });
            }
        });
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            subscribe_live(
                &format!("ws://{address}"),
                16,
                Arc::new(Stats::default()),
                shutdown,
                step,
            ),
        )
        .await
        .expect("must not hang waiting for the acknowledgement");
        assert!(
            matches!(&result, Err(RpcError::Timeout(what)) if what.contains("acknowledgement")),
            "{:?}",
            result.err()
        );
        mute_ws.abort();
    }

    #[test]
    fn gap_fill_rereads_the_last_seen_block() {
        assert_eq!(gap_fill_start(0), None);
        assert_eq!(gap_fill_start(100), Some(100));
    }

    /// The HTTP side fails right after the reconnect (same provider as the
    /// WSS that just dropped), then recovers. The gap must still be filled,
    /// starting at the last seen block itself.
    #[tokio::test]
    async fn gap_fill_retries_until_the_http_side_recovers() {
        use axum::extract::State;
        use std::sync::atomic::AtomicUsize;

        use crate::{config::test_config, enrichment::Catalog, hub::EventHub};

        #[derive(Clone, Default)]
        struct Mock {
            requests: Arc<AtomicUsize>,
            windows: Calls,
        }
        async fn rpc(
            State(mock): State<Mock>,
            Json(request): Json<Value>,
        ) -> axum::response::Response {
            use axum::response::IntoResponse;
            // First two requests fail: one at the transport level, one as an
            // RPC error body.
            match mock.requests.fetch_add(1, Ordering::SeqCst) {
                0 => return axum::http::StatusCode::BAD_GATEWAY.into_response(),
                1 => {
                    return Json(
                        json!({"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"busy"}}),
                    )
                    .into_response();
                }
                _ => {}
            }
            let result = match request["method"].as_str().unwrap() {
                "eth_blockNumber" => json!("0x78"), // 120
                "eth_getLogs" => {
                    let hex = |key: &str| {
                        u64::from_str_radix(
                            request["params"][0][key]
                                .as_str()
                                .unwrap()
                                .trim_start_matches("0x"),
                            16,
                        )
                        .unwrap()
                    };
                    mock.windows
                        .lock()
                        .unwrap()
                        .push((hex("fromBlock"), hex("toBlock")));
                    json!([])
                }
                other => panic!("unexpected method {other}"),
            };
            Json(json!({"jsonrpc": "2.0", "id": 1, "result": result})).into_response()
        }

        let mock = Mock::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route("/", post(rpc)).with_state(mock.clone());
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let http = HttpRpc::new(format!("http://{address}")).unwrap();
        let (batch_tx, _batch_rx) = mpsc::channel(4);
        let (storage_tx, _storage_rx) = mpsc::channel(16);
        let processor = Arc::new(Processor::new(
            Arc::new(test_config()),
            Arc::new(Catalog::new(Vec::new())),
            Arc::new(EventHub::new(16)),
            batch_tx,
            storage_tx,
            Arc::new(Stats::default()),
            Arc::new(crate::forfeit::ForfeitBlocklist::disabled()),
            0,
        ));
        let delays = [Duration::from_millis(5); 3];

        let head = gap_fill(&http, &processor, 100, 1000, "gapfill[0]", &delays)
            .await
            .expect("recovers within the retries");
        assert_eq!(head, 120);
        assert_eq!(*mock.windows.lock().unwrap(), vec![(100, 120)]);

        // Out of retries: the error is reported, not swallowed.
        mock.requests.store(0, Ordering::SeqCst);
        assert!(
            gap_fill(&http, &processor, 100, 1000, "gapfill[0]", &delays[..1])
                .await
                .is_err()
        );
        server.abort();
    }

    /// The provider's API key is part of the HTTP RPC URL; a transport error
    /// must not carry the URL into the journal.
    #[tokio::test]
    async fn http_errors_do_not_leak_the_rpc_url() {
        // Nothing listens on port 1.
        let http = HttpRpc::new("http://127.0.0.1:1/SECRET-API-KEY".into()).unwrap();
        let error = http.latest_block().await.unwrap_err();
        let text = format!("{error} / {error:?}");
        assert!(!text.contains("SECRET-API-KEY"), "{text}");
    }

    #[tokio::test]
    async fn silent_session_with_logs_on_chain_is_torn_down_as_stalled() {
        let (result, _) = run_silent_session(1).await;
        assert!(
            matches!(result, Err(RpcError::Stalled { logs: 1, .. })),
            "expected a stall, got {result:?}"
        );
    }

    #[tokio::test]
    async fn silent_session_on_a_quiet_chain_is_kept() {
        let (result, _) = run_silent_session(0).await;
        assert!(
            result.is_ok(),
            "quiet chain must not force a reconnect: {result:?}"
        );
    }
}
