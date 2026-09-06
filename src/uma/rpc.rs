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
    let delay = if session_lasted >= HEALTHY_SESSION {
        RECONNECT_BACKOFF_INITIAL
    } else {
        current
    };
    (delay, (delay * 2).min(RECONNECT_BACKOFF_MAX))
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
        match run_live_session(
            &source,
            &url,
            config.live_buffer,
            &processor,
            &stats,
            &any_connected,
            gap_fill,
            shutdown.clone(),
        )
        .await
        {
            Ok(()) if *shutdown.borrow() => break,
            Ok(()) => warn!(source, "Polygon live subscription ended; reconnecting"),
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
        let from = self
            .stats
            .latest_block
            .load(Ordering::Relaxed)
            .saturating_add(1);
        if from <= 1 {
            return; // nothing observed yet; the startup backfill owns the range
        }
        let source = format!("gapfill[{}]", self.index);
        tokio::spawn(async move {
            let head = match self.http.latest_block().await {
                Ok(head) => head,
                Err(error) => {
                    warn!(%error, source, "gap-fill after reconnect: cannot read head");
                    return;
                }
            };
            if from > head {
                return;
            }
            match backfill_range(
                &self.http,
                &self.processor,
                from,
                head,
                self.config.backfill_batch_blocks,
                &source,
            )
            .await
            {
                Ok(()) => info!(from, to = head, source, "gap-fill after reconnect complete"),
                Err(error) => {
                    warn!(%error, from, to = head, source, "gap-fill after reconnect failed")
                }
            }
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_live_session(
    source: &str,
    url: &str,
    live_buffer: usize,
    processor: &Arc<Processor>,
    stats: &Arc<Stats>,
    any_connected: &watch::Sender<bool>,
    gap_fill: Option<GapFill>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), RpcError> {
    let (mut live, reader) =
        subscribe_live(url, live_buffer, stats.clone(), shutdown.clone()).await?;
    mark_source_connected(stats);
    any_connected.send_replace(true);
    info!(source, "Polygon signal subscription connected");
    // Subscribed first, then catch up: the new subscription bounds the gap
    // from above, so nothing between "last seen" and "now" can slip past.
    if let Some(gap_fill) = gap_fill {
        gap_fill.spawn();
    }

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() { break; }
            }
            message = live.recv() => {
                let Some(log) = message else { return Err(RpcError::Closed); };
                let block = parse_hex_u64(&log.block_number, "blockNumber").unwrap_or_default();
                processor.process(log, now_us(), source).await;
                if block > 0 {
                    processor.checkpoint(block);
                }
            }
        }
    }
    reader.abort();
    Ok(())
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
        Some(checkpoint.saturating_add(1))
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

async fn subscribe_live(
    url: &str,
    live_buffer: usize,
    stats: Arc<Stats>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(mpsc::Receiver<RpcLog>, JoinHandle<()>), RpcError> {
    let (mut socket, _) = connect_async(url).await?;
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
                    break;
                }
            }
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
            Message::Close(_) => return Err(RpcError::Closed),
            _ => {}
        }
    }

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
            .await?;
        let body: RpcResponse<T> = response.json().await?;
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
        // A long healthy session drops straight back to the initial delay,
        // regardless of how high the climb had gotten.
        let (delay, next) = reconnect_backoff(capped, HEALTHY_SESSION);
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
}
