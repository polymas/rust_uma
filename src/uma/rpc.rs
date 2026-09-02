use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
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
        Ok(client) => client,
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
                config.live_buffer,
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

/// One racer's forever-reconnecting live subscription.
async fn live_worker(
    index: usize,
    url: String,
    live_buffer: usize,
    processor: Arc<Processor>,
    stats: Arc<Stats>,
    any_connected: watch::Sender<bool>,
    mut shutdown: watch::Receiver<bool>,
) {
    let source = format!("wss[{index}]");
    let mut backoff = Duration::from_secs(1);
    loop {
        if *shutdown.borrow() {
            break;
        }
        match run_live_session(
            &source,
            &url,
            live_buffer,
            &processor,
            &stats,
            &any_connected,
            shutdown.clone(),
        )
        .await
        {
            Ok(()) if *shutdown.borrow() => break,
            Ok(()) => warn!(source, "Polygon live subscription ended; reconnecting"),
            Err(error) => warn!(%error, source, "Polygon live subscription failed; reconnecting"),
        }
        mark_source_disconnected(&stats);
        Stats::increment(&stats.rpc_reconnects);
        tokio::select! {
            _ = shutdown.changed() => if *shutdown.borrow() { break; },
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

async fn run_live_session(
    source: &str,
    url: &str,
    live_buffer: usize,
    processor: &Arc<Processor>,
    stats: &Arc<Stats>,
    any_connected: &watch::Sender<bool>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), RpcError> {
    let (mut live, reader) = subscribe_live(url, live_buffer, shutdown.clone()).await?;
    mark_source_connected(stats);
    any_connected.send_replace(true);
    info!(source, "Polygon signal subscription connected");

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

    if let Some(mut cursor) = from
        && cursor <= subscribed_head
    {
        info!(
            from = cursor,
            to = subscribed_head,
            "starting signal backfill after live subscription"
        );
        while cursor <= subscribed_head {
            let end = cursor
                .saturating_add(config.backfill_batch_blocks.max(1) - 1)
                .min(subscribed_head);
            let mut logs = http.get_logs(cursor, end).await?;
            logs.sort_unstable_by_key(|log| {
                (
                    parse_hex_u64(&log.block_number, "blockNumber").unwrap_or_default(),
                    parse_hex_u64(&log.log_index, "logIndex").unwrap_or_default(),
                )
            });
            for log in logs {
                processor.process(log, now_us(), "backfill").await;
            }
            processor.checkpoint(end);
            cursor = end.saturating_add(1);
        }
    }
    Ok(())
}

async fn subscribe_live(
    url: &str,
    live_buffer: usize,
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
    socket
        .send(Message::Text(request.to_string().into()))
        .await?;
    loop {
        let message = socket.next().await.ok_or(RpcError::Closed)??;
        match message {
            Message::Text(text) => {
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
}
