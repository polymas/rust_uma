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
    decode::{RpcLog, TOPIC_DISPUTE_PRICE, TOPIC_PROPOSE_PRICE, parse_hex_u64},
    pipeline::Processor,
    stats::Stats,
    storage::Storage,
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
    let mut backoff = Duration::from_secs(1);
    loop {
        if *shutdown.borrow() {
            break;
        }
        match run_session(
            config.clone(),
            storage.clone(),
            &http,
            processor.clone(),
            stats.clone(),
            shutdown.clone(),
        )
        .await
        {
            Ok(()) if *shutdown.borrow() => break,
            Ok(()) => warn!("Polygon subscription ended; reconnecting"),
            Err(error) => warn!(%error, "Polygon subscription session failed; reconnecting"),
        }
        stats.rpc_connected.store(false, Ordering::Relaxed);
        Stats::increment(&stats.rpc_reconnects);
        tokio::select! {
            _ = shutdown.changed() => if *shutdown.borrow() { break; },
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

async fn run_session(
    config: Arc<Config>,
    storage: Storage,
    http: &HttpRpc,
    processor: Arc<Processor>,
    stats: Arc<Stats>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), RpcError> {
    let (mut live, reader) = subscribe_live(config.clone(), shutdown.clone()).await?;
    stats.rpc_connected.store(true, Ordering::Relaxed);
    info!("Polygon signal subscription connected");

    let subscribed_head = http.latest_block().await?;
    let checkpoint = storage.load_checkpoint().unwrap_or_default();
    let from = if checkpoint > 0 {
        Some(checkpoint.saturating_add(1))
    } else if let Some(start) = config.start_block {
        Some(start)
    } else {
        processor.checkpoint(subscribed_head);
        None
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
            let mut logs = http
                .get_logs(cursor, end, &config.contract_addresses)
                .await?;
            logs.sort_unstable_by_key(|log| {
                (
                    parse_hex_u64(&log.block_number, "blockNumber").unwrap_or_default(),
                    parse_hex_u64(&log.log_index, "logIndex").unwrap_or_default(),
                )
            });
            for log in logs {
                processor.process(log, now_us()).await;
            }
            processor.checkpoint(end);
            cursor = end.saturating_add(1);
        }
    }

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() { break; }
            }
            message = live.recv() => {
                let Some(log) = message else { return Err(RpcError::Closed); };
                let block = parse_hex_u64(&log.block_number, "blockNumber").unwrap_or_default();
                processor.process(log, now_us()).await;
                if block > 0 {
                    processor.checkpoint(block);
                }
            }
        }
    }
    reader.abort();
    stats.rpc_connected.store(false, Ordering::Relaxed);
    Ok(())
}

async fn subscribe_live(
    config: Arc<Config>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(mpsc::Receiver<RpcLog>, JoinHandle<()>), RpcError> {
    let (mut socket, _) = connect_async(config.polygon_wss_url.as_str()).await?;
    let address: Value = if config.contract_addresses.len() == 1 {
        json!(config.contract_addresses[0])
    } else {
        json!(config.contract_addresses)
    };
    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": ["logs", {
            "address": address,
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

    let (tx, rx) = mpsc::channel(config.live_buffer.max(1));
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

    async fn get_logs(
        &self,
        from: u64,
        to: u64,
        addresses: &[String],
    ) -> Result<Vec<RpcLog>, RpcError> {
        let address = if addresses.len() == 1 {
            json!(addresses[0])
        } else {
            json!(addresses)
        };
        self.call(
            "eth_getLogs",
            json!([{
                "fromBlock": format!("0x{from:x}"),
                "toBlock": format!("0x{to:x}"),
                "address": address,
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
