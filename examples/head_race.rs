//! 实验进程：记录 RPC 服务商（生产用的 getblock）推新块和推 ProposePrice/DisputePrice log 的时刻，
//! 和 polytest 上 P2P 轻量节点收到同一块的时刻做对比。
//!
//! 同一个 WSS 端点开两条连接：`newHeads`、`logs`（topic 过滤与生产一致）。只写 ndjson，不接生产热路径。
//!
//! 环境变量：`PROBE_WSS`（默认取 `WSS_RPC_LIST` 第一个 / `WSS_RPC`）、`PROBE_DURATION_SECONDS`（默认 2400）、
//! `PROBE_OUT`（默认 `./head_race.ndjson`）。

use std::{
    fs::OpenOptions,
    io::Write,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::{SinkExt, StreamExt};
use rust_uma::uma::events::{TOPIC_DISPUTE_PRICE, TOPIC_PROPOSE_PRICE};
use serde_json::{Value, json};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

fn wss_url() -> Option<String> {
    let nonempty = |key: &str| std::env::var(key).ok().filter(|v| !v.trim().is_empty());
    nonempty("PROBE_WSS")
        .or_else(|| {
            nonempty("WSS_RPC_LIST").and_then(|list| {
                list.split(',')
                    .map(str::trim)
                    .find(|s| !s.is_empty())
                    .map(String::from)
            })
        })
        .or_else(|| nonempty("WSS_RPC"))
        .or_else(|| nonempty("POLYGON_WSS_URL"))
}

async fn run(url: String, params: Value, kind: &'static str, out: Arc<Mutex<std::fs::File>>) {
    loop {
        let (mut socket, _) = match connect_async(&url).await {
            Ok(s) => s,
            Err(error) => {
                warn!(kind, %error, "connect failed");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        let req = json!({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": params});
        if socket
            .send(Message::Text(req.to_string().into()))
            .await
            .is_err()
        {
            continue;
        }
        info!(kind, "subscribed");
        while let Some(msg) = socket.next().await {
            let at = now_us();
            match msg {
                Ok(Message::Text(text)) => {
                    let Ok(v) = serde_json::from_str::<Value>(text.as_ref()) else {
                        continue;
                    };
                    let Some(r) = v.pointer("/params/result") else {
                        continue;
                    };
                    let rec = match kind {
                        "head" => {
                            json!({"kind": "head", "us": at, "hash": r.get("hash"), "number": r.get("number")})
                        }
                        _ => {
                            json!({"kind": "log", "us": at, "hash": r.get("blockHash"), "number": r.get("blockNumber"),
                                    "tx": r.get("transactionHash"), "log_index": r.get("logIndex")})
                        }
                    };
                    let mut f = out.lock().unwrap();
                    let _ = writeln!(f, "{rec}");
                }
                Ok(Message::Ping(p)) => {
                    if socket.send(Message::Pong(p)).await.is_err() {
                        break;
                    }
                }
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
        warn!(kind, "stream closed, reconnecting");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_target(false).init();
    let url = wss_url().expect("no WSS endpoint");
    let duration: u64 = std::env::var("PROBE_DURATION_SECONDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2400);
    let path = std::env::var("PROBE_OUT").unwrap_or_else(|_| "head_race.ndjson".into());
    let out = Arc::new(Mutex::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("open out"),
    ));
    info!(duration, out = %path, "head race starting");
    tokio::spawn(run(url.clone(), json!(["newHeads"]), "head", out.clone()));
    tokio::spawn(run(
        url,
        json!(["logs", {"topics": [[TOPIC_PROPOSE_PRICE, TOPIC_DISPUTE_PRICE]]}]),
        "log",
        out,
    ));
    tokio::time::sleep(Duration::from_secs(duration)).await;
    info!("done");
}
