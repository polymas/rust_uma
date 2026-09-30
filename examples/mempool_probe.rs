//! 实验进程：监听 mempool 里的 pending ProposePrice/DisputePrice 交易，测它比
//! `eth_subscribe logs`（生产 rust-uma 用的那条流）早多少、命中率多少。
//!
//! 不接生产热路径，只读 RPC、写一个 ndjson 文件 + 每分钟一行汇总日志。
//! 同一个 WSS 端点开两条连接：
//! - `newPendingTransactions`（优先带 `true` 拿完整交易；服务商不支持就退回只要 hash）
//! - `logs`，topic 过滤和生产完全一致
//!
//! 所有 pending hash 都记首见时刻，logs 到达时按 tx hash 对上就是命中，
//! `lead = log 到达 - pending 首见`。完整交易模式下额外看 input 里有没有
//! OOv2 的 propose/dispute 选择器（直接调用 / 被 multicall 之类包在里面）。
//!
//! 环境变量：
//! - `PROBE_WSS`：可选，默认取 `WSS_RPC_LIST` 第一个，再退到 `WSS_RPC` / `POLYGON_WSS_URL`
//! - `PROBE_DURATION_SECONDS`：跑多久自动退出，默认 600（pending 流量大，先控时长看配额）
//! - `PROBE_OUT`：ndjson 输出路径，默认 `./mempool_probe.ndjson`

use std::{
    collections::{HashMap, HashSet},
    fs::OpenOptions,
    io::Write,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::{SinkExt, StreamExt};
use rust_uma::uma::events::{TOPIC_DISPUTE_PRICE, TOPIC_PROPOSE_PRICE};
use serde_json::{Value, json};
use sha3::{Digest, Keccak256};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

const SIGNATURES: &[(&str, &str)] = &[
    (
        "proposePrice",
        "proposePrice(address,bytes32,uint256,bytes,int256)",
    ),
    (
        "proposePriceFor",
        "proposePriceFor(address,address,bytes32,uint256,bytes,int256)",
    ),
    (
        "disputePrice",
        "disputePrice(address,bytes32,uint256,bytes)",
    ),
    (
        "disputePriceFor",
        "disputePriceFor(address,address,bytes32,uint256,bytes)",
    ),
];

/// pending 首见记录保留多久；Polygon 上正常交易几秒内就进块，10 分钟足够。
const PENDING_TTL_US: u64 = 600_000_000;
/// 选择器命中的 pending 交易超过这么久还没等到 log，算作"没上链"（失败/被抢/丢弃）。
const NO_LOG_AFTER_US: u64 = 120_000_000;

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

struct Seen {
    at_us: u64,
    func: Option<&'static str>,
    embedded: bool,
    to: Option<String>,
}

#[derive(Default)]
struct State {
    pending: HashMap<String, Seen>,
    logged_tx: HashSet<String>,
    mode: &'static str,
    pending_msgs: u64,
    selector_direct: u64,
    selector_embedded: u64,
    selector_no_log: u64,
    late_pending: u64,
    log_txs: u64,
    hits: u64,
    leads_us: Vec<i64>,
    window_pending_msgs: u64,
    started_us: u64,
}

struct Probe {
    state: Mutex<State>,
    out: Mutex<std::fs::File>,
    selectors: Vec<(&'static str, String)>,
}

impl Probe {
    fn emit(&self, record: Value) {
        let mut out = self.out.lock().unwrap();
        let _ = writeln!(out, "{record}");
    }

    fn classify(&self, input: &str) -> (Option<&'static str>, bool) {
        let body = input.trim_start_matches("0x");
        for (name, selector) in &self.selectors {
            if body.starts_with(selector.as_str()) {
                return (Some(name), false);
            }
        }
        for (name, selector) in &self.selectors {
            if body.contains(selector.as_str()) {
                return (Some(name), true);
            }
        }
        (None, false)
    }

    fn on_pending(&self, tx: &Value, at_us: u64) {
        let (hash, func, embedded, to, from) = match tx {
            Value::String(hash) => (hash.to_lowercase(), None, false, None, None),
            Value::Object(obj) => {
                let Some(hash) = obj.get("hash").and_then(Value::as_str) else {
                    return;
                };
                let input = obj
                    .get("input")
                    .or_else(|| obj.get("data"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let (func, embedded) = self.classify(input);
                let to = obj.get("to").and_then(Value::as_str).map(str::to_lowercase);
                let from = obj
                    .get("from")
                    .and_then(Value::as_str)
                    .map(str::to_lowercase);
                (hash.to_lowercase(), func, embedded, to, from)
            }
            _ => return,
        };
        let mut state = self.state.lock().unwrap();
        state.pending_msgs += 1;
        state.window_pending_msgs += 1;
        if state.logged_tx.contains(&hash) {
            state.late_pending += 1;
            return;
        }
        if state.pending.contains_key(&hash) {
            return;
        }
        if func.is_some() {
            if embedded {
                state.selector_embedded += 1;
            } else {
                state.selector_direct += 1;
            }
            drop(state);
            self.emit(json!({
                "kind": "pending_match", "hash": hash, "func": func, "embedded": embedded,
                "to": to, "from": from, "seen_us": at_us,
            }));
            state = self.state.lock().unwrap();
        }
        state.pending.insert(
            hash,
            Seen {
                at_us,
                func,
                embedded,
                to,
            },
        );
    }

    fn on_log(&self, log: &Value, at_us: u64) {
        if log.get("removed").and_then(Value::as_bool) == Some(true) {
            return;
        }
        let Some(hash) = log.get("transactionHash").and_then(Value::as_str) else {
            return;
        };
        let hash = hash.to_lowercase();
        let topic0 = log
            .get("topics")
            .and_then(|t| t.get(0))
            .and_then(Value::as_str)
            .unwrap_or("");
        let event = if topic0.eq_ignore_ascii_case(TOPIC_PROPOSE_PRICE) {
            "ProposePrice"
        } else if topic0.eq_ignore_ascii_case(TOPIC_DISPUTE_PRICE) {
            "DisputePrice"
        } else {
            "other"
        };
        let block = log
            .get("blockNumber")
            .and_then(Value::as_str)
            .and_then(|b| u64::from_str_radix(b.trim_start_matches("0x"), 16).ok());
        let emitter = log.get("address").and_then(Value::as_str);

        let mut state = self.state.lock().unwrap();
        if !state.logged_tx.insert(hash.clone()) {
            return;
        }
        state.log_txs += 1;
        let seen = state.pending.get(&hash);
        let record = match seen {
            Some(seen) => {
                let lead = at_us as i64 - seen.at_us as i64;
                let record = json!({
                    "kind": "log", "hash": hash, "event": event, "block": block,
                    "emitter": emitter, "log_us": at_us, "pending_us": seen.at_us,
                    "lead_ms": lead as f64 / 1000.0, "func": seen.func,
                    "embedded": seen.embedded, "to": seen.to,
                });
                state.hits += 1;
                state.leads_us.push(lead);
                record
            }
            None => json!({
                "kind": "log", "hash": hash, "event": event, "block": block,
                "emitter": emitter, "log_us": at_us, "pending_us": null,
            }),
        };
        drop(state);
        self.emit(record);
    }

    fn sweep(&self) {
        let now = now_us();
        let mut expired = Vec::new();
        let mut state = self.state.lock().unwrap();
        let logged = std::mem::take(&mut state.logged_tx);
        state.pending.retain(|hash, seen| {
            let age = now.saturating_sub(seen.at_us);
            if seen.func.is_some() && age > NO_LOG_AFTER_US && !logged.contains(hash) {
                expired.push((hash.clone(), seen.func, seen.to.clone(), seen.at_us));
                return false;
            }
            age < PENDING_TTL_US
        });
        state.logged_tx = logged;
        state.selector_no_log += expired.len() as u64;
        // logged_tx 只用来去重和判 late_pending，保留规模有限即可。
        if state.logged_tx.len() > 200_000 {
            state.logged_tx.clear();
        }
        drop(state);
        for (hash, func, to, seen_us) in expired {
            self.emit(json!({
                "kind": "pending_no_log", "hash": hash, "func": func, "to": to, "seen_us": seen_us,
            }));
        }
    }

    fn summary(&self, window_secs: f64) -> Value {
        let mut state = self.state.lock().unwrap();
        let mut leads = state.leads_us.clone();
        leads.sort_unstable();
        let pct = |p: f64| -> Option<f64> {
            if leads.is_empty() {
                return None;
            }
            let idx = ((leads.len() - 1) as f64 * p).round() as usize;
            Some(leads[idx] as f64 / 1000.0)
        };
        let rate = state.window_pending_msgs as f64 / window_secs;
        state.window_pending_msgs = 0;
        json!({
            "kind": "summary", "at_us": now_us(), "mode": state.mode,
            "pending_msgs": state.pending_msgs, "pending_per_sec": (rate * 10.0).round() / 10.0,
            "pending_per_sec_overall": (state.pending_msgs as f64 / ((now_us() - state.started_us) as f64 / 1e6).max(1.0) * 10.0).round() / 10.0,
            "pending_tracked": state.pending.len(),
            "selector_direct": state.selector_direct, "selector_embedded": state.selector_embedded,
            "selector_no_log": state.selector_no_log, "late_pending": state.late_pending,
            "log_txs": state.log_txs, "hits": state.hits,
            "hit_rate": if state.log_txs > 0 { state.hits as f64 / state.log_txs as f64 } else { 0.0 },
            "lead_ms_p10": pct(0.10), "lead_ms_p50": pct(0.50), "lead_ms_p90": pct(0.90),
            "lead_ms_min": pct(0.0), "lead_ms_max": pct(1.0),
        })
    }
}

async fn subscribe(
    url: &str,
    params: Value,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    String,
> {
    let (mut socket, _) = connect_async(url).await.map_err(|e| e.to_string())?;
    let request = json!({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": params});
    socket
        .send(Message::Text(request.to_string().into()))
        .await
        .map_err(|e| e.to_string())?;
    loop {
        let message = socket
            .next()
            .await
            .ok_or("closed before subscribe reply")?
            .map_err(|e| e.to_string())?;
        if let Message::Text(text) = message {
            let reply: Value = serde_json::from_str(text.as_ref()).map_err(|e| e.to_string())?;
            if let Some(error) = reply.get("error") {
                return Err(format!("subscribe rejected: {error}"));
            }
            if reply.get("id") == Some(&json!(1)) {
                return Ok(socket);
            }
        }
    }
}

enum Stream {
    Pending,
    Logs,
}

async fn run_stream(url: String, kind: Stream, probe: Arc<Probe>) {
    let mut full_tx = true;
    loop {
        let params = match kind {
            Stream::Pending if full_tx => json!(["newPendingTransactions", true]),
            Stream::Pending => json!(["newPendingTransactions"]),
            Stream::Logs => {
                json!(["logs", {"topics": [[TOPIC_PROPOSE_PRICE, TOPIC_DISPUTE_PRICE]]}])
            }
        };
        let label = match kind {
            Stream::Pending => "pending",
            Stream::Logs => "logs",
        };
        let mut socket = match subscribe(&url, params).await {
            Ok(socket) => {
                if let Stream::Pending = kind {
                    probe.state.lock().unwrap().mode =
                        if full_tx { "full_tx" } else { "hash_only" };
                }
                info!(stream = label, full_tx, "subscribed");
                socket
            }
            Err(error) => {
                warn!(stream = label, full_tx, %error, "subscribe failed");
                if matches!(kind, Stream::Pending) && full_tx && error.contains("rejected") {
                    full_tx = false;
                    continue;
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        while let Some(message) = socket.next().await {
            let at_us = now_us();
            match message {
                Ok(Message::Text(text)) => {
                    let Ok(value) = serde_json::from_str::<Value>(text.as_ref()) else {
                        continue;
                    };
                    let Some(result) = value.pointer("/params/result") else {
                        continue;
                    };
                    match kind {
                        Stream::Pending => probe.on_pending(result, at_us),
                        Stream::Logs => probe.on_log(result, at_us),
                    }
                }
                Ok(Message::Ping(payload)) => {
                    if socket.send(Message::Pong(payload)).await.is_err() {
                        break;
                    }
                }
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
        warn!(stream = label, "stream closed, reconnecting");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
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

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_target(false).init();
    let url = wss_url().expect("no WSS endpoint: set PROBE_WSS or WSS_RPC_LIST / WSS_RPC");
    let duration: u64 = std::env::var("PROBE_DURATION_SECONDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(600);
    let out_path =
        std::env::var("PROBE_OUT").unwrap_or_else(|_| "mempool_probe.ndjson".to_string());
    let out = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&out_path)
        .expect("open PROBE_OUT");
    let selectors = SIGNATURES
        .iter()
        .map(|(name, sig)| (*name, hex::encode(&Keccak256::digest(sig.as_bytes())[..4])))
        .collect::<Vec<_>>();
    info!(?selectors, duration, out = %out_path, "mempool probe starting");

    let probe = Arc::new(Probe {
        state: Mutex::new(State {
            mode: "connecting",
            started_us: now_us(),
            ..State::default()
        }),
        out: Mutex::new(out),
        selectors,
    });
    tokio::spawn(run_stream(url.clone(), Stream::Pending, probe.clone()));
    tokio::spawn(run_stream(url, Stream::Logs, probe.clone()));

    let started = tokio::time::Instant::now();
    let deadline = started + Duration::from_secs(duration);
    let mut tick = tokio::time::interval(Duration::from_secs(60));
    tick.tick().await;
    loop {
        tokio::select! {
            _ = tick.tick() => {
                probe.sweep();
                let summary = probe.summary(60.0);
                info!("{summary}");
                probe.emit(summary);
            }
            _ = tokio::time::sleep_until(deadline) => break,
        }
    }
    probe.sweep();
    let summary = probe.summary(started.elapsed().as_secs_f64().max(1.0));
    info!(final = true, "{summary}");
    probe.emit(summary);
}
