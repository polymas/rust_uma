//! mempool-uma 的两路输入：
//! - `run_pending_feed`：从本机 P2P 轻量节点（Go，`-feed-listen`）读 pending 交易行，
//!   解析 calldata → 过滤 → 合成日志 → `Processor::process`（热路径，只查内存）。
//! - `run_confirm_feed`：链上确认的 ProposePrice / DisputePrice（启动时 HTTP 回补最近 N 小时 +
//!   WSS 日志订阅），只用来更新去重状态和提交地址打分，不推给下游。

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use sha3::{Digest, Keccak256};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpStream,
    sync::watch,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use super::{
    calldata::{CallKind, RequestKey, decode_input, synth_log},
    gate::{Gate, Verdict},
};
use crate::{
    pipeline::Processor,
    stats::Stats,
    uma::events::{RpcLog, TOPIC_DISPUTE_PRICE, TOPIC_PROPOSE_PRICE},
};

fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

/// P2P 节点推来的一行（与 Go 探针 `p2p_match` 记录同字段）。
#[derive(Deserialize)]
struct PendingLine {
    kind: String,
    hash: String,
    from: String,
    #[serde(default)]
    to: String,
    #[serde(default)]
    input: String,
    #[serde(default)]
    first_us: u64,
    #[serde(default)]
    announce_us: u64,
    #[serde(default)]
    tip_gwei: u64,
}

fn addr20(s: &str) -> Option<[u8; 20]> {
    hex::decode(s.trim_start_matches("0x"))
        .ok()?
        .try_into()
        .ok()
}

pub async fn run_pending_feed(
    addr: String,
    processor: Arc<Processor>,
    gate: Arc<Mutex<Gate>>,
    stats: Arc<Stats>,
    mut shutdown: watch::Receiver<bool>,
) {
    use std::sync::atomic::Ordering;
    // /healthz 的 rpc_connected 在 mempool-uma 里表示"与 P2P 节点的 pending 推送连着"。
    let set_connected = |on: bool| {
        stats.rpc_connected.store(on, Ordering::Relaxed);
        stats
            .rpc_sources_connected
            .store(u64::from(on), Ordering::Relaxed);
    };
    loop {
        let stream = tokio::select! {
            _ = shutdown.changed() => return,
            s = TcpStream::connect(&addr) => s,
        };
        let Ok(stream) = stream else {
            warn!(%addr, "pending feed: connect failed, retrying");
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        };
        let _ = stream.set_nodelay(true);
        info!(%addr, "pending feed connected");
        set_connected(true);
        let mut lines = BufReader::new(stream).lines();
        loop {
            let line = tokio::select! {
                _ = shutdown.changed() => return,
                l = lines.next_line() => l,
            };
            let Ok(Some(line)) = line else { break };
            let Ok(p) = serde_json::from_str::<PendingLine>(&line) else {
                continue;
            };
            if p.kind != "p2p_match" || p.input.is_empty() {
                continue;
            }
            let Ok(input) = hex::decode(p.input.trim_start_matches("0x")) else {
                continue;
            };
            let Some(from) = addr20(&p.from) else {
                continue;
            };
            let calls = decode_input(&input);
            if calls.is_empty() {
                continue;
            }
            let first = if p.first_us > 0 {
                p.first_us
            } else {
                p.announce_us
            };
            let emit: Vec<_> = {
                let mut g = gate.lock().unwrap_or_else(|e| e.into_inner());
                calls
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| {
                        g.check(c, from, p.tip_gwei, &p.hash, now_us()) == Verdict::Emit
                    })
                    .map(|(i, c)| synth_log(c, &p.hash, i, &p.to, &from))
                    .collect()
            };
            for log in emit {
                processor.process(log, first, "mempool").await;
            }
        }
        set_connected(false);
        warn!("pending feed disconnected, reconnecting");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// 从链上日志取出请求键和报价（DisputePrice 的报价是被争议的那个）。
pub fn parse_onchain(log: &RpcLog) -> Option<(CallKind, RequestKey, [u8; 32])> {
    let kind = match log.topics.first()?.to_ascii_lowercase().as_str() {
        t if t == TOPIC_PROPOSE_PRICE => CallKind::Propose,
        t if t == TOPIC_DISPUTE_PRICE => CallKind::Dispute,
        _ => return None,
    };
    let requester: [u8; 20] = hex::decode(log.topics.get(1)?.trim_start_matches("0x"))
        .ok()?
        .get(12..)?
        .try_into()
        .ok()?;
    let data = hex::decode(log.data.trim_start_matches("0x")).ok()?;
    let w = |i: usize| -> Option<[u8; 32]> { data.get(i * 32..(i + 1) * 32)?.try_into().ok() };
    let ts = w(1)?;
    let off = usize::try_from(u64::from_be_bytes(w(2)?[24..].try_into().ok()?)).ok()?;
    let len = usize::try_from(u64::from_be_bytes(
        data.get(off + 24..off + 32)?.try_into().ok()?,
    ))
    .ok()?;
    let anc = data.get(off + 32..off + 32 + len)?;
    let key = RequestKey {
        requester,
        question_id: Keccak256::digest(anc).into(),
        timestamp: u64::from_be_bytes(ts[24..].try_into().ok()?),
    };
    Some((kind, key, w(3)?))
}

fn apply(gate: &Mutex<Gate>, logs: &[RpcLog]) -> usize {
    let mut g = gate.lock().unwrap_or_else(|e| e.into_inner());
    let now = now_us();
    let mut n = 0;
    for log in logs.iter().filter(|l| !l.removed) {
        if let Some((kind, key, price)) = parse_onchain(log) {
            g.on_confirmed(
                kind,
                key,
                price,
                &log.transaction_hash.to_ascii_lowercase(),
                now,
            );
            n += 1;
        }
    }
    n
}

async fn http_call(
    client: &reqwest::Client,
    url: &str,
    method: &str,
    params: Value,
) -> Option<Value> {
    let r: Value = client
        .post(url)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;
    r.get("result").cloned()
}

/// 启动回补 + 实时订阅。回补范围按 1.5 秒一个块估算，失败的块段跳过（只影响去重的完整性，
/// 不影响推送；漏掉的已提案请求最坏情况是被推一次"迟到"信号）。
pub async fn run_confirm_feed(
    http_url: String,
    wss_url: String,
    backfill_hours: u64,
    chunk: u64,
    gate: Arc<Mutex<Gate>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .expect("http client");
    if let Some(head) = http_call(&client, &http_url, "eth_blockNumber", json!([]))
        .await
        .and_then(|v| {
            v.as_str()
                .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        })
    {
        let from = head.saturating_sub(backfill_hours * 2400);
        let (mut applied, mut failed) = (0usize, 0usize);
        let mut lo = from;
        while lo <= head {
            let hi = (lo + chunk - 1).min(head);
            let params = json!([{"fromBlock": format!("0x{lo:x}"), "toBlock": format!("0x{hi:x}"),
                                 "topics": [[TOPIC_PROPOSE_PRICE, TOPIC_DISPUTE_PRICE]]}]);
            match http_call(&client, &http_url, "eth_getLogs", params)
                .await
                .and_then(|v| serde_json::from_value::<Vec<RpcLog>>(v).ok())
            {
                Some(logs) => applied += apply(&gate, &logs),
                None => failed += 1,
            }
            lo = hi + 1;
            if *shutdown.borrow() {
                return;
            }
        }
        info!(
            from,
            head,
            applied,
            failed_chunks = failed,
            "confirm feed: backfill done"
        );
    } else {
        warn!("confirm feed: backfill skipped (eth_blockNumber failed)");
    }
    loop {
        let Ok((mut ws, _)) = connect_async(&wss_url).await else {
            warn!("confirm feed: wss connect failed, retrying");
            tokio::time::sleep(Duration::from_secs(3)).await;
            continue;
        };
        let sub = json!({"jsonrpc":"2.0","id":1,"method":"eth_subscribe",
                         "params":["logs", {"topics": [[TOPIC_PROPOSE_PRICE, TOPIC_DISPUTE_PRICE]]}]});
        if ws
            .send(Message::Text(sub.to_string().into()))
            .await
            .is_err()
        {
            continue;
        }
        info!("confirm feed: subscribed");
        loop {
            let msg = tokio::select! {
                _ = shutdown.changed() => return,
                m = ws.next() => m,
            };
            match msg {
                Some(Ok(Message::Text(t))) => {
                    if let Ok(v) = serde_json::from_str::<Value>(&t)
                        && let Some(log) = v
                            .pointer("/params/result")
                            .and_then(|r| serde_json::from_value::<RpcLog>(r.clone()).ok())
                    {
                        apply(&gate, std::slice::from_ref(&log));
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => {}
                _ => break,
            }
        }
        warn!("confirm feed: stream closed, reconnecting");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mempool::calldata::decode_input;

    /// 链上日志算出的请求键必须和 calldata 算出的一致，否则去重会失效。
    #[test]
    fn onchain_key_matches_calldata_key() {
        for raw in [
            include_str!("../testdata/mempool/propose_direct.json"),
            include_str!("../testdata/mempool/propose_multicall.json"),
            include_str!("../testdata/mempool/dispute_direct.json"),
        ] {
            let fx: Value = serde_json::from_str(raw).unwrap();
            let input =
                hex::decode(fx["input"].as_str().unwrap().trim_start_matches("0x")).unwrap();
            let logs: Vec<RpcLog> = serde_json::from_value(fx["logs"].clone()).unwrap();
            for (call, log) in decode_input(&input).iter().zip(&logs) {
                let (kind, key, price) = parse_onchain(log).unwrap();
                assert_eq!(kind, call.kind);
                assert_eq!(key, call.request_key());
                if kind == CallKind::Propose {
                    assert_eq!(price, call.price);
                }
            }
        }
    }
}
