//! 面板专用的实时推送：console 保持一条到 tinyuma 的 ws，帧原样广播给面板
//! 浏览器（页面是 HTTPS，不能直连明文 ws）。只服务面板，不在业务热路径上；
//! 业务下游走 edge。

use std::{
    collections::VecDeque,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use tokio::sync::broadcast;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::ClientRequestBuilder, http::Uri},
};
use tracing::{info, warn};

use super::{SharedState, now_ms};

const SUBPROTOCOL: &str = "uma.pb.v1";
const RECENT_FRAMES: usize = 48;
/// 连上游时回拉这么多序号的历史，面板一打开就有最近的事件可看。
const WARMUP_SEQUENCES: u64 = 60;
const BROADCAST_CAPACITY: usize = 256;

pub struct Feed {
    tx: broadcast::Sender<Bytes>,
    recent: Mutex<VecDeque<Bytes>>,
    pub connected: AtomicBool,
    pub frames_total: AtomicU64,
    pub last_frame_at_ms: AtomicU64,
    pub reconnects: AtomicU64,
}

#[derive(Serialize)]
pub struct FeedStatus {
    pub connected: bool,
    pub frames_total: u64,
    pub last_frame_at_ms: u64,
    pub reconnects: u64,
    pub viewers: usize,
}

impl Default for Feed {
    fn default() -> Self {
        Self {
            tx: broadcast::channel(BROADCAST_CAPACITY).0,
            recent: Mutex::new(VecDeque::with_capacity(RECENT_FRAMES)),
            connected: AtomicBool::new(false),
            frames_total: AtomicU64::new(0),
            last_frame_at_ms: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
        }
    }
}

impl Feed {
    /// Snapshot of recent frames + a live receiver, taken under one lock so a
    /// viewer never misses or duplicates a frame across the boundary.
    pub fn subscribe(&self) -> (Vec<Bytes>, broadcast::Receiver<Bytes>) {
        let recent = self.recent.lock().unwrap_or_else(|e| e.into_inner());
        let rx = self.tx.subscribe();
        (recent.iter().cloned().collect(), rx)
    }

    fn publish(&self, frame: Bytes) {
        {
            let mut recent = self.recent.lock().unwrap_or_else(|e| e.into_inner());
            if recent.len() == RECENT_FRAMES {
                recent.pop_front();
            }
            recent.push_back(frame.clone());
            let _ = self.tx.send(frame);
        }
        self.frames_total.fetch_add(1, Ordering::Relaxed);
        self.last_frame_at_ms.store(now_ms(), Ordering::Relaxed);
    }

    pub fn status(&self) -> FeedStatus {
        FeedStatus {
            connected: self.connected.load(Ordering::Relaxed),
            frames_total: self.frames_total.load(Ordering::Relaxed),
            last_frame_at_ms: self.last_frame_at_ms.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            viewers: self.tx.receiver_count(),
        }
    }
}

pub async fn run_feed(state: SharedState) {
    let url = state.config.upstream_ws.clone();
    let mut backoff = Duration::from_secs(1);
    let mut first = true;
    loop {
        if !first {
            state.feed.reconnects.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(15));
        }
        first = false;
        let started = tokio::time::Instant::now();
        // 首次/重连都从 tinyuma 最新序号往前回拉一小段；启动时等 poller 先拿到
        // 最新序号（最多 10s），拿不到就只要实时。
        let mut latest = state.upstream.snapshot().latest_sequence;
        for _ in 0..10 {
            if latest > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            latest = state.upstream.snapshot().latest_sequence;
        }
        let dial = if latest > WARMUP_SEQUENCES {
            format!("{url}?after_sequence={}", latest - WARMUP_SEQUENCES)
        } else {
            url.clone()
        };
        match session(&state, &dial).await {
            Ok(()) => info!(url, "panel feed closed by upstream"),
            Err(error) => warn!(url, %error, "panel feed session ended"),
        }
        state.feed.connected.store(false, Ordering::Relaxed);
        if started.elapsed() >= Duration::from_secs(30) {
            backoff = Duration::from_secs(1);
        }
    }
}

async fn session(state: &SharedState, url: &str) -> Result<(), String> {
    let uri: Uri = url.parse().map_err(|e| format!("bad url: {e}"))?;
    let request = ClientRequestBuilder::new(uri).with_sub_protocol(SUBPROTOCOL);
    let (mut ws, _) = tokio::time::timeout(Duration::from_secs(10), connect_async(request))
        .await
        .map_err(|_| "connect timeout".to_owned())?
        .map_err(|e| format!("connect: {e}"))?;
    info!(url, "panel feed connected to tinyuma");
    state.feed.connected.store(true, Ordering::Relaxed);
    let mut ping = tokio::time::interval(Duration::from_secs(20));
    ping.tick().await;
    loop {
        tokio::select! {
            _ = ping.tick() => {
                ws.send(Message::Ping(Vec::new().into())).await.map_err(|e| format!("ping: {e}"))?;
            }
            next = tokio::time::timeout(Duration::from_secs(60), ws.next()) => {
                match next.map_err(|_| "no message for 60s".to_owned())? {
                    None => return Ok(()),
                    Some(Err(e)) => return Err(format!("read: {e}")),
                    Some(Ok(Message::Binary(bytes))) => state.feed.publish(bytes),
                    Some(Ok(Message::Close(frame))) => {
                        return Err(format!("close {:?}", frame.map(|f| u16::from(f.code))));
                    }
                    Some(Ok(_)) => {}
                }
            }
        }
    }
}
