//! uma-edge：tinyuma 的扇出节点。对上游只保持一条连接，本地帧环 + 每客户端
//! 有界队列，帧原样转发。热路径是 `upstream.rs` 收帧 → `hub.publish`，
//! 中间只有解析两个 varint、一次 `Arc` 分配和 N 次 `try_send`。

pub mod auth;
pub mod frame;
pub mod heartbeat;
pub mod hub;
pub mod ring;
pub mod server;
pub mod upstream;

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use crate::config::{ConfigError, nonempty, optional_parse, parse, parse_bool};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const COMMIT: &str = env!("RUST_UMA_GIT_COMMIT");
pub const SUBPROTOCOL: &str = "uma.pb.v1";
pub const WS_PATH: &str = "/uma/v1/ws";
pub const MAX_REPLAY: usize = 4096;

#[derive(Clone)]
pub struct EdgeConfig {
    pub listen_addr: SocketAddr,
    pub upstream_url: String,
    pub console_url: Option<String>,
    pub console_token: String,
    pub node_id: String,
    pub advertise_host: String,
    pub advertise_port: u16,
    pub advertise_tls: bool,
    pub client_queue: usize,
    pub ring_frames: usize,
    pub max_clients: usize,
    pub admin_token: String,
    /// 应急兜底：console 不可用时的静态 token；正常情况下由 console 下发。
    pub static_client_tokens: Vec<String>,
    pub drain_batch: usize,
    pub drain_interval: Duration,
    pub drain_timeout: Duration,
    pub heartbeat_interval: Duration,
}

impl EdgeConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let listen_addr: SocketAddr = parse("EDGE_LISTEN_ADDR", "0.0.0.0:8012")?;
        let node_id = nonempty("EDGE_NODE_ID")
            .or_else(|| {
                std::fs::read_to_string("/etc/hostname")
                    .ok()
                    .map(|s| s.trim().to_owned())
                    .filter(|s| !s.is_empty())
            })
            .ok_or(ConfigError::Missing("EDGE_NODE_ID"))?;
        Ok(Self {
            listen_addr,
            upstream_url: nonempty("EDGE_UPSTREAM_URL")
                .unwrap_or_else(|| "ws://43.131.1.194:8011/uma/v1/ws".to_owned()),
            console_url: nonempty("EDGE_CONSOLE_URL").map(|u| u.trim_end_matches('/').to_owned()),
            console_token: nonempty("EDGE_CONSOLE_TOKEN").unwrap_or_default(),
            node_id,
            advertise_host: nonempty("EDGE_ADVERTISE_HOST").unwrap_or_default(),
            advertise_port: optional_parse("EDGE_ADVERTISE_PORT")?.unwrap_or(listen_addr.port()),
            advertise_tls: parse_bool("EDGE_ADVERTISE_TLS", false)?,
            client_queue: parse("EDGE_CLIENT_QUEUE", "128")?,
            ring_frames: parse("EDGE_RING_FRAMES", "4096")?,
            max_clients: parse("EDGE_MAX_CLIENTS", "1000")?,
            admin_token: nonempty("EDGE_ADMIN_TOKEN").unwrap_or_default(),
            static_client_tokens: nonempty("EDGE_CLIENT_TOKENS")
                .map(|s| {
                    s.split(',')
                        .map(|t| t.trim().to_owned())
                        .filter(|t| !t.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            drain_batch: parse("EDGE_DRAIN_BATCH", "20")?,
            drain_interval: Duration::from_millis(parse("EDGE_DRAIN_INTERVAL_MS", "2000")?),
            drain_timeout: Duration::from_secs(parse("EDGE_DRAIN_TIMEOUT_SECONDS", "600")?),
            heartbeat_interval: Duration::from_secs(parse("EDGE_HEARTBEAT_INTERVAL_SECONDS", "5")?),
        })
    }
}

/// 全部计数都是 Relaxed 原子：热路径只做 fetch_add。
#[derive(Default)]
pub struct EdgeStats {
    pub upstream_connected: AtomicBool,
    pub upstream_reconnects: AtomicU64,
    pub last_frame_at_ms: AtomicU64,
    pub frames_total: AtomicU64,
    pub bytes_total: AtomicU64,
    pub bad_frames_total: AtomicU64,
    pub last_event_sequence: AtomicU64,
    pub upstream_lag_last_us: AtomicU64,
    pub upstream_lag_max_us: AtomicU64,
    pub clients_accepted: AtomicU64,
    pub clients_rejected: AtomicU64,
    pub rejected_token: AtomicU64,
    pub rejected_draining: AtomicU64,
    pub rejected_full: AtomicU64,
    pub rejected_subprotocol: AtomicU64,
    pub rejected_replay: AtomicU64,
    pub slow_clients_disconnected: AtomicU64,
    pub deliveries: AtomicU64,
    pub fanout_last_us: AtomicU64,
    pub fanout_max_us: AtomicU64,
    pub draining: AtomicBool,
}

impl EdgeStats {
    pub fn record_fanout(&self, us: u64) {
        self.fanout_last_us.store(us, Ordering::Relaxed);
        self.fanout_max_us.fetch_max(us, Ordering::Relaxed);
    }
}

pub struct EdgeState {
    pub config: EdgeConfig,
    pub stats: Arc<EdgeStats>,
    pub hub: hub::Hub,
    pub auth: auth::TokenSet,
    pub started_at_ms: u64,
    pub http: reqwest::Client,
}

pub type SharedState = Arc<EdgeState>;

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}
