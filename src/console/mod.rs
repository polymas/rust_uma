//! uma-console: 扇出集群的控制台。收 edge 心跳、管 token、聚合 tinyuma 状态、
//! 发布公开节点列表。全部状态在内存里，只有 token 表和节点管理位落盘
//! （见 `store.rs`）。这里没有热路径——它不碰任何帧。

pub mod feed;
pub mod registry;
pub mod server;
pub mod store;
pub mod tokens;
pub mod upstream;

use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use crate::config::{ConfigError, nonempty, parse};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const COMMIT: &str = env!("RUST_UMA_GIT_COMMIT");

#[derive(Clone)]
pub struct ConsoleConfig {
    pub listen_addr: SocketAddr,
    pub node_token: String,
    pub admin_token: String,
    pub panel_token: String,
    pub data_dir: PathBuf,
    pub stale_after: Duration,
    pub list_ttl: Duration,
    pub upstream_http: String,
    /// tinyuma 的 ws 地址，面板实时推送用；默认由 upstream_http 推导。
    pub upstream_ws: String,
    pub upstream_dashboard_token: Option<String>,
    pub upstream_poll: Duration,
    pub llms_cache: Duration,
}

impl ConsoleConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            listen_addr: parse("CONSOLE_LISTEN_ADDR", "0.0.0.0:8013")?,
            node_token: nonempty("CONSOLE_NODE_TOKEN")
                .ok_or(ConfigError::Missing("CONSOLE_NODE_TOKEN"))?,
            admin_token: nonempty("CONSOLE_ADMIN_TOKEN")
                .ok_or(ConfigError::Missing("CONSOLE_ADMIN_TOKEN"))?,
            panel_token: nonempty("CONSOLE_PANEL_TOKEN")
                .ok_or(ConfigError::Missing("CONSOLE_PANEL_TOKEN"))?,
            data_dir: parse("CONSOLE_DATA_DIR", "/var/lib/uma-console")?,
            stale_after: Duration::from_secs(parse("CONSOLE_STALE_AFTER_SECONDS", "15")?),
            list_ttl: Duration::from_secs(parse("CONSOLE_LIST_TTL_SECONDS", "30")?),
            upstream_http: nonempty("CONSOLE_UPSTREAM_HTTP")
                .unwrap_or_else(|| "http://43.131.1.194:8011".to_owned())
                .trim_end_matches('/')
                .to_owned(),
            upstream_ws: nonempty("CONSOLE_UPSTREAM_WS").unwrap_or_else(|| {
                let http = nonempty("CONSOLE_UPSTREAM_HTTP")
                    .unwrap_or_else(|| "http://43.131.1.194:8011".to_owned());
                let host = http.trim_end_matches('/');
                let ws = host
                    .replacen("https://", "wss://", 1)
                    .replacen("http://", "ws://", 1);
                format!("{ws}/uma/v1/ws")
            }),
            upstream_dashboard_token: nonempty("CONSOLE_UPSTREAM_DASHBOARD_TOKEN"),
            upstream_poll: Duration::from_secs(parse("CONSOLE_UPSTREAM_POLL_SECONDS", "3")?),
            llms_cache: Duration::from_secs(parse("CONSOLE_LLMS_CACHE_SECONDS", "300")?),
        })
    }
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Everything the HTTP handlers and background pollers share.
pub struct ConsoleState {
    pub config: ConsoleConfig,
    pub registry: registry::Registry,
    pub tokens: tokens::TokenStore,
    pub upstream: upstream::UpstreamCache,
    pub feed: feed::Feed,
    pub http: reqwest::Client,
    pub started_at_ms: u64,
}

pub type SharedState = Arc<ConsoleState>;
