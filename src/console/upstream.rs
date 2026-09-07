//! 定时拉 tinyuma 的 `/healthz` 与 `/uma/v1/dashboard-data`，以及 `/llms.txt`。
//! 失败沿用上一次成功的结果并标 stale，面板永远有东西可看。

use std::{sync::RwLock, time::Duration};

use serde::Serialize;
use serde_json::Value;
use tracing::{info, warn};

use super::{SharedState, now_ms};

#[derive(Clone, Default, Serialize)]
pub struct UpstreamSnapshot {
    pub base_url: String,
    pub fetched_at_ms: u64,
    pub ok: bool,
    pub error: Option<String>,
    pub healthz: Option<Value>,
    pub dashboard: Option<Value>,
    /// Convenience for the panel: tinyuma's newest event sequence, used to
    /// compute each edge's lag.
    pub latest_sequence: u64,
}

#[derive(Default)]
struct LlmsCache {
    fetched_at_ms: u64,
    body: Option<String>,
}

#[derive(Default)]
pub struct UpstreamCache {
    snapshot: RwLock<UpstreamSnapshot>,
    llms: RwLock<LlmsCache>,
}

impl UpstreamCache {
    pub fn snapshot(&self) -> UpstreamSnapshot {
        self.snapshot
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn store(&self, snapshot: UpstreamSnapshot) {
        *self.snapshot.write().unwrap_or_else(|e| e.into_inner()) = snapshot;
    }
}

pub async fn run_upstream_poller(state: SharedState) {
    let mut ticker = tokio::time::interval(state.config.upstream_poll);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut failures: u64 = 0;
    loop {
        ticker.tick().await;
        match fetch(&state).await {
            Ok((healthz, dashboard)) => {
                if failures > 0 {
                    info!(failures, "tinyuma upstream poll recovered");
                }
                failures = 0;
                let latest_sequence = dashboard
                    .as_ref()
                    .and_then(|d| d.get("event_ring_latest_sequence"))
                    .or_else(|| healthz.get("event_ring_latest_sequence"))
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                state.upstream.store(UpstreamSnapshot {
                    base_url: state.config.upstream_http.clone(),
                    fetched_at_ms: now_ms(),
                    ok: true,
                    error: None,
                    healthz: Some(healthz),
                    dashboard,
                    latest_sequence,
                });
            }
            Err(error) => {
                failures += 1;
                if failures == 1 || failures.is_multiple_of(12) {
                    warn!(%error, failures, "tinyuma upstream poll failed");
                }
                let mut snapshot = state.upstream.snapshot();
                snapshot.ok = false;
                snapshot.error = Some(error);
                snapshot.base_url = state.config.upstream_http.clone();
                state.upstream.store(snapshot);
            }
        }
    }
}

async fn fetch(state: &SharedState) -> Result<(Value, Option<Value>), String> {
    let base = &state.config.upstream_http;
    let healthz = get_json(state, &format!("{base}/healthz")).await?;
    let dashboard = match &state.config.upstream_dashboard_token {
        Some(token) => Some(
            get_json(
                state,
                &format!("{base}/uma/v1/dashboard-data?token={token}"),
            )
            .await?,
        ),
        None => None,
    };
    Ok((healthz, dashboard))
}

async fn get_json(state: &SharedState, url: &str) -> Result<Value, String> {
    let response = state
        .http
        .get(url)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| format!("{}: {e}", redact(url)))?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("{}: HTTP {status}", redact(url)));
    }
    response
        .json()
        .await
        .map_err(|e| format!("{}: {e}", redact(url)))
}

/// Keeps the dashboard token out of logs.
fn redact(url: &str) -> &str {
    url.split("?token=").next().unwrap_or(url)
}

/// Cluster contract + tinyuma's own llms.txt, refreshed at most every
/// `llms_cache`; a failed refresh keeps serving the previous copy.
pub async fn llms_text(state: &SharedState, cluster: &str) -> String {
    let now = now_ms();
    let cached = {
        let llms = state
            .upstream
            .llms
            .read()
            .unwrap_or_else(|e| e.into_inner());
        if llms.body.is_some()
            && now.saturating_sub(llms.fetched_at_ms) < state.config.llms_cache.as_millis() as u64
        {
            llms.body.clone()
        } else {
            None
        }
    };
    let upstream = match cached {
        Some(body) => body,
        None => {
            let url = format!("{}/llms.txt", state.config.upstream_http);
            let fetched = async {
                state
                    .http
                    .get(&url)
                    .timeout(Duration::from_secs(5))
                    .send()
                    .await
                    .ok()?
                    .error_for_status()
                    .ok()?
                    .text()
                    .await
                    .ok()
            }
            .await;
            let mut llms = state
                .upstream
                .llms
                .write()
                .unwrap_or_else(|e| e.into_inner());
            match fetched {
                Some(body) => {
                    llms.fetched_at_ms = now;
                    llms.body = Some(body.clone());
                    body
                }
                None => llms
                    .body
                    .clone()
                    .unwrap_or_else(|| format!("(上游 {url} 暂时不可达，稍后重试)\n")),
            }
        }
    };
    format!("{cluster}\n\n---\n\n{upstream}")
}
