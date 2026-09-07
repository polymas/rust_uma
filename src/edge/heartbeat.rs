//! 心跳上报 + 执行 console 指令（drain、token 集合、心跳周期）。
//! 失败只记日志（第 1 次和每 12 次），不影响服务。

use std::{sync::atomic::Ordering, time::Duration};

use tracing::{info, warn};

use crate::console::registry::{Directive, Heartbeat};

use super::{COMMIT, SUBPROTOCOL, SharedState, VERSION, WS_PATH, now_ms, server::start_drain};

pub fn build_heartbeat(state: &SharedState) -> Heartbeat {
    let s = &state.stats;
    let (ring, capacity) = state.hub.ring_stats();
    let draining = s.draining.load(Ordering::Relaxed);
    let upstream_connected = s.upstream_connected.load(Ordering::Relaxed);
    Heartbeat {
        node_id: state.config.node_id.clone(),
        version: VERSION.to_owned(),
        commit: COMMIT.to_owned(),
        started_at_ms: state.started_at_ms,
        sent_at_ms: now_ms(),
        advertise_host: state.config.advertise_host.clone(),
        port: state.config.advertise_port,
        tls: state.config.advertise_tls,
        path: WS_PATH.to_owned(),
        subprotocol: SUBPROTOCOL.to_owned(),
        ready: !draining && upstream_connected,
        draining,
        upstream_url: state.config.upstream_url.clone(),
        upstream_connected,
        upstream_reconnects_total: s.upstream_reconnects.load(Ordering::Relaxed),
        last_frame_at_ms: s.last_frame_at_ms.load(Ordering::Relaxed),
        frames_total: s.frames_total.load(Ordering::Relaxed),
        bytes_total: s.bytes_total.load(Ordering::Relaxed),
        bad_frames_total: s.bad_frames_total.load(Ordering::Relaxed),
        last_event_sequence: s.last_event_sequence.load(Ordering::Relaxed),
        ring_frames: ring.count as u64,
        ring_capacity: capacity as u64,
        upstream_lag_last_us: s.upstream_lag_last_us.load(Ordering::Relaxed),
        upstream_lag_max_us: s.upstream_lag_max_us.load(Ordering::Relaxed),
        clients: state.hub.clients() as u64,
        max_clients: state.config.max_clients as u64,
        clients_accepted_total: s.clients_accepted.load(Ordering::Relaxed),
        clients_rejected_total: s.clients_rejected.load(Ordering::Relaxed),
        slow_clients_disconnected_total: s.slow_clients_disconnected.load(Ordering::Relaxed),
        deliveries_total: s.deliveries.load(Ordering::Relaxed),
        fanout_last_us: s.fanout_last_us.load(Ordering::Relaxed),
        fanout_max_us: s.fanout_max_us.load(Ordering::Relaxed),
        tokens_version: state.auth.version(),
        clients_by_token: state.hub.clients_by_token(),
    }
}

pub async fn run_heartbeat(state: SharedState) {
    let Some(console) = state.config.console_url.clone() else {
        warn!("EDGE_CONSOLE_URL not set: no heartbeat, no console-issued tokens");
        return;
    };
    if state.config.console_token.is_empty() {
        warn!("EDGE_CONSOLE_TOKEN empty: heartbeats will be rejected");
    }
    let url = format!("{console}/api/v1/nodes/heartbeat");
    let mut interval = state.config.heartbeat_interval;
    let mut failures = 0u64;
    loop {
        match post(&state, &url).await {
            Ok(directive) => {
                if failures > 0 {
                    info!(failures, "console heartbeat recovered");
                }
                failures = 0;
                apply(&state, directive, &mut interval);
            }
            Err(error) => {
                failures += 1;
                if failures == 1 || failures.is_multiple_of(12) {
                    warn!(%error, failures, "console heartbeat failed");
                }
            }
        }
        tokio::time::sleep(interval).await;
    }
}

async fn post(state: &SharedState, url: &str) -> Result<Directive, String> {
    let body = build_heartbeat(state);
    let response = state
        .http
        .post(url)
        .bearer_auth(&state.config.console_token)
        .timeout(Duration::from_secs(4))
        .json(&body)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("HTTP {status}"));
    }
    response.json().await.map_err(|e| e.to_string())
}

pub fn apply(state: &SharedState, directive: Directive, interval: &mut Duration) {
    if directive.heartbeat_seconds > 0 {
        let next = Duration::from_secs(directive.heartbeat_seconds);
        if next != *interval {
            info!(
                seconds = directive.heartbeat_seconds,
                "console changed heartbeat interval"
            );
            *interval = next;
        }
    }
    if let Some(grants) = directive.tokens {
        let count = grants.len();
        if state.auth.replace(directive.tokens_version, grants) {
            let kicked = state.hub.kick_tokens(|id| state.auth.is_live(id));
            info!(
                version = directive.tokens_version,
                tokens = count,
                kicked,
                "token set updated from console"
            );
        }
    }
    if directive.drain && !state.stats.draining.load(Ordering::Relaxed) {
        info!("console requested drain");
        start_drain(state.clone(), false);
    }
}
