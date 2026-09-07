//! 唯一的上游连接。退避重连、after_sequence 续连、1013 后一次不带游标、
//! 20s Ping / 60s 无消息判死。每帧：parse → Frame → hub.publish。

use std::{sync::atomic::Ordering, time::Duration};

use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use tokio::time::{Instant, timeout};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::ClientRequestBuilder, http::Uri},
};
use tracing::{info, warn};

use super::{SUBPROTOCOL, SharedState, frame::parse_frame, ring::Frame};

const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(15);
const PING_EVERY: Duration = Duration::from_secs(20);
const DEAD_AFTER: Duration = Duration::from_secs(60);
const CURSOR_REJECTED: u16 = 1013;
/// Frames older than this are treated as replay, not as a latency sample.
const LIVE_LAG_MAX_US: u64 = 10_000_000;

/// Dial URL. Once we have a cursor the upstream has not rejected, it replaces
/// any `after_sequence` in the configured URL. Before that (fresh start) a
/// configured `after_sequence` is honoured, so `?after_sequence=0` warms the
/// ring with everything tinyuma still holds; after a 1013 nothing is sent.
pub fn dial_url(base: &str, last_event_sequence: u64, cursor_rejected: bool) -> String {
    let (path, query) = match base.split_once('?') {
        Some((p, q)) => (p, q),
        None => (base, ""),
    };
    let keep_configured = last_event_sequence == 0 && !cursor_rejected;
    let mut params: Vec<&str> = query
        .split('&')
        .filter(|kv| !kv.is_empty() && (keep_configured || !kv.starts_with("after_sequence=")))
        .collect();
    let cursor;
    if last_event_sequence > 0 && !cursor_rejected {
        cursor = format!("after_sequence={last_event_sequence}");
        params.push(&cursor);
    }
    if params.is_empty() {
        path.to_owned()
    } else {
        format!("{path}?{}", params.join("&"))
    }
}

pub async fn run_upstream(state: SharedState) {
    let mut backoff = BACKOFF_MIN;
    let mut last_event_sequence = 0u64;
    let mut cursor_rejected = false;
    let mut first = true;
    loop {
        if !first {
            state
                .stats
                .upstream_reconnects
                .fetch_add(1, Ordering::Relaxed);
            let jitter = rand::rng().random_range(0.0..0.25);
            let wait = backoff.mul_f64(1.0 + jitter);
            tokio::time::sleep(wait).await;
            backoff = (backoff * 2).min(BACKOFF_MAX);
        }
        first = false;

        let url = dial_url(
            &state.config.upstream_url,
            last_event_sequence,
            cursor_rejected,
        );
        let started = Instant::now();
        match session(&state, &url, &mut last_event_sequence).await {
            SessionEnd::CursorRejected => {
                warn!(
                    last_event_sequence,
                    "upstream rejected cursor (1013); next dial without it"
                );
                cursor_rejected = true;
            }
            SessionEnd::Other(reason) => {
                warn!(%reason, url = %redact(&url), "upstream session ended");
            }
        }
        state
            .stats
            .upstream_connected
            .store(false, Ordering::Relaxed);
        // A session that lived a while was healthy: reset the backoff so a
        // one-off drop reconnects in 1s instead of inheriting old growth.
        if started.elapsed() >= Duration::from_secs(30) {
            backoff = BACKOFF_MIN;
        }
    }
}

enum SessionEnd {
    CursorRejected,
    Other(String),
}

async fn session(state: &SharedState, url: &str, last_event_sequence: &mut u64) -> SessionEnd {
    let uri: Uri = match url.parse() {
        Ok(uri) => uri,
        Err(e) => return SessionEnd::Other(format!("bad upstream url: {e}")),
    };
    let request = ClientRequestBuilder::new(uri).with_sub_protocol(SUBPROTOCOL);
    let (mut ws, response) = match timeout(Duration::from_secs(10), connect_async(request)).await {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => return SessionEnd::Other(format!("connect: {e}")),
        Err(_) => return SessionEnd::Other("connect timeout".into()),
    };
    let negotiated = response
        .headers()
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .map(str::trim);
    if negotiated != Some(SUBPROTOCOL) {
        let _ = ws.close(None).await;
        return SessionEnd::Other(format!(
            "upstream did not negotiate {SUBPROTOCOL}: {negotiated:?}"
        ));
    }
    info!(url = %redact(url), "upstream connected");
    state
        .stats
        .upstream_connected
        .store(true, Ordering::Relaxed);

    let mut ping = tokio::time::interval(PING_EVERY);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ping.tick().await;
    loop {
        tokio::select! {
            _ = ping.tick() => {
                if let Err(e) = ws.send(Message::Ping(Vec::new().into())).await {
                    return SessionEnd::Other(format!("ping: {e}"));
                }
            }
            next = timeout(DEAD_AFTER, ws.next()) => {
                let message = match next {
                    Err(_) => return SessionEnd::Other("no frame or pong for 60s".into()),
                    Ok(None) => return SessionEnd::Other("closed".into()),
                    Ok(Some(Err(e))) => return SessionEnd::Other(format!("read: {e}")),
                    Ok(Some(Ok(m))) => m,
                };
                match message {
                    Message::Binary(bytes) => on_frame(state, bytes, last_event_sequence),
                    Message::Close(frame) => {
                        let code = frame.as_ref().map(|f| u16::from(f.code)).unwrap_or(0);
                        if code == CURSOR_REJECTED {
                            return SessionEnd::CursorRejected;
                        }
                        return SessionEnd::Other(format!("close {code}"));
                    }
                    // Text/Ping/Pong/Frame: not forwarded (ping is auto-ponged by tungstenite).
                    _ => {}
                }
            }
        }
    }
}

/// The hot path: parse two varints, allocate one Arc, fan out.
fn on_frame(state: &SharedState, bytes: bytes::Bytes, last_event_sequence: &mut u64) {
    let recv_at_us = super::now_us();
    let mut batch_sequence = 0;
    let mut event_sequence = *last_event_sequence;
    match parse_frame(&bytes) {
        Ok(info) => {
            batch_sequence = info.batch_sequence;
            if info.last_event_sequence > 0 {
                event_sequence = info.last_event_sequence;
            }
            // Replayed frames (warm-up / after_sequence catch-up) carry an old
            // sent_at; only live frames say anything about the link.
            if info.sent_at_us > 0 {
                let lag = recv_at_us.saturating_sub(info.sent_at_us);
                if lag <= LIVE_LAG_MAX_US {
                    state
                        .stats
                        .upstream_lag_last_us
                        .store(lag, Ordering::Relaxed);
                    state
                        .stats
                        .upstream_lag_max_us
                        .fetch_max(lag, Ordering::Relaxed);
                }
            }
        }
        Err(_) => {
            state.stats.bad_frames_total.fetch_add(1, Ordering::Relaxed);
        }
    }
    *last_event_sequence = event_sequence;
    state
        .stats
        .last_event_sequence
        .store(event_sequence, Ordering::Relaxed);
    state.stats.frames_total.fetch_add(1, Ordering::Relaxed);
    state
        .stats
        .bytes_total
        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
    state
        .stats
        .last_frame_at_ms
        .store(recv_at_us / 1000, Ordering::Relaxed);
    state.hub.publish(Frame {
        offset: 0,
        batch_sequence,
        event_sequence,
        bytes,
        recv_at_us,
    });
}

fn redact(url: &str) -> &str {
    url.split("token=").next().unwrap_or(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dial_url_cursor_rules() {
        let base = "ws://h:8011/uma/v1/ws";
        assert_eq!(dial_url(base, 0, false), base);
        assert_eq!(
            dial_url(base, 42, false),
            "ws://h:8011/uma/v1/ws?after_sequence=42"
        );
        assert_eq!(dial_url(base, 42, true), base);
        assert_eq!(
            dial_url("ws://h/ws?after_sequence=1&x=y", 7, false),
            "ws://h/ws?x=y&after_sequence=7"
        );
        assert_eq!(dial_url("ws://h/ws?after_sequence=1", 7, true), "ws://h/ws");
        // fresh start honours the configured warm-up cursor; after 1013 it is dropped
        assert_eq!(
            dial_url("ws://h/ws?after_sequence=0", 0, false),
            "ws://h/ws?after_sequence=0"
        );
        assert_eq!(dial_url("ws://h/ws?after_sequence=0", 0, true), "ws://h/ws");
    }
}
