//! 唯一的上游连接。退避重连、after_sequence 续连、1013 后一次不带游标、
//! 20s Ping / 60s 无消息判死。每帧：parse → Frame → hub.publish。

use std::{sync::atomic::Ordering, time::Duration};

use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time::{Instant, sleep_until, timeout},
};
use tokio_tungstenite::{
    WebSocketStream, connect_async,
    tungstenite::{Message, client::ClientRequestBuilder, http::Uri},
};
use tracing::{info, warn};

use super::{SUBPROTOCOL, SharedState, frame::parse_frame, ring::Frame};

const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(15);
const PING_EVERY: Duration = Duration::from_secs(20);
const DEAD_AFTER: Duration = Duration::from_secs(60);
/// A Ping is a few bytes; if the socket will not take it for this long the
/// send buffer is full of unacknowledged data and the link is gone.
const PING_SEND_TIMEOUT: Duration = Duration::from_secs(10);
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
        match session(&state, &url, &mut last_event_sequence, &mut cursor_rejected).await {
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

async fn session(
    state: &SharedState,
    url: &str,
    last_event_sequence: &mut u64,
    cursor_rejected: &mut bool,
) -> SessionEnd {
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
    pump(
        state,
        &mut ws,
        last_event_sequence,
        cursor_rejected,
        PING_EVERY,
        DEAD_AFTER,
    )
    .await
}

/// Reads one established connection until it ends. Generic over the stream
/// (and takes the two periods as arguments) so tests can drive it over an
/// in-memory pipe with short timers.
async fn pump<S>(
    state: &SharedState,
    ws: &mut WebSocketStream<S>,
    last_event_sequence: &mut u64,
    cursor_rejected: &mut bool,
    ping_every: Duration,
    dead_after: Duration,
) -> SessionEnd
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut ping = tokio::time::interval(ping_every);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ping.tick().await;
    // The deadline is tied to the last message actually received. It used to
    // be `timeout(DEAD_AFTER, ws.next())` built inside the `select!`: every
    // 20s the ping branch won, the loop went round, and a fresh 60s timeout
    // was created — so it could never fire, and a silently dead upstream was
    // only noticed when the kernel gave up on TCP (about 15 minutes).
    let mut last_rx = Instant::now();
    loop {
        tokio::select! {
            _ = ping.tick() => {
                match timeout(PING_SEND_TIMEOUT, ws.send(Message::Ping(Vec::new().into()))).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => return SessionEnd::Other(format!("ping: {e}")),
                    Err(_) => return SessionEnd::Other("ping send timeout".into()),
                }
            }
            _ = sleep_until(last_rx + dead_after) => {
                return SessionEnd::Other(format!(
                    "no frame or pong for {}s",
                    dead_after.as_secs()
                ));
            }
            next = ws.next() => {
                let message = match next {
                    None => return SessionEnd::Other("closed".into()),
                    Some(Err(e)) => return SessionEnd::Other(format!("read: {e}")),
                    Some(Ok(m)) => m,
                };
                // Any message, Pong included, proves the link is alive.
                last_rx = Instant::now();
                match message {
                    Message::Binary(bytes) => {
                        if on_frame(state, bytes, last_event_sequence) {
                            // The upstream is feeding us sequences again, so
                            // `last_event_sequence` is a cursor it will accept.
                            // Without this the flag stayed set for the life of
                            // the process: after one 1013 every later reconnect
                            // dialled without a cursor and lost the frames sent
                            // while it was away.
                            *cursor_rejected = false;
                        }
                    }
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

/// The hot path: parse two varints, allocate one Arc, fan out. Returns
/// whether the frame carried an event sequence (a usable resume cursor).
fn on_frame(state: &SharedState, bytes: bytes::Bytes, last_event_sequence: &mut u64) -> bool {
    let recv_at_us = super::now_us();
    let mut batch_sequence = 0;
    let mut event_sequence = *last_event_sequence;
    let mut sequenced = false;
    match parse_frame(&bytes) {
        Ok(info) => {
            batch_sequence = info.batch_sequence;
            if info.last_event_sequence > 0 {
                event_sequence = info.last_event_sequence;
                sequenced = true;
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
    sequenced
}

fn redact(url: &str) -> &str {
    url.split("token=").next().unwrap_or(url)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use prost::Message as _;
    use tokio::io::DuplexStream;
    use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Role, frame::coding::CloseCode};

    use super::*;
    use crate::{
        edge::{EdgeConfig, EdgeState, EdgeStats, auth::TokenSet, hub::Hub, now_ms},
        wire::{MAGIC, SCHEMA_VERSION, pb},
    };

    fn test_state() -> SharedState {
        let config = EdgeConfig {
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            upstream_url: String::new(),
            console_url: None,
            console_token: String::new(),
            node_id: "test".into(),
            advertise_host: "127.0.0.1".into(),
            advertise_port: 0,
            advertise_tls: false,
            client_queue: 16,
            ring_frames: 16,
            max_clients: 10,
            admin_token: String::new(),
            static_client_tokens: Vec::new(),
            drain_batch: 1,
            drain_interval: Duration::from_millis(50),
            drain_timeout: Duration::from_secs(1),
            heartbeat_interval: Duration::from_secs(5),
        };
        let stats = Arc::new(EdgeStats::default());
        Arc::new(EdgeState {
            hub: Hub::new(config.ring_frames, config.client_queue, stats.clone()),
            auth: TokenSet::new(Vec::new()),
            config,
            stats,
            started_at_ms: now_ms(),
            http: reqwest::Client::new(),
        })
    }

    /// An uncompressed wire frame whose batch holds one event with `sequence`.
    fn frame_with_sequence(sequence: u64) -> bytes::Bytes {
        let payload = pb::UmaBatch {
            schema_version: 1,
            batch_sequence: 1,
            sent_at_us: 0,
            events: vec![pb::UmaEvent {
                sequence,
                ..Default::default()
            }],
        }
        .encode_to_vec();
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.push(0);
        out.push(SCHEMA_VERSION);
        out.extend_from_slice(&[0, 0]);
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(&payload);
        out.into()
    }

    async fn pipe() -> (WebSocketStream<DuplexStream>, WebSocketStream<DuplexStream>) {
        let (a, b) = tokio::io::duplex(1 << 16);
        (
            WebSocketStream::from_raw_socket(a, Role::Client, None).await,
            WebSocketStream::from_raw_socket(b, Role::Server, None).await,
        )
    }

    const TEST_PING: Duration = Duration::from_millis(20);
    const TEST_DEAD: Duration = Duration::from_millis(150);

    /// The upstream stops answering but the connection is never closed (a
    /// blackholed TCP path). Before the fix the ping branch kept rebuilding
    /// the dead-timeout, so this never returned.
    #[tokio::test]
    async fn silent_upstream_is_declared_dead() {
        let state = test_state();
        // Keep the peer end alive but never read it: no Pong ever comes back.
        let (mut client, _peer) = pipe().await;
        let (mut sequence, mut rejected) = (0, false);
        let end = timeout(
            Duration::from_secs(5),
            pump(
                &state,
                &mut client,
                &mut sequence,
                &mut rejected,
                TEST_PING,
                TEST_DEAD,
            ),
        )
        .await
        .expect("a silent upstream must end the session");
        assert!(
            matches!(&end, SessionEnd::Other(reason) if reason.starts_with("no frame or pong")),
        );
    }

    /// Pongs alone must keep an idle session alive: UMA is often quiet for
    /// minutes, and that is not a dead link.
    #[tokio::test]
    async fn pongs_keep_an_idle_session_alive() {
        let state = test_state();
        let (mut client, mut peer) = pipe().await;
        // Reading is what makes tungstenite answer each Ping with a Pong.
        let responder = tokio::spawn(async move { while peer.next().await.is_some() {} });
        let (mut sequence, mut rejected) = (0, false);
        let outcome = timeout(
            TEST_DEAD * 4,
            pump(
                &state,
                &mut client,
                &mut sequence,
                &mut rejected,
                TEST_PING,
                TEST_DEAD,
            ),
        )
        .await;
        assert!(outcome.is_err(), "session ended although pongs kept coming");
        responder.abort();
    }

    #[tokio::test]
    async fn sequenced_frame_clears_cursor_rejected_and_1013_reports_it() {
        let state = test_state();

        // After a 1013 the edge dials without a cursor. Once frames with
        // sequences arrive, the next reconnect must resume from them.
        let (mut client, mut peer) = pipe().await;
        peer.send(Message::Binary(frame_with_sequence(42)))
            .await
            .unwrap();
        peer.close(None).await.unwrap();
        let (mut sequence, mut rejected) = (7, true);
        let end = pump(
            &state,
            &mut client,
            &mut sequence,
            &mut rejected,
            PING_EVERY,
            DEAD_AFTER,
        )
        .await;
        assert!(matches!(end, SessionEnd::Other(_)));
        assert_eq!(sequence, 42);
        assert!(!rejected, "a sequenced frame makes the cursor usable again");
        assert_eq!(
            dial_url("ws://h/ws", sequence, rejected),
            "ws://h/ws?after_sequence=42"
        );

        // A session that delivered nothing leaves the flag alone, and a
        // 1013 close is reported as such.
        let (mut client, mut peer) = pipe().await;
        peer.close(Some(CloseFrame {
            code: CloseCode::from(CURSOR_REJECTED),
            reason: "".into(),
        }))
        .await
        .unwrap();
        let (mut sequence, mut rejected) = (42, true);
        let end = pump(
            &state,
            &mut client,
            &mut sequence,
            &mut rejected,
            PING_EVERY,
            DEAD_AFTER,
        )
        .await;
        assert!(matches!(end, SessionEnd::CursorRejected));
        assert!(rejected);
    }

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
