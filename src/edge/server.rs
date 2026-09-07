//! edge 的 HTTP/WS 入口。握手顺序：token → draining 503 → 满 503 →
//! 子协议 400 → `_replay` 400 → 升级。每个连接一个写任务 + 本任务读，
//! 任一方退出都注销并收尾另一方，不泄漏任务。

use std::{
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{
        ConnectInfo, Query, State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket},
    },
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use serde::Deserialize;
use tokio::sync::watch;
use tracing::{info, warn};

use super::{
    MAX_REPLAY, SUBPROTOCOL, SharedState, WS_PATH,
    heartbeat::build_heartbeat,
    hub::{CLOSE_RESTART, Subscription},
    ring::Frame,
};

const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const CLIENT_PING: Duration = Duration::from_secs(30);
const CLIENT_DEAD: Duration = Duration::from_secs(90);

pub fn router(state: SharedState) -> Router {
    Router::new()
        .route(WS_PATH, get(websocket))
        .route("/edge/ready", get(ready))
        .route("/edge/healthz", get(healthz))
        .route("/edge/admin/clients", get(admin_clients))
        .route("/edge/admin/rebalance", post(admin_rebalance))
        .route("/edge/admin/drain", post(admin_drain))
        .with_state(state)
}

pub async fn serve(
    state: SharedState,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), std::io::Error> {
    let listener = tokio::net::TcpListener::bind(state.config.listen_addr).await?;
    axum::serve(
        listener,
        router(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        while !*shutdown.borrow() {
            if shutdown.changed().await.is_err() {
                break;
            }
        }
    })
    .await
}

// ---------- health ----------

async fn ready(State(state): State<SharedState>) -> Response {
    let ok = !state.stats.draining.load(Ordering::Relaxed)
        && state.stats.upstream_connected.load(Ordering::Relaxed);
    if ok {
        (StatusCode::OK, "ok").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready").into_response()
    }
}

async fn healthz(State(state): State<SharedState>) -> Response {
    Json(build_heartbeat(&state)).into_response()
}

// ---------- websocket ----------

#[derive(Deserialize)]
struct WsQuery {
    token: Option<String>,
    #[serde(rename = "_replay")]
    replay: Option<String>,
}

async fn websocket(
    State(state): State<SharedState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Query(query): Query<WsQuery>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let ip = client_ip(&headers, peer);
    let reject = |status: StatusCode, msg: &'static str, counter: &std::sync::atomic::AtomicU64| {
        state.stats.clients_rejected.fetch_add(1, Ordering::Relaxed);
        counter.fetch_add(1, Ordering::Relaxed);
        warn!(%ip, port = peer.port(), status = status.as_u16(), reason = msg, "handshake rejected");
        (status, msg).into_response()
    };
    let secret = query
        .token
        .clone()
        .or_else(|| bearer(&headers).map(str::to_owned))
        .unwrap_or_default();
    let Some(token_id) = state.auth.check(&secret) else {
        let msg = if secret.is_empty() {
            "missing token"
        } else {
            "invalid token"
        };
        return reject(StatusCode::UNAUTHORIZED, msg, &state.stats.rejected_token);
    };
    if state.stats.draining.load(Ordering::Relaxed) {
        return reject(
            StatusCode::SERVICE_UNAVAILABLE,
            "draining",
            &state.stats.rejected_draining,
        );
    }
    if state.hub.clients() >= state.config.max_clients {
        return reject(
            StatusCode::SERVICE_UNAVAILABLE,
            "node full",
            &state.stats.rejected_full,
        );
    }
    let offered = headers
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.split(',').any(|p| p.trim() == SUBPROTOCOL));
    if !offered {
        return reject(
            StatusCode::BAD_REQUEST,
            "Sec-WebSocket-Protocol: uma.pb.v1 is required",
            &state.stats.rejected_subprotocol,
        );
    }
    let replay = match parse_replay(query.replay.as_deref()) {
        Some(n) => n,
        None => {
            return reject(
                StatusCode::BAD_REQUEST,
                "invalid _replay (0..=4096 or all)",
                &state.stats.rejected_replay,
            );
        }
    };
    let port = peer.port();
    ws.protocols([SUBPROTOCOL])
        .on_upgrade(move |socket| session(socket, state, ip, port, token_id, replay))
}

pub fn parse_replay(value: Option<&str>) -> Option<usize> {
    match value.map(str::trim) {
        None | Some("") => Some(0),
        Some("all") => Some(MAX_REPLAY),
        Some(n) => n.parse::<usize>().ok().filter(|n| *n <= MAX_REPLAY),
    }
}

async fn session(
    socket: WebSocket,
    state: SharedState,
    ip: String,
    port: u16,
    token_id: String,
    replay: usize,
) {
    state.stats.clients_accepted.fetch_add(1, Ordering::Relaxed);
    let sub = state
        .hub
        .subscribe(replay, ip.clone(), port, Some(token_id.clone()));
    let id = sub.id;
    info!(client = id, %ip, port, token = %token_id, replay = sub.replay.len(), "client connected");
    let (sink, mut stream) = socket.split();
    let mut writer = tokio::spawn(write_loop(sink, sub));

    // Reader: discard data frames, treat any message as liveness, 90s dead.
    let reader = async {
        loop {
            match tokio::time::timeout(CLIENT_DEAD, stream.next()).await {
                Err(_) => break "idle 90s",
                Ok(None) => break "closed",
                Ok(Some(Err(_))) => break "read error",
                Ok(Some(Ok(Message::Close(_)))) => break "client close",
                Ok(Some(Ok(_))) => {}
            }
        }
    };
    let why = tokio::select! {
        why = reader => why,
        _ = &mut writer => "writer done",
    };
    state.hub.unsubscribe(id);
    // Give the writer a moment to flush queued frames and the close frame.
    if tokio::time::timeout(Duration::from_secs(2), &mut writer)
        .await
        .is_err()
    {
        writer.abort();
    }
    info!(client = id, %ip, port, why, "client disconnected");
}

async fn write_loop(mut sink: SplitSink<WebSocket, Message>, mut sub: Subscription) {
    for frame in std::mem::take(&mut sub.replay) {
        if send_frame(&mut sink, &frame).await.is_err() {
            return;
        }
    }
    let mut ping = tokio::time::interval(CLIENT_PING);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ping.tick().await;
    loop {
        tokio::select! {
            next = sub.rx.recv() => match next {
                Some(frame) => {
                    if send_frame(&mut sink, &frame).await.is_err() {
                        return;
                    }
                }
                None => break,
            },
            _ = ping.tick() => {
                if timed(sink.send(Message::Ping(Vec::new().into()))).await.is_err() {
                    return;
                }
            }
        }
    }
    let code = sub.close_code.load(Ordering::Relaxed);
    let reason = match code {
        1000 => "bye",
        1008 => "slow client or token revoked",
        1012 => "node draining",
        _ => "",
    };
    let _ = timed(sink.send(Message::Close(Some(CloseFrame {
        code,
        reason: reason.into(),
    }))))
    .await;
    // Let the close frame reach the peer before the socket is dropped.
    tokio::time::sleep(Duration::from_secs(1)).await;
}

async fn send_frame(
    sink: &mut SplitSink<WebSocket, Message>,
    frame: &Arc<Frame>,
) -> Result<(), ()> {
    timed(sink.send(Message::Binary(frame.bytes.clone()))).await
}

async fn timed<F: std::future::Future<Output = Result<(), axum::Error>>>(f: F) -> Result<(), ()> {
    tokio::time::timeout(WRITE_TIMEOUT, f)
        .await
        .map_err(|_| ())?
        .map_err(|_| ())
}

// ---------- admin ----------

async fn admin_clients(
    State(state): State<SharedState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    require_admin(&state, &headers)?;
    let now = super::now_ms();
    let clients: Vec<serde_json::Value> = state
        .hub
        .client_list()
        .into_iter()
        .map(|c| {
            serde_json::json!({
                "id": c.id, "ip": c.ip, "port": c.port, "token_id": c.token_id,
                "connected_at_ms": c.connected_at_ms,
                "connected_seconds": now.saturating_sub(c.connected_at_ms) / 1000,
            })
        })
        .collect();
    Ok(Json(serde_json::json!({
        "node_id": state.config.node_id,
        "clients": clients,
    })))
}

#[derive(Deserialize)]
struct RebalanceBody {
    count: usize,
}

async fn admin_rebalance(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Json(body): Json<RebalanceBody>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    require_admin(&state, &headers)?;
    if !(1..=100).contains(&body.count) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let released = state.hub.release(body.count, CLOSE_RESTART);
    info!(released, "admin rebalance");
    Ok(Json(serde_json::json!({"released": released})))
}

async fn admin_drain(
    State(state): State<SharedState>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<serde_json::Value>), StatusCode> {
    require_admin(&state, &headers)?;
    start_drain(state.clone(), true);
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({"draining": true, "clients": state.hub.clients()})),
    ))
}

fn require_admin(state: &SharedState, headers: &HeaderMap) -> Result<(), StatusCode> {
    let expected = state.config.admin_token.as_str();
    if expected.is_empty() || bearer(headers) != Some(expected) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(())
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|v| !v.is_empty())
}

fn client_ip(headers: &HeaderMap, peer: SocketAddr) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| peer.ip().to_string())
}

// ---------- drain ----------

/// One-way. Releases `drain_batch` clients every `drain_interval` with 1012
/// until none remain or `drain_timeout`; then exits the process if
/// `exit_after` (admin/SIGTERM) — console-initiated drains stay up so the
/// panel keeps seeing the node until systemd/ops decide.
pub fn start_drain(state: SharedState, exit_after: bool) {
    if state.stats.draining.swap(true, Ordering::SeqCst) {
        return;
    }
    tokio::spawn(async move {
        let clients = state.hub.clients();
        info!(clients, exit_after, "drain started");
        let deadline = tokio::time::Instant::now() + state.config.drain_timeout;
        loop {
            let remaining = state.hub.clients();
            if remaining == 0 {
                info!("drain complete");
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                warn!(remaining, "drain timed out");
                break;
            }
            let released = state.hub.release(state.config.drain_batch, CLOSE_RESTART);
            info!(released, remaining = remaining - released, "drain batch");
            tokio::time::sleep(state.config.drain_interval).await;
        }
        if exit_after {
            // give close frames a moment to flush
            tokio::time::sleep(Duration::from_millis(1500)).await;
            info!("drain finished; exiting");
            std::process::exit(0);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_parsing() {
        assert_eq!(parse_replay(None), Some(0));
        assert_eq!(parse_replay(Some("")), Some(0));
        assert_eq!(parse_replay(Some("5")), Some(5));
        assert_eq!(parse_replay(Some("4096")), Some(4096));
        assert_eq!(parse_replay(Some("all")), Some(4096));
        assert_eq!(parse_replay(Some("4097")), None);
        assert_eq!(parse_replay(Some("-1")), None);
        assert_eq!(parse_replay(Some("x")), None);
    }
}
