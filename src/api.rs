use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use axum::{
    Json, Router,
    body::Body,
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket},
    },
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::{
    config::Config,
    enrichment::Catalog,
    hub::{EventHub, FrameHub, FrameReadError},
    stats::Stats,
};

const SUBPROTOCOL: &str = "uma.pb.v1";

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub events: Arc<EventHub>,
    pub frames: Arc<FrameHub>,
    pub catalog: Arc<Catalog>,
    pub stats: Arc<Stats>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/metrics", get(metrics))
        .route("/llms.txt", get(llms))
        .route("/uma/v1/ws", get(websocket))
        .route("/dashboard", get(dashboard_page))
        .route("/uma/v1/dashboard-data", get(dashboard_data))
        .with_state(state)
}

pub async fn serve(
    state: AppState,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), std::io::Error> {
    let listener = tokio::net::TcpListener::bind(state.config.api_addr).await?;
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async move {
            while !*shutdown.borrow() {
                if shutdown.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    rpc_connected: bool,
    rpc_sources_connected: u64,
    rpc_sources_configured: usize,
    rpc_reconnects_total: u64,
    rpc_logs_received_total: u64,
    events_decoded_total: u64,
    decode_errors_total: u64,
    duplicates_total: u64,
    enrichment_hits_total: u64,
    enrichment_hits_via_market_id_total: u64,
    enrichment_misses_total: u64,
    catalog_markets: u64,
    catalog_reconcile_gaps_closed_total: u64,
    last_upstream_received_at_us: u64,
    last_broadcast_at_us: u64,
    subscribers: u64,
    slow_clients_dropped_total: u64,
    storage_queue_dropped_total: u64,
    latest_block: u64,
    event_ring_oldest_sequence: u64,
    event_ring_latest_sequence: u64,
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let rpc_connected = state.stats.rpc_connected.load(Ordering::Relaxed);
    let (oldest, latest) = state.events.bounds();
    Json(HealthResponse {
        status: if rpc_connected { "ok" } else { "degraded" },
        rpc_connected,
        rpc_sources_connected: state.stats.rpc_sources_connected.load(Ordering::Relaxed),
        rpc_sources_configured: state.config.wss_rpc_urls.len(),
        rpc_reconnects_total: state.stats.rpc_reconnects.load(Ordering::Relaxed),
        rpc_logs_received_total: state.stats.rpc_logs_received.load(Ordering::Relaxed),
        events_decoded_total: state.stats.events_decoded.load(Ordering::Relaxed),
        decode_errors_total: state.stats.decode_errors.load(Ordering::Relaxed),
        duplicates_total: state.stats.duplicates.load(Ordering::Relaxed),
        enrichment_hits_total: state.stats.enrichment_hits.load(Ordering::Relaxed),
        enrichment_hits_via_market_id_total: state
            .stats
            .enrichment_hits_via_market_id
            .load(Ordering::Relaxed),
        enrichment_misses_total: state.stats.enrichment_misses.load(Ordering::Relaxed),
        catalog_markets: state.stats.catalog_markets.load(Ordering::Relaxed),
        catalog_reconcile_gaps_closed_total: state
            .stats
            .catalog_reconcile_gaps_closed
            .load(Ordering::Relaxed),
        last_upstream_received_at_us: state
            .stats
            .last_upstream_received_at_us
            .load(Ordering::Relaxed),
        last_broadcast_at_us: state.stats.last_broadcast_at_us.load(Ordering::Relaxed),
        subscribers: state.stats.subscribers.load(Ordering::Relaxed),
        slow_clients_dropped_total: state.stats.slow_clients_dropped.load(Ordering::Relaxed),
        storage_queue_dropped_total: state.stats.storage_queue_dropped.load(Ordering::Relaxed),
        latest_block: state.stats.latest_block.load(Ordering::Relaxed),
        event_ring_oldest_sequence: oldest,
        event_ring_latest_sequence: latest,
    })
}

async fn metrics(State(state): State<AppState>) -> Response {
    let (oldest, latest) = state.events.bounds();
    let values = [
        (
            "rust_uma_rpc_connected",
            state.stats.rpc_connected.load(Ordering::Relaxed) as u64,
        ),
        (
            "rust_uma_rpc_sources_connected",
            state.stats.rpc_sources_connected.load(Ordering::Relaxed),
        ),
        (
            "rust_uma_rpc_sources_configured",
            state.config.wss_rpc_urls.len() as u64,
        ),
        (
            "rust_uma_rpc_reconnects_total",
            state.stats.rpc_reconnects.load(Ordering::Relaxed),
        ),
        (
            "rust_uma_rpc_logs_received_total",
            state.stats.rpc_logs_received.load(Ordering::Relaxed),
        ),
        (
            "rust_uma_events_decoded_total",
            state.stats.events_decoded.load(Ordering::Relaxed),
        ),
        (
            "rust_uma_decode_errors_total",
            state.stats.decode_errors.load(Ordering::Relaxed),
        ),
        (
            "rust_uma_duplicates_total",
            state.stats.duplicates.load(Ordering::Relaxed),
        ),
        (
            "rust_uma_enrichment_hits_total",
            state.stats.enrichment_hits.load(Ordering::Relaxed),
        ),
        (
            "rust_uma_enrichment_hits_via_market_id_total",
            state
                .stats
                .enrichment_hits_via_market_id
                .load(Ordering::Relaxed),
        ),
        (
            "rust_uma_enrichment_misses_total",
            state.stats.enrichment_misses.load(Ordering::Relaxed),
        ),
        ("rust_uma_catalog_markets", state.catalog.len() as u64),
        (
            "rust_uma_catalog_reconcile_gaps_closed_total",
            state
                .stats
                .catalog_reconcile_gaps_closed
                .load(Ordering::Relaxed),
        ),
        (
            "rust_uma_subscribers",
            state.stats.subscribers.load(Ordering::Relaxed),
        ),
        (
            "rust_uma_slow_clients_dropped_total",
            state.stats.slow_clients_dropped.load(Ordering::Relaxed),
        ),
        ("rust_uma_event_ring_oldest_sequence", oldest),
        ("rust_uma_event_ring_latest_sequence", latest),
    ];
    let body = values
        .into_iter()
        .map(|(name, value)| format!("{name} {value}\n"))
        .collect::<String>();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(Body::from(body))
        .expect("metrics response")
}

/// Static shell for the read-only ops dashboard. Not gated on its own — it
/// contains no data, only JS that calls `dashboard_data` with the token from
/// its own query string, so an unauthorized visitor sees an empty page that
/// fails to load, not a leak.
async fn dashboard_page() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .body(Body::from(include_str!("../internal/api/dashboard.html")))
        .expect("dashboard response")
}

#[derive(Deserialize)]
struct DashboardQuery {
    token: Option<String>,
}

#[derive(Serialize)]
struct DashboardData {
    server_time_us: u64,
    rpc_connected: bool,
    rpc_sources_connected: u64,
    rpc_sources_configured: usize,
    rpc_reconnects_total: u64,
    rpc_logs_received_total: u64,
    rpc_bytes_received_total: u64,
    rpc_bytes_sent_total: u64,
    events_decoded_total: u64,
    decode_errors_total: u64,
    duplicates_total: u64,
    enrichment_hits_total: u64,
    enrichment_hits_via_market_id_total: u64,
    enrichment_misses_total: u64,
    /// Hits within the trailing `RECENT_ENRICHMENT_WINDOW` (see pipeline.rs)
    /// — a live complement to the all-time totals above.
    enrichment_recent_hits: u64,
    /// Size of that trailing window (<=1000; smaller right after startup).
    enrichment_recent_total: u64,
    catalog_markets: u64,
    catalog_reconcile_gaps_closed_total: u64,
    last_upstream_received_at_us: u64,
    last_broadcast_at_us: u64,
    subscribers: u64,
    ws_frames_sent_total: u64,
    ws_bytes_sent_total: u64,
    slow_clients_dropped_total: u64,
    storage_queue_dropped_total: u64,
    latest_block: u64,
    event_ring_oldest_sequence: u64,
    event_ring_latest_sequence: u64,
}

/// Requires `?token=` to exactly match `DASHBOARD_TOKEN`. An unset
/// `DASHBOARD_TOKEN` closes this route entirely rather than defaulting open.
fn check_dashboard_token(state: &AppState, provided: Option<&str>) -> Result<(), ApiError> {
    let expected = state.config.dashboard_token.as_deref();
    match (expected, provided) {
        (Some(expected), Some(provided)) if !expected.is_empty() && expected == provided => Ok(()),
        _ => Err(ApiError {
            status: StatusCode::FORBIDDEN,
            message: "invalid or missing token",
        }),
    }
}

async fn dashboard_data(
    State(state): State<AppState>,
    Query(query): Query<DashboardQuery>,
) -> Result<Json<DashboardData>, ApiError> {
    check_dashboard_token(&state, query.token.as_deref())?;
    let (oldest, latest) = state.events.bounds();
    Ok(Json(DashboardData {
        server_time_us: crate::wire::now_us(),
        rpc_connected: state.stats.rpc_connected.load(Ordering::Relaxed),
        rpc_sources_connected: state.stats.rpc_sources_connected.load(Ordering::Relaxed),
        rpc_sources_configured: state.config.wss_rpc_urls.len(),
        rpc_reconnects_total: state.stats.rpc_reconnects.load(Ordering::Relaxed),
        rpc_logs_received_total: state.stats.rpc_logs_received.load(Ordering::Relaxed),
        rpc_bytes_received_total: state.stats.rpc_bytes_received.load(Ordering::Relaxed),
        rpc_bytes_sent_total: state.stats.rpc_bytes_sent.load(Ordering::Relaxed),
        events_decoded_total: state.stats.events_decoded.load(Ordering::Relaxed),
        decode_errors_total: state.stats.decode_errors.load(Ordering::Relaxed),
        duplicates_total: state.stats.duplicates.load(Ordering::Relaxed),
        enrichment_hits_total: state.stats.enrichment_hits.load(Ordering::Relaxed),
        enrichment_hits_via_market_id_total: state
            .stats
            .enrichment_hits_via_market_id
            .load(Ordering::Relaxed),
        enrichment_misses_total: state.stats.enrichment_misses.load(Ordering::Relaxed),
        enrichment_recent_hits: state.stats.enrichment_recent_hits.load(Ordering::Relaxed),
        enrichment_recent_total: state.stats.enrichment_recent_total.load(Ordering::Relaxed),
        catalog_markets: state.catalog.len() as u64,
        catalog_reconcile_gaps_closed_total: state
            .stats
            .catalog_reconcile_gaps_closed
            .load(Ordering::Relaxed),
        last_upstream_received_at_us: state
            .stats
            .last_upstream_received_at_us
            .load(Ordering::Relaxed),
        last_broadcast_at_us: state.stats.last_broadcast_at_us.load(Ordering::Relaxed),
        subscribers: state.stats.subscribers.load(Ordering::Relaxed),
        ws_frames_sent_total: state.stats.ws_frames_sent.load(Ordering::Relaxed),
        ws_bytes_sent_total: state.stats.ws_bytes_sent.load(Ordering::Relaxed),
        slow_clients_dropped_total: state.stats.slow_clients_dropped.load(Ordering::Relaxed),
        storage_queue_dropped_total: state.stats.storage_queue_dropped.load(Ordering::Relaxed),
        latest_block: state.stats.latest_block.load(Ordering::Relaxed),
        event_ring_oldest_sequence: oldest,
        event_ring_latest_sequence: latest,
    }))
}

async fn llms() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(include_str!("../internal/api/llms.txt")))
        .expect("llms response")
}

#[derive(Deserialize)]
struct WsQuery {
    after_sequence: Option<u64>,
}

async fn websocket(
    State(state): State<AppState>,
    Query(query): Query<WsQuery>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let offered = headers
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.split(',').any(|item| item.trim() == SUBPROTOCOL));
    if !offered {
        return (
            StatusCode::BAD_REQUEST,
            "Sec-WebSocket-Protocol: uma.pb.v1 is required",
        )
            .into_response();
    }
    ws.protocols([SUBPROTOCOL])
        .on_upgrade(move |socket| websocket_session(socket, state, query.after_sequence))
}

async fn websocket_session(mut socket: WebSocket, state: AppState, requested_after: Option<u64>) {
    state.stats.subscribers.fetch_add(1, Ordering::Relaxed);
    let _guard = SubscriberGuard(state.stats.clone());
    let mut notifications = state.frames.subscribe();
    let mut after = requested_after.unwrap_or_else(|| state.frames.latest_sequence());
    let mut heartbeat = tokio::time::interval(Duration::from_secs(20));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        match send_available(&mut socket, &state, &mut after).await {
            Ok(()) => {}
            Err(FrameReadError::Lagged) => {
                state
                    .stats
                    .slow_clients_dropped
                    .fetch_add(1, Ordering::Relaxed);
                let _ = socket
                    .send(Message::Close(Some(CloseFrame {
                        code: 1013,
                        reason: "cursor older than frame ring".into(),
                    })))
                    .await;
                break;
            }
        }
        tokio::select! {
            changed = notifications.changed() => if changed.is_err() { break; },
            _ = heartbeat.tick() => {
                if timed_send(&mut socket, Message::Ping(Vec::new().into()), state.config.ws_write_timeout).await.is_err() {
                    break;
                }
            }
            message = socket.recv() => match message {
                Some(Ok(Message::Ping(payload))) => {
                    if timed_send(&mut socket, Message::Pong(payload), state.config.ws_write_timeout).await.is_err() { break; }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                _ => {}
            }
        }
    }
}

async fn send_available(
    socket: &mut WebSocket,
    state: &AppState,
    after: &mut u64,
) -> Result<(), FrameReadError> {
    for frame in state.frames.after(*after)? {
        let frame_len = frame.bytes.len() as u64;
        if timed_send(
            socket,
            Message::Binary(frame.bytes.clone()),
            state.config.ws_write_timeout,
        )
        .await
        .is_err()
        {
            return Err(FrameReadError::Lagged);
        }
        state.stats.ws_frames_sent.fetch_add(1, Ordering::Relaxed);
        state
            .stats
            .ws_bytes_sent
            .fetch_add(frame_len, Ordering::Relaxed);
        *after = (*after).max(frame.last_sequence);
    }
    Ok(())
}

async fn timed_send(socket: &mut WebSocket, message: Message, timeout: Duration) -> Result<(), ()> {
    tokio::time::timeout(timeout, socket.send(message))
        .await
        .map_err(|_| ())?
        .map_err(|_| ())
}

struct SubscriberGuard(Arc<Stats>);

impl Drop for SubscriberGuard {
    fn drop(&mut self) {
        self.0.subscribers.fetch_sub(1, Ordering::Relaxed);
    }
}

struct ApiError {
    status: StatusCode,
    message: &'static str,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({"error": self.message})),
        )
            .into_response()
    }
}
