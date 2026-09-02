use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use axum::{
    Json, Router,
    body::Body,
    extract::{
        Path, Query, State, WebSocketUpgrade,
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
    model::{EventKind, EventRecord, MarketEnrichment, hex_prefixed, uint256_decimal},
    stats::Stats,
    uma::events::decode_fixed,
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
        .route("/uma/v1/events", get(events))
        .route(
            "/uma/v1/events/{transaction_hash}/{log_index}",
            get(event_lookup),
        )
        .route("/uma/v1/markets/{condition_id}", get(market_lookup))
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
struct EventQuery {
    #[serde(default)]
    after_sequence: u64,
    #[serde(default = "default_limit")]
    limit: usize,
    event_type: Option<String>,
}

fn default_limit() -> usize {
    100
}

#[derive(Serialize)]
struct EventListResponse {
    data: Vec<EventDto>,
    count: usize,
    oldest_sequence: u64,
    latest_sequence: u64,
    next_sequence: Option<u64>,
}

async fn events(
    State(state): State<AppState>,
    Query(query): Query<EventQuery>,
) -> Result<Json<EventListResponse>, ApiError> {
    let kind = parse_kind(query.event_type.as_deref())?;
    let limit = query.limit.clamp(1, 500);
    let records = state.events.query(query.after_sequence, limit, kind);
    let data = records
        .iter()
        .map(|event| event_dto(event, &state.catalog))
        .collect::<Vec<_>>();
    let next_sequence = records.last().map(|event| event.sequence);
    let (oldest_sequence, latest_sequence) = state.events.bounds();
    Ok(Json(EventListResponse {
        count: data.len(),
        data,
        oldest_sequence,
        latest_sequence,
        next_sequence,
    }))
}

async fn event_lookup(
    State(state): State<AppState>,
    Path((transaction_hash, log_index)): Path<(String, u32)>,
) -> Result<Json<EventDto>, ApiError> {
    let hash = decode_fixed::<32>(&transaction_hash, "transaction_hash")
        .map_err(|_| ApiError::bad_request("invalid transaction hash"))?;
    let event = state
        .events
        .find(&hash, log_index)
        .ok_or_else(ApiError::not_found)?;
    Ok(Json(event_dto(&event, &state.catalog)))
}

async fn market_lookup(
    State(state): State<AppState>,
    Path(condition_id): Path<String>,
) -> Result<Json<MarketDto>, ApiError> {
    let condition_id = decode_fixed::<32>(&condition_id, "condition_id")
        .map_err(|_| ApiError::bad_request("invalid condition ID"))?;
    let market = state
        .catalog
        .get(&condition_id)
        .ok_or_else(ApiError::not_found)?;
    Ok(Json(market_dto(&market)))
}

#[derive(Serialize)]
struct EventDto {
    sequence: u64,
    event_type: &'static str,
    block_number: u64,
    block_hash: String,
    transaction_hash: String,
    transaction_index: Option<u32>,
    log_index: u32,
    market_id: u64,
    condition_id: String,
    token_ids: Vec<String>,
    tag_ids: Vec<u32>,
    price_raw: String,
    requester: String,
    proposer: String,
    disputer: Option<String>,
    upstream_received_at_us: u64,
    /// 0 if this event hasn't been through a broadcast batch yet (e.g. a
    /// storage-replayed record from before this field existed). Dashboard-only,
    /// not part of the WSS wire schema — see `EventRecord::broadcast_at_us`.
    broadcast_at_us: u64,
    removed: bool,
    enrichment_status: &'static str,
    contract_address: String,
    identifier: String,
    request_timestamp: u64,
    question_id: String,
    question: String,
    resolution: ResolutionDto,
    initializer: Option<String>,
    expiration_timestamp: Option<u64>,
    currency: Option<String>,
}

#[derive(Serialize)]
struct ResolutionDto {
    p1: Option<String>,
    p2: Option<String>,
    p3: Option<String>,
    p4: Option<String>,
}

#[derive(Serialize)]
struct MarketDto {
    market_id: u64,
    condition_id: String,
    token_ids: Vec<String>,
    tag_ids: Vec<u32>,
}

fn event_dto(event: &EventRecord, catalog: &Catalog) -> EventDto {
    let chain = event.event.chain();
    let request = event.event.request();
    let ancillary = &request.ancillary;
    // Re-resolve against the current catalog (it may have grown since this
    // event was first processed), preferring market_id the same way the hot
    // path does, then fall back to the snapshot captured at processing time.
    let enrichment = catalog
        .resolve(request.ancillary.market_id, &request.condition_id)
        .or_else(|| event.enrichment.clone());
    let condition_id = enrichment
        .as_ref()
        .map(|value| value.condition_id)
        .unwrap_or(request.condition_id);
    let (expiration_timestamp, currency) = match &event.event {
        crate::uma::events::common::UmaEvent::ProposePrice(value) => (
            Some(value.expiration_timestamp),
            Some(hex_prefixed(&value.currency)),
        ),
        crate::uma::events::common::UmaEvent::DisputePrice(_) => (None, None),
    };
    EventDto {
        sequence: event.sequence,
        event_type: event.event.kind().as_str(),
        block_number: chain.block_number,
        block_hash: hex_prefixed(&chain.block_hash),
        transaction_hash: hex_prefixed(&chain.transaction_hash),
        transaction_index: chain.transaction_index,
        log_index: chain.log_index,
        market_id: event.event.market_id(),
        condition_id: hex_prefixed(&condition_id),
        token_ids: enrichment
            .as_ref()
            .map(|value| value.token_ids.iter().map(uint256_decimal).collect())
            .unwrap_or_default(),
        tag_ids: enrichment
            .as_ref()
            .map(|value| value.tag_ids.clone())
            .unwrap_or_default(),
        price_raw: hex_prefixed(&request.proposed_price),
        requester: hex_prefixed(&request.requester),
        proposer: hex_prefixed(&request.proposer),
        disputer: event.event.disputer().map(|value| hex_prefixed(value)),
        upstream_received_at_us: chain.upstream_received_at_us,
        broadcast_at_us: event.broadcast_at_us(),
        removed: chain.removed,
        enrichment_status: if enrichment.is_some() { "hit" } else { "miss" },
        contract_address: hex_prefixed(&chain.contract_address),
        identifier: hex_prefixed(&request.identifier),
        request_timestamp: request.timestamp,
        question_id: hex_prefixed(&ancillary.question_id),
        question: ancillary.question.clone(),
        resolution: ResolutionDto {
            p1: ancillary.resolution.p1.clone(),
            p2: ancillary.resolution.p2.clone(),
            p3: ancillary.resolution.p3.clone(),
            p4: ancillary.resolution.p4.clone(),
        },
        initializer: ancillary
            .initializer
            .as_ref()
            .map(|value| hex_prefixed(value)),
        expiration_timestamp,
        currency,
    }
}

fn market_dto(market: &MarketEnrichment) -> MarketDto {
    MarketDto {
        market_id: market.market_id,
        condition_id: hex_prefixed(&market.condition_id),
        token_ids: market.token_ids.iter().map(uint256_decimal).collect(),
        tag_ids: market.tag_ids.clone(),
    }
}

fn parse_kind(value: Option<&str>) -> Result<Option<EventKind>, ApiError> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(None),
        Some("propose") => Ok(Some(EventKind::Propose)),
        Some("dispute") => Ok(Some(EventKind::Dispute)),
        Some(_) => Err(ApiError::bad_request(
            "event_type must be propose or dispute",
        )),
    }
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

impl ApiError {
    fn bad_request(message: &'static str) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message,
        }
    }

    fn not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: "not found",
        }
    }
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
