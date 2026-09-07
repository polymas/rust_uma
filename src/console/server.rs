//! 控制台 HTTP 路由。三种鉴权：节点心跳（Bearer node token）、管理（Bearer
//! admin token）、面板数据（?token= panel token）。公开的只有节点列表、
//! llms.txt、healthz 和面板壳。

use std::{collections::BTreeMap, net::SocketAddr};

use axum::{
    Json, Router,
    body::Body,
    extract::{
        ConnectInfo, DefaultBodyLimit, Path, Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tracing::{info, warn};

use super::{
    COMMIT, SharedState, VERSION,
    feed::FeedStatus,
    now_ms,
    registry::{Directive, Heartbeat, NodeView},
    tokens::TokenView,
    upstream::{UpstreamSnapshot, llms_text},
};

const CLUSTER_LLMS: &str = include_str!("../../internal/console/llms_cluster.txt");
const INDEX_HTML: &str = include_str!("../../internal/console/index.html");
const HEARTBEAT_BODY_LIMIT: usize = 64 * 1024;

pub fn router(state: SharedState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/healthz", get(healthz))
        .route("/llms.txt", get(llms))
        .route("/api/v1/nodes", get(public_nodes))
        .route(
            "/api/v1/nodes/heartbeat",
            post(heartbeat).layer(DefaultBodyLimit::max(HEARTBEAT_BODY_LIMIT)),
        )
        .route("/api/v1/panel", get(panel))
        .route("/api/v1/panel/ws", get(panel_ws))
        .route("/api/v1/admin/nodes", get(admin_nodes))
        .route("/api/v1/admin/nodes/{id}/{action}", post(admin_node_action))
        .route("/api/v1/admin/nodes/{id}/note", put(admin_node_note))
        .route("/api/v1/admin/nodes/{id}/release", post(admin_node_release))
        .route(
            "/api/v1/admin/balance",
            get(admin_balance).post(admin_balance_set),
        )
        .route(
            "/api/v1/admin/tokens",
            get(admin_tokens).post(admin_token_create),
        )
        .route(
            "/api/v1/admin/tokens/{id}/{action}",
            post(admin_token_action),
        )
        .route("/api/v1/admin/tokens/{id}/name", put(admin_token_rename))
        .route("/api/v1/admin/tokens/{id}", delete(admin_token_delete))
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

// ---------- 公开 ----------

async fn index() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .body(Body::from(INDEX_HTML))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
    serving_nodes: usize,
    known_nodes: usize,
    upstream_ok: bool,
    version: &'static str,
    commit: &'static str,
    uptime_s: u64,
}

async fn healthz(State(state): State<SharedState>) -> Json<Health> {
    let (serving, known) = state.registry.counts();
    Json(Health {
        status: "ok",
        serving_nodes: serving,
        known_nodes: known,
        upstream_ok: state.upstream.snapshot().ok,
        version: VERSION,
        commit: COMMIT,
        uptime_s: now_ms().saturating_sub(state.started_at_ms) / 1000,
    })
}

async fn llms(State(state): State<SharedState>) -> Response {
    let body = llms_text(&state, CLUSTER_LLMS).await;
    ([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], body).into_response()
}

async fn public_nodes(State(state): State<SharedState>) -> Response {
    let list = state.registry.public_list(state.config.list_ttl);
    let cache = format!("public, max-age={}", list.ttl_s);
    (
        [
            (header::CACHE_CONTROL, cache),
            (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*".to_owned()),
        ],
        Json(list),
    )
        .into_response()
}

// ---------- 节点心跳 ----------

async fn heartbeat(
    State(state): State<SharedState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<Directive>, ApiError> {
    require_bearer(&headers, &state.config.node_token)?;
    let hb: Heartbeat =
        serde_json::from_slice(&body).map_err(|_| ApiError::bad("invalid heartbeat json"))?;
    if hb.node_id.trim().is_empty() {
        return Err(ApiError::bad("node_id is required"));
    }
    let remote_ip = client_ip(&headers, peer);
    let edge_tokens_version = hb.tokens_version;
    let node_id = hb.node_id.clone();
    let outcome = state.registry.heartbeat(hb, remote_ip.clone());
    if outcome.first_seen {
        info!(node_id, remote_ip, "edge node registered");
    }
    if outcome.restarted {
        info!(node_id, "edge node restarted; cleared desired_drain");
    }
    if outcome.release > 0 {
        info!(
            node_id,
            release = outcome.release,
            reason = outcome.release_reason,
            "releasing clients"
        );
    }
    let (tokens_version, grants) = state.tokens.grants();
    let tokens = (edge_tokens_version != tokens_version).then_some(grants);
    Ok(Json(Directive {
        drain: outcome.desired_drain,
        drain_batch: 0,
        drain_interval_ms: 0,
        heartbeat_seconds: 0,
        release: outcome.release,
        tokens_version,
        tokens,
    }))
}

// ---------- 面板 ----------

#[derive(Deserialize)]
struct PanelQuery {
    token: Option<String>,
}

#[derive(Serialize)]
struct TokenUsage {
    #[serde(flatten)]
    token: TokenView,
    clients: u64,
    nodes: BTreeMap<String, u64>,
}

#[derive(Serialize)]
struct Panel {
    now_ms: u64,
    console: Health,
    upstream: UpstreamSnapshot,
    nodes: Vec<NodeView>,
    tokens: Vec<TokenUsage>,
    stale_after_ms: u64,
    feed: FeedStatus,
    settings: super::registry::Settings,
}

async fn panel(
    State(state): State<SharedState>,
    Query(query): Query<PanelQuery>,
) -> Result<Json<Panel>, ApiError> {
    if query.token.as_deref() != Some(state.config.panel_token.as_str()) {
        return Err(ApiError {
            status: StatusCode::FORBIDDEN,
            message: "invalid or missing token",
        });
    }
    let nodes = state.registry.admin_list();
    let mut usage: BTreeMap<String, (u64, BTreeMap<String, u64>)> = BTreeMap::new();
    for node in &nodes {
        for (token_id, count) in &node.heartbeat.clients_by_token {
            let entry = usage.entry(token_id.clone()).or_default();
            entry.0 += count;
            entry.1.insert(node.heartbeat.node_id.clone(), *count);
        }
    }
    let tokens = state
        .tokens
        .list()
        .into_iter()
        .map(|token| {
            let (clients, nodes) = usage.remove(&token.id).unwrap_or_default();
            TokenUsage {
                token,
                clients,
                nodes,
            }
        })
        .collect();
    let Json(console) = healthz(State(state.clone())).await;
    Ok(Json(Panel {
        now_ms: now_ms(),
        console,
        upstream: state.upstream.snapshot(),
        nodes,
        tokens,
        stale_after_ms: state.config.stale_after.as_millis() as u64,
        feed: state.feed.status(),
        settings: state.registry.settings(),
    }))
}

/// 面板实时推送：panel token 鉴权后，先发最近的帧再接实时；浏览器慢了就断
/// （Lagged），刷新页面即可。不是业务接口。
async fn panel_ws(
    State(state): State<SharedState>,
    Query(query): Query<PanelQuery>,
    ws: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    if query.token.as_deref() != Some(state.config.panel_token.as_str()) {
        return Err(ApiError {
            status: StatusCode::FORBIDDEN,
            message: "invalid or missing token",
        });
    }
    Ok(ws.on_upgrade(move |socket| panel_ws_session(socket, state)))
}

async fn panel_ws_session(mut socket: WebSocket, state: SharedState) {
    let (recent, mut rx) = state.feed.subscribe();
    for frame in recent {
        if socket.send(Message::Binary(frame)).await.is_err() {
            return;
        }
    }
    loop {
        tokio::select! {
            next = rx.recv() => match next {
                Ok(frame) => {
                    if socket.send(Message::Binary(frame)).await.is_err() {
                        return;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => return,
                Err(_) => return,
            },
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                _ => {}
            }
        }
    }
}

// ---------- 管理：节点 ----------

async fn admin_nodes(
    State(state): State<SharedState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_bearer(&headers, &state.config.admin_token)?;
    Ok(Json(serde_json::json!({
        "nodes": state.registry.admin_list(),
        "now_ms": now_ms(),
    })))
}

async fn admin_node_action(
    State(state): State<SharedState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path((id, action)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_bearer(&headers, &state.config.admin_token)?;
    let known = match action.as_str() {
        "drain" => state.registry.set_drain(&id, true),
        "undrain" => state.registry.set_drain(&id, false),
        "disable" => state.registry.set_disabled(&id, true)?,
        "enable" => state.registry.set_disabled(&id, false)?,
        "forget" => state.registry.forget(&id)?,
        _ => return Err(ApiError::bad("unknown action")),
    };
    if !known {
        return Err(ApiError::not_found("unknown node"));
    }
    info!(node_id = %id, action, from = %client_ip(&headers, peer), "admin node action");
    Ok(Json(
        serde_json::json!({"ok": true, "node_id": id, "action": action}),
    ))
}

#[derive(Deserialize)]
struct NoteBody {
    note: String,
}

#[derive(Deserialize)]
struct ReleaseBody {
    count: u64,
}

/// 人工让某节点释放 count 个连接（1012），按 release_batch 分批经心跳下发；
/// count=0 取消未下发的部分。
async fn admin_node_release(
    State(state): State<SharedState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<ReleaseBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_bearer(&headers, &state.config.admin_token)?;
    if body.count > 100_000 {
        return Err(ApiError::bad("count too large"));
    }
    if !state.registry.request_release(&id, body.count) {
        return Err(ApiError::not_found("unknown node"));
    }
    info!(node_id = %id, count = body.count, from = %client_ip(&headers, peer), "admin release requested");
    Ok(Json(
        serde_json::json!({"ok": true, "node_id": id, "pending_release": body.count}),
    ))
}

async fn admin_balance(
    State(state): State<SharedState>,
    headers: HeaderMap,
) -> Result<Json<super::registry::Settings>, ApiError> {
    require_bearer(&headers, &state.config.admin_token)?;
    Ok(Json(state.registry.settings()))
}

#[derive(Deserialize)]
struct BalanceBody {
    enabled: bool,
}

async fn admin_balance_set(
    State(state): State<SharedState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<BalanceBody>,
) -> Result<Json<super::registry::Settings>, ApiError> {
    require_bearer(&headers, &state.config.admin_token)?;
    state.registry.set_auto_balance(body.enabled)?;
    info!(enabled = body.enabled, from = %client_ip(&headers, peer), "auto-balance toggled");
    Ok(Json(state.registry.settings()))
}

async fn admin_node_note(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<NoteBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_bearer(&headers, &state.config.admin_token)?;
    if !state.registry.set_note(&id, &body.note)? {
        return Err(ApiError::not_found("unknown node"));
    }
    Ok(Json(serde_json::json!({"ok": true})))
}

// ---------- 管理：token ----------

async fn admin_tokens(
    State(state): State<SharedState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_bearer(&headers, &state.config.admin_token)?;
    Ok(Json(serde_json::json!({
        "version": state.tokens.version(),
        "tokens": state.tokens.list(),
    })))
}

#[derive(Deserialize)]
struct NameBody {
    name: String,
}

async fn admin_token_create(
    State(state): State<SharedState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<NameBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_bearer(&headers, &state.config.admin_token)?;
    if body.name.trim().is_empty() {
        return Err(ApiError::bad("name is required"));
    }
    let token = state.tokens.create(&body.name)?;
    info!(token_id = %token.id, name = %token.name, from = %client_ip(&headers, peer), "token created");
    Ok(Json(serde_json::json!({
        "id": token.id,
        "name": token.name,
        "secret": token.secret,
        "enabled": token.enabled,
        "created_at_ms": token.created_at_ms,
        "version": state.tokens.version(),
    })))
}

async fn admin_token_action(
    State(state): State<SharedState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path((id, action)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_bearer(&headers, &state.config.admin_token)?;
    let known = match action.as_str() {
        "enable" => state.tokens.set_enabled(&id, true)?,
        "disable" => state.tokens.set_enabled(&id, false)?,
        _ => return Err(ApiError::bad("unknown action")),
    };
    if !known {
        return Err(ApiError::not_found("unknown token"));
    }
    info!(token_id = %id, action, from = %client_ip(&headers, peer), "admin token action");
    Ok(Json(
        serde_json::json!({"ok": true, "version": state.tokens.version()}),
    ))
}

async fn admin_token_rename(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<NameBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_bearer(&headers, &state.config.admin_token)?;
    if !state.tokens.rename(&id, &body.name)? {
        return Err(ApiError::not_found("unknown token"));
    }
    Ok(Json(serde_json::json!({"ok": true})))
}

async fn admin_token_delete(
    State(state): State<SharedState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_bearer(&headers, &state.config.admin_token)?;
    if !state.tokens.delete(&id)? {
        return Err(ApiError::not_found("unknown token"));
    }
    info!(token_id = %id, from = %client_ip(&headers, peer), "token deleted");
    Ok(Json(
        serde_json::json!({"ok": true, "version": state.tokens.version()}),
    ))
}

// ---------- 通用 ----------

fn require_bearer(headers: &HeaderMap, expected: &str) -> Result<(), ApiError> {
    let provided = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim);
    if expected.is_empty() || provided != Some(expected) {
        return Err(ApiError {
            status: StatusCode::UNAUTHORIZED,
            message: "unauthorized",
        });
    }
    Ok(())
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

pub struct ApiError {
    status: StatusCode,
    message: &'static str,
}

impl ApiError {
    fn bad(message: &'static str) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message,
        }
    }
    fn not_found(message: &'static str) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message,
        }
    }
}

impl From<std::io::Error> for ApiError {
    fn from(error: std::io::Error) -> Self {
        warn!(%error, "console persistence failed");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "persistence failed",
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
