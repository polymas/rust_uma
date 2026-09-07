use std::sync::Arc;

use rust_uma::edge::{
    COMMIT, EdgeConfig, EdgeState, EdgeStats, VERSION, auth::TokenSet, heartbeat::run_heartbeat,
    hub::Hub, now_ms, server, upstream::run_upstream,
};
use tokio::sync::watch;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .compact()
        .init();
    if let Err(error) = run().await {
        error!(%error, "uma-edge stopped with an error");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = EdgeConfig::from_env()?;
    let stats = Arc::new(EdgeStats::default());
    info!(
        version = VERSION,
        commit = COMMIT,
        node_id = %config.node_id,
        listen = %config.listen_addr,
        upstream = %config.upstream_url,
        console = config.console_url.as_deref().unwrap_or("-"),
        ring = config.ring_frames,
        queue = config.client_queue,
        max_clients = config.max_clients,
        static_tokens = config.static_client_tokens.len(),
        "uma-edge starting"
    );
    if config.admin_token.is_empty() {
        warn!("EDGE_ADMIN_TOKEN empty: admin endpoints disabled");
    }
    let hub = Hub::new(config.ring_frames, config.client_queue, stats.clone());
    let auth = TokenSet::new(config.static_client_tokens.clone());
    let state = Arc::new(EdgeState {
        config,
        stats,
        hub,
        auth,
        started_at_ms: now_ms(),
        http: reqwest::Client::builder().build()?,
    });

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(run_upstream(state.clone()));
    tokio::spawn(run_heartbeat(state.clone()));
    let http = tokio::spawn(server::serve(state.clone(), shutdown_rx));

    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("received SIGINT"),
        _ = terminate() => info!("received SIGTERM"),
    }
    // Drain (1012) then exit; new handshakes get 503 immediately.
    server::start_drain(state.clone(), true);
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(
        state.config.drain_timeout + std::time::Duration::from_secs(5),
        http,
    )
    .await;
    Ok(())
}

async fn terminate() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut sig) = signal(SignalKind::terminate()) {
            sig.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    }
    #[cfg(not(unix))]
    std::future::pending::<()>().await;
}
