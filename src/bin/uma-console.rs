use std::sync::Arc;

use rust_uma::console::{
    COMMIT, ConsoleConfig, ConsoleState, VERSION,
    feed::{Feed, run_feed},
    now_ms,
    registry::Registry,
    server,
    tokens::TokenStore,
    upstream::{UpstreamCache, run_upstream_poller},
};
use tokio::sync::watch;
use tracing::{error, info};
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
        error!(%error, "uma-console stopped with an error");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = ConsoleConfig::from_env()?;
    let registry = Registry::open(config.data_dir.join("nodes.json"), config.stale_after)?;
    let tokens = TokenStore::open(config.data_dir.join("tokens.json"))?;
    info!(
        version = VERSION,
        commit = COMMIT,
        listen = %config.listen_addr,
        upstream = %config.upstream_http,
        data_dir = %config.data_dir.display(),
        tokens = tokens.list().len(),
        tokens_version = tokens.version(),
        "uma-console starting"
    );
    let state = Arc::new(ConsoleState {
        config,
        registry,
        tokens,
        upstream: UpstreamCache::default(),
        feed: Feed::default(),
        http: reqwest::Client::builder().build()?,
        started_at_ms: now_ms(),
    });

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(run_upstream_poller(state.clone()));
    tokio::spawn(run_feed(state.clone()));
    let server = tokio::spawn(server::serve(state, shutdown_rx));

    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("received SIGINT"),
        _ = terminate() => info!("received SIGTERM"),
    }
    let _ = shutdown_tx.send(true);
    server.await??;
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
