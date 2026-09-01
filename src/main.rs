use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use rust_uma::{
    api::{AppState, serve},
    config::Config,
    enrichment::{
        Catalog, GammaClient, PersistHandle, RepairHandle, run_catalog_persister, run_catalog_sync,
        run_repair_worker,
    },
    hub::{EventHub, FrameHub},
    pipeline::{Processor, run_batcher},
    rpc::run_rpc_loop,
    stats::Stats,
    storage::{Storage, run_storage_writer},
};
use tokio::sync::{mpsc, watch};
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
        error!(%error, "rust_uma stopped with an error");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(Config::from_env()?);
    let storage = Storage::open(config.data_dir.clone())?;
    let catalog_rows = storage.load_catalog()?;
    let catalog = Arc::new(Catalog::new(catalog_rows));
    let events = Arc::new(EventHub::new(config.event_ring_capacity));
    let recovered = storage.load_events(config.event_ring_capacity)?;
    let mut initial_sequence = 0;
    for event in recovered {
        initial_sequence = initial_sequence.max(event.sequence);
        events.insert(event);
    }
    let frames = Arc::new(FrameHub::new(config.frame_ring_capacity));
    let stats = Arc::new(Stats::default());
    stats
        .catalog_markets
        .store(catalog.len() as u64, Ordering::Relaxed);
    stats
        .latest_block
        .store(storage.load_checkpoint()?, Ordering::Relaxed);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (batch_tx, batch_rx) = mpsc::channel(config.live_buffer.max(1));
    let (storage_tx, storage_rx) = mpsc::channel(config.live_buffer.max(1));
    let (repair, repair_rx) = RepairHandle::channel(4096);
    let (persist, persist_rx) = PersistHandle::channel();
    let gamma = GammaClient::new(config.gamma_base_url.clone())?;

    let processor = Arc::new(Processor::new(
        config.clone(),
        catalog.clone(),
        repair.clone(),
        events.clone(),
        batch_tx,
        storage_tx,
        stats.clone(),
        initial_sequence,
    ));

    let tasks = vec![
        tokio::spawn(run_storage_writer(
            storage.clone(),
            events.clone(),
            config.event_ring_capacity,
            storage_rx,
            shutdown_rx.clone(),
        )),
        tokio::spawn(run_batcher(
            config.clone(),
            frames.clone(),
            stats.clone(),
            batch_rx,
            shutdown_rx.clone(),
        )),
        tokio::spawn(run_repair_worker(
            gamma.clone(),
            catalog.clone(),
            repair,
            repair_rx,
            persist.clone(),
            stats.clone(),
            shutdown_rx.clone(),
        )),
        tokio::spawn(run_catalog_sync(
            config.clone(),
            gamma,
            catalog.clone(),
            persist,
            stats.clone(),
            shutdown_rx.clone(),
        )),
        tokio::spawn(run_catalog_persister(
            storage.clone(),
            catalog.clone(),
            persist_rx,
            shutdown_rx.clone(),
        )),
        tokio::spawn(run_rpc_loop(
            config.clone(),
            storage,
            processor,
            stats.clone(),
            shutdown_rx.clone(),
        )),
    ];

    let state = AppState {
        config: config.clone(),
        events,
        frames,
        catalog,
        stats,
    };
    info!(address=%config.api_addr, recovered_events=initial_sequence, "rust_uma API listening");
    tokio::select! {
        result = serve(state, shutdown_rx.clone()) => result?,
        _ = shutdown_signal() => info!("shutdown signal received"),
    }
    let _ = shutdown_tx.send(true);
    for task in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    }
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
