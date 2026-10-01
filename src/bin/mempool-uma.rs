//! mempool-uma：和 rust-uma 同一套富化 / 编码 / WSS，事件来源换成内存池 pending 交易。
//!
//! 与 `main.rs`（rust-uma）的差异只有三处，其余逐行对应：
//! 1. 不跑 `run_rpc_loop`（链上日志订阅 + 补拉），改跑 `mempool::feed::run_pending_feed`；
//! 2. 不维护 uma.cursor（没有区块游标可言）；WAL 与 rust-uma 一样读写，重启后事件历史、
//!    `after_sequence` 续传和面板累计数都接得上（2026-10-01 用户改为写 WAL）；
//! 3. 多跑一个 `run_confirm_feed`，只用链上确认事件更新去重状态和提交地址打分。
//!
//! 额外环境变量（其余与 rust-uma 相同，见 `.env.example`）：
//! - `MEMPOOL_FEED_ADDR`：P2P 节点的本机推送地址，默认 `127.0.0.1:8014`
//! - `MEMPOOL_SENDER_SEED`：提交地址先验（`{"0x地址": [一致, 不一致]}`），可选
//! - `MEMPOOL_BACKFILL_HOURS`：启动回补链上确认事件的小时数，默认 3
//! - `MEMPOOL_BACKFILL_CHUNK`：回补每次 eth_getLogs 的块数，默认 100
//! - `MEMPOOL_TRUST_MIN_N` / `MEMPOOL_TRUST_MIN_RATE` / `MEMPOOL_TIP_QUANTILE`：过滤门槛，默认 50 / 0.98 / 0.05
//!
//! `WSS_RPC` / `HTTP_RPC` 在这里只给确认订阅和回补用，不在热路径上。

use std::{
    env,
    path::PathBuf,
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};

use rust_uma::{
    api::{AppState, serve},
    config::Config,
    enrichment::{
        Catalog, GammaClient, run_catalog_reconcile, run_catalog_sync, run_new_market_watch,
        sync_catalog_before_uma,
    },
    hub::{EventHub, FrameHub},
    mempool::{
        feed::{run_confirm_feed, run_pending_feed},
        gate::{Gate, GateConfig},
    },
    pipeline::{Processor, run_batcher},
    stats::Stats,
    storage::{Storage, run_storage_writer},
};
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .compact()
        .init();
    if let Err(error) = run().await {
        error!(%error, "mempool-uma stopped with an error");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(Config::from_env()?);
    let storage = Storage::open(config.data_dir.clone())?;
    let catalog = Arc::new(Catalog::new(storage.load_catalog()?));
    let events = Arc::new(EventHub::new(config.event_ring_capacity));
    // WAL 恢复，与 main.rs 相同：读不了就当空历史，不让一个旧文件拖垮启动。
    let recovered = storage
        .load_events(config.event_ring_capacity)
        .unwrap_or_else(|error| {
            warn!(%error, "event WAL unreadable; starting with empty event history");
            Vec::new()
        });
    let mut initial_sequence = 0;
    let mut last_broadcast_sequence = 0;
    for event in recovered {
        initial_sequence = initial_sequence.max(event.sequence);
        if event.enrichment.is_some() {
            last_broadcast_sequence = last_broadcast_sequence.max(event.sequence);
        }
        events.insert(event);
    }
    let frames = Arc::new(FrameHub::resuming_after(
        config.frame_ring_capacity,
        last_broadcast_sequence,
    ));
    let stats = Arc::new(Stats::default());
    if let Some(snapshot) = storage.load_enrichment_stats()? {
        stats
            .enrichment_hits
            .store(snapshot.hits, Ordering::Relaxed);
        stats
            .enrichment_hits_via_market_id
            .store(snapshot.hits_via_market_id, Ordering::Relaxed);
        stats
            .enrichment_misses
            .store(snapshot.misses, Ordering::Relaxed);
    }
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (batch_tx, batch_rx) = mpsc::channel(config.live_buffer.max(1));
    let (storage_tx, storage_rx) = mpsc::channel(config.live_buffer.max(1));
    let gamma = GammaClient::new(config.gamma_base_url.clone())?;

    let changed = sync_catalog_before_uma(
        &gamma,
        &catalog,
        &storage,
        config.closed_market_lookback_days,
    )
    .await?;
    stats
        .catalog_markets
        .store(catalog.len() as u64, Ordering::Relaxed);
    info!(
        changed,
        markets = catalog.len(),
        "Gamma catalog synchronized before mempool feed"
    );

    let gate_cfg = GateConfig {
        trust_min_n: env_or("MEMPOOL_TRUST_MIN_N", 50),
        trust_min_rate: env_or("MEMPOOL_TRUST_MIN_RATE", 0.98),
        tip_quantile: env_or("MEMPOOL_TIP_QUANTILE", 0.05),
        ..GateConfig::default()
    };
    let mut gate = Gate::new(gate_cfg);
    let senders_path: PathBuf = config.data_dir.join("mempool_senders.json");
    if let Ok(n) = gate.load_senders(&senders_path) {
        info!(n, "loaded sender scores");
    }
    if let Ok(seed) = env::var("MEMPOOL_SENDER_SEED")
        && let Ok(n) = gate.load_senders(seed.as_ref())
    {
        info!(n, seed, "loaded sender prior");
    }
    let gate = Arc::new(Mutex::new(gate));

    let processor = Arc::new(Processor::new(
        config.clone(),
        catalog.clone(),
        events.clone(),
        batch_tx,
        storage_tx,
        stats.clone(),
        initial_sequence,
    ));

    let mut tasks = vec![
        tokio::spawn(run_storage_writer(
            storage.clone(),
            events.clone(),
            config.event_ring_capacity,
            stats.clone(),
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
        tokio::spawn(run_catalog_sync(
            config.clone(),
            gamma.clone(),
            catalog.clone(),
            storage.clone(),
            stats.clone(),
            shutdown_rx.clone(),
        )),
        tokio::spawn(run_new_market_watch(
            config.clone(),
            gamma.clone(),
            catalog.clone(),
            stats.clone(),
            shutdown_rx.clone(),
        )),
        tokio::spawn(run_catalog_reconcile(
            config.clone(),
            gamma,
            catalog.clone(),
            storage.clone(),
            stats.clone(),
            shutdown_rx.clone(),
        )),
        tokio::spawn(run_confirm_feed(
            config.polygon_rpc_url.clone(),
            config.polygon_wss_url.clone(),
            env_or("MEMPOOL_BACKFILL_HOURS", 3),
            env_or("MEMPOOL_BACKFILL_CHUNK", 100),
            gate.clone(),
            shutdown_rx.clone(),
        )),
        tokio::spawn(run_pending_feed(
            env::var("MEMPOOL_FEED_ADDR").unwrap_or_else(|_| "127.0.0.1:8014".into()),
            processor,
            gate.clone(),
            stats.clone(),
            shutdown_rx.clone(),
        )),
    ];
    // 每分钟：清理过期状态、落盘地址打分、打一行过滤统计
    {
        let gate = gate.clone();
        let mut shutdown = shutdown_rx.clone();
        tasks.push(tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            loop {
                tokio::select! {
                    _ = shutdown.changed() => break,
                    _ = tick.tick() => {}
                }
                let mut g = gate.lock().unwrap_or_else(|e| e.into_inner());
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_micros() as u64;
                g.prune(now);
                let _ = g.save_senders(&senders_path);
                let floor = g.tip_floor(now);
                info!(
                    emitted = g.emitted,
                    dropped = ?g.dropped,
                    requests = g.requests(),
                    trusted_senders = g.trusted_count(),
                    tip_floor = ?floor,
                    "mempool gate"
                );
            }
        }));
    }

    let state = AppState {
        config: config.clone(),
        events,
        frames,
        catalog,
        stats,
    };
    info!(address = %config.api_addr, recovered_events = initial_sequence, "mempool-uma API listening");
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
    let _ = tokio::signal::ctrl_c().await;
}
