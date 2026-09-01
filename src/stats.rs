use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[derive(Default)]
pub struct Stats {
    pub rpc_connected: AtomicBool,
    pub rpc_reconnects: AtomicU64,
    pub rpc_logs_received: AtomicU64,
    pub events_decoded: AtomicU64,
    pub decode_errors: AtomicU64,
    pub duplicates: AtomicU64,
    pub enrichment_hits: AtomicU64,
    pub enrichment_misses: AtomicU64,
    pub catalog_markets: AtomicU64,
    pub last_upstream_received_at_us: AtomicU64,
    pub last_broadcast_at_us: AtomicU64,
    pub subscribers: AtomicU64,
    pub slow_clients_dropped: AtomicU64,
    pub storage_queue_dropped: AtomicU64,
    pub latest_block: AtomicU64,
}

impl Stats {
    pub fn increment(value: &AtomicU64) {
        value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_max(value: &AtomicU64, candidate: u64) {
        let mut current = value.load(Ordering::Relaxed);
        while candidate > current {
            match value.compare_exchange_weak(
                current,
                candidate,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }
}
