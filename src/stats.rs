use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[derive(Default)]
pub struct Stats {
    pub rpc_connected: AtomicBool,
    pub rpc_reconnects: AtomicU64,
    /// Count of currently-connected WSS racers (0..=wss_rpc_urls.len()).
    /// Useful to confirm multi-source racing is actually up in production.
    pub rpc_sources_connected: AtomicU64,
    pub rpc_logs_received: AtomicU64,
    pub events_decoded: AtomicU64,
    pub decode_errors: AtomicU64,
    pub duplicates: AtomicU64,
    pub enrichment_hits: AtomicU64,
    /// Subset of enrichment_hits resolved via market_id (covers both standard
    /// and Neg Risk adapters). enrichment_hits minus this is the count resolved
    /// only via the on-chain derived binary condition_id.
    pub enrichment_hits_via_market_id: AtomicU64,
    pub enrichment_misses: AtomicU64,
    pub catalog_markets: AtomicU64,
    /// Markets added by a from-scratch reconciliation walk that the
    /// cursor-driven incremental sync had permanently skipped (Gamma's
    /// keyset pagination can silently drop entries, especially within a
    /// batch of markets created with near-identical updatedAt — see
    /// `enrichment::run_catalog_reconcile`). Should trend toward 0 per pass;
    /// sustained non-zero values mean the incremental sync is leaking.
    pub catalog_reconcile_gaps_closed: AtomicU64,
    pub last_upstream_received_at_us: AtomicU64,
    pub last_broadcast_at_us: AtomicU64,
    pub subscribers: AtomicU64,
    pub slow_clients_dropped: AtomicU64,
    pub storage_queue_dropped: AtomicU64,
    pub latest_block: AtomicU64,
    /// Raw bytes read off the upstream WSS RPC socket(s) (subscribe request's
    /// response frames plus every eth_subscription notification). Dashboard-only
    /// counter, not used by any hot-path decision.
    pub rpc_bytes_received: AtomicU64,
    /// Raw bytes written to the upstream WSS RPC socket(s) (essentially just
    /// the one-shot eth_subscribe request per (re)connect). Dashboard-only.
    pub rpc_bytes_sent: AtomicU64,
    /// Frames written to downstream `/uma/v1/ws` subscribers, summed across
    /// all connections. Dashboard-only counter for consumption throughput.
    pub ws_frames_sent: AtomicU64,
    pub ws_bytes_sent: AtomicU64,
}

impl Stats {
    pub fn increment(value: &AtomicU64) {
        value.fetch_add(1, Ordering::Relaxed);
    }

    /// Saturating decrement; never wraps below zero even if calls race or
    /// double-fire (e.g. a worker's disconnect path runs twice).
    pub fn decrement_saturating(value: &AtomicU64) {
        let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_sub(1))
        });
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
