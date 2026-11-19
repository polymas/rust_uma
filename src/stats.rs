use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use serde::{Deserialize, Serialize};

/// One upstream feed's outcome tally in the multi-WSS "赛马" race — how many
/// decodable logs it delivered vs. how many of those it was the fastest copy
/// to win the `(tx_hash, log_index)` dedup race for (see
/// `pipeline::Processor::process`). Dashboard-only ("抢达率" — win rate);
/// never used for correctness, and not persisted across restarts (like most
/// of `Stats`, it's a live-process snapshot).
#[derive(Debug, Default, Clone, Copy)]
pub struct SourceRaceStats {
    pub received: u64,
    pub won: u64,
}

/// The subset of `Stats` that survives a restart — see
/// `storage::{load_enrichment_stats, save_enrichment_stats}`. Deliberately
/// small: only the all-time enrichment counters, not every counter in
/// `Stats` (most of them, like `rpc_reconnects` or `subscribers`, are
/// meaningful only for the current process's uptime and a stale value from a
/// previous run would be actively misleading).
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct EnrichmentStatsSnapshot {
    pub hits: u64,
    pub hits_via_market_id: u64,
    pub misses: u64,
}

#[derive(Default)]
pub struct Stats {
    pub rpc_connected: AtomicBool,
    pub rpc_reconnects: AtomicU64,
    /// Subset of `rpc_reconnects` forced by the stall watchdog: a session
    /// that stayed open but stopped pushing logs the chain did have.
    pub rpc_stall_reconnects: AtomicU64,
    /// Count of currently-connected WSS racers (0..=wss_rpc_urls.len()).
    /// Useful to confirm multi-source racing is actually up in production.
    pub rpc_sources_connected: AtomicU64,
    pub rpc_logs_received: AtomicU64,
    pub events_decoded: AtomicU64,
    /// Logs that were malformed (bad hex/ABI/topic/UTF-8). Requests from
    /// other projects on the same oracle are NOT counted here — see
    /// `foreign_requests_skipped`.
    pub decode_errors: AtomicU64,
    /// Well-formed ProposePrice/DisputePrice logs whose ancillary data has no
    /// `market_id`, i.e. requests from non-Polymarket requesters sharing the
    /// UMA oracle. Skipped by design; counted only for visibility.
    pub foreign_requests_skipped: AtomicU64,
    pub duplicates: AtomicU64,
    pub enrichment_hits: AtomicU64,
    /// Subset of enrichment_hits resolved via market_id (covers both standard
    /// and Neg Risk adapters). enrichment_hits minus this is the count resolved
    /// only via the on-chain derived binary condition_id.
    pub enrichment_hits_via_market_id: AtomicU64,
    pub enrichment_misses: AtomicU64,
    /// Hits within the trailing window of the most recent
    /// `pipeline::RECENT_ENRICHMENT_WINDOW` processed events (currently
    /// 1000) — a "how are we doing right now" complement to the all-time
    /// `enrichment_hits`/`enrichment_misses`, which a long-lived process can
    /// leave close to 100% even during a fresh, ongoing miss streak.
    /// Maintained alongside `Processor`'s rolling window (see pipeline.rs);
    /// not persisted — it's a live snapshot, restarting empty is correct.
    pub enrichment_recent_hits: AtomicU64,
    /// Size of the trailing window above (saturates at 1000; smaller only
    /// right after startup before 1000 events have been processed).
    pub enrichment_recent_total: AtomicU64,
    pub catalog_markets: AtomicU64,
    /// Markets added by a from-scratch reconciliation walk that the
    /// cursor-driven incremental sync had permanently skipped (Gamma's
    /// keyset pagination can silently drop entries, especially within a
    /// batch of markets created with near-identical updatedAt — see
    /// `enrichment::run_catalog_reconcile`). Should trend toward 0 per pass;
    /// sustained non-zero values mean the incremental sync is leaking.
    pub catalog_reconcile_gaps_closed: AtomicU64,
    /// Markets `enrichment::run_new_market_watch` added ahead of the regular
    /// `updatedAt` refresh — how often the new-market fast path mattered.
    pub catalog_new_markets_added: AtomicU64,
    /// Markets removed by `enrichment::Catalog::evict_unseen` because Gamma
    /// has not listed them for `CATALOG_RETENTION_DAYS`.
    pub catalog_markets_evicted: AtomicU64,
    pub last_upstream_received_at_us: AtomicU64,
    pub last_broadcast_at_us: AtomicU64,
    pub subscribers: AtomicU64,
    /// Downstream `/uma/v1/ws` connections dropped because a write timed out
    /// (`WS_WRITE_TIMEOUT_MS`). Dropped without a close frame — see
    /// `api::websocket_session`.
    pub slow_clients_dropped: AtomicU64,
    /// Downstream connections closed with 1013 because their `after_sequence`
    /// is older than the frame ring can replay. Counted separately from
    /// `slow_clients_dropped`: 1013 tells the client to drop its cursor, a
    /// write timeout must not.
    pub ws_cursor_rejected: AtomicU64,
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
    /// Per-source (`"wss[N]"` racer tag, or `"backfill"`) race tallies — see
    /// `SourceRaceStats`. Keyed by the same `source` string `Processor::process`
    /// receives, so the key set isn't known statically (racer count comes from
    /// `Config::wss_rpc_urls`, which can change between deploys) — hence a
    /// `Mutex<HashMap>` rather than fixed `AtomicU64` fields. Each hold is O(1)
    /// with no I/O, same pattern as `Processor::recent_enrichment`.
    pub source_race: Mutex<HashMap<String, SourceRaceStats>>,
    /// ProposePrice/DisputePrice logs dropped because their emitter is not in
    /// `UMA_CONTRACT_ADDRESSES` (only counted while that allowlist is set).
    pub off_list_events: AtomicU64,
    /// The subset of `off_list_events` that would have resolved in the
    /// catalog, i.e. would have been broadcast without the allowlist.
    pub off_list_catalog_hits: AtomicU64,
    /// The same two tallies per emitter — see `OffListEmitter`.
    pub off_list_emitters: Mutex<HashMap<[u8; 20], OffListEmitter>>,
}

/// Emitters tracked individually in `Stats::off_list_emitters`. Anyone can
/// deploy a contract that emits these topics, so the table is capped; past
/// the cap only the two global counters keep counting.
pub const OFF_LIST_EMITTERS_MAX: usize = 64;

/// What one emitter outside `UMA_CONTRACT_ADDRESSES` has sent.
///
/// The point is the ratio. A contract Polymarket has migrated to resolves in
/// the catalog almost every time; an unrelated project that happens to use
/// the same ancillary layout with its own id numbering resolves only when an
/// id collides (2 of 85 for 0x57827d48… over 22 hours, 2026-11-18).
/// uma-console alerts on a high ratio (`console::alerts`, rule
/// `off_list_emitter`), which is how a stale allowlist gets noticed instead
/// of silently dropping every real event.
#[derive(Debug, Clone, Default)]
pub struct OffListEmitter {
    pub events: u64,
    pub catalog_hits: u64,
    pub first_seen_us: u64,
    pub last_seen_us: u64,
    pub last_requester: [u8; 20],
    pub last_hit_market_id: u64,
    pub last_hit_tx: String,
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

    /// Records one source's outcome in the dedup race for a single decoded
    /// log: always tallies a delivery, and a win only if `won` (this source's
    /// copy was the one that actually got stored — see call sites in
    /// `pipeline::Processor::process`).
    pub fn record_source_race(&self, source: &str, won: bool) {
        let mut map = self
            .source_race
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let entry = map.entry(source.to_owned()).or_default();
        entry.received += 1;
        if won {
            entry.won += 1;
        }
    }

    /// Tallies one log from an emitter outside the allowlist. `hit` carries
    /// `(market_id, tx)` when the event would have resolved in the catalog.
    /// Returns whether this emitter was seen for the first time.
    pub fn record_off_list(
        &self,
        emitter: [u8; 20],
        requester: [u8; 20],
        now_us: u64,
        hit: Option<(u64, &str)>,
    ) -> bool {
        Self::increment(&self.off_list_events);
        if hit.is_some() {
            Self::increment(&self.off_list_catalog_hits);
        }
        let mut map = self
            .off_list_emitters
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let first = !map.contains_key(&emitter);
        if first && map.len() >= OFF_LIST_EMITTERS_MAX {
            return false;
        }
        let entry = map.entry(emitter).or_insert_with(|| OffListEmitter {
            first_seen_us: now_us,
            ..Default::default()
        });
        entry.events += 1;
        entry.last_seen_us = now_us;
        entry.last_requester = requester;
        if let Some((market_id, tx)) = hit {
            entry.catalog_hits += 1;
            entry.last_hit_market_id = market_id;
            entry.last_hit_tx = tx.to_owned();
        }
        first
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
