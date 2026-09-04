use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio::sync::{mpsc, watch};
use tracing::{debug, error, warn};

use crate::{
    config::Config,
    enrichment::Catalog,
    hub::{EventHub, FrameHub},
    model::{EventKey, EventKind, EventRecord, PriceOutcome, hex_prefixed},
    stats::Stats,
    storage::StorageCommand,
    uma::events::{RpcLog, decode_signal_log},
    wire::{WireConfig, encode_frame, encoded_event_len, now_us},
};

/// Size of the trailing enrichment-outcome window backing
/// `Stats::enrichment_recent_hits`/`enrichment_recent_total` — a "how are we
/// doing right now" complement to the all-time counters (see the doc comment
/// on those fields).
const RECENT_ENRICHMENT_WINDOW: usize = 1000;

pub struct Processor {
    config: Arc<Config>,
    catalog: Arc<Catalog>,
    events: Arc<EventHub>,
    batch_tx: mpsc::Sender<Arc<EventRecord>>,
    storage_tx: mpsc::Sender<StorageCommand>,
    stats: Arc<Stats>,
    sequence: AtomicU64,
    /// Ring of the last `RECENT_ENRICHMENT_WINDOW` enrichment outcomes
    /// (true = hit). A plain `Mutex` is fine here: multiple WSS racers can
    /// call `process` concurrently, but each hold is O(1) with no I/O —
    /// nowhere near the hot-path cost of the network calls this project
    /// actually needs to keep off the hot path.
    recent_enrichment: Mutex<VecDeque<bool>>,
}

impl Processor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Arc<Config>,
        catalog: Arc<Catalog>,
        events: Arc<EventHub>,
        batch_tx: mpsc::Sender<Arc<EventRecord>>,
        storage_tx: mpsc::Sender<StorageCommand>,
        stats: Arc<Stats>,
        initial_sequence: u64,
    ) -> Self {
        Self {
            config,
            catalog,
            events,
            batch_tx,
            storage_tx,
            stats,
            sequence: AtomicU64::new(initial_sequence),
            recent_enrichment: Mutex::new(VecDeque::with_capacity(RECENT_ENRICHMENT_WINDOW)),
        }
    }

    /// `source` identifies which upstream feed delivered this log (a WSS racer
    /// index/tag, or "backfill"). It's only used for tracing/verification and
    /// the `Stats::source_race` win-rate tally — deduplication and correctness
    /// never depend on it.
    pub async fn process(&self, raw: RpcLog, received_at_us: u64, source: &str) {
        Stats::increment(&self.stats.rpc_logs_received);
        self.stats
            .last_upstream_received_at_us
            .store(received_at_us, Ordering::Relaxed);
        let decoded = match decode_signal_log(
            &raw,
            received_at_us,
            &self.config.contract_address_bytes,
            self.config.require_market_id,
        ) {
            Ok(event) => event,
            Err(error) => {
                Stats::increment(&self.stats.decode_errors);
                debug!(%error, tx=%raw.transaction_hash, "discarding undecodable RPC log");
                return;
            }
        };
        let chain = decoded.chain();
        let key = EventKey {
            transaction_hash: chain.transaction_hash,
            log_index: chain.log_index,
            removed: chain.removed,
        };
        if self.events.contains(&key) {
            Stats::increment(&self.stats.duplicates);
            self.stats.record_source_race(source, false);
            return;
        }
        // Resolve enrichment by market_id first: Gamma's condition_id is
        // authoritative for every adapter type (standard and Neg Risk alike),
        // and the catalog is pre-warmed, so this is a local O(1) lookup with no
        // extra RPC round trip. The on-chain derived condition_id only covers
        // standard binary UmaCtfAdapter markets and is the fallback for the
        // rare event whose ancillary data omits market_id.
        let market_id = decoded.request().ancillary.market_id;
        let enrichment = self
            .catalog
            .resolve(market_id, &decoded.request().condition_id);
        self.record_recent_enrichment(enrichment.is_some());
        if enrichment.is_some() {
            Stats::increment(&self.stats.enrichment_hits);
            if market_id.is_some() {
                Stats::increment(&self.stats.enrichment_hits_via_market_id);
            }
        } else {
            Stats::increment(&self.stats.enrichment_misses);
            // At `warn!` (not `debug!`) deliberately: this is the only record
            // of the event — an enrichment miss is no longer broadcast (see
            // below, `batch_tx.send` is skipped for it), so this line and the
            // stats counters above are the entire "local record" of it. Must
            // be visible in production logs at the default log level, not
            // only when someone happens to be running with RUST_LOG=debug.
            // Carries enough to investigate without a follow-up query:
            // market_id (or its absence — a different root cause than
            // "market_id present but uncached"), the derived condition_id
            // (what a standard-adapter lookup would have used), requester
            // (which Adapter this is — helps spot Neg Risk/unknown adapter
            // patterns), and the tx/block to cross-reference on-chain.
            warn!(
                tx = %raw.transaction_hash,
                block = decoded.chain().block_number,
                kind = ?decoded.kind(),
                market_id = ?market_id,
                derived_condition_id = %hex_prefixed(&decoded.request().condition_id),
                requester = %hex_prefixed(&decoded.request().requester),
                source,
                "enrichment miss: not broadcasting, logged locally only"
            );
        }
        debug!(
            source,
            tx = %raw.transaction_hash,
            market_id = ?market_id,
            enriched = enrichment.is_some(),
            "event accepted"
        );
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let block_number = decoded.chain().block_number;
        // A DisputePrice's price is the value under dispute, not a fresh
        // answer — only a Propose's own price is a real signal (see
        // proto/uma.proto's UmaEvent.price_outcome doc comment).
        let price_outcome = match decoded.kind() {
            EventKind::Propose => {
                PriceOutcome::from_propose_price(&decoded.request().proposed_price)
            }
            EventKind::Dispute => PriceOutcome::Unspecified,
        };
        let enriched = enrichment.is_some();
        let record = Arc::new(EventRecord {
            sequence,
            event: decoded,
            enrichment,
            price_outcome,
        });
        if !self.events.insert(record.clone()) {
            Stats::increment(&self.stats.duplicates);
            self.stats.record_source_race(source, false);
            return;
        }
        self.stats.record_source_race(source, true);
        Stats::increment(&self.stats.events_decoded);
        Stats::set_max(&self.stats.latest_block, block_number);

        if self
            .storage_tx
            .try_send(StorageCommand::Event(record.clone()))
            .is_err()
        {
            Stats::increment(&self.stats.storage_queue_dropped);
        }
        // An enrichment miss has no token_ids to act on — nothing downstream
        // can do with it, so it stops here: dedup ring + WAL + the `warn!`
        // above are its only record, never `batch_tx`/WSS. See the doc
        // comment above the miss branch.
        if enriched && self.batch_tx.send(record).await.is_err() {
            error!("batch pipeline stopped");
        }
    }

    /// Pushes one outcome into the trailing window and republishes the
    /// window's hit count / size onto `Stats` — see `RECENT_ENRICHMENT_WINDOW`
    /// and the doc comment on `Stats::enrichment_recent_hits`.
    fn record_recent_enrichment(&self, hit: bool) {
        let mut window = self
            .recent_enrichment
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        window.push_back(hit);
        if hit {
            Stats::increment(&self.stats.enrichment_recent_hits);
        }
        if window.len() > RECENT_ENRICHMENT_WINDOW && window.pop_front() == Some(true) {
            Stats::decrement_saturating(&self.stats.enrichment_recent_hits);
        }
        self.stats
            .enrichment_recent_total
            .store(window.len() as u64, Ordering::Relaxed);
    }

    pub fn checkpoint(&self, block: u64) {
        if self
            .storage_tx
            .try_send(StorageCommand::Checkpoint(block))
            .is_err()
        {
            Stats::increment(&self.stats.storage_queue_dropped);
        }
    }
}

pub async fn run_batcher(
    config: Arc<Config>,
    frames: Arc<FrameHub>,
    stats: Arc<Stats>,
    mut rx: mpsc::Receiver<Arc<EventRecord>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let wire_config = WireConfig {
        zstd_threshold: config.zstd_threshold,
        max_decompressed_bytes: config.max_decompressed_bytes,
    };
    let mut batch_sequence = 0_u64;
    let mut pending = None;
    loop {
        let first = if let Some(event) = pending.take() {
            event
        } else {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() { break; }
                    continue;
                }
                event = rx.recv() => match event {
                    Some(event) => event,
                    None => break,
                }
            }
        };
        let mut estimated_bytes = encoded_event_len(&first);
        let mut batch = vec![first];
        while batch.len() < config.batch_max_events.max(1) {
            let Ok(next) = rx.try_recv() else {
                break;
            };
            let next_size = encoded_event_len(&next);
            if estimated_bytes + next_size > config.batch_max_bytes && !batch.is_empty() {
                pending = Some(next);
                break;
            }
            estimated_bytes += next_size;
            batch.push(next);
        }
        batch_sequence = batch_sequence.wrapping_add(1).max(1);
        match encode_frame(batch_sequence, &batch, &wire_config) {
            Ok(frame) => {
                stats
                    .last_broadcast_at_us
                    .store(now_us(), Ordering::Relaxed);
                frames.publish(Arc::new(frame));
            }
            Err(error) => error!(%error, events=batch.len(), "encode WSS batch"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::test_config,
        model::{BetType, Category, MarketEnrichment},
        uma::events::{RpcLog, TOPIC_PROPOSE_PRICE},
    };

    fn abi_word(value: u64) -> [u8; 32] {
        let mut word = [0_u8; 32];
        word[24..].copy_from_slice(&value.to_be_bytes());
        word
    }

    fn address_word(byte: u8) -> [u8; 32] {
        let mut word = [0_u8; 32];
        word[12..].fill(byte);
        word
    }

    /// Minimal but genuinely ABI-decodable ProposePrice log — just enough for
    /// `decode_signal_log` to succeed with `market_id: 42` in the ancillary
    /// text, so these tests exercise the real decode path rather than a
    /// hand-built `UmaEvent`. Mirrors
    /// `uma::events::tests::log` (kept local rather than shared/exported —
    /// this only needs "some valid propose log", not decode-correctness
    /// coverage, which belongs to that module).
    fn propose_log() -> RpcLog {
        let ancillary = b"q: Will it happen? res_data: p1: 0, p2: 1, p3: 0.5, market_id: 42, initializer: 1111111111111111111111111111111111111111";
        let offset = 6 * 32;
        let mut data = vec![0_u8; offset + 32 + ancillary.len()];
        data[..32].fill(0x44);
        data[32..64].copy_from_slice(&abi_word(100));
        data[64..96].copy_from_slice(&abi_word(offset as u64));
        data[96..128].fill(0x55);
        data[128..160].copy_from_slice(&abi_word(200));
        data[160..192].copy_from_slice(&address_word(0x66));
        data[offset..offset + 32].copy_from_slice(&abi_word(ancillary.len() as u64));
        data[offset + 32..].copy_from_slice(ancillary);
        RpcLog {
            address: format!("0x{}", "aa".repeat(20)),
            topics: vec![
                TOPIC_PROPOSE_PRICE.into(),
                format!("0x{}", hex::encode(address_word(1))),
                format!("0x{}", hex::encode(address_word(2))),
            ],
            data: format!("0x{}", hex::encode(data)),
            block_number: "0xa".into(),
            block_hash: format!("0x{}", "bb".repeat(32)),
            transaction_hash: format!("0x{}", "cc".repeat(32)),
            transaction_index: Some("0x2".into()),
            log_index: "0x1".into(),
            removed: false,
        }
    }

    /// Builds a `Processor` wired to channels the test can poll, plus the
    /// `Catalog` it should resolve against (empty catalog => every event
    /// misses enrichment; a catalog seeded with `market_id: 42` => hits).
    fn build_processor(
        catalog: Catalog,
    ) -> (
        Processor,
        mpsc::Receiver<Arc<EventRecord>>,
        mpsc::Receiver<StorageCommand>,
    ) {
        let (batch_tx, batch_rx) = mpsc::channel(4);
        let (storage_tx, storage_rx) = mpsc::channel(4);
        let processor = Processor::new(
            Arc::new(test_config()),
            Arc::new(catalog),
            Arc::new(EventHub::new(16)),
            batch_tx,
            storage_tx,
            Arc::new(Stats::default()),
            0,
        );
        (processor, batch_rx, storage_rx)
    }

    #[tokio::test]
    async fn enrichment_miss_is_recorded_locally_but_never_broadcast() {
        let (processor, mut batch_rx, mut storage_rx) = build_processor(Catalog::new(Vec::new()));

        processor.process(propose_log(), 1, "test").await;

        // Never handed to the batcher — nothing to broadcast downstream.
        assert!(batch_rx.try_recv().is_err());
        // Still the "local record": written to storage (events.wal).
        assert!(matches!(
            storage_rx.try_recv(),
            Ok(StorageCommand::Event(_))
        ));
        assert_eq!(processor.stats.enrichment_misses.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn enrichment_hit_is_broadcast() {
        let market = MarketEnrichment {
            market_id: 42,
            condition_id: [9; 32],
            token_ids: vec![[1; 32], [2; 32]],
            tag_ids: vec![],
            category: Category::Unspecified,
            bet_type: BetType::Unspecified,
            neg_risk: false,
        };
        let (processor, mut batch_rx, mut storage_rx) = build_processor(Catalog::new(vec![market]));

        processor.process(propose_log(), 1, "test").await;

        assert!(batch_rx.try_recv().is_ok());
        assert!(matches!(
            storage_rx.try_recv(),
            Ok(StorageCommand::Event(_))
        ));
        assert_eq!(processor.stats.enrichment_hits.load(Ordering::Relaxed), 1);
    }

    /// Two racers deliver the same log; whichever wins the dedup race (here,
    /// "wss[0]" — first to call `process`) gets credited a win, the loser
    /// only a delivery. Exercises `Stats::record_source_race`'s three call
    /// sites in `Processor::process` end to end, not just the helper itself.
    #[tokio::test]
    async fn source_race_credits_the_winner_and_not_the_duplicate() {
        let market = MarketEnrichment {
            market_id: 42,
            condition_id: [9; 32],
            token_ids: vec![[1; 32], [2; 32]],
            tag_ids: vec![],
            category: Category::Unspecified,
            bet_type: BetType::Unspecified,
            neg_risk: false,
        };
        let (processor, mut batch_rx, _storage_rx) = build_processor(Catalog::new(vec![market]));

        processor.process(propose_log(), 1, "wss[0]").await;
        processor.process(propose_log(), 2, "wss[1]").await;

        assert!(batch_rx.try_recv().is_ok()); // only the winner's copy is broadcast
        assert!(batch_rx.try_recv().is_err());

        let race = processor.stats.source_race.lock().unwrap();
        assert_eq!(race["wss[0]"].received, 1);
        assert_eq!(race["wss[0]"].won, 1);
        assert_eq!(race["wss[1]"].received, 1);
        assert_eq!(race["wss[1]"].won, 0);
    }
}
