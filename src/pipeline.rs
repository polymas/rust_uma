use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
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

pub struct Processor {
    config: Arc<Config>,
    catalog: Arc<Catalog>,
    events: Arc<EventHub>,
    batch_tx: mpsc::Sender<Arc<EventRecord>>,
    storage_tx: mpsc::Sender<StorageCommand>,
    stats: Arc<Stats>,
    sequence: AtomicU64,
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
        }
    }

    /// `source` identifies which upstream feed delivered this log (a WSS racer
    /// index/tag, or "backfill"). It is only used for tracing/verification —
    /// deduplication and correctness never depend on it.
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
        if enrichment.is_some() {
            Stats::increment(&self.stats.enrichment_hits);
            if market_id.is_some() {
                Stats::increment(&self.stats.enrichment_hits_via_market_id);
            }
        } else {
            Stats::increment(&self.stats.enrichment_misses);
            // At `warn!` (not `debug!`) deliberately: every miss is a real
            // event broadcast downstream with empty enrichment, right now,
            // permanently — this must be visible in production logs at the
            // default log level, not only when someone happens to be running
            // with RUST_LOG=debug. Carries enough to investigate without a
            // follow-up query: market_id (or its absence — a different root
            // cause than "market_id present but uncached"), the derived
            // condition_id (what a standard-adapter lookup would have used),
            // requester (which Adapter this is — helps spot Neg Risk/unknown
            // adapter patterns), and the tx/block to cross-reference on-chain.
            warn!(
                tx = %raw.transaction_hash,
                block = decoded.chain().block_number,
                kind = ?decoded.kind(),
                market_id = ?market_id,
                derived_condition_id = %hex_prefixed(&decoded.request().condition_id),
                requester = %hex_prefixed(&decoded.request().requester),
                source,
                "enrichment miss: broadcasting without token_ids/tag_ids"
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
        let record = Arc::new(EventRecord {
            sequence,
            event: decoded,
            enrichment,
            price_outcome,
        });
        if !self.events.insert(record.clone()) {
            Stats::increment(&self.stats.duplicates);
            return;
        }
        Stats::increment(&self.stats.events_decoded);
        Stats::set_max(&self.stats.latest_block, block_number);

        if self
            .storage_tx
            .try_send(StorageCommand::Event(record.clone()))
            .is_err()
        {
            Stats::increment(&self.stats.storage_queue_dropped);
        }
        if self.batch_tx.send(record).await.is_err() {
            error!("batch pipeline stopped");
        }
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
