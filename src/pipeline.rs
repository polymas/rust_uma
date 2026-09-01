use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use tokio::sync::{mpsc, watch};
use tracing::{debug, error};

use crate::{
    config::Config,
    decode::{RpcLog, decode_signal_log},
    enrichment::{Catalog, RepairHandle},
    hub::{EventHub, FrameHub},
    model::{EventKey, EventRecord},
    stats::Stats,
    storage::StorageCommand,
    wire::{WireConfig, encode_frame, encoded_event_len, now_us},
};

pub struct Processor {
    config: Arc<Config>,
    catalog: Arc<Catalog>,
    repair: RepairHandle,
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
        repair: RepairHandle,
        events: Arc<EventHub>,
        batch_tx: mpsc::Sender<Arc<EventRecord>>,
        storage_tx: mpsc::Sender<StorageCommand>,
        stats: Arc<Stats>,
        initial_sequence: u64,
    ) -> Self {
        Self {
            config,
            catalog,
            repair,
            events,
            batch_tx,
            storage_tx,
            stats,
            sequence: AtomicU64::new(initial_sequence),
        }
    }

    pub async fn process(&self, raw: RpcLog, received_at_us: u64) {
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
        let key = EventKey {
            transaction_hash: decoded.transaction_hash,
            log_index: decoded.log_index,
            removed: decoded.removed,
        };
        if self.events.contains(&key) {
            Stats::increment(&self.stats.duplicates);
            return;
        }
        let enrichment = self.catalog.get(decoded.market_id);
        if enrichment.is_some() {
            Stats::increment(&self.stats.enrichment_hits);
        } else {
            Stats::increment(&self.stats.enrichment_misses);
            self.repair.enqueue(decoded.market_id);
        }
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let block_number = decoded.block_number;
        let record = Arc::new(EventRecord {
            sequence,
            event: decoded,
            enrichment,
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
