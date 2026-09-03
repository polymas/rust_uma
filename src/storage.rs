use std::{
    collections::VecDeque,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use prost::Message;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tracing::{error, warn};

use crate::{
    hub::EventHub,
    model::{BetType, Category, EventRecord, MarketEnrichment},
    stats::{EnrichmentStatsSnapshot, Stats},
    wire::pb,
};

const CATALOG_MAGIC: &[u8; 8] = b"UMACAT2\0";
// Previous catalog.bin format (no category/bet_type bytes per record) —
// recognized on load so an upgrade doesn't hard-fail startup on a
// still-`UMACAT1\0` cache from before this field existed. See "升级" in
// docs/WORKFLOW.md: cross-version cache data must never silently
// misdeserialize, but a clean, well-understood old-format cache is a case we
// can self-heal from (full Gamma re-sync), not one that needs a hard error.
const CATALOG_MAGIC_V1: &[u8; 8] = b"UMACAT1\0";
const WAL_MAGIC: &[u8; 8] = b"UMAWAL1\0";
const MAX_WAL_RECORD: usize = 1 << 20;

#[derive(Clone)]
pub struct Storage {
    dir: Arc<PathBuf>,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("invalid storage format: {0}")]
    Format(&'static str),
    #[error("protobuf decode failed: {0}")]
    Protobuf(#[from] prost::DecodeError),
}

impl Storage {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let dir = path.into();
        fs::create_dir_all(&dir)?;
        Ok(Self { dir: Arc::new(dir) })
    }

    fn catalog_path(&self) -> PathBuf {
        self.dir.join("catalog.bin")
    }

    fn wal_path(&self) -> PathBuf {
        self.dir.join("events.wal")
    }

    fn enrichment_cursor_path(&self) -> PathBuf {
        self.dir.join("enrichment.cursor")
    }

    fn closed_market_cursor_path(&self) -> PathBuf {
        self.dir.join("enrichment_closed.cursor")
    }

    fn uma_cursor_path(&self) -> PathBuf {
        self.dir.join("uma.cursor")
    }

    fn enrichment_stats_path(&self) -> PathBuf {
        self.dir.join("enrichment_stats.json")
    }

    pub fn load_catalog(&self) -> Result<Vec<MarketEnrichment>, StorageError> {
        let path = self.catalog_path();
        if !path.exists() {
            return Ok(Vec::new());
        }
        let mut reader = BufReader::new(File::open(path)?);
        let mut magic = [0_u8; 8];
        reader.read_exact(&mut magic)?;
        if magic == *CATALOG_MAGIC_V1 {
            warn!(
                "catalog.bin is the pre-category/bet_type format ({:?}); \
                 discarding it and re-syncing the full catalog from Gamma \
                 instead of failing startup",
                String::from_utf8_lossy(CATALOG_MAGIC_V1)
            );
            return Ok(Vec::new());
        }
        if magic != *CATALOG_MAGIC {
            return Err(StorageError::Format("catalog magic"));
        }
        let count = read_u32(&mut reader)? as usize;
        let mut markets = Vec::with_capacity(count);
        for _ in 0..count {
            let market_id = read_u64(&mut reader)?;
            let mut condition_id = [0_u8; 32];
            reader.read_exact(&mut condition_id)?;
            let token_count = read_u16(&mut reader)? as usize;
            let mut token_ids = Vec::with_capacity(token_count);
            for _ in 0..token_count {
                let mut token = [0_u8; 32];
                reader.read_exact(&mut token)?;
                token_ids.push(token);
            }
            let tag_count = read_u16(&mut reader)? as usize;
            let mut tag_ids = Vec::with_capacity(tag_count);
            for _ in 0..tag_count {
                tag_ids.push(read_u32(&mut reader)?);
            }
            let category = Category::from_proto(read_u8(&mut reader)? as i32);
            // `BetType` values are `CCCBBB`-encoded (see proto/uma.proto) and
            // can reach into the low hundred-thousands, so unlike `category`
            // this doesn't fit in one byte.
            let bet_type = BetType::from_proto(read_i32(&mut reader)?);
            markets.push(MarketEnrichment {
                market_id,
                condition_id,
                token_ids,
                tag_ids,
                category,
                bet_type,
            });
        }
        Ok(markets)
    }

    pub fn save_catalog(&self, markets: &[MarketEnrichment]) -> Result<(), StorageError> {
        let path = self.catalog_path();
        let temp = temporary_path(&path);
        let mut ordered = markets.iter().collect::<Vec<_>>();
        ordered.sort_unstable_by_key(|market| market.market_id);
        let file = File::create(&temp)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(CATALOG_MAGIC)?;
        writer.write_all(&(ordered.len() as u32).to_be_bytes())?;
        for market in ordered {
            let token_count: u16 = market
                .token_ids
                .len()
                .try_into()
                .map_err(|_| StorageError::Format("too many token IDs"))?;
            let tag_count: u16 = market
                .tag_ids
                .len()
                .try_into()
                .map_err(|_| StorageError::Format("too many tag IDs"))?;
            writer.write_all(&market.market_id.to_be_bytes())?;
            writer.write_all(&market.condition_id)?;
            writer.write_all(&token_count.to_be_bytes())?;
            for token in &market.token_ids {
                writer.write_all(token)?;
            }
            writer.write_all(&tag_count.to_be_bytes())?;
            for tag in &market.tag_ids {
                writer.write_all(&tag.to_be_bytes())?;
            }
            writer.write_all(&[market.category.to_proto() as u8])?;
            writer.write_all(&market.bet_type.to_proto().to_be_bytes())?;
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
        drop(writer);
        fs::rename(temp, path)?;
        Ok(())
    }

    pub fn load_events(&self, capacity: usize) -> Result<Vec<Arc<EventRecord>>, StorageError> {
        let path = self.wal_path();
        if !path.exists() {
            return Ok(Vec::new());
        }
        let mut reader = BufReader::new(File::open(path)?);
        let mut magic = [0_u8; 8];
        reader.read_exact(&mut magic)?;
        if &magic != WAL_MAGIC {
            return Err(StorageError::Format("event WAL magic"));
        }
        let mut events = VecDeque::with_capacity(capacity.min(65_536));
        while let Some(value) = read_u32_eof(&mut reader)? {
            let length = value as usize;
            if length > MAX_WAL_RECORD {
                warn!(length, "stopping WAL recovery at oversized tail record");
                break;
            }
            let expected_crc = match read_u32_eof(&mut reader)? {
                Some(value) => value,
                None => break,
            };
            let mut payload = vec![0_u8; length];
            if let Err(error) = reader.read_exact(&mut payload) {
                if error.kind() == io::ErrorKind::UnexpectedEof {
                    warn!("ignoring incomplete WAL tail record");
                    break;
                }
                return Err(error.into());
            }
            if crc32fast::hash(&payload) != expected_crc {
                warn!("ignoring WAL tail after CRC mismatch");
                break;
            }
            let proto = pb::UmaEvent::decode(payload.as_slice())?;
            if let Some(event) = EventRecord::from_proto(proto) {
                events.push_back(Arc::new(event));
                while events.len() > capacity.max(1) {
                    events.pop_front();
                }
            }
        }
        Ok(events.into())
    }

    pub fn load_enrichment_cursor(&self) -> Result<Option<String>, StorageError> {
        let path = self.enrichment_cursor_path();
        if !path.exists() {
            return Ok(None);
        }
        let value = fs::read_to_string(path)?;
        let value = value.trim();
        Ok((!value.is_empty()).then(|| value.to_owned()))
    }

    pub fn save_enrichment_cursor(&self, cursor: &str) -> Result<(), StorageError> {
        atomic_write(&self.enrichment_cursor_path(), cursor.as_bytes())
    }

    /// Cursor for the separate "recently-closed Gamma markets" incremental
    /// sync (see `enrichment::sync_both`), independent of the active-market
    /// cursor above since the two queries (`closed=false` vs `closed=true`)
    /// are unrelated streams.
    pub fn load_closed_market_cursor(&self) -> Result<Option<String>, StorageError> {
        let path = self.closed_market_cursor_path();
        if !path.exists() {
            return Ok(None);
        }
        let value = fs::read_to_string(path)?;
        let value = value.trim();
        Ok((!value.is_empty()).then(|| value.to_owned()))
    }

    pub fn save_closed_market_cursor(&self, cursor: &str) -> Result<(), StorageError> {
        atomic_write(&self.closed_market_cursor_path(), cursor.as_bytes())
    }

    pub fn load_uma_cursor(&self) -> Result<Option<u64>, StorageError> {
        let path = self.uma_cursor_path();
        if !path.exists() {
            return Ok(None);
        }
        let value = fs::read_to_string(path)?;
        value
            .trim()
            .parse()
            .map(Some)
            .map_err(|_| StorageError::Format("UMA cursor"))
    }

    pub fn save_uma_cursor(&self, block: u64) -> Result<(), StorageError> {
        atomic_write(&self.uma_cursor_path(), block.to_string().as_bytes())
    }

    /// All-time enrichment hit/miss counters, so a restart resumes the
    /// running total instead of dropping back to zero (the point of this
    /// dashboard metric is to reflect the service's whole operating history,
    /// not just this process's uptime).
    pub fn load_enrichment_stats(&self) -> Result<Option<EnrichmentStatsSnapshot>, StorageError> {
        let path = self.enrichment_stats_path();
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(path)?;
        Ok(Some(
            serde_json::from_slice(&bytes).map_err(|_| StorageError::Format("enrichment stats"))?,
        ))
    }

    pub fn save_enrichment_stats(
        &self,
        snapshot: &EnrichmentStatsSnapshot,
    ) -> Result<(), StorageError> {
        let bytes =
            serde_json::to_vec(snapshot).map_err(|_| StorageError::Format("enrichment stats"))?;
        atomic_write(&self.enrichment_stats_path(), &bytes)
    }
}

fn atomic_write(path: &Path, value: &[u8]) -> Result<(), StorageError> {
    let temp = temporary_path(path);
    let mut file = File::create(&temp)?;
    file.write_all(value)?;
    file.sync_all()?;
    drop(file);
    fs::rename(temp, path)?;
    Ok(())
}

pub enum StorageCommand {
    Event(Arc<EventRecord>),
    Checkpoint(u64),
}

pub async fn run_storage_writer(
    storage: Storage,
    event_hub: Arc<EventHub>,
    event_capacity: usize,
    stats: Arc<Stats>,
    mut commands: mpsc::Receiver<StorageCommand>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut journal = match Journal::open(storage.wal_path()) {
        Ok(value) => value,
        Err(error) => {
            error!(%error, "event journal unavailable");
            return;
        }
    };
    let mut uma_cursor = storage.load_uma_cursor().ok().flatten().unwrap_or_default();
    let mut last_enrichment_snapshot = EnrichmentStatsSnapshot::default();
    let mut flush = tokio::time::interval(Duration::from_secs(1));
    flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() { break; }
            }
            _ = flush.tick() => {
                if let Err(error) = journal.flush() {
                    error!(%error, "flush event journal");
                }
                if uma_cursor > 0 && let Err(error) = storage.save_uma_cursor(uma_cursor) {
                    error!(%error, uma_cursor, "save UMA cursor");
                }
                last_enrichment_snapshot = persist_enrichment_stats(&storage, &stats, last_enrichment_snapshot);
            }
            command = commands.recv() => {
                match command {
                    Some(StorageCommand::Event(event)) => {
                        if let Err(error) = journal.append(&event) {
                            error!(%error, sequence=event.sequence, "append event journal");
                        }
                        if journal.records > event_capacity.saturating_mul(2).max(2) {
                            let snapshot = event_hub.snapshot();
                            if let Err(error) = journal.rewrite(&snapshot) {
                                error!(%error, "compact event journal");
                            }
                        }
                    }
                    Some(StorageCommand::Checkpoint(block)) => uma_cursor = uma_cursor.max(block),
                    None => break,
                }
            }
        }
    }
    let _ = journal.flush();
    if uma_cursor > 0 {
        let _ = storage.save_uma_cursor(uma_cursor);
    }
    persist_enrichment_stats(&storage, &stats, last_enrichment_snapshot);
}

/// Reads the current all-time enrichment counters off `stats` and persists
/// them if they moved since `previous` — skipping the write entirely while
/// idle (nothing to enrich between ticks) avoids a needless disk write every
/// second. Returns the snapshot actually observed, for the caller to pass
/// back in as `previous` next time.
fn persist_enrichment_stats(
    storage: &Storage,
    stats: &Stats,
    previous: EnrichmentStatsSnapshot,
) -> EnrichmentStatsSnapshot {
    let current = EnrichmentStatsSnapshot {
        hits: stats.enrichment_hits.load(Ordering::Relaxed),
        hits_via_market_id: stats.enrichment_hits_via_market_id.load(Ordering::Relaxed),
        misses: stats.enrichment_misses.load(Ordering::Relaxed),
    };
    if current.hits == previous.hits
        && current.hits_via_market_id == previous.hits_via_market_id
        && current.misses == previous.misses
    {
        return previous;
    }
    if let Err(error) = storage.save_enrichment_stats(&current) {
        error!(%error, "save enrichment stats");
    }
    current
}

struct Journal {
    path: PathBuf,
    writer: BufWriter<File>,
    records: usize,
}

impl Journal {
    fn open(path: PathBuf) -> Result<Self, StorageError> {
        let exists = path.exists();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        let mut writer = BufWriter::new(file);
        if !exists || writer.get_ref().metadata()?.len() == 0 {
            writer.write_all(WAL_MAGIC)?;
            writer.flush()?;
        }
        Ok(Self {
            path,
            writer,
            records: 0,
        })
    }

    fn append(&mut self, event: &EventRecord) -> Result<(), StorageError> {
        let payload = event.to_proto().encode_to_vec();
        if payload.len() > MAX_WAL_RECORD {
            return Err(StorageError::Format("event WAL record too large"));
        }
        self.writer
            .write_all(&(payload.len() as u32).to_be_bytes())?;
        self.writer
            .write_all(&crc32fast::hash(&payload).to_be_bytes())?;
        self.writer.write_all(&payload)?;
        self.records += 1;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), StorageError> {
        self.writer.flush()?;
        Ok(())
    }

    fn rewrite(&mut self, events: &[Arc<EventRecord>]) -> Result<(), StorageError> {
        self.writer.flush()?;
        let temp = temporary_path(&self.path);
        let mut writer = BufWriter::new(File::create(&temp)?);
        writer.write_all(WAL_MAGIC)?;
        for event in events {
            let payload = event.to_proto().encode_to_vec();
            writer.write_all(&(payload.len() as u32).to_be_bytes())?;
            writer.write_all(&crc32fast::hash(&payload).to_be_bytes())?;
            writer.write_all(&payload)?;
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
        drop(writer);
        fs::rename(&temp, &self.path)?;
        self.writer = BufWriter::new(
            OpenOptions::new()
                .append(true)
                .read(true)
                .open(&self.path)?,
        );
        self.records = events.len();
        Ok(())
    }
}

fn temporary_path(path: &Path) -> PathBuf {
    path.with_extension(format!(
        "{}.tmp",
        path.extension().and_then(|v| v.to_str()).unwrap_or("bin")
    ))
}

fn read_i32(reader: &mut impl Read) -> io::Result<i32> {
    let mut value = [0_u8; 4];
    reader.read_exact(&mut value)?;
    Ok(i32::from_be_bytes(value))
}

fn read_u8(reader: &mut impl Read) -> io::Result<u8> {
    let mut value = [0_u8; 1];
    reader.read_exact(&mut value)?;
    Ok(value[0])
}

fn read_u16(reader: &mut impl Read) -> io::Result<u16> {
    let mut value = [0_u8; 2];
    reader.read_exact(&mut value)?;
    Ok(u16::from_be_bytes(value))
}

fn read_u32(reader: &mut impl Read) -> io::Result<u32> {
    let mut value = [0_u8; 4];
    reader.read_exact(&mut value)?;
    Ok(u32::from_be_bytes(value))
}

fn read_u32_eof(reader: &mut impl Read) -> io::Result<Option<u32>> {
    let mut value = [0_u8; 4];
    match reader.read_exact(&mut value) {
        Ok(()) => Ok(Some(u32::from_be_bytes(value))),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(error) => Err(error),
    }
}

fn read_u64(reader: &mut impl Read) -> io::Result<u64> {
    let mut value = [0_u8; 8];
    reader.read_exact(&mut value)?;
    Ok(u64::from_be_bytes(value))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn compact_catalog_round_trip() {
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path()).unwrap();
        let market = MarketEnrichment {
            market_id: 42,
            condition_id: [1; 32],
            token_ids: vec![[2; 32], [3; 32]],
            tag_ids: vec![1, 64, 100_021],
            category: Category::Sports,
            bet_type: BetType::Moneyline,
        };
        storage.save_catalog(std::slice::from_ref(&market)).unwrap();
        assert_eq!(storage.load_catalog().unwrap(), vec![market]);
    }

    #[test]
    fn old_catalog_format_self_heals_to_an_empty_catalog_instead_of_hard_failing() {
        // A `UMACAT1\0` file (pre category/bet_type) must not crash startup —
        // it should be treated like "no cache yet" and trigger a full Gamma
        // re-sync. Hand-written in the old (still-supported-for-reading)
        // wire shape: magic, count=1, one record with no trailing
        // category/bet_type bytes.
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path()).unwrap();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(CATALOG_MAGIC_V1);
        bytes.extend_from_slice(&1_u32.to_be_bytes()); // count
        bytes.extend_from_slice(&42_u64.to_be_bytes()); // market_id
        bytes.extend_from_slice(&[7; 32]); // condition_id
        bytes.extend_from_slice(&0_u16.to_be_bytes()); // token_count
        bytes.extend_from_slice(&0_u16.to_be_bytes()); // tag_count
        fs::write(dir.path().join("catalog.bin"), &bytes).unwrap();

        assert_eq!(storage.load_catalog().unwrap(), Vec::new());
    }

    #[test]
    fn independent_cursors_round_trip() {
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path()).unwrap();
        storage.save_enrichment_cursor("gamma-watermark").unwrap();
        storage
            .save_closed_market_cursor("gamma-closed-watermark")
            .unwrap();
        storage.save_uma_cursor(123).unwrap();
        assert_eq!(
            storage.load_enrichment_cursor().unwrap().as_deref(),
            Some("gamma-watermark")
        );
        assert_eq!(
            storage.load_closed_market_cursor().unwrap().as_deref(),
            Some("gamma-closed-watermark")
        );
        assert_eq!(storage.load_uma_cursor().unwrap(), Some(123));
        assert!(dir.path().join("enrichment.cursor").is_file());
        assert!(dir.path().join("enrichment_closed.cursor").is_file());
        assert!(dir.path().join("uma.cursor").is_file());
    }

    #[test]
    fn enrichment_stats_round_trip_and_default_to_none() {
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path()).unwrap();
        assert!(storage.load_enrichment_stats().unwrap().is_none());

        let snapshot = EnrichmentStatsSnapshot {
            hits: 42,
            hits_via_market_id: 40,
            misses: 3,
        };
        storage.save_enrichment_stats(&snapshot).unwrap();
        let loaded = storage.load_enrichment_stats().unwrap().unwrap();
        assert_eq!(loaded.hits, 42);
        assert_eq!(loaded.hits_via_market_id, 40);
        assert_eq!(loaded.misses, 3);
    }

    #[tokio::test]
    async fn storage_writer_persists_enrichment_stats_across_a_restart() {
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path()).unwrap();
        let event_hub = Arc::new(EventHub::new(16));
        let stats = Arc::new(Stats::default());
        stats.enrichment_hits.store(7, Ordering::Relaxed);
        stats
            .enrichment_hits_via_market_id
            .store(5, Ordering::Relaxed);
        stats.enrichment_misses.store(2, Ordering::Relaxed);

        let (_storage_tx, storage_rx) = mpsc::channel(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let writer = tokio::spawn(run_storage_writer(
            storage.clone(),
            event_hub,
            16,
            stats,
            storage_rx,
            shutdown_rx,
        ));
        // No flush.tick() has necessarily fired yet — shutdown must still
        // persist the latest snapshot on its way out (mirrors the uma_cursor
        // final-flush behavior right above it in run_storage_writer).
        let _ = shutdown_tx.send(true);
        writer.await.unwrap();

        // Simulates "process restarted": a fresh Storage handle over the same
        // directory must see what the previous run persisted.
        let restarted = Storage::open(dir.path()).unwrap();
        let snapshot = restarted.load_enrichment_stats().unwrap().unwrap();
        assert_eq!(snapshot.hits, 7);
        assert_eq!(snapshot.hits_via_market_id, 5);
        assert_eq!(snapshot.misses, 2);
    }
}
