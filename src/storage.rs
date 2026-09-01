use std::{
    collections::VecDeque,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use prost::Message;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tracing::{error, warn};

use crate::{
    hub::EventHub,
    model::{EventRecord, MarketEnrichment},
    wire::pb,
};

const CATALOG_MAGIC: &[u8; 8] = b"UMACAT1\0";
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

    fn checkpoint_path(&self) -> PathBuf {
        self.dir.join("checkpoint.bin")
    }

    pub fn load_catalog(&self) -> Result<Vec<MarketEnrichment>, StorageError> {
        let path = self.catalog_path();
        if !path.exists() {
            return Ok(Vec::new());
        }
        let mut reader = BufReader::new(File::open(path)?);
        let mut magic = [0_u8; 8];
        reader.read_exact(&mut magic)?;
        if &magic != CATALOG_MAGIC {
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
            markets.push(MarketEnrichment {
                market_id,
                condition_id,
                token_ids,
                tag_ids,
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

    pub fn load_checkpoint(&self) -> Result<u64, StorageError> {
        let path = self.checkpoint_path();
        if !path.exists() {
            return Ok(0);
        }
        let mut value = [0_u8; 8];
        File::open(path)?.read_exact(&mut value)?;
        Ok(u64::from_be_bytes(value))
    }

    pub fn save_checkpoint(&self, block: u64) -> Result<(), StorageError> {
        let path = self.checkpoint_path();
        let temp = temporary_path(&path);
        let mut file = File::create(&temp)?;
        file.write_all(&block.to_be_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(temp, path)?;
        Ok(())
    }
}

pub enum StorageCommand {
    Event(Arc<EventRecord>),
    Checkpoint(u64),
}

pub async fn run_storage_writer(
    storage: Storage,
    event_hub: Arc<EventHub>,
    event_capacity: usize,
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
    let mut checkpoint = storage.load_checkpoint().unwrap_or_default();
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
                if checkpoint > 0 && let Err(error) = storage.save_checkpoint(checkpoint) {
                    error!(%error, checkpoint, "save checkpoint");
                }
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
                    Some(StorageCommand::Checkpoint(block)) => checkpoint = checkpoint.max(block),
                    None => break,
                }
            }
        }
    }
    let _ = journal.flush();
    if checkpoint > 0 {
        let _ = storage.save_checkpoint(checkpoint);
    }
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
        };
        storage.save_catalog(std::slice::from_ref(&market)).unwrap();
        assert_eq!(storage.load_catalog().unwrap(), vec![market]);
    }

    #[test]
    fn checkpoint_round_trip() {
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path()).unwrap();
        storage.save_checkpoint(123).unwrap();
        assert_eq!(storage.load_checkpoint().unwrap(), 123);
    }
}
