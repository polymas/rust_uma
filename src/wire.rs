use std::{io::Cursor, sync::Arc};

use bytes::{BufMut, Bytes, BytesMut};
use prost::Message;
use thiserror::Error;

use crate::model::EventRecord;

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/polyuma.wire.v1.rs"));
}

pub const MAGIC: &[u8; 4] = b"UMA1";
pub const HEADER_LEN: usize = 12;
pub const FLAG_ZSTD: u8 = 1;
pub const SCHEMA_VERSION: u8 = 1;

#[derive(Clone)]
pub struct WireConfig {
    pub zstd_threshold: usize,
    pub max_decompressed_bytes: usize,
}

#[derive(Clone)]
pub struct WireFrame {
    pub batch_sequence: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub bytes: Bytes,
}

#[derive(Debug, Error)]
pub enum WireError {
    #[error("batch is empty")]
    EmptyBatch,
    #[error("protobuf payload exceeds configured decompressed limit")]
    Oversized,
    #[error("zstd encode failed: {0}")]
    Zstd(#[from] std::io::Error),
}

pub fn encoded_event_len(event: &EventRecord) -> usize {
    event.to_proto().encoded_len()
}

pub fn encode_frame(
    batch_sequence: u64,
    events: &[Arc<EventRecord>],
    config: &WireConfig,
) -> Result<WireFrame, WireError> {
    let first = events.first().ok_or(WireError::EmptyBatch)?.sequence;
    let last = events.last().ok_or(WireError::EmptyBatch)?.sequence;
    let batch = pb::UmaBatch {
        schema_version: SCHEMA_VERSION as u32,
        batch_sequence,
        sent_at_us: now_us(),
        events: events.iter().map(|event| event.to_proto()).collect(),
    };
    let payload = batch.encode_to_vec();
    if payload.len() > config.max_decompressed_bytes {
        return Err(WireError::Oversized);
    }
    let (flags, body) = if payload.len() >= config.zstd_threshold {
        (
            FLAG_ZSTD,
            zstd::stream::encode_all(Cursor::new(&payload), 1)?,
        )
    } else {
        (0, payload.clone())
    };
    let mut framed = BytesMut::with_capacity(HEADER_LEN + body.len());
    framed.extend_from_slice(MAGIC);
    framed.put_u8(flags);
    framed.put_u8(SCHEMA_VERSION);
    framed.put_u16(0);
    framed.put_u32(payload.len() as u32);
    framed.extend_from_slice(&body);
    Ok(WireFrame {
        batch_sequence,
        first_sequence: first,
        last_sequence: last,
        bytes: framed.freeze(),
    })
}

pub fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use prost::Message;

    use super::*;
    use crate::model::{EventRecord, PriceOutcome, test_uma_event};

    fn event(sequence: u64) -> Arc<EventRecord> {
        Arc::new(EventRecord {
            sequence,
            event: test_uma_event(2, 4),
            enrichment: None,
            price_outcome: PriceOutcome::Unspecified,
        })
    }

    #[test]
    fn frame_round_trip_without_compression() {
        let frame = encode_frame(
            7,
            &[event(11)],
            &WireConfig {
                zstd_threshold: usize::MAX,
                max_decompressed_bytes: 1024,
            },
        )
        .unwrap();
        assert_eq!(&frame.bytes[..4], MAGIC);
        assert_eq!(frame.bytes[4], 0);
        let batch = pb::UmaBatch::decode(&frame.bytes[HEADER_LEN..]).unwrap();
        assert_eq!(batch.batch_sequence, 7);
        assert_eq!(batch.events[0].sequence, 11);
        assert_eq!(batch.events[0].market_id, 4);
    }

    #[test]
    fn frame_round_trip_with_zstd() {
        let frame = encode_frame(
            8,
            &[event(12)],
            &WireConfig {
                zstd_threshold: 1,
                max_decompressed_bytes: 1024,
            },
        )
        .unwrap();
        assert_eq!(frame.bytes[4], FLAG_ZSTD);
        let payload = zstd::stream::decode_all(Cursor::new(&frame.bytes[HEADER_LEN..])).unwrap();
        let batch = pb::UmaBatch::decode(payload.as_slice()).unwrap();
        assert_eq!(batch.events[0].sequence, 12);
    }
}
