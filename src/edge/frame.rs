//! 帧头校验 + 手写 varint 走查。热路径上只要 `batch_sequence`、`sent_at_us`
//! 和最大的 `events[].sequence`，不做 prost 全量解码。压缩帧解压后同样走查；
//! 解压失败不算错误（edge 不是校验器），序号置 0 由上一帧继承。

use thiserror::Error;

use crate::wire::{FLAG_ZSTD, HEADER_LEN, MAGIC};

/// 与 tinyuma 的 `WIRE_MAX_DECOMPRESSED_BYTES` 默认一致。
pub const MAX_DECOMPRESSED: usize = 262_144;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FrameInfo {
    pub compressed: bool,
    pub schema: u8,
    pub payload_size: usize,
    pub batch_sequence: u64,
    pub sent_at_us: u64,
    /// 0 表示没解出来（压缩帧解压失败或没有事件）。
    pub last_event_sequence: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BadFrame {
    #[error("frame shorter than header")]
    TooShort,
    #[error("bad magic")]
    BadMagic,
    #[error("truncated varint")]
    TruncatedVarint,
    #[error("truncated field")]
    TruncatedField,
    #[error("unsupported wire type {0}")]
    WireType(u8),
}

pub fn parse_frame(bytes: &[u8]) -> Result<FrameInfo, BadFrame> {
    if bytes.len() < HEADER_LEN {
        return Err(BadFrame::TooShort);
    }
    if &bytes[..4] != MAGIC {
        return Err(BadFrame::BadMagic);
    }
    let flags = bytes[4];
    let schema = bytes[5];
    let payload_size = u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
    let body = &bytes[HEADER_LEN..];
    let mut info = FrameInfo {
        compressed: flags & FLAG_ZSTD != 0,
        schema,
        payload_size,
        ..Default::default()
    };
    if info.compressed {
        let cap = payload_size.min(MAX_DECOMPRESSED);
        if let Ok(payload) = zstd::bulk::decompress(body, cap) {
            walk_batch(&payload, &mut info)?;
        }
    } else {
        walk_batch(body, &mut info)?;
    }
    Ok(info)
}

fn walk_batch(payload: &[u8], info: &mut FrameInfo) -> Result<(), BadFrame> {
    let mut cursor = Cursor {
        buf: payload,
        pos: 0,
    };
    while !cursor.done() {
        let key = cursor.varint()?;
        let field = key >> 3;
        let wire = (key & 7) as u8;
        match (field, wire) {
            (2, 0) => info.batch_sequence = cursor.varint()?,
            (3, 0) => info.sent_at_us = cursor.varint()?,
            (4, 2) => {
                let event = cursor.bytes()?;
                let seq = walk_event(event)?;
                info.last_event_sequence = info.last_event_sequence.max(seq);
            }
            _ => cursor.skip(wire)?,
        }
    }
    Ok(())
}

fn walk_event(payload: &[u8]) -> Result<u64, BadFrame> {
    let mut cursor = Cursor {
        buf: payload,
        pos: 0,
    };
    let mut seq = 0;
    while !cursor.done() {
        let key = cursor.varint()?;
        let wire = (key & 7) as u8;
        if key >> 3 == 1 && wire == 0 {
            seq = cursor.varint()?;
        } else {
            cursor.skip(wire)?;
        }
    }
    Ok(seq)
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn done(&self) -> bool {
        self.pos >= self.buf.len()
    }

    fn varint(&mut self) -> Result<u64, BadFrame> {
        let mut value = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = *self.buf.get(self.pos).ok_or(BadFrame::TruncatedVarint)?;
            self.pos += 1;
            if shift < 64 {
                value |= u64::from(byte & 0x7f) << shift;
            }
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
            if shift > 70 {
                return Err(BadFrame::TruncatedVarint);
            }
        }
    }

    fn bytes(&mut self) -> Result<&'a [u8], BadFrame> {
        let len = self.varint()? as usize;
        let end = self.pos.checked_add(len).ok_or(BadFrame::TruncatedField)?;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or(BadFrame::TruncatedField)?;
        self.pos = end;
        Ok(slice)
    }

    fn skip(&mut self, wire: u8) -> Result<(), BadFrame> {
        match wire {
            0 => {
                self.varint()?;
            }
            1 => self.advance(8)?,
            2 => {
                self.bytes()?;
            }
            5 => self.advance(4)?,
            other => return Err(BadFrame::WireType(other)),
        }
        Ok(())
    }

    fn advance(&mut self, n: usize) -> Result<(), BadFrame> {
        let end = self.pos.checked_add(n).ok_or(BadFrame::TruncatedField)?;
        if end > self.buf.len() {
            return Err(BadFrame::TruncatedField);
        }
        self.pos = end;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{SCHEMA_VERSION, pb};
    use prost::Message;

    fn batch(batch_sequence: u64, seqs: &[u64]) -> Vec<u8> {
        pb::UmaBatch {
            schema_version: 1,
            batch_sequence,
            sent_at_us: 1_700_000_000_000_000,
            events: seqs
                .iter()
                .map(|s| pb::UmaEvent {
                    sequence: *s,
                    transaction_hash: vec![0xab; 300],
                    ..Default::default()
                })
                .collect(),
        }
        .encode_to_vec()
    }

    fn frame(payload: &[u8], compress: bool) -> Vec<u8> {
        let body = if compress {
            zstd::bulk::compress(payload, 1).unwrap()
        } else {
            payload.to_vec()
        };
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.push(if compress { FLAG_ZSTD } else { 0 });
        out.push(SCHEMA_VERSION);
        out.extend_from_slice(&[0, 0]);
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(&body);
        out
    }

    #[test]
    fn extracts_sequences_plain_and_compressed() {
        let payload = batch(77, &[100, 101, 105]);
        let info = parse_frame(&frame(&payload, false)).unwrap();
        assert_eq!(info.batch_sequence, 77);
        assert_eq!(info.last_event_sequence, 105);
        assert_eq!(info.sent_at_us, 1_700_000_000_000_000);
        assert!(!info.compressed);

        let info = parse_frame(&frame(&payload, true)).unwrap();
        assert!(info.compressed);
        assert_eq!(info.batch_sequence, 77);
        assert_eq!(info.last_event_sequence, 105);
        assert_eq!(info.payload_size, payload.len());
    }

    #[test]
    fn corrupt_zstd_is_not_an_error() {
        let payload = batch(1, &[1]);
        let mut bytes = frame(&payload, true);
        let len = bytes.len();
        bytes[len - 5..].fill(0xff);
        let info = parse_frame(&bytes).unwrap();
        assert_eq!(info.last_event_sequence, 0);
    }

    #[test]
    fn rejects_garbage() {
        assert_eq!(parse_frame(b"UMA"), Err(BadFrame::TooShort));
        assert_eq!(parse_frame(b"NOPE00000000"), Err(BadFrame::BadMagic));
        let payload = batch(5, &[9]);
        let mut truncated = frame(&payload, false);
        truncated.truncate(truncated.len() - 1);
        assert!(parse_frame(&truncated).is_err());
        let mut bad_varint = frame(&[0x10, 0xff, 0xff], false);
        bad_varint.truncate(bad_varint.len());
        assert_eq!(parse_frame(&bad_varint), Err(BadFrame::TruncatedVarint));
    }
}
