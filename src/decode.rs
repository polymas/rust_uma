use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::model::{DecodedEvent, EventKind};

pub const TOPIC_PROPOSE_PRICE: &str =
    "0x6e51dd00371aabffa82cd401592f76ed51e98a9ea4b58751c70463a2c78b5ca1";
pub const TOPIC_DISPUTE_PRICE: &str =
    "0x5165909c3d1c01c5d1e121ac6f6d01dda1ba24bc9e1f975b5a375339c15be7f3";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcLog {
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
    pub block_number: String,
    #[serde(default)]
    pub block_hash: String,
    pub transaction_hash: String,
    pub log_index: String,
    #[serde(default)]
    pub removed: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum DecodeError {
    #[error("emitter is not in the configured allowlist")]
    Emitter,
    #[error("unsupported or missing topic")]
    Topic,
    #[error("invalid hex in {0}")]
    Hex(&'static str),
    #[error("invalid numeric value in {0}")]
    Number(&'static str),
    #[error("ABI payload is too short")]
    ShortData,
    #[error("invalid dynamic ABI offset")]
    DynamicOffset,
    #[error("Polymarket market_id is absent")]
    MissingMarketId,
}

pub fn decode_signal_log(
    raw: &RpcLog,
    received_at_us: u64,
    allowed_emitters: &[[u8; 20]],
    require_market_id: bool,
) -> Result<DecodedEvent, DecodeError> {
    let emitter = decode_fixed::<20>(&raw.address, "address")?;
    if !allowed_emitters.iter().any(|allowed| allowed == &emitter) {
        return Err(DecodeError::Emitter);
    }
    let topic0 = raw
        .topics
        .first()
        .ok_or(DecodeError::Topic)?
        .to_ascii_lowercase();
    let kind = match topic0.as_str() {
        TOPIC_PROPOSE_PRICE => EventKind::Propose,
        TOPIC_DISPUTE_PRICE => EventKind::Dispute,
        _ => return Err(DecodeError::Topic),
    };
    let required_topics = if kind == EventKind::Propose { 3 } else { 4 };
    if raw.topics.len() < required_topics {
        return Err(DecodeError::Topic);
    }
    let data = decode_hex(&raw.data, "data")?;
    let required_words = if kind == EventKind::Propose { 6 } else { 4 };
    if data.len() < required_words * 32 {
        return Err(DecodeError::ShortData);
    }
    let ancillary = dynamic_word(&data, 2)?;
    let market_id = parse_market_id(ancillary).unwrap_or_default();
    if require_market_id && market_id == 0 {
        return Err(DecodeError::MissingMarketId);
    }
    let requester = topic_address(&raw.topics[1])?;
    let proposer = topic_address(&raw.topics[2])?;
    let disputer = if kind == EventKind::Dispute {
        Some(topic_address(&raw.topics[3])?)
    } else {
        None
    };

    Ok(DecodedEvent {
        kind,
        block_number: parse_hex_u64(&raw.block_number, "blockNumber")?,
        block_hash: decode_fixed::<32>(&raw.block_hash, "blockHash")?,
        transaction_hash: decode_fixed::<32>(&raw.transaction_hash, "transactionHash")?,
        log_index: parse_hex_u64(&raw.log_index, "logIndex")?
            .try_into()
            .map_err(|_| DecodeError::Number("logIndex"))?,
        market_id,
        price_raw: data[3 * 32..4 * 32].try_into().expect("word length"),
        requester,
        proposer,
        disputer,
        upstream_received_at_us: received_at_us,
        removed: raw.removed,
    })
}

fn topic_address(value: &str) -> Result<[u8; 20], DecodeError> {
    let topic = decode_fixed::<32>(value, "topic")?;
    Ok(topic[12..].try_into().expect("address slice"))
}

fn dynamic_word(data: &[u8], word: usize) -> Result<&[u8], DecodeError> {
    let offset_word = data
        .get(word * 32..(word + 1) * 32)
        .ok_or(DecodeError::ShortData)?;
    if offset_word[..24].iter().any(|byte| *byte != 0) {
        return Err(DecodeError::DynamicOffset);
    }
    let offset = u64::from_be_bytes(offset_word[24..].try_into().expect("u64 slice")) as usize;
    let length_word = data
        .get(offset..offset + 32)
        .ok_or(DecodeError::DynamicOffset)?;
    if length_word[..24].iter().any(|byte| *byte != 0) {
        return Err(DecodeError::DynamicOffset);
    }
    let length = u64::from_be_bytes(length_word[24..].try_into().expect("u64 slice")) as usize;
    data.get(offset + 32..offset + 32 + length)
        .ok_or(DecodeError::DynamicOffset)
}

pub fn parse_market_id(ancillary: &[u8]) -> Option<u64> {
    const NEEDLE: &[u8] = b"market_id:";
    let start = ancillary
        .windows(NEEDLE.len())
        .position(|window| window == NEEDLE)?
        + NEEDLE.len();
    let mut digits = ancillary[start..]
        .iter()
        .copied()
        .skip_while(|byte| byte.is_ascii_whitespace());
    let mut value = 0_u64;
    let mut found = false;
    for byte in digits.by_ref() {
        if !byte.is_ascii_digit() {
            break;
        }
        found = true;
        value = value.checked_mul(10)?.checked_add((byte - b'0') as u64)?;
    }
    found.then_some(value)
}

pub fn parse_hex_u64(value: &str, field: &'static str) -> Result<u64, DecodeError> {
    let raw = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value);
    u64::from_str_radix(raw, 16).map_err(|_| DecodeError::Number(field))
}

pub fn decode_fixed<const N: usize>(
    value: &str,
    field: &'static str,
) -> Result<[u8; N], DecodeError> {
    let decoded = decode_hex(value, field)?;
    decoded.try_into().map_err(|_| DecodeError::Hex(field))
}

fn decode_hex(value: &str, field: &'static str) -> Result<Vec<u8>, DecodeError> {
    let raw = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value);
    hex::decode(raw).map_err(|_| DecodeError::Hex(field))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn word(value: u64) -> [u8; 32] {
        let mut result = [0_u8; 32];
        result[24..].copy_from_slice(&value.to_be_bytes());
        result
    }

    fn topic_address_hex(byte: u8) -> String {
        format!("0x{}{}", "00".repeat(12), format!("{byte:02x}").repeat(20))
    }

    fn log(topic: &str, dispute: bool) -> RpcLog {
        let ancillary = b"q: test, market_id: 4042121, res_data: p1: 0";
        let words = if dispute { 4 } else { 6 };
        let dynamic_offset = words * 32;
        let mut data = vec![0_u8; dynamic_offset + 32 + ancillary.len()];
        data[2 * 32..3 * 32].copy_from_slice(&word(dynamic_offset as u64));
        data[3 * 32..4 * 32].fill(0xaa);
        data[dynamic_offset..dynamic_offset + 32].copy_from_slice(&word(ancillary.len() as u64));
        data[dynamic_offset + 32..].copy_from_slice(ancillary);
        let mut topics = vec![topic.to_owned(), topic_address_hex(1), topic_address_hex(2)];
        if dispute {
            topics.push(topic_address_hex(3));
        }
        RpcLog {
            address: format!("0x{}", "11".repeat(20)),
            topics,
            data: format!("0x{}", hex::encode(data)),
            block_number: "0xa".into(),
            block_hash: format!("0x{}", "22".repeat(32)),
            transaction_hash: format!("0x{}", "33".repeat(32)),
            log_index: "0x4".into(),
            removed: false,
        }
    }

    #[test]
    fn decodes_propose_and_preserves_int256_bytes() {
        let allowed = [[0x11; 20]];
        let decoded =
            decode_signal_log(&log(TOPIC_PROPOSE_PRICE, false), 99, &allowed, true).unwrap();
        assert_eq!(decoded.kind, EventKind::Propose);
        assert_eq!(decoded.market_id, 4_042_121);
        assert_eq!(decoded.price_raw, [0xaa; 32]);
        assert_eq!(decoded.requester, [1; 20]);
        assert!(decoded.disputer.is_none());
    }

    #[test]
    fn decodes_dispute() {
        let allowed = [[0x11; 20]];
        let decoded =
            decode_signal_log(&log(TOPIC_DISPUTE_PRICE, true), 99, &allowed, true).unwrap();
        assert_eq!(decoded.kind, EventKind::Dispute);
        assert_eq!(decoded.disputer, Some([3; 20]));
    }

    #[test]
    fn rejects_wrong_emitter() {
        let error = decode_signal_log(&log(TOPIC_PROPOSE_PRICE, false), 99, &[[0x12; 20]], true)
            .unwrap_err();
        assert_eq!(error, DecodeError::Emitter);
    }

    #[test]
    fn parses_market_id_without_regex() {
        assert_eq!(parse_market_id(b"x, market_id: 12345, y"), Some(12345));
        assert_eq!(parse_market_id(b"x"), None);
    }
}
