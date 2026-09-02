pub mod ancillary;
pub mod common;
mod dispute_price;
mod propose_price;

use serde::{Deserialize, Serialize};
use sha3::{Digest, Keccak256};
use thiserror::Error;

use self::{
    ancillary::parse_ancillary,
    common::{ChainLog, PriceRequest, UmaEvent},
};

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
    #[serde(default)]
    pub transaction_index: Option<String>,
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
    #[error("ancillary data is not UTF-8")]
    AncillaryUtf8,
    #[error("Polymarket market_id is absent")]
    MissingMarketId,
}

pub fn decode_signal_log(
    raw: &RpcLog,
    received_at_us: u64,
    allowed_emitters: &[[u8; 20]],
    require_market_id: bool,
) -> Result<UmaEvent, DecodeError> {
    let emitter = decode_fixed::<20>(&raw.address, "address")?;
    if !allowed_emitters.iter().any(|allowed| allowed == &emitter) {
        return Err(DecodeError::Emitter);
    }
    let event = match raw.topics.first().map(|topic| topic.to_ascii_lowercase()) {
        Some(topic) if topic == TOPIC_PROPOSE_PRICE => propose_price::parse(raw, received_at_us)?,
        Some(topic) if topic == TOPIC_DISPUTE_PRICE => dispute_price::parse(raw, received_at_us)?,
        _ => return Err(DecodeError::Topic),
    };
    if require_market_id && event.market_id() == 0 {
        return Err(DecodeError::MissingMarketId);
    }
    Ok(event)
}

fn parse_data(raw: &RpcLog, words: usize) -> Result<Vec<u8>, DecodeError> {
    let data = decode_hex(&raw.data, "data")?;
    (data.len() >= words * 32)
        .then_some(data)
        .ok_or(DecodeError::ShortData)
}

fn build_chain(raw: &RpcLog, received_at_us: u64) -> Result<ChainLog, DecodeError> {
    Ok(ChainLog {
        contract_address: decode_fixed::<20>(&raw.address, "address")?,
        block_number: parse_hex_u64(&raw.block_number, "blockNumber")?,
        block_hash: decode_fixed::<32>(&raw.block_hash, "blockHash")?,
        transaction_hash: decode_fixed::<32>(&raw.transaction_hash, "transactionHash")?,
        transaction_index: raw
            .transaction_index
            .as_deref()
            .map(|value| parse_hex_u64(value, "transactionIndex"))
            .transpose()?
            .map(|value| {
                value
                    .try_into()
                    .map_err(|_| DecodeError::Number("transactionIndex"))
            })
            .transpose()?,
        log_index: parse_hex_u64(&raw.log_index, "logIndex")?
            .try_into()
            .map_err(|_| DecodeError::Number("logIndex"))?,
        upstream_received_at_us: received_at_us,
        removed: raw.removed,
    })
}

fn parse_request(raw: &RpcLog, data: &[u8]) -> Result<PriceRequest, DecodeError> {
    let requester = topic_address(&raw.topics[1])?;
    let ancillary = parse_ancillary(dynamic_word(data, 2)?)?;
    Ok(PriceRequest {
        condition_id: derive_binary_condition_id(&requester, &ancillary.question_id),
        requester,
        proposer: topic_address(&raw.topics[2])?,
        identifier: word(data, 0),
        timestamp: word_u64(data, 1, "timestamp")?,
        ancillary,
        proposed_price: word(data, 3),
    })
}

/// CTF condition ID for Polymarket's binary UMA adapters:
/// keccak256(abi.encodePacked(adapter, question_id, uint256(2))).
pub fn derive_binary_condition_id(requester: &[u8; 20], question_id: &[u8; 32]) -> [u8; 32] {
    let mut encoded = [0_u8; 84];
    encoded[..20].copy_from_slice(requester);
    encoded[20..52].copy_from_slice(question_id);
    encoded[83] = 2;
    Keccak256::digest(encoded).into()
}

fn topic_address(value: &str) -> Result<[u8; 20], DecodeError> {
    let topic = decode_fixed::<32>(value, "topic")?;
    Ok(topic[12..].try_into().expect("address slice"))
}

fn word(data: &[u8], index: usize) -> [u8; 32] {
    data[index * 32..(index + 1) * 32]
        .try_into()
        .expect("validated ABI word")
}

fn word_address(data: &[u8], index: usize) -> [u8; 20] {
    data[index * 32 + 12..(index + 1) * 32]
        .try_into()
        .expect("validated ABI address")
}

fn word_u64(data: &[u8], index: usize, field: &'static str) -> Result<u64, DecodeError> {
    let value = word(data, index);
    if value[..24].iter().any(|byte| *byte != 0) {
        return Err(DecodeError::Number(field));
    }
    Ok(u64::from_be_bytes(
        value[24..].try_into().expect("u64 word"),
    ))
}

fn dynamic_word(data: &[u8], index: usize) -> Result<&[u8], DecodeError> {
    let offset = word_u64(data, index, "dynamic offset")? as usize;
    let length_word = data
        .get(offset..offset + 32)
        .ok_or(DecodeError::DynamicOffset)?;
    if length_word[..24].iter().any(|byte| *byte != 0) {
        return Err(DecodeError::DynamicOffset);
    }
    let length = u64::from_be_bytes(length_word[24..].try_into().expect("length")) as usize;
    data.get(offset + 32..offset + 32 + length)
        .ok_or(DecodeError::DynamicOffset)
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
    decode_hex(value, field)?
        .try_into()
        .map_err(|_| DecodeError::Hex(field))
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
    use crate::uma::events::common::UmaEvent;

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

    fn topic_address(byte: u8) -> String {
        format!("0x{}", hex::encode(address_word(byte)))
    }

    fn log(topic: &str, dispute: bool) -> RpcLog {
        let ancillary = b"q: Will it happen? res_data: p1: 0, p2: 1, p3: 0.5, market_id: 42, initializer: 1111111111111111111111111111111111111111";
        let words = if dispute { 4 } else { 6 };
        let offset = words * 32;
        let mut data = vec![0_u8; offset + 32 + ancillary.len()];
        data[..32].fill(0x44);
        data[32..64].copy_from_slice(&abi_word(100));
        data[64..96].copy_from_slice(&abi_word(offset as u64));
        data[96..128].fill(0x55);
        if !dispute {
            data[128..160].copy_from_slice(&abi_word(200));
            data[160..192].copy_from_slice(&address_word(0x66));
        }
        data[offset..offset + 32].copy_from_slice(&abi_word(ancillary.len() as u64));
        data[offset + 32..].copy_from_slice(ancillary);
        let mut topics = vec![topic.into(), topic_address(1), topic_address(2)];
        if dispute {
            topics.push(topic_address(3));
        }
        RpcLog {
            address: format!("0x{}", "aa".repeat(20)),
            topics,
            data: format!("0x{}", hex::encode(data)),
            block_number: "0xa".into(),
            block_hash: format!("0x{}", "bb".repeat(32)),
            transaction_hash: format!("0x{}", "cc".repeat(32)),
            transaction_index: Some("0x2".into()),
            log_index: "0x4".into(),
            removed: false,
        }
    }

    #[test]
    fn propose_parser_returns_all_business_fields() {
        let event =
            decode_signal_log(&log(TOPIC_PROPOSE_PRICE, false), 999, &[[0xaa; 20]], true).unwrap();
        let UmaEvent::ProposePrice(event) = event else {
            panic!("wrong event")
        };
        assert_eq!(event.chain.contract_address, [0xaa; 20]);
        assert_eq!(event.chain.transaction_index, Some(2));
        assert_eq!(event.request.identifier, [0x44; 32]);
        assert_eq!(event.request.timestamp, 100);
        assert_eq!(event.request.ancillary.market_id, Some(42));
        assert_eq!(
            event.request.condition_id,
            derive_binary_condition_id(&[1; 20], &event.request.ancillary.question_id)
        );
        assert_eq!(event.request.ancillary.question, "Will it happen?");
        assert_eq!(event.request.proposed_price, [0x55; 32]);
        assert_eq!(event.expiration_timestamp, 200);
        assert_eq!(event.currency, [0x66; 20]);
    }

    #[test]
    fn dispute_parser_returns_all_business_fields() {
        let event =
            decode_signal_log(&log(TOPIC_DISPUTE_PRICE, true), 999, &[[0xaa; 20]], true).unwrap();
        let UmaEvent::DisputePrice(event) = event else {
            panic!("wrong event")
        };
        assert_eq!(event.request.requester, [1; 20]);
        assert_eq!(event.request.proposer, [2; 20]);
        assert_eq!(event.disputer, [3; 20]);
        assert_eq!(event.request.ancillary.resolution.p2.as_deref(), Some("1"));
    }

    #[test]
    fn derives_verified_polygon_condition_id() {
        // Polygon tx 0x8fe36e6b5843d91c93504161f2d969ac79940222a8ccddbd5d21d65fd3354031.
        let requester =
            decode_fixed::<20>("0x157ce2d672854c848c9b79c49a8cc6cc89176a49", "requester").unwrap();
        let question_id = decode_fixed::<32>(
            "0xd8953cc3529be11caeaf510a70b3065c681463c0b56b4b117479f35d3a6bc80a",
            "question_id",
        )
        .unwrap();
        let expected = decode_fixed::<32>(
            "0xab6a43129f0cd1ae5fee650d64da2f849f2e572cb613e295ca1f6f83fe2d0774",
            "condition_id",
        )
        .unwrap();

        assert_eq!(
            derive_binary_condition_id(&requester, &question_id),
            expected
        );
    }

    #[test]
    fn neg_risk_event_binary_formula_yields_wrong_condition_id() {
        // Real Polygon tx
        // 0x96bdaafcd0f9f498f4b76e0f7169c8e28aee2d851b39197f3cee16825a43db6d:
        // ProposePrice for Gamma market 907474 ("Will Candidate Z win the 2026
        // Massachusetts Governor Republican primary election?"), a Neg Risk
        // market (requester is Polymarket's "Neg Risk UMA CTF Adapter",
        // 0x2F5e3684cb1F318ec51b00Edba38d79Ac2c0aA9d). Captured via eth_getLogs
        // and cross-checked against https://gamma-api.polymarket.com/markets/907474
        // in the same session that added this test.
        let raw = RpcLog {
            address: "0xee3afe347d5c74317041e2618c49534daf887c24".into(),
            topics: vec![
                TOPIC_PROPOSE_PRICE.into(),
                "0x0000000000000000000000002f5e3684cb1f318ec51b00edba38d79ac2c0aa9d".into(),
                "0x000000000000000000000000ca323ed4e6dd651368c754d7c5a9d345e5c81829".into(),
            ],
            data: "0x5945535f4f525f4e4f5f51554552590000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006939e7e600000000000000000000000000000000000000000000000000000000000000c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006a978df20000000000000000000000002791bca1f2de4661ed88a30c99a7a9449aa8417400000000000000000000000000000000000000000000000000000000000003fa713a207469746c653a2057696c6c2043616e646964617465205a2077696e207468652032303236204d61737361636875736574747320476f7665726e6f722052657075626c6963616e207072696d61727920656c656374696f6e3f2c206465736372697074696f6e3a2054686973206d61726b65742077696c6c207265736f6c7665206163636f7264696e6720746f207468652077696e6e6572206f66207468652052657075626c6963616e205072696d61727920666f7220476f7665726e6f72206f66204d6173736163687573657474732c207363686564756c656420746f2074616b6520706c616365206f6e2053657074656d62657220312c20323032362e205265736f6c7574696f6e2077696c6c206265206261736564206f6e20746865206f766572616c6c2077696e6e6572206f6620746865207072696d6172792c20696e636c7564696e6720616e7920706f74656e7469616c207365636f6e6420726f756e64206f722072756e2d6f66662e0a0a4966206e6f2032303236204d6173736163687573657474732047756265726e61746f7269616c2052657075626c6963616e205072696d6172792074616b657320706c6163652c2074686973206d61726b65742077696c6c207265736f6c766520746f20e2809c4f746865722ee2809d0a0a546865207265736f6c7574696f6e20736f7572636520666f722074686973206d61726b65742077696c6c20626520746865206669727374206f6666696369616c20616e6e6f756e63656d656e74206f662074686520726573756c74732066726f6d20746865204d6173736163687573657474732052657075626c6963616e2050617274793b20686f77657665722c20616e206f7665727768656c6d696e6720636f6e73656e737573206f66206372656469626c65207265706f7274696e67206d617920737566666963652e206d61726b65745f69643a20393037343734207265735f646174613a2070313a20302c2070323a20312c2070333a20302e352e20576865726520703120636f72726573706f6e647320746f204e6f2c20703220746f205965732c20703320746f20756e6b6e6f776e2e20546869732072657175657374204d555354206f6e6c79207265736f6c766520746f207031206f722070322e202055706461746573206d61646520627920746865207175657374696f6e2063726561746f7220766961207468652062756c6c6574696e20626f617264206174203078324635653336383463623146333138656335316230304564626133386437394163326330614139642073686f756c6420626520636f6e736964657265642e2c696e697469616c697a65723a39313433306361643264333937353736363439393731376661306436366137386438313465356335000000000000".into(),
            block_number: "0x58c2286".into(),
            block_hash: "0xf50109a4c9ab6bbf0360e5909c78fadce725a2374fe01b31994bae2680a99932".into(),
            transaction_hash: "0x96bdaafcd0f9f498f4b76e0f7169c8e28aee2d851b39197f3cee16825a43db6d".into(),
            transaction_index: Some("0x6f".into()),
            log_index: "0x816".into(),
            removed: false,
        };
        let emitter = decode_fixed::<20>(&raw.address, "address").unwrap();
        let event = decode_signal_log(&raw, 1, &[emitter], true).unwrap();
        let request = event.request();

        assert_eq!(request.ancillary.market_id, Some(907_474));

        // Gamma's authoritative condition_id for market 907474 (confirmed to equal
        // keccak256(NegRiskAdapter ++ realQuestionId ++ 2), where realQuestionId is
        // NOT keccak256(ancillary_data) — see Catalog::resolve's doc comment).
        let gamma_condition_id = decode_fixed::<32>(
            "0xa50547851bf565603ad7e866d9d2aa2c6c2ee77b2d390e581bf2e8a53b466902",
            "condition_id",
        )
        .unwrap();

        // The binary-adapter formula must NOT be trusted for this market: it
        // produces a different value than Gamma's real condition_id. This is why
        // enrichment resolution (Catalog::resolve) tries market_id first.
        assert_ne!(request.condition_id, gamma_condition_id);
    }
}
