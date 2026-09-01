use std::sync::Arc;

use serde::Serialize;

use crate::wire::pb;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EventKind {
    Propose,
    Dispute,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Propose => "propose",
            Self::Dispute => "dispute",
        }
    }

    pub fn to_proto(self) -> i32 {
        match self {
            Self::Propose => pb::EventType::Propose as i32,
            Self::Dispute => pb::EventType::Dispute as i32,
        }
    }

    pub fn from_proto(value: i32) -> Option<Self> {
        match pb::EventType::try_from(value).ok()? {
            pb::EventType::Propose => Some(Self::Propose),
            pb::EventType::Dispute => Some(Self::Dispute),
            pb::EventType::Unspecified => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketEnrichment {
    pub market_id: u64,
    pub condition_id: [u8; 32],
    pub token_ids: Vec<[u8; 32]>,
    pub tag_ids: Vec<u32>,
}

#[derive(Clone, Debug)]
pub struct DecodedEvent {
    pub kind: EventKind,
    pub block_number: u64,
    pub block_hash: [u8; 32],
    pub transaction_hash: [u8; 32],
    pub log_index: u32,
    pub market_id: u64,
    pub price_raw: [u8; 32],
    pub requester: [u8; 20],
    pub proposer: [u8; 20],
    pub disputer: Option<[u8; 20]>,
    pub upstream_received_at_us: u64,
    pub removed: bool,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct EventKey {
    pub transaction_hash: [u8; 32],
    pub log_index: u32,
    pub removed: bool,
}

#[derive(Clone, Debug)]
pub struct EventRecord {
    pub sequence: u64,
    pub event: DecodedEvent,
    pub enrichment: Option<Arc<MarketEnrichment>>,
}

impl EventRecord {
    pub fn key(&self) -> EventKey {
        EventKey {
            transaction_hash: self.event.transaction_hash,
            log_index: self.event.log_index,
            removed: self.event.removed,
        }
    }

    pub fn to_proto(&self) -> pb::UmaEvent {
        let enrichment = self.enrichment.as_deref();
        pb::UmaEvent {
            sequence: self.sequence,
            event_type: self.event.kind.to_proto(),
            block_number: self.event.block_number,
            transaction_hash: self.event.transaction_hash.to_vec(),
            log_index: self.event.log_index,
            market_id: self.event.market_id,
            condition_id: enrichment
                .map(|v| v.condition_id.to_vec())
                .unwrap_or_default(),
            token_ids: enrichment
                .map(|v| v.token_ids.iter().map(|id| id.to_vec()).collect())
                .unwrap_or_default(),
            tag_ids: enrichment.map(|v| v.tag_ids.clone()).unwrap_or_default(),
            price_raw: self.event.price_raw.to_vec(),
            requester: self.event.requester.to_vec(),
            proposer: self.event.proposer.to_vec(),
            disputer: self.event.disputer.map(|v| v.to_vec()).unwrap_or_default(),
            upstream_received_at_us: self.event.upstream_received_at_us,
            block_hash: self.event.block_hash.to_vec(),
            removed: self.event.removed,
            enrichment_status: if enrichment.is_some() {
                pb::EnrichmentStatus::Hit as i32
            } else {
                pb::EnrichmentStatus::Miss as i32
            },
        }
    }

    pub fn from_proto(value: pb::UmaEvent) -> Option<Self> {
        let condition_id = fixed::<32>(&value.condition_id);
        let enrichment = condition_id.map(|condition_id| {
            Arc::new(MarketEnrichment {
                market_id: value.market_id,
                condition_id,
                token_ids: value
                    .token_ids
                    .iter()
                    .filter_map(|v| fixed::<32>(v))
                    .collect(),
                tag_ids: value.tag_ids.clone(),
            })
        });
        Some(Self {
            sequence: value.sequence,
            event: DecodedEvent {
                kind: EventKind::from_proto(value.event_type)?,
                block_number: value.block_number,
                block_hash: fixed::<32>(&value.block_hash)?,
                transaction_hash: fixed::<32>(&value.transaction_hash)?,
                log_index: value.log_index,
                market_id: value.market_id,
                price_raw: fixed::<32>(&value.price_raw)?,
                requester: fixed::<20>(&value.requester)?,
                proposer: fixed::<20>(&value.proposer)?,
                disputer: if value.disputer.is_empty() {
                    None
                } else {
                    fixed::<20>(&value.disputer)
                },
                upstream_received_at_us: value.upstream_received_at_us,
                removed: value.removed,
            },
            enrichment,
        })
    }
}

pub fn fixed<const N: usize>(value: &[u8]) -> Option<[u8; N]> {
    value.try_into().ok()
}

pub fn hex_prefixed(value: &[u8]) -> String {
    let mut result = String::with_capacity(2 + value.len() * 2);
    result.push_str("0x");
    result.push_str(&hex::encode(value));
    result
}

pub fn uint256_decimal(value: &[u8; 32]) -> String {
    if value.iter().all(|byte| *byte == 0) {
        return "0".into();
    }
    let mut working = *value;
    let mut digits = Vec::with_capacity(78);
    while working.iter().any(|byte| *byte != 0) {
        let mut remainder = 0_u16;
        for byte in &mut working {
            let current = (remainder << 8) | *byte as u16;
            *byte = (current / 10) as u8;
            remainder = current % 10;
        }
        digits.push(b'0' + remainder as u8);
    }
    digits.reverse();
    String::from_utf8(digits).expect("decimal digits")
}

#[cfg(test)]
mod tests {
    use super::uint256_decimal;

    #[test]
    fn formats_uint256_decimal() {
        let mut value = [0_u8; 32];
        value[30..].copy_from_slice(&[1, 2]);
        assert_eq!(uint256_decimal(&value), "258");
    }
}
